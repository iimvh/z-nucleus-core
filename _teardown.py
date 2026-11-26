from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Coroutine
from typing import TYPE_CHECKING, Any

from ._actor import Actor, adopt, spawn_detached
from ._errors import Errors, report
from ._lifecycle import Role
from ._slot import Slot, SlotState

if TYPE_CHECKING:
    from ._fiber import Fiber

_inflight: set[asyncio.Task[Any]] = set()  # asyncio itself only keeps weak references to tasks


class _Frame:
    """scope is None: dispose `fiber`; otherwise: finish component slot `scope` of `fiber`."""

    __slots__ = ("fiber", "may_wait", "scope")

    def __init__(self, fiber: Fiber, scope: Slot | None, may_wait: bool) -> None:
        self.fiber = fiber
        self.scope = scope
        self.may_wait = may_wait


class Teardown:
    """The teardown engine, for one operation.

    Runs iteratively over an explicit work stack, so the depth of the tree is unbounded. When a
    component slot is met, push "finish the slot", then "dispose its child fiber" on top: the child
    finishes first.

    When it has to wait (work is being run or released, or a child fiber disposed, by someone else):
     - if waiting is safe, it waits;
     - if waiting would deadlock (it is waiting on itself, or WaitGraph refuses), it DEFERS: it pops the
       whole stack and leaves the fibers suspended. Whoever holds the blocking work resumes them, in
       the correct order, when that work finishes.

    "Opportunistic" frames (may_wait False) are pushed when an operation happens to finish work inside a
    suspended fiber: it helps continue the teardown, but never waits; it defers again instead.

    Two rules replace the locks of a threaded kernel:
     - no await between deciding (a state check, a refused wait) and acting on it (a state change,
       pushing a frame, deferring);
     - every change someone may be waiting for is followed by Fiber.wake().

    Cancelling the calling task never abandons a teardown: the cancellation is noted in `errors`, the
    teardown finishes, and the operation raises it at the end.
    """

    __slots__ = ("errors", "me", "work")

    def __init__(self, me: Actor, errors: Errors) -> None:
        self.me = me
        self.errors = errors
        self.work: list[_Frame] = []

    # ---------------- entry points ----------------

    async def dispose_fiber(self, f: Fiber) -> None:
        """Disposes f, waiting when needed. Returns early only when waiting is impossible."""
        try:
            if await self._begin(f, True) is None:
                await self._run()
        except GeneratorExit:
            self._defer(None)
            raise

    async def release_slot(self, f: Fiber, s: Slot) -> None:
        """Disposes exactly one slot, and its subtree if it is a component."""
        try:
            while True:
                if s.is_settled:
                    return
                if s.state is SlotState.ACTIVE:
                    s.begin_release(self.me)
                    break
                if s.state is not SlotState.RELEASING or s.owner is None:
                    raise RuntimeError(f"Core bug: handle of slot '{s.name}' is {s.state.name}")
                if not await self._wait(f, s.owner):
                    return  # whoever is releasing it will finish
            s.signal.cancel(self.errors)
            if s.child is None:
                await self._run_undo(f, s)
                self._finalize(f, s)
            else:
                self.work.append(_Frame(f, s, True))
                blocker = await self._begin(s.child, True)
                if blocker is not None:
                    self._defer(blocker)
                    return
            await self._run()
        except GeneratorExit:
            self._defer(None)
            raise

    async def resume_if_idle(self, f: Fiber) -> None:
        """Work just finished in a fiber being disposed: if nobody is disposing it, help continue."""
        try:
            self._maybe_resume(f)
            await self._run()
        except GeneratorExit:
            self._defer(None)
            raise

    # ---------------- main loop ----------------

    async def _run(self) -> None:
        while self.work:
            top = self.work[-1]
            if top.scope is not None:
                await self._finish_scope(top.fiber, top.scope)
                continue
            blocker = await self._step(top)
            if blocker is not None:
                self._defer(blocker)
                return

    async def _begin(self, f: Fiber, may_wait: bool) -> Actor | None:
        """Starts or takes over the disposal of f. Returns the blocker if it cannot proceed."""
        while True:
            role = f.life.begin(self.me)
            if role is Role.DONE:
                return None
            if role is Role.STARTED or role is Role.RESUMED:
                self.work.append(_Frame(f, None, may_wait))
                f.wake()  # the disposer changed: await_disposed() must check for a cycle again
                if role is Role.STARTED:
                    f.cancel_source.cancel(self.errors)
                return None
            disposer = f.life.disposer
            assert disposer is not None
            if role is Role.REENTRANT or not may_wait or not await self._wait(f, disposer):
                return disposer

    async def _step(self, frame: _Frame) -> Actor | None:
        """Handles the newest slot of a fiber. Returns the blocker if it cannot proceed."""
        f = frame.fiber
        while True:
            s = f.stack.last
            if s is None:
                f.life.finish(self.me)
                f.wake()
                self.work.pop()
                self._after_disposed(f)
                return None
            if s.state is SlotState.ACTIVE:
                s.begin_release(self.me)
                break
            if s.owner is None:
                raise RuntimeError(f"Core bug: slot '{s.name}' is {s.state.name} but still in the list")
            # PENDING: run()/apply() is still running; RELEASING: being released through its handle.
            owner = s.owner
            if not frame.may_wait or not await self._wait(f, owner):
                return owner
        s.signal.cancel(self.errors)
        if s.child is None:
            await self._run_undo(f, s)
            self._finalize(f, s)
            return None
        self.work.append(_Frame(f, s, frame.may_wait))
        return await self._begin(s.child, frame.may_wait)

    async def _finish_scope(self, f: Fiber, s: Slot) -> None:
        """The child fiber of a component slot is DISPOSED: run the component's own undo."""
        if s.child is not None and not s.child.is_disposed:
            raise RuntimeError(f"Core bug: finishing component '{s.name}' before its child fiber is disposed")
        self.work.pop()
        await self._run_undo(f, s)
        self._finalize(f, s)

    def _finalize(self, f: Fiber, s: Slot) -> None:
        f.stack.remove(s)
        s.finish_release(self.me)
        f.wake()
        s.signal.detach()
        self._maybe_resume(f)

    def _after_disposed(self, c: Fiber) -> None:
        """c was just DISPOSED. If no frame of this engine waits for it, report to the parent."""
        if self.work or c.parent is None or c.scope_slot is None:
            return
        p, x = c.parent, c.scope_slot
        if x.state is SlotState.ACTIVE and x.release_requested:
            x.begin_release(self.me)
            self.work.append(_Frame(p, x, False))
        else:
            self._maybe_resume(p)

    def _maybe_resume(self, f: Fiber) -> None:
        if f.life.is_suspended:
            f.life.begin(self.me)
            self.work.append(_Frame(f, None, False))
            f.wake()

    def _defer(self, blocker: Actor | None) -> None:
        while self.work:
            fr = self.work.pop()
            if fr.scope is None:
                fr.fiber.life.release(self.me, blocker)
            else:
                fr.scope.revert(self.me)
            fr.fiber.wake()

    # ---------------- waiting ----------------

    async def _wait(self, f: Fiber, owner: Actor) -> bool:
        """Waits for a change in f. Returns False, without waiting, if waiting would deadlock."""
        graph = f.domain.graph
        if not graph.try_enter(self.me, owner):
            return False
        fut = f.add_waiter(self.me)
        try:
            await fut
        except asyncio.CancelledError as e:
            self.errors.note_cancelled(e)
        finally:
            f.drop_waiter(fut)
            graph.exit(self.me)
        return True

    async def _run_undo(self, f: Fiber, s: Slot) -> None:
        if s.undo is None:
            return
        try:
            result = s.undo()
        except BaseException as e:
            self.errors.add(s.name, e)
            return
        if not inspect.isawaitable(result):
            return
        job = _UndoJob(result, s.name, self.me)
        while not job.is_done:
            try:
                await asyncio.wait({job.task})
            except asyncio.CancelledError as e:
                self.errors.note_cancelled(e)
            except GeneratorExit:
                job.when_done(lambda error: self._finish_abandoned(f, s, error))
                raise
        self.errors.add(s.name, job.error)

    def _finish_abandoned(self, f: Fiber, s: Slot, error: BaseException | None) -> None:
        """The coroutine of this engine was closed while the undo of s was running; it has now finished."""
        f.stack.remove(s)
        s.finish_release(self.me)
        f.wake()
        s.signal.detach()
        if error is not None:
            report(error, f"Undo of '{s.name}' failed")
        continue_later(f)


class _UndoJob:
    """An asynchronous undo, in a task of its own so that cancelling the disposer does not abort it."""

    __slots__ = ("_actor", "_aw", "_callback", "_name", "_started", "error", "task")

    def __init__(self, aw: Awaitable[object], name: str, actor: Actor) -> None:
        self._aw = aw
        self._name = name
        self._actor = actor
        self._started = False
        self._callback: Callable[[BaseException | None], None] | None = None
        self.error: BaseException | None = None
        self.task = self._start()

    def _start(self) -> asyncio.Task[None]:
        # Not loop.create_task(): an eager task factory would run the undo inside this constructor.
        task = asyncio.Task(self._guard(), loop=asyncio.get_running_loop(), name=f"undo:{self._name}")
        adopt(task, self._actor)
        _inflight.add(task)
        task.add_done_callback(self._on_done)
        return task

    async def _guard(self) -> None:
        # The error is stored, not raised: a task that RAISES KeyboardInterrupt/SystemExit stops the event loop,
        # in the middle of the teardown.
        self._started = True
        try:
            await self._aw
        except BaseException as e:
            self.error = e

    def _on_done(self, task: asyncio.Task[None]) -> None:
        _inflight.discard(task)
        if task.cancelled() and not self._started:
            # Cancelled before its first step (asyncio.run() shutting down): the undo has not run yet.
            self.task = self._start()
        elif self._callback is not None:
            self._callback(self.error)

    @property
    def is_done(self) -> bool:
        return self.task.done() and not (self.task.cancelled() and not self._started)

    def when_done(self, callback: Callable[[BaseException | None], None]) -> None:
        if self.is_done:
            callback(self.error)
        else:
            self._callback = callback


def continue_later(f: Fiber) -> None:
    """Resumes f, if it is suspended, in a new task: the operation that should do it was closed."""

    async def resume() -> None:
        with Actor.begin() as me:
            errors = Errors()
            await Teardown(me, errors).resume_if_idle(f)
            errors.raise_if_any()

    run_later(f, resume)


def release_later(f: Fiber, s: Slot) -> None:
    async def release() -> None:
        with Actor.begin() as me:
            errors = Errors()
            await Teardown(me, errors).release_slot(f, s)
            errors.raise_if_any()

    run_later(f, release)


def run_later(f: Fiber, job: Callable[[], Coroutine[Any, Any, None]]) -> None:
    async def guarded() -> None:
        try:
            await job()
        except Exception as e:
            report(e, f"Disposal of '{f.name}' failed")

    def start() -> None:
        spawn_detached(guarded())

    loop = f.domain.loop
    if loop is not None and not loop.is_closed():
        loop.call_soon(start)
