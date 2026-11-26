from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Any

from ._actor import Actor
from ._errors import Errors
from ._exceptions import DeadlockError, FiberClosedError, FiberFullError
from ._lifecycle import Lifecycle, State
from ._signal import CancelSource, Signal
from ._slot import Slot, UndoStack
from ._teardown import Teardown, continue_later, release_later
from ._waitgraph import WaitGraph, graph_of

type Undo = Callable[[], object]
type Effect = Callable[[Signal], Undo | None | Awaitable[Undo | None]]
type Component = Callable[[Fiber], Undo | None | Awaitable[Undo | None]]

DEFAULT_MAX_SLOTS = 100_000

_DESCRIBE_LINE_LIMIT = 500


class _Domain:
    """What all the fibers of one tree share: the event loop they are bound to."""

    __slots__ = ("_graph", "loop")

    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self._graph: WaitGraph | None = None

    def bind(self) -> None:
        loop = asyncio.get_running_loop()
        if self.loop is None:
            self.loop = loop
            self._graph = graph_of(loop)
        elif self.loop is not loop:
            raise RuntimeError("This tree of fibers is bound to a different event loop")

    @property
    def graph(self) -> WaitGraph:
        assert self._graph is not None
        return self._graph


class Fiber:
    """A node of the teardown tree. It owns an undo list.

    Guarantees:
     1. Ordering: disposal runs in reverse START order (a slot is reserved before run() starts),
        children before parents.
     2. Exactly once: each undo runs at most once, however many tasks dispose concurrently.
     3. No silent loss: a failing undo does not stop the others; errors are gathered in a DisposeError.
     4. Completion: dispose() returns True when the fiber is DISPOSED. It returns False only when waiting
        is impossible: the caller itself holds unfinished work the teardown needs (dispose() called from
        inside run() or an undo of the same tree), or waiting would close a deadlock cycle. Teardown
        then resumes, in the correct order, as soon as the unfinished work completes.
     5. Closing: once teardown has started, effect()/use() raise FiberClosedError without calling run().
        Work already running is notified through its Signal and undone when it returns.
     6. Unbounded depth: teardown and cancellation are iterative.
     7. Isolation: a component only receives its own fiber (by convention: underscored names and
        attributes not documented here are internal).
     8. Bounded memory: work that needs no undo is not retained; each fiber has a slot quota.
     9. Cancelling a task never abandons a teardown: it finishes first, then CancelledError is raised.

    One tree belongs to one event loop and is not thread-safe.
    """

    __slots__ = (
        "__weakref__", "_max_slots", "_name", "_waiters",
        "cancel_source", "domain", "life", "parent", "scope_slot", "stack",
    )

    _max_slots: int
    _name: str
    _waiters: dict[asyncio.Future[None], Actor]
    cancel_source: CancelSource
    domain: _Domain
    life: Lifecycle
    parent: Fiber | None
    scope_slot: Slot | None  # the slot of the parent that owns this fiber
    stack: UndoStack

    def __init__(self) -> None:
        raise TypeError("Use Fiber.root() to create a tree, or fiber.use() to create a child")

    @classmethod
    def _make(
        cls, name: str, parent: Fiber | None, signal: CancelSource, max_slots: int, domain: _Domain
    ) -> Fiber:
        f = object.__new__(cls)
        f._name = name
        f._max_slots = max_slots
        f._waiters = {}
        f.cancel_source = signal
        f.domain = domain
        f.life = Lifecycle()
        f.parent = parent
        f.scope_slot = None
        f.stack = UndoStack()
        return f

    @staticmethod
    def root(name: str = "root", *, max_slots: int = DEFAULT_MAX_SLOTS) -> Fiber:
        """Creates a new tree; the only way to obtain a fiber without a parent."""
        if max_slots <= 0:
            raise ValueError("max_slots must be > 0")
        return Fiber._make(name, None, CancelSource(name), max_slots, _Domain())

    @property
    def name(self) -> str:
        return self._name

    @property
    def signal(self) -> Signal:
        """Cancelled as soon as this fiber starts disposing."""
        return self.cancel_source

    @property
    def is_alive(self) -> bool:
        return self.life.is_alive

    @property
    def is_disposed(self) -> bool:
        return self.life.state is State.DISPOSED

    @property
    def size(self) -> int:
        """Number of work items this fiber manages (not disposed yet)."""
        return len(self.stack)

    # ---------------- creating work ----------------

    async def effect(self, name: str | None, run: Effect) -> Handle:
        """Runs a piece of work and keeps the undo it returns (None: nothing to undo).

        run and the undo may be synchronous or asynchronous. If run raises, it must have cleaned up
        what it opened. The slot is reserved when this coroutine starts, i.e. when it is awaited.
        """
        slot_name = _name_of(name, run, "effect")
        self.domain.bind()
        with Actor.begin() as me:
            slot = self._reserve(slot_name, me, False)
            undo: Undo | None = None
            failure: BaseException | None = None
            try:
                undo = await _call(run, slot.signal, slot_name)
            except GeneratorExit:
                self._settle(slot, None, True)
                continue_later(self)
                raise
            except BaseException as e:
                failure = e
            errors = Errors()
            return await self._complete(slot, undo, failure, Teardown(me, errors))

    async def use(self, name: str | None, component: Component) -> Handle:
        """Runs a component inside its own child fiber. The handle undoes the whole branch.

        The component's own cleanup (what it returns) runs after everything in its fiber is undone. If
        it raises, what it created is disposed first, and the error is raised unchanged.
        """
        slot_name = _name_of(name, component, "component")
        self.domain.bind()
        with Actor.begin() as me:
            slot = self._reserve(slot_name, me, True)
            child = slot.child
            assert child is not None
            undo: Undo | None = None
            failure: BaseException | None = None
            try:
                undo = await _call(component, child, slot_name)
            except GeneratorExit:
                slot.activate(None)  # the child stays owned, so that its contents are undone
                self.wake()
                release_later(self, slot)
                continue_later(self)
                raise
            except BaseException as e:
                failure = e
            td = Teardown(me, Errors())
            if failure is not None:
                try:
                    await td.dispose_fiber(child)
                except GeneratorExit:
                    self._abandon_failed(slot, child)
                    raise
            return await self._complete(slot, undo, failure, td)

    # ---------------- disposal ----------------

    async def dispose(self) -> bool:
        """Undoes everything in this fiber. Returns whether it is DISPOSED (see guarantee 4)."""
        self.domain.bind()
        with Actor.begin() as me:
            errors = Errors()
            await Teardown(me, errors).dispose_fiber(self)
            errors.raise_if_any()
            return self.is_disposed

    async def await_disposed(self, timeout: float | None = None) -> bool:
        """Waits until this fiber is disposed, without starting the disposal. Returns False on timeout.

        Unlike dispose(), it can be cancelled. Raises DeadlockError if the wait can never complete, e.g.
        when called from inside an undo of this same fiber.
        """
        self.domain.bind()
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        graph = self.domain.graph
        with Actor.begin() as me:
            while True:
                if self.is_disposed:
                    return True
                disposer = self.life.disposer
                if me.is_within(disposer or self.life.blocked_by, strict=True):
                    raise DeadlockError(
                        f"Waiting for fiber '{self._name}' to be disposed, but its disposal needs the "
                        f"waiting task '{me.name}' to finish its own work first")
                remaining = None if deadline is None else deadline - loop.time()
                if remaining is not None and remaining <= 0:
                    return False
                if disposer is not None and not graph.try_enter(me, disposer, strict=True):
                    raise DeadlockError(
                        f"Waiting for fiber '{self._name}' would close a deadlock cycle with task "
                        f"'{disposer.name}'")
                fut = self.add_waiter(me)
                try:
                    await asyncio.wait({fut}, timeout=remaining)
                finally:
                    self.drop_waiter(fut)
                    graph.exit(me)

    async def __aenter__(self) -> Fiber:
        return self

    async def __aexit__(
        self, t: type[BaseException] | None, e: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.domain.bind()
        with Actor.begin() as me:
            errors = Errors()
            await Teardown(me, errors).dispose_fiber(self)
            outcome = errors.outcome(e)
        if outcome is not None and outcome is not e:
            raise outcome

    def describe(self) -> str:
        """Snapshot of the tree, for diagnosing hangs. The most recent item (undone first) comes first."""
        out: list[str] = []
        todo: list[tuple[Fiber, int]] = [(self, 0)]
        while todo and len(out) < _DESCRIBE_LINE_LIMIT:
            f, depth = todo.pop()
            line = f"{_indent(depth)}fiber '{f._name}' {f.life.state.name}"
            if f.life.disposer is not None:
                line += f" (being disposed by {f.life.disposer.name})"
            elif f.life.blocked_by is not None:
                line += f" (suspended, waiting for {f.life.blocked_by.name})"
            out.append(line)
            kids: list[tuple[Fiber, int]] = []
            for s in reversed(f.stack.snapshot()):
                if len(out) >= _DESCRIBE_LINE_LIMIT:
                    break
                by = f" by {s.owner.name}" if s.owner is not None else ""
                out.append(f"{_indent(depth + 1)}- [{s.state.name}{by}] {s.name}")
                if s.child is not None:
                    kids.append((s.child, depth + 2))
            todo.extend(reversed(kids))
        if todo:
            out.append("... (truncated)")
        return "\n".join(out) + "\n"

    def __repr__(self) -> str:
        return f"Fiber[{self._name}, {self.life.state.name}, {len(self.stack)} items]"

    # ---------------- internals ----------------

    def add_waiter(self, me: Actor) -> asyncio.Future[None]:
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters[fut] = me
        return fut

    def drop_waiter(self, fut: asyncio.Future[None]) -> None:
        self._waiters.pop(fut, None)

    def wake(self) -> None:
        """Something changed: every waiter checks its condition again.

        Their edges leave the wait graph now, not when they get to run: until then they are not really
        waiting, and a stale edge would refuse other waits for no reason.
        """
        if not self._waiters:
            return
        waiters, self._waiters = self._waiters, {}
        for fut, actor in waiters.items():
            self.domain.graph.exit(actor)
            if not fut.done():
                fut.set_result(None)

    def _reserve(self, name: str, me: Actor, scope: bool) -> Slot:
        # Before running: disposal order is start order, with no gap between run() and registration.
        if not self.life.is_alive:
            raise FiberClosedError(f"Fiber '{self._name}' has started disposing; rejecting new work '{name}'")
        if len(self.stack) >= self._max_slots:
            raise FiberFullError(
                f"Fiber '{self._name}' is full ({self._max_slots} items); rejecting '{name}'. "
                "Dispose some work, or split it into child fibers.")
        signal = self.cancel_source.child(name)
        child = (
            Fiber._make(name, self, signal.child(name), self._max_slots, self.domain) if scope else None
        )
        slot = Slot(name, child, signal, me)
        if child is not None:
            child.scope_slot = slot
        self.stack.push(slot)
        return slot

    def _settle(self, slot: Slot, undo: Undo | None, failed: bool) -> bool:
        """run()/apply() is over: records its result. Returns whether nothing is left to undo."""
        child = slot.child
        if failed and child is not None and not child.is_disposed:
            # Its disposal was deferred: keep it owned, and finish the slot when it completes.
            slot.activate(None)
            slot.release_requested = True
            settled = False
        elif failed:
            self.stack.remove(slot)
            slot.fail()
            settled = True
        elif undo is None and child is None:
            self.stack.remove(slot)
            slot.settle_empty()
            settled = True
        else:
            slot.activate(undo)
            settled = False
        self.wake()
        if settled:
            slot.signal.detach()
        return settled

    def _abandon_failed(self, slot: Slot, child: Fiber) -> None:
        """The coroutine was closed while undoing what a failed component had created."""
        if not self._settle(slot, None, True):
            continue_later(child)
        continue_later(self)

    async def _complete(
        self, slot: Slot, undo: Undo | None, failure: BaseException | None, td: Teardown
    ) -> Handle:
        self._settle(slot, undo, failure is not None)
        closed = not self.life.is_alive
        if closed:
            await td.resume_if_idle(self)
        primary = failure
        if primary is None and closed:
            primary = FiberClosedError(
                f"Fiber '{self._name}' was disposed while '{slot.name}' was running; its result has been "
                "or will be cleaned up automatically, do not use it")
        outcome = td.errors.outcome(primary)
        if outcome is None:
            return Handle(self, slot)
        try:
            raise outcome
        finally:
            del failure, primary, outcome  # no reference cycle through the traceback


class Handle:
    """Returned by effect()/use(): undoes exactly that work item."""

    __slots__ = ("_fiber", "_slot")

    def __init__(self, fiber: Fiber, slot: Slot) -> None:
        self._fiber = fiber
        self._slot = slot

    @property
    def is_disposed(self) -> bool:
        return self._slot.is_settled

    async def dispose(self) -> bool:
        """Undoes the work. Returns whether it is done (False: deferred, see Fiber guarantee 4)."""
        if self._slot.is_settled:
            return True
        self._fiber.domain.bind()
        with Actor.begin() as me:
            errors = Errors()
            await Teardown(me, errors).release_slot(self._fiber, self._slot)
            errors.raise_if_any()
            return self._slot.is_settled


async def _call(fn: Callable[[Any], Any], arg: object, name: str) -> Undo | None:
    result = fn(arg)
    if inspect.isawaitable(result):
        result = await result
    if result is None or callable(result):
        return result  # type: ignore[no-any-return]
    hint = ""
    if inspect.iscoroutine(result):
        result.close()
        hint = " (return the function itself: cleanup, not cleanup())"
    raise TypeError(f"'{name}' must return its undo as a callable, or None; got {type(result).__name__}{hint}")


def _name_of(given: str | None, fn: object, fallback: str) -> str:
    if not callable(fn):
        raise TypeError(f"{fallback} must be callable, got {type(fn).__name__}")
    if given is not None:
        return given
    name = getattr(fn, "__name__", None) or type(fn).__name__
    return fallback if name == "<lambda>" else name


def _indent(depth: int) -> str:
    return "  " * min(depth, 40)
