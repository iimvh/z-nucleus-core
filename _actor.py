"""Who is doing the work: what the thread is in a thread-based kernel."""

from __future__ import annotations

import asyncio
import contextvars
import weakref
from collections.abc import Coroutine, Iterator
from types import TracebackType
from typing import Any

_current: contextvars.ContextVar[Actor | None] = contextvars.ContextVar("z_nucleus_actor", default=None)
_logical: weakref.WeakKeyDictionary[asyncio.Task[Any], object] = weakref.WeakKeyDictionary()
_detached: set[asyncio.Task[Any]] = set()  # asyncio itself only keeps weak references to tasks


class Actor:
    """One running operation of the core (effect, use, dispose, ...), used as a context manager.

    Its parent is the operation whose user code called it. The parent travels in a ContextVar, so a
    task started by user code (gather, TaskGroup, create_task) still knows which operation it belongs
    to: that operation probably awaits it, so waiting for that operation could deadlock.

    Finished actors own nothing and wait for nothing; they are skipped and unlinked, so chains do not
    grow with the number of operations.
    """

    __slots__ = ("_finished", "_parent", "_token", "logical", "name")

    def __init__(self, name: str = "?", parent: Actor | None = None, logical: object = None) -> None:
        self.name = name
        self.logical: object = logical if logical is not None else object()
        self._parent = parent
        self._finished = False
        self._token: contextvars.Token[Actor | None] | None = None

    @classmethod
    def begin(cls) -> Actor:
        """A new actor for the current task, inside the operation that is running now."""
        task = asyncio.current_task()
        parent = _current.get()
        if parent is not None and parent._finished:
            parent = parent._live_parent()
        if task is None:
            return cls("?", parent)
        # An undo runs in a task of its own, but logically it is part of its disposer's task.
        return cls(task.get_name(), parent, _logical.get(task, task))

    def __enter__(self) -> Actor:
        self._token = _current.set(self)
        return self

    def __exit__(
        self, t: type[BaseException] | None, e: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.finish()
        token, self._token = self._token, None
        if token is not None:
            try:
                _current.reset(token)
            except ValueError:
                pass  # the coroutine was closed from another context

    def finish(self) -> None:
        self._finished = True
        self.logical = None
        self._parent = self._live_parent()

    @property
    def is_finished(self) -> bool:
        return self._finished

    def chain(self, strict: bool = False) -> Iterator[Actor]:
        """This actor and its unfinished ancestors.

        strict: only the ancestors in the same logical task, i.e. those that certainly await this one.
        """
        yield self
        a = self._live_parent()
        while a is not None:
            if not strict or a.logical is self.logical:
                yield a
            a = a._live_parent()

    def is_within(self, other: Actor | None, strict: bool = False) -> bool:
        """Whether this actor is `other` or runs inside it."""
        return other is not None and any(a is other for a in self.chain(strict))

    def _live_parent(self) -> Actor | None:
        p = self._parent
        while p is not None and p._finished:
            p = p._parent
        self._parent = p
        return p

    def __repr__(self) -> str:
        return f"Actor({self.name})"


def adopt(task: asyncio.Task[Any], actor: Actor) -> None:
    """Makes `task` logically part of the actor's task: the actor really awaits it."""
    if actor.logical is not None:
        _logical[task] = actor.logical


def spawn_detached[T](coro: Coroutine[Any, Any, T], *, name: str | None = None) -> asyncio.Task[T]:
    """Starts a task that does not belong to the operation that is running now.

    Use it for long-lived tasks (a server, a connection, a watchdog) started inside run()/apply() that
    are not awaited by it. A task started with asyncio.create_task() there counts as part of the
    running operation, so its dispose() may be deferred for fear of a deadlock.
    """
    ctx = contextvars.copy_context()
    ctx.run(_current.set, None)
    task = asyncio.get_running_loop().create_task(coro, name=name, context=ctx)
    _detached.add(task)
    task.add_done_callback(_detached.discard)
    return task
