from __future__ import annotations

import asyncio
import inspect
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable

from ._errors import Errors
from ._exceptions import SignalCancelledError


class Signal(ABC):
    """A read-only cancellation signal: running work learns "my fiber is being disposed, stop early".

    It is cooperative: the core never cancels the task that runs user work.
     - Once cancelled, it stays cancelled.
     - Each listener runs exactly once, unless it was unregistered before the cancellation.
     - Registering after cancellation runs the listener immediately; its error goes to the caller.
    """

    __slots__ = ()

    @property
    @abstractmethod
    def is_cancelled(self) -> bool: ...

    @abstractmethod
    def raise_if_cancelled(self) -> None:
        """Raises SignalCancelledError if already cancelled."""

    @abstractmethod
    def on_cancel(self, listener: Callable[[], object]) -> Callable[[], None]:
        """Registers a synchronous listener. Returns a function that unregisters it (idempotent)."""

    @abstractmethod
    async def wait(self, timeout: float | None = None) -> bool:
        """Waits until cancelled, at most `timeout` seconds. Returns whether it is cancelled.

        Use it instead of asyncio.sleep() in work that must stop as soon as its fiber is disposed.
        """


class CancelSource(Signal):
    """A hierarchical signal: cancelling a parent cancels its descendants, iteratively.

    Everything is synchronous, so registering and cancelling cannot interleave. Unregistering and
    detaching are O(1); after cancellation the collections are dropped.
    """

    __slots__ = ("_cancelled", "_children", "_listeners", "_parent", "label")

    def __init__(self, label: str) -> None:
        self.label = label
        self._cancelled = False
        self._parent: CancelSource | None = None
        self._listeners: dict[object, Callable[[], object]] | None = None
        self._children: dict[CancelSource, None] | None = None

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled

    def raise_if_cancelled(self) -> None:
        if self._cancelled:
            raise SignalCancelledError(f"'{self.label}' has been cancelled")

    def on_cancel(self, listener: Callable[[], object]) -> Callable[[], None]:
        if not callable(listener):
            raise TypeError("listener must be callable")
        if inspect.iscoroutinefunction(listener):
            raise TypeError("on_cancel listeners must be synchronous")
        if self._cancelled:
            _run_listener(listener)
            return _noop
        key = object()
        if self._listeners is None:
            self._listeners = {}
        self._listeners[key] = listener

        def unregister() -> None:
            if self._listeners is not None:
                self._listeners.pop(key, None)

        return unregister

    async def wait(self, timeout: float | None = None) -> bool:
        if self._cancelled:
            return True
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        def wake() -> None:
            if not fut.done():
                fut.set_result(None)

        unregister = self.on_cancel(wake)
        try:
            await asyncio.wait({fut}, timeout=timeout)
        finally:
            unregister()
        return self._cancelled

    def child(self, label: str) -> CancelSource:
        """Creates a child signal, already cancelled if this one is."""
        c = CancelSource(label)
        if self._cancelled:
            c._cancelled = True
            return c
        c._parent = self
        if self._children is None:
            self._children = {}
        self._children[c] = None
        return c

    def detach(self) -> None:
        """Detaches from the parent (the owner of this signal is done), so it is not kept alive."""
        p, self._parent = self._parent, None
        if p is not None and p._children is not None:
            p._children.pop(self, None)

    def cancel(self, errors: Errors) -> None:
        """Cancels this signal and its descendants; listener errors are collected."""
        work = deque([self])
        while work:
            s = work.popleft()
            if s._cancelled:
                continue
            s._cancelled = True
            listeners, children = s._listeners, s._children
            s._listeners = s._children = None
            for listener in list(listeners.values()) if listeners else ():
                try:
                    _run_listener(listener)
                except BaseException as e:
                    errors.add(f"on_cancel:{s.label}", e)
            if children:
                work.extend(children)

    @property
    def listener_count(self) -> int:
        return len(self._listeners) if self._listeners else 0

    @property
    def child_count(self) -> int:
        return len(self._children) if self._children else 0


def _run_listener(listener: Callable[[], object]) -> None:
    result = listener()
    if inspect.iscoroutine(result):
        result.close()
        raise TypeError("on_cancel listeners must be synchronous, but this one returned a coroutine")


def _noop() -> None:
    pass
