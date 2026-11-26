from __future__ import annotations

import asyncio
import logging

from ._exceptions import DisposeError, Failure, FiberClosedError, attach

FATAL = (MemoryError, KeyboardInterrupt, SystemExit)


class Errors:
    """Collects the errors of one operation: one failing undo must not stop the others, nor be lost.

    Which error is raised at the end, highest priority first:
      1. a fatal error (MemoryError, KeyboardInterrupt, SystemExit), unchanged;
      2. the asyncio.CancelledError of a caller that was cancelled while the core was waiting
         (the core finishes first: cancellation never abandons a teardown);
      3. the primary error (what run()/apply() raised, or FiberClosedError);
      4. DisposeError.
    The others are attached to it (see suppressed()). asyncio discards a CancelledError together with
    whatever is attached to it, so errors hidden behind one are also sent to the loop's exception handler.

    The user's exceptions are not modified while collecting, and at most MAX_KEPT are kept.
    """

    MAX_KEPT = 100

    __slots__ = ("_cancelled", "_dropped", "_fatal", "_kept")

    def __init__(self) -> None:
        self._kept: list[Failure] = []
        self._dropped = 0
        self._fatal: BaseException | None = None
        self._cancelled: asyncio.CancelledError | None = None

    def add(self, source: str, error: BaseException | None) -> None:
        if error is None:
            return
        if self._fatal is None and isinstance(error, FATAL):
            self._fatal = error
        if len(self._kept) < self.MAX_KEPT:
            self._kept.append(Failure(source, error))
        else:
            self._dropped += 1

    def note_cancelled(self, error: asyncio.CancelledError) -> None:
        """The calling task was cancelled during a wait of the core; raised once the work is finished."""
        if self._cancelled is None:
            self._cancelled = error

    @property
    def count(self) -> int:
        return len(self._kept) + self._dropped

    def outcome(self, primary: BaseException | None = None) -> BaseException | None:
        """What the operation must raise, or None. Call it once, at the end."""
        if isinstance(primary, FATAL):
            top: BaseException | None = primary
        else:
            top = self._fatal or self._cancelled or primary
        if isinstance(primary, asyncio.CancelledError) and self._fatal is None:
            top = primary
        rest = self._rest(top, primary)
        if top is None:
            return rest
        hidden = [e for e in (primary, rest) if e is not None and e is not top]
        for e in hidden:
            attach(top, e)
        if isinstance(top, asyncio.CancelledError):
            lost = [e for e in hidden if not isinstance(e, FiberClosedError)]
            if lost and top.__cause__ is None:
                top.__cause__ = lost[0]
            for e in lost:
                report(e, "Error hidden by the cancellation of the calling task")
        elif self._cancelled is not None and top is not self._cancelled:
            attach(top, self._cancelled)
        return top

    def raise_if_any(self) -> None:
        exc = self.outcome()
        if exc is not None:
            raise exc

    def _rest(self, top: BaseException | None, primary: BaseException | None) -> DisposeError | None:
        rest = [f for f in self._kept if f.error is not top and f.error is not primary]
        if not rest and not self._dropped:
            return None
        return DisposeError(rest, self._dropped)


def report(exc: BaseException, message: str) -> None:
    """Reports an error that has no caller to go to."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        logging.getLogger("z_nucleus").error(message, exc_info=exc)
        return
    loop.call_exception_handler({"message": message, "exception": exc})
