"""Public exceptions, and how secondary errors are attached to a primary one."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass

_SHOWN = 5
_MAX_TEXT = 200
_SUPPRESSED = "__z_suppressed__"


class FiberClosedError(RuntimeError):
    """The fiber has started disposing, so it rejects new work.

    Also raised when the fiber is disposed while a piece of work is still running: that work's
    result has been or will be cleaned up automatically, and must not be used.
    """


class FiberFullError(RuntimeError):
    """The fiber already manages its maximum number of work items."""


class DeadlockError(RuntimeError):
    """An explicit wait (await_disposed) that can never complete is refused instead of hanging."""


class SignalCancelledError(Exception):
    """Raised by Signal.raise_if_cancelled(). Unrelated to asyncio.CancelledError."""


class UndoCancelledError(Exception):
    """Stands for an asyncio.CancelledError that came out of an undo (__cause__ is the original)."""


class WrappedError(Exception):
    """Stands for an error that is not an Exception and so cannot be in an ExceptionGroup."""


@dataclass(frozen=True, slots=True)
class Failure:
    """One error: `source` is the name of the work item that caused it."""

    source: str
    error: BaseException


class DisposeError(ExceptionGroup[Exception]):
    """One or more undos raised. When this is raised, ALL other undos have already run.

    failures: each error with the name of its work item, in the order they occurred. An error that is
    not an Exception is kept as is in `failures`, and appears wrapped in `exceptions`.
    """

    failures: tuple[Failure, ...]
    dropped: int

    def __new__(cls, failures: Sequence[Failure], dropped: int = 0) -> DisposeError:
        members = [_as_exception(f) for f in failures]
        if not members:
            members = [WrappedError(f"{dropped} error(s) were not retained")]
        self = super().__new__(cls, _summary(failures, dropped), members)
        self.failures = tuple(failures)
        self.dropped = dropped
        return self

    def __init__(self, failures: Sequence[Failure], dropped: int = 0) -> None:
        super().__init__(failures, dropped)  # type: ignore[arg-type]  # args, as copy and pickle need

    @property
    def total(self) -> int:
        return len(self.failures) + self.dropped

    def derive(self, excs: Sequence[Exception], /) -> ExceptionGroup[Exception]:  # type: ignore[override]
        return ExceptionGroup(self.message, excs)


def suppressed(exc: BaseException) -> tuple[BaseException, ...]:
    """The secondary errors that were attached to `exc` because only one error could be raised."""
    found = getattr(exc, _SUPPRESSED, ())
    return found if isinstance(found, tuple) else ()


def attach(target: BaseException, extra: BaseException) -> None:
    """Attaches `extra` to `target` (see suppressed()). Never raises."""
    if target is extra or any(e is extra for e in suppressed(target)):
        return
    try:
        setattr(target, _SUPPRESSED, (*suppressed(target), extra))
        target.add_note(f"suppressed: {_cap(safe_text(extra))}")
    except Exception:
        pass  # the target refuses attributes: nothing can be done, and nothing may be raised


def safe_text(exc: BaseException) -> str:
    """Text of a user error, whose __str__ may be broken."""
    name = type(exc).__name__
    try:
        text = str(exc)
    except Exception:
        return f"{name} (str() raised)"
    return f"{name}: {text}" if text else name


def _as_exception(f: Failure) -> Exception:
    if isinstance(f.error, Exception):
        return f.error
    wrapped: Exception
    if isinstance(f.error, asyncio.CancelledError):
        wrapped = UndoCancelledError(f"'{f.source}' was cancelled while it was being undone")
    else:
        wrapped = WrappedError(safe_text(f.error))
    wrapped.__cause__ = f.error
    return wrapped


def _summary(failures: Sequence[Failure], dropped: int) -> str:
    parts = [f"[{_cap(f.source)}] {_cap(safe_text(f.error))}" for f in failures[:_SHOWN]]
    more = len(failures) - len(parts) + dropped
    if more > 0:
        parts.append(f"... and {more} more error(s)")
    return f"Disposal failed with {len(failures) + dropped} error(s): " + "; ".join(parts)


def _cap(s: str) -> str:
    return s if len(s) <= _MAX_TEXT else s[:_MAX_TEXT] + "..."
