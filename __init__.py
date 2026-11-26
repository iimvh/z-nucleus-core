"""z-nucleus: an asyncio kernel for structured lifecycle management."""

from ._actor import spawn_detached
from ._escalation import EscalatingUndo
from ._exceptions import (
    DeadlockError,
    DisposeError,
    Failure,
    FiberClosedError,
    FiberFullError,
    SignalCancelledError,
    UndoCancelledError,
    WrappedError,
    suppressed,
)
from ._fiber import DEFAULT_MAX_SLOTS, Component, Effect, Fiber, Handle, Undo
from ._signal import Signal

__all__ = [
    "DEFAULT_MAX_SLOTS",
    "Component",
    "DeadlockError",
    "DisposeError",
    "Effect",
    "EscalatingUndo",
    "Failure",
    "Fiber",
    "FiberClosedError",
    "FiberFullError",
    "Handle",
    "Signal",
    "SignalCancelledError",
    "Undo",
    "UndoCancelledError",
    "WrappedError",
    "spawn_detached",
    "suppressed",
]
