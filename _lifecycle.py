from __future__ import annotations

from enum import Enum

from ._actor import Actor


class State(Enum):
    ALIVE = "ALIVE"
    DISPOSING = "DISPOSING"
    DISPOSED = "DISPOSED"


class Role(Enum):
    STARTED = "STARTED"      # ALIVE -> DISPOSING; the caller is the disposer and must signal cancellation
    RESUMED = "RESUMED"      # the disposal was suspended; the caller takes it over
    REENTRANT = "REENTRANT"  # the caller runs inside the current disposer (from an undo or a listener)
    BUSY = "BUSY"            # someone else is disposing
    DONE = "DONE"            # already DISPOSED


class Lifecycle:
    """ALIVE -> DISPOSING -> DISPOSED, one direction only.

    While DISPOSING, either `disposer` is the actor disposing it, or `disposer` is None: the disposal
    is SUSPENDED because waiting would deadlock, and `blocked_by` is the actor that blocked it. The
    next actor that needs the fiber takes over.
    """

    __slots__ = ("blocked_by", "disposer", "state")

    def __init__(self) -> None:
        self.state = State.ALIVE
        self.disposer: Actor | None = None
        self.blocked_by: Actor | None = None

    @property
    def is_alive(self) -> bool:
        return self.state is State.ALIVE

    @property
    def is_suspended(self) -> bool:
        return self.state is State.DISPOSING and self.disposer is None

    def begin(self, me: Actor) -> Role:
        if self.state is State.ALIVE:
            self.state = State.DISPOSING
            self.disposer = me
            return Role.STARTED
        if self.state is State.DISPOSED:
            return Role.DONE
        if self.disposer is None:
            self.disposer = me
            self.blocked_by = None
            return Role.RESUMED
        return Role.REENTRANT if me.is_within(self.disposer) else Role.BUSY

    def release(self, me: Actor, blocker: Actor | None) -> None:
        """The disposer suspends the disposal."""
        self._require_disposer(me, "release")
        self.disposer = None
        self.blocked_by = blocker

    def finish(self, me: Actor) -> None:
        self._require_disposer(me, "finish")
        self.state = State.DISPOSED
        self.disposer = None
        self.blocked_by = None

    def _require_disposer(self, me: Actor, op: str) -> None:
        if self.state is not State.DISPOSING or self.disposer is not me:
            raise RuntimeError(
                f"Core bug: {op} while state={self.state.name}, disposer={self.disposer}, called by={me}")
