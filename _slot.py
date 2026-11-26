from __future__ import annotations

from collections.abc import Callable
from enum import Enum
from typing import TYPE_CHECKING

from ._actor import Actor
from ._signal import CancelSource

if TYPE_CHECKING:
    from ._fiber import Fiber

type Undo = Callable[[], object]


class SlotState(Enum):
    PENDING = "PENDING"
    ACTIVE = "ACTIVE"
    RELEASING = "RELEASING"
    RELEASED = "RELEASED"
    FAILED = "FAILED"


class Slot:
    """One entry of the undo list: work that has run (or is running), and how to undo it.

    Every transition not drawn here is a bug inside the core and raises:

      PENDING --activate--> ACTIVE --begin_release--> RELEASING --finish_release--> RELEASED
         |                     ^                          |
         |                     +--------- revert ---------+  (the releaser had to defer)
         +--fail---------> FAILED    (run/apply raised: nothing to undo)
         +--settle_empty-> RELEASED  (run returned None: nothing to undo)

    owner: PENDING -> the actor running run(); RELEASING -> the actor releasing it; otherwise None.
    Once settled, a slot drops its references to the undo and the child fiber.
    """

    __slots__ = (
        "_child", "_owner", "_state", "_undo", "list", "name", "next", "prev",
        "release_requested", "signal",
    )

    def __init__(self, name: str, child: Fiber | None, signal: CancelSource, runner: Actor) -> None:
        self.name = name
        self.signal = signal
        self.release_requested = False  # a release was requested but had to be deferred
        self._child = child
        self._state = SlotState.PENDING
        self._owner: Actor | None = runner
        self._undo: Undo | None = None
        # Only UndoStack touches these.
        self.list: UndoStack | None = None
        self.prev: Slot | None = None
        self.next: Slot | None = None

    @property
    def state(self) -> SlotState:
        return self._state

    @property
    def owner(self) -> Actor | None:
        return self._owner

    @property
    def undo(self) -> Undo | None:
        return self._undo

    @property
    def child(self) -> Fiber | None:
        return self._child

    @property
    def is_settled(self) -> bool:
        return self._state in (SlotState.RELEASED, SlotState.FAILED)

    def activate(self, undo: Undo | None) -> None:
        self._require(SlotState.PENDING, "activate")
        self._undo = undo
        self._state = SlotState.ACTIVE
        self._owner = None

    def fail(self) -> None:
        self._require(SlotState.PENDING, "fail")
        self._state = SlotState.FAILED
        self._owner = None
        self._child = None

    def settle_empty(self) -> None:
        self._require(SlotState.PENDING, "settle_empty")
        if self._child is not None:
            raise RuntimeError(f"Core bug: settle_empty on component slot '{self.name}'")
        self._state = SlotState.RELEASED
        self._owner = None

    def begin_release(self, releaser: Actor) -> None:
        self._require(SlotState.ACTIVE, "begin_release")
        self._state = SlotState.RELEASING
        self._owner = releaser

    def finish_release(self, releaser: Actor) -> None:
        self._require(SlotState.RELEASING, "finish_release")
        self._require_owner(releaser, "finish_release")
        self._state = SlotState.RELEASED
        self._owner = None
        self._undo = None
        self._child = None

    def revert(self, releaser: Actor) -> None:
        self._require(SlotState.RELEASING, "revert")
        self._require_owner(releaser, "revert")
        self._state = SlotState.ACTIVE
        self._owner = None
        self.release_requested = True

    def _require(self, expected: SlotState, op: str) -> None:
        if self._state is not expected:
            raise RuntimeError(f"Core bug: {op} while slot '{self.name}' is {self._state.name}")

    def _require_owner(self, actor: Actor, op: str) -> None:
        if self._owner is not actor:
            raise RuntimeError(
                f"Core bug: {op} on slot '{self.name}' by {actor} but the owner is {self._owner}")


class UndoStack:
    """The undo list: a doubly linked list, LIFO, every operation O(1).

    A dict would not do: finding its last key is not O(1) after many deletions at the end.
    """

    __slots__ = ("_size", "_tail")

    def __init__(self) -> None:
        self._tail: Slot | None = None
        self._size = 0

    def push(self, s: Slot) -> None:
        if s.list is not None or s.prev is not None or s.next is not None:
            raise RuntimeError(f"Core bug: slot '{s.name}' is already in a list")
        s.list = self
        s.prev = self._tail
        if self._tail is not None:
            self._tail.next = s
        self._tail = s
        self._size += 1

    def remove(self, s: Slot) -> None:
        if s.list is not self:
            raise RuntimeError(f"Core bug: slot '{s.name}' is not in this list")
        if s.prev is not None:
            s.prev.next = s.next
        if s.next is not None:
            s.next.prev = s.prev
        else:
            self._tail = s.prev
        s.prev = s.next = s.list = None
        self._size -= 1

    @property
    def last(self) -> Slot | None:
        """The newest slot (undone first), or None."""
        return self._tail

    def __len__(self) -> int:
        return self._size

    def snapshot(self) -> list[Slot]:
        """The slots, from oldest to newest."""
        out: list[Slot] = []
        s = self._tail
        while s is not None:
            out.append(s)
            s = s.prev
        out.reverse()
        return out
