from __future__ import annotations

import asyncio
import threading
import weakref

from ._actor import Actor


class WaitGraph:
    """Detects deadlocks BEFORE waiting. One graph per event loop.

    An actor about to wait records "I wait for X". An operation also cannot proceed while something
    running inside it waits, so the edge counts for the ancestors of the waiter too. If X (directly or
    not) waits for the waiter or one of its ancestors, the wait is refused, so the graph never has a cycle.

    strict edges (await_disposed) count only for ancestors in the same logical task: a background task
    that merely watches a fiber does not block the operation that started it.

    Limitation: only waits made by the core are visible. If user code waits (a future, a lock, ...) for
    something that waits for the core, the graph cannot see it.
    """

    __slots__ = ("_chains", "_targets")

    def __init__(self) -> None:
        self._chains: dict[Actor, list[Actor]] = {}             # waiter -> the actors its edge counts for
        self._targets: dict[Actor, dict[Actor, Actor]] = {}     # actor -> {waiter: what it waits for}

    def try_enter(self, me: Actor, owner: Actor, strict: bool = False) -> bool:
        if me in self._chains:
            raise RuntimeError(f"Core bug: {me} is already waiting")
        chain = list(me.chain(strict))
        mine = set(chain)
        seen: set[Actor] = set()
        todo = [owner]
        while todo:
            a = todo.pop()
            if a in mine:
                return False
            if a in seen:
                continue
            seen.add(a)
            todo.extend(self._targets.get(a, {}).values())
        self._chains[me] = chain
        for a in chain:
            self._targets.setdefault(a, {})[me] = owner
        return True

    def exit(self, me: Actor) -> None:
        """Idempotent."""
        for a in self._chains.pop(me, ()):
            edges = self._targets[a]
            del edges[me]
            if not edges:
                del self._targets[a]

    def __len__(self) -> int:
        return len(self._chains)


_graphs: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, WaitGraph] = weakref.WeakKeyDictionary()
_graphs_lock = threading.Lock()  # several loops may run in several threads


def graph_of(loop: asyncio.AbstractEventLoop) -> WaitGraph:
    with _graphs_lock:
        g = _graphs.get(loop)
        if g is None:
            g = _graphs[loop] = WaitGraph()
        return g
