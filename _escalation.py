from __future__ import annotations

import asyncio
import math
from collections.abc import Callable
from contextlib import suppress
from typing import Literal

from ._errors import Errors, report


class EscalatingUndo:
    """A single-use undo for one resource, with optional escalation before its deadline.

    The synchronous callbacks request a stop and must return promptly. The event
    belongs to the resource's observer, which sets it only after confirming that
    the resource has stopped. A timeout or a successful request does not confirm
    anything. Use the undo and its event on the same asyncio event loop.
    """

    __slots__ = ("_confirmed", "_force", "_grace", "_request", "_stop", "_used", "_watcher")

    def __init__(
        self,
        request_stop: Callable[[], None],
        request_force: Callable[[], None],
        confirmed: asyncio.Event,
        *,
        grace: float,
    ) -> None:
        if not math.isfinite(grace) or grace < 0:
            raise ValueError("grace must be finite and nonnegative")
        self._stop = request_stop
        self._force = request_force
        self._confirmed = confirmed
        self._grace = grace
        self._request: asyncio.Future[Literal[True]] | None = None
        self._used = False
        self._watcher: asyncio.Task[None] | None = None

    @staticmethod
    def for_process(
        process: asyncio.subprocess.Process,
        request_stop: Callable[[], None],
        *,
        grace: float,
    ) -> EscalatingUndo:
        """Bind escalation to this process, and confirm exit through its wait method.

        Call on the process's event loop. The caller must drain piped output.
        The caller supplies the soft-stop protocol. Only this process is managed.
        """
        confirmed = asyncio.Event()

        def request_force() -> None:
            if process.returncode is None:
                with suppress(ProcessLookupError):
                    process.kill()

        def stop_if_running() -> None:
            if process.returncode is None:
                request_stop()

        async def observe_exit() -> None:
            try:
                await process.wait()
            except Exception as error:
                report(error, f"Could not confirm exit of process {process.pid}")
            else:
                confirmed.set()

        undo = EscalatingUndo(stop_if_running, request_force, confirmed, grace=grace)
        undo._watcher = asyncio.get_running_loop().create_task(observe_exit())
        return undo

    def request_escalation(self) -> bool:
        """Request force early for this active undo. True means accepted, not stopped."""
        request = self._request
        if request is None or request.done() or self._confirmed.is_set():
            return False
        if request.get_loop() is not asyncio.get_running_loop():
            raise RuntimeError("escalation must run on the undo's event loop")
        request.set_result(True)
        return True

    async def __call__(self) -> None:
        if self._used:
            raise RuntimeError("an EscalatingUndo can only be run once")
        self._used = True
        if self._confirmed.is_set():
            return

        loop = asyncio.get_running_loop()
        request: asyncio.Future[Literal[True]] = loop.create_future()
        self._request = request
        confirmation = loop.create_task(self._confirmed.wait())
        errors = Errors()
        try:
            self._send(self._stop, "request_stop", errors)
            await asyncio.wait((confirmation, request), timeout=self._grace,
                               return_when=asyncio.FIRST_COMPLETED)
            self._request = None
            if not self._confirmed.is_set():
                self._send(self._force, "request_force", errors)
            await confirmation
        finally:
            self._request = None
            if not confirmation.done():
                confirmation.cancel()
                with suppress(asyncio.CancelledError):
                    await confirmation
        errors.raise_if_any()

    @staticmethod
    def _send(callback: Callable[[], None], name: str, errors: Errors) -> None:
        try:
            callback()
        except BaseException as error:
            errors.add(name, error)
