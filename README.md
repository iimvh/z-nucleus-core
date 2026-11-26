# z-nucleus core

The public Python package provides `Fiber` and `Handle` for structured cleanup
on one asyncio event loop. Python 3.12 or later is required.

To use the public repository directly, place it at `src/z_nucleus` and set
`PYTHONPATH=src` when running your application. The package uses only the
standard library.

## Shutdown with escalation

Return an `EscalatingUndo` from an effect or component when shutdown has a soft
request, a force request, and an independent way to confirm completion. Each
undo belongs to one resource. Pass a fresh undo for each new resource instance.

```python
from z_nucleus import EscalatingUndo, Fiber


async def run_app(start_process, request_stop):
    async with Fiber.root() as root:
        async def start(_signal):
            process = await start_process()
            return EscalatingUndo.for_process(
                process, lambda: request_stop(process), grace=2.0
            )

        handle = await root.effect("worker", start)
        await handle.dispose()
```

`request_stop` is a synchronous, nonblocking callback for the application's soft
stop protocol. `for_process()` holds the supplied `asyncio.subprocess.Process`
and waits for its exit. The caller must drain any piped output. The adapter
manages one process. Process trees require resource-specific callbacks and
confirmation.

For another resource type, construct
`EscalatingUndo(request_stop, request_force, confirmed, grace=seconds)` with an
`asyncio.Event`. The observer sets it once after verifying that the resource
has stopped. Run the observer and disposal on the same event loop.

After the grace period the undo sends one force request if confirmation is still
pending. `undo.request_escalation()` can request it sooner while that undo is
active. Its return value reports acceptance. Disposal waits for confirmation
even after sending the force request. If confirmation never arrives, disposal
continues to wait. Request errors are reported through the ordinary teardown
error path after confirmation.

When exposing this to a tool, the host should bind the active undo to the call
using runtime context. Letting a caller choose any undo from a registry can
still stop the wrong resource. The core holds the callbacks for the chosen undo
and the subprocess adapter uses its original process object.
