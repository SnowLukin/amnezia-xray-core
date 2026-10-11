#!/usr/bin/env python3
"""Optional TCP rate wrapper for the adaptive stand's original chaos proxy.

CLI is unchanged: <control_port> <listen:upstream>... [udp:<listen:upstream>].
Set rate:<positive bits/sec> before connecting. Only connections that start
in rate mode use the 4096-byte rate pump; all other connections delegate to
the original pump. Each direction of each rate connection has its own budget.
Changes from rate to pass, or to a different rate, apply to the next read.
UDP continues to use the original whitelist and rejects rate mode.

The original chaos.py has an unconditional asyncio.run(main()) entry point.
Import its unchanged definitions after verifying/removing only that entry
point, patch the two callback bindings, and run the same original main().
No production or testing source needs to change to use this wrapper directly.
"""

import ast
import asyncio
import contextvars
import inspect
from pathlib import Path
import sys
import types


# Installed at <repo>/testing/adaptive/quality/speed/quality_rate_proxy.py.
ORIGINAL_CHAOS_PATH = Path(__file__).resolve().parents[4] / "testing" / "adaptive" / "chaos.py"


def load_original_chaos(path=ORIGINAL_CHAOS_PATH):
    """Load definitions without starting the original server during import."""
    path = Path(path).resolve()
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    entry = tree.body[-1] if tree.body else None
    call = entry.value if isinstance(entry, ast.Expr) else None
    func = call.func if isinstance(call, ast.Call) else None
    main_call = call.args[0] if isinstance(call, ast.Call) and len(call.args) == 1 else None
    expected_entry = (
        isinstance(func, ast.Attribute)
        and isinstance(func.value, ast.Name)
        and func.value.id == "asyncio"
        and func.attr == "run"
        and not call.keywords
        and isinstance(main_call, ast.Call)
        and isinstance(main_call.func, ast.Name)
        and main_call.func.id == "main"
        and not main_call.args
        and not main_call.keywords
    )
    if not expected_entry:
        raise RuntimeError(f"original chaos entry point changed; inspect its API before using this wrapper: {path}")
    tree.body.pop()
    module = types.ModuleType("_quality_speed_original_chaos")
    module.__file__ = str(path)
    module.__package__ = ""
    sys.modules[module.__name__] = module
    exec(compile(tree, str(path), "exec"), module.__dict__)
    expected_pump = ("port", "reader", "writer", "counter", "other_writer", "direction")
    if tuple(inspect.signature(module.pump).parameters) != expected_pump:
        raise RuntimeError("original chaos pump API changed; inspect it before using this wrapper")
    if not inspect.iscoroutinefunction(module.main) or not inspect.iscoroutinefunction(module.handle):
        raise RuntimeError("original chaos main/handle API changed")
    return module


chaos = load_original_chaos()
original_pump = chaos.pump
original_handle = chaos.handle
_connection_initial_mode = contextvars.ContextVar("quality_rate_connection_initial_mode", default=None)


async def handle(port, upstream, reader, writer):
    # Capture the accepted connection's initial mode once. Context propagates
    # to both pump tasks created by the unchanged original handle/gather.
    # Only inspect the name here; let the original handler retain its own
    # argument parsing and connection-accounting order, even for invalid modes.
    initial_name = chaos.MODES.get(port, "pass").partition(":")[0]
    token = _connection_initial_mode.set(initial_name)
    try:
        return await original_handle(port, upstream, reader, writer)
    finally:
        _connection_initial_mode.reset(token)


async def pump(port, reader, writer, counter, other_writer, direction):
    initial_mode = _connection_initial_mode.get()
    if initial_mode is None:
        initial_mode = chaos.MODES.get(port, "pass").partition(":")[0]
    if initial_mode != "rate":
        return await original_pump(port, reader, writer, counter, other_writer, direction)

    loop = asyncio.get_running_loop()
    deadline = loop.time()
    try:
        while True:
            data = await reader.read(4096)
            if not data:
                break
            if direction == 0:
                chaos.count(port, 0, len(data))
            while chaos.mode_of(port)[0] == "pause":
                await asyncio.sleep(0.05)
            name, arg = chaos.mode_of(port)
            if name == "blackhole":
                continue
            if name == "reset":
                chaos.rst(writer)
                chaos.rst(other_writer)
                return
            if name == "delay":
                await asyncio.sleep(arg / 1000)
            if name in ("stall_after", "reset_after"):
                allowed = max(0, arg - counter[0])
                if allowed < len(data):
                    if name == "reset_after":
                        chaos.rst(writer)
                        chaos.rst(other_writer)
                        return
                    data = data[:allowed]
            if data and name == "rate":
                if arg <= 0:
                    raise ValueError("rate mode requires a positive integer bits/sec")
                # Charge this chunk before forwarding. Idle time, a blocked
                # drain, and scheduler delay never accumulate catch-up credit.
                # The mode selected for this read governs this chunk; the next
                # read observes a subsequent control change.
                deadline = max(deadline, loop.time()) + len(data) * 8 / arg
                await asyncio.sleep(max(0.0, deadline - loop.time()))
            counter[0] += len(data)
            if data:
                if direction == 1:
                    chaos.count(port, 1, len(data))
                other_writer.write(data)
                await other_writer.drain()
    except (ConnectionError, asyncio.CancelledError, OSError):
        pass
    finally:
        # These are the original frozen/close and accounting semantics; only
        # initial rate connections replace the read size and rate scheduling.
        name, arg = chaos.mode_of(port)
        frozen = name == "blackhole" or (name == "stall_after" and counter[0] >= arg)
        if frozen:
            try:
                while not other_writer.is_closing():
                    await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                pass
        try:
            other_writer.close()
        except Exception:
            pass


chaos.handle = handle
chaos.pump = pump


if __name__ == "__main__":
    asyncio.run(chaos.main())
