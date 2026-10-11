#!/usr/bin/env python3
"""Public libc process CPU clock for a live foreign PID (all its threads).

No encoded clock IDs, live-thread schedstat summation or silent tick fallback.
The small preflight is executable only when its CLI is explicitly invoked.
"""
import argparse
import ctypes
import json
import os
from pathlib import Path
import platform
import select
import subprocess
import sys
import time


class Timespec(ctypes.Structure):
    _fields_ = [("tv_sec", ctypes.c_long), ("tv_nsec", ctypes.c_long)]


_libc = None


def libc():
    global _libc
    if _libc is None:
        if sys.platform != "linux" or ctypes.sizeof(ctypes.c_long) != 8:
            raise RuntimeError("This timespec binding requires native 64-bit Linux")
        candidate = ctypes.CDLL(None, use_errno=True)
        # Missing public API is a hard failure, never a fallback CPU gate.
        candidate.clock_getcpuclockid.argtypes = (ctypes.c_int, ctypes.POINTER(ctypes.c_int))
        candidate.clock_getcpuclockid.restype = ctypes.c_int
        for name in ("clock_gettime", "clock_getres"):
            function = getattr(candidate, name)
            function.argtypes = (ctypes.c_int, ctypes.POINTER(Timespec))
            function.restype = ctypes.c_int
        _libc = candidate
    return _libc


def cpu_clock_sample(pid):
    if not isinstance(pid, int) or pid <= 0:
        raise ValueError("A live foreign process PID is required")
    api = libc()
    clock_id = ctypes.c_int()
    error = api.clock_getcpuclockid(pid, ctypes.byref(clock_id))
    # Unlike clock_gettime, this API returns the positive errno value directly.
    if error != 0:
        raise OSError(error, f"clock_getcpuclockid({pid}): {os.strerror(error)}")
    resolution, measured = Timespec(), Timespec()
    for name, target in (("clock_getres", resolution), ("clock_gettime", measured)):
        ctypes.set_errno(0)
        if getattr(api, name)(clock_id, ctypes.byref(target)) != 0:
            error = ctypes.get_errno()
            raise OSError(error, f"{name}(PID {pid}): {os.strerror(error)}")
        if target.tv_sec < 0 or not 0 <= target.tv_nsec < 1_000_000_000:
            raise RuntimeError(f"Invalid {name} timespec for PID {pid}")
    resolution_ns = resolution.tv_sec * 1_000_000_000 + resolution.tv_nsec
    if resolution_ns <= 0:
        raise RuntimeError("clock_getres returned no usable process clock resolution")
    cpu_ns = measured.tv_sec * 1_000_000_000 + measured.tv_nsec
    return {"pid": pid, "clock_id_returned_by_libc": clock_id.value,
            "cpu_nanoseconds": cpu_ns, "cpu_seconds": cpu_ns / 1e9,
            "timespec_sec": measured.tv_sec, "timespec_nsec": measured.tv_nsec,
            "clock_getres_nanoseconds": resolution_ns,
            "sample_wall_ns": time.time_ns(), "sample_monotonic_ns": time.monotonic_ns(),
            "method": "libc clock_getcpuclockid(PID) + clock_gettime; process total across all threads"}


def preflight(output):
    if not Path("/.dockerenv").exists() or os.environ.get("XRAY_QUALITY_ISOLATED") != "1" or platform.machine() not in ("aarch64", "arm64"):
        raise RuntimeError("Preflight requires the dedicated native arm64 none-network container")
    output.mkdir(parents=True, exist_ok=True)
    # Worker threads terminate before the second snapshot; process CPU must
    # retain their consumed CPU even though no live-thread schedstat remains.
    program = """import sys, threading, time
print('READY', flush=True)
if sys.stdin.readline().strip() != 'GO':
    raise SystemExit(2)
def busy():
    stop = time.perf_counter() + 0.15
    value = 0
    while time.perf_counter() < stop:
        value = (value * 3 + 1) % 1000003
for _ in range(2):
    thread = threading.Thread(target=busy)
    thread.start()
    thread.join()
print('DONE', flush=True)
sys.stdin.readline()
"""
    child = subprocess.Popen([sys.executable, "-u", "-c", program], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    result = {"child_pid": child.pid, "native_machine": platform.machine(),
              "kernel": platform.release(), "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
              "test": "foreign process CPU retains two exited worker threads", "ok": False}
    def await_line(expected):
        ready, _, _ = select.select([child.stdout], [], [], 10)
        if not ready:
            raise TimeoutError(f"controlled CPU child did not report {expected} within 10s")
        line = child.stdout.readline().strip()
        if line != expected:
            raise RuntimeError(f"controlled CPU child reported {line!r}, expected {expected}")
    try:
        await_line("READY")
        result["parallel_legacy_proc_stat_before"] = Path(f"/proc/{child.pid}/stat").read_text()
        result["before"] = cpu_clock_sample(child.pid)
        child.stdin.write("GO\n")
        child.stdin.flush()
        await_line("DONE")
        result["after"] = cpu_clock_sample(child.pid)
        result["parallel_legacy_proc_stat_after"] = Path(f"/proc/{child.pid}/stat").read_text()
        result["delta_nanoseconds"] = result["after"]["cpu_nanoseconds"] - result["before"]["cpu_nanoseconds"]
        result["ok"] = result["delta_nanoseconds"] > 20_000_000
        if not result["ok"]:
            raise RuntimeError("Process CPU clock did not retain a meaningful worker CPU delta")
    except BaseException as error:
        result["error"] = repr(error)
        raise
    finally:
        if child.poll() is None:
            child.terminate()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()
        result["child_stderr"] = child.stderr.read()
        (output / "cpu-clock-preflight.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("verb", choices=("sample", "preflight"))
    parser.add_argument("--pid", type=int)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.verb == "sample":
        if args.pid is None:
            parser.error("sample requires --pid live client PID")
        print(json.dumps(cpu_clock_sample(args.pid), indent=2))
    else:
        if args.output is None:
            parser.error("preflight requires --output")
        print(json.dumps(preflight(args.output), indent=2))


if __name__ == "__main__":
    main()
