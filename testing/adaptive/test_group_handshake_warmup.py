"""Pure checks for the server TLS setup boundary."""

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import group_handshake_warmup
import run_groups


def trace(ident, stamp, phase, name="", error=""):
    return json.dumps({"bench_tls_handshake": True, "ts_ns": stamp, "id": ident,
                       "remote": "127.0.0.1:40000", "serverName": name,
                       "phase": phase, "error": error}) + "\n"


class Clock:
    def __init__(self, on_sleep=None):
        self.now = 0.0
        self.on_sleep = on_sleep

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds
        if self.on_sleep:
            self.on_sleep(self.now)


class ServerTLSWarmupTest(unittest.TestCase):
    def test_waits_for_failed_setup_handshake_and_quiet_trace(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "server.log"
            path.write_text(trace(1, 1_000_000_000, "begin")
                            + trace(1, 1_020_000_000, "end", "example.com", "bad certificate")
                            + trace(2, 1_000_000_000, "begin"))
            appended = False

            def finish(now):
                nonlocal appended
                if now >= 0.5 and not appended:
                    with path.open("a") as f:
                        f.write(trace(2, 2_000_000_000, "end", "example.com", "ignored records"))
                    appended = True

            clock = Clock(finish)
            ready = group_handshake_warmup.wait_for_server_tls_idle(
                path, timeout=2, quiet=0.2, poll=0.1, clock=clock.time, sleep=clock.sleep)
            self.assertTrue(appended)
            self.assertEqual(ready["handshakes"], 2)
            self.assertGreaterEqual(ready["waited_s"], 0.7)

    def test_open_handshake_times_out_and_runner_reports_fail(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "server.log"
            path.write_text(trace(1, 1_000_000_000, "begin"))
            clock = Clock()
            with self.assertRaisesRegex(TimeoutError, "open IDs \\[1\\]"):
                group_handshake_warmup.wait_for_server_tls_idle(
                    path, timeout=0.5, quiet=0.2, poll=0.1, clock=clock.time, sleep=clock.sleep)
            called = []
            scenario = SimpleNamespace(name="case", slow=False, run=lambda *args: called.append(True))
            with patch.object(run_groups.bench, "SERVER_BIN", "instrumented"), patch.object(
                    run_groups.group_handshake_warmup, "wait_for_server_tls_idle",
                    side_effect=TimeoutError("setup still open")):
                checks = run_groups.WarmScenario(scenario).run(None, None, temp)
            self.assertFalse(checks[0]["ok"])
            self.assertIn("setup still open", checks[0]["value"])
            self.assertEqual(called, [])


if __name__ == "__main__":
    unittest.main()
