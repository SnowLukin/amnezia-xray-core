import subprocess
import tempfile
import unittest
from unittest import mock

import bench


class FakeProcess:
    def __init__(self, events, stop_error=None):
        self.events = events
        self.stop_error = stop_error

    def start(self, config, log, env):
        self.events.append("start")

    def stop(self):
        self.events.append("stop")
        if self.stop_error is not None:
            raise self.stop_error


class FakeThread:
    def __init__(self, target, events, start_error, launched=True):
        self.target = target
        self.events = events
        self.start_error = start_error
        self.launched = launched

    def start(self):
        self.events.append("sampler launched" if self.launched else "sampler not launched")
        raise self.start_error

    def join(self, timeout):
        self.events.append(("join", timeout))
        if not self.launched:
            raise RuntimeError("cannot join thread before it is started")
        self.target()


class ClientLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.work = tempfile.TemporaryDirectory()
        self.addCleanup(self.work.cleanup)
        self.events = []
        self.process = FakeProcess(self.events)
        self.factory = mock.patch.object(bench, "client_process", return_value=self.process)
        self.factory.start()
        self.addCleanup(self.factory.stop)
        self.closed = mock.patch.object(bench, "wait_closed", side_effect=self.port_closed)
        self.wait_closed = self.closed.start()
        self.addCleanup(self.closed.stop)

    def port_closed(self, port):
        self.events.append("closed")

    def client(self, health=False):
        config = {"log": {"loglevel": "warning"}}
        if health:
            config["healthObservatory"] = {"telemetry": {"events": {"isEnabled": True}, "report": {"isEnabled": True}}}
        return bench.Client("unused-xray", config, self.work.name, "lifecycle")

    def test_readiness_failure_stops_client_before_reraising_original(self):
        for error in (RuntimeError("not ready"), KeyboardInterrupt()):
            with self.subTest(error=type(error).__name__):
                self.events.clear()
                client = self.client()
                with mock.patch.object(bench, "wait_ready", side_effect=error), \
                        mock.patch.object(bench.threading, "Thread") as thread:
                    with self.assertRaises(type(error)) as raised:
                        client.__enter__()
                self.assertIs(raised.exception, error)
                self.assertEqual(self.events, ["start", "stop", "closed"])
                self.wait_closed.assert_called_with(bench.SOCKS_PORT)
                thread.assert_not_called()

    def test_partial_sampler_start_stops_sampler_before_process_cleanup(self):
        for error in (RuntimeError("start failed"), KeyboardInterrupt()):
            with self.subTest(error=type(error).__name__):
                self.events.clear()
                client = self.client(health=True)
                def make_thread(target, daemon):
                    return FakeThread(target, self.events, error)
                with mock.patch.object(bench, "wait_ready"), \
                        mock.patch.object(bench.threading, "Thread", side_effect=make_thread), \
                        mock.patch.object(client, "memory") as memory, \
                        mock.patch.object(client, "drain") as drain:
                    with self.assertRaises(type(error)) as raised:
                        client.__enter__()
                self.assertIs(raised.exception, error)
                self.assertEqual(self.events, ["start", "sampler launched", ("join", 5), "stop", "closed"])
                memory.assert_not_called()
                drain.assert_not_called()

    def test_unstarted_sampler_join_does_not_replace_start_error(self):
        error = RuntimeError("start failed")
        client = self.client()
        def make_thread(target, daemon):
            return FakeThread(target, self.events, error, launched=False)
        with mock.patch.object(bench, "wait_ready"), \
                mock.patch.object(bench.threading, "Thread", side_effect=make_thread):
            with self.assertRaises(RuntimeError) as raised:
                client.__enter__()
        self.assertIs(raised.exception, error)
        self.assertEqual(self.events, ["start", "sampler not launched", ("join", 5), "stop", "closed"])

    def test_partial_sampler_start_cleanup_failure_aborts_invocation(self):
        client = self.client(health=True)
        error = RuntimeError("stop failed")
        self.process.stop_error = error
        def make_thread(target, daemon):
            return FakeThread(target, self.events, KeyboardInterrupt())
        with mock.patch.object(bench, "wait_ready"), \
                mock.patch.object(bench.threading, "Thread", side_effect=make_thread), \
                mock.patch.object(client, "memory") as memory, \
                mock.patch.object(client, "drain") as drain:
            with self.assertRaises(SystemExit) as raised:
                client.__enter__()
        self.assertIs(raised.exception.__cause__, error)
        self.assertEqual(self.events, ["start", "sampler launched", ("join", 5), "stop", "closed"])
        memory.assert_not_called()
        drain.assert_not_called()

    def test_failed_cleanup_aborts_instead_of_reraising_readiness_error(self):
        for failure in ("stop", "closed"):
            with self.subTest(failure=failure):
                self.events.clear()
                error = RuntimeError("cleanup failed")
                self.process.stop_error = error if failure == "stop" else None
                self.wait_closed.side_effect = error if failure == "closed" else self.port_closed
                with mock.patch.object(bench, "wait_ready", side_effect=RuntimeError("not ready")):
                    with self.assertRaises(SystemExit) as raised:
                        self.client().__enter__()
                self.assertIs(raised.exception.__cause__, error)
                self.assertEqual(self.events[:2], ["start", "stop"])
                self.wait_closed.assert_called_with(bench.SOCKS_PORT)

    def test_exit_cleans_up_even_when_drain_or_report_write_fails(self):
        for failure in ("drain", "write"):
            with self.subTest(failure=failure):
                self.events.clear()
                client = self.client(health=True)
                thread = mock.Mock()
                with mock.patch.object(bench, "wait_ready"), \
                        mock.patch.object(bench.threading, "Thread", return_value=thread):
                    client.__enter__()
                error = RuntimeError(failure)
                with mock.patch.object(client, "outbound_bytes", return_value=(0, 0)), \
                        mock.patch.object(client, "drain", side_effect=error if failure == "drain" else None), \
                        mock.patch.object(client, "_check_reports", return_value={}), \
                        mock.patch.object(bench, "REPORT_CHECKS", []), \
                        mock.patch.object(bench.json, "dump", side_effect=error if failure == "write" else None):
                    with self.assertRaises(RuntimeError) as raised:
                        client.__exit__(None, None, None)
                self.assertIs(raised.exception, error)
                self.assertEqual(self.events, ["start", "stop", "closed"])
                thread.join.assert_called_once_with(timeout=5)

    def test_exit_cleanup_failure_aborts_invocation(self):
        client = self.client()
        with mock.patch.object(bench, "wait_ready"), \
                mock.patch.object(bench.threading, "Thread"):
            client.__enter__()
        error = subprocess.TimeoutExpired("owned-client", 5)
        self.process.stop_error = error
        with mock.patch.object(client, "outbound_bytes", return_value=(0, 0)):
            with self.assertRaises(SystemExit) as raised:
                client.__exit__(None, None, None)
        self.assertIs(raised.exception.__cause__, error)
        self.assertEqual(self.events, ["start", "stop", "closed"])


class ProcessCleanupTest(unittest.TestCase):
    def test_local_process_kills_and_reaps_after_terminate_timeout(self):
        process = bench.LocalProcess("unused-xray")
        process.proc = mock.Mock()
        process.proc.wait.side_effect = [subprocess.TimeoutExpired("owned-client", 5), 0]
        process.stop()
        self.assertEqual(process.proc.mock_calls,
                         [mock.call.terminate(), mock.call.wait(timeout=5),
                          mock.call.kill(), mock.call.wait(timeout=5)])

    def test_local_process_propagates_failed_reap(self):
        process = bench.LocalProcess("unused-xray")
        process.proc = mock.Mock()
        error = subprocess.TimeoutExpired("owned-client", 5)
        process.proc.wait.side_effect = [subprocess.TimeoutExpired("owned-client", 5), error]
        with self.assertRaises(subprocess.TimeoutExpired) as raised:
            process.stop()
        self.assertIs(raised.exception, error)
        process.proc.kill.assert_called_once_with()
        self.assertEqual(process.proc.wait.call_count, 2)

    def test_stand_reaps_owned_process_after_kill(self):
        stand = object.__new__(bench.Stand)
        owned = mock.Mock()
        owned.wait.side_effect = [subprocess.TimeoutExpired("owned-server", 5), 0]
        stand.procs = [owned]
        stand.udp_targets = None
        stand.__exit__()
        self.assertEqual(owned.mock_calls,
                         [mock.call.terminate(), mock.call.wait(timeout=5),
                          mock.call.kill(), mock.call.wait(timeout=5)])

    def test_port_still_open_after_timeout_is_an_error(self):
        with mock.patch.object(bench.time, "time", side_effect=[0, 0, 6]), \
                mock.patch.object(bench.time, "sleep"), \
                mock.patch.object(bench.socket, "create_connection") as connect:
            with self.assertRaisesRegex(RuntimeError, "did not close"):
                bench.wait_closed(bench.SOCKS_PORT)
        connect.assert_called_once_with(("127.0.0.1", bench.SOCKS_PORT), timeout=0.2)
        connect.return_value.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
