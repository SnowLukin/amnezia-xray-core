"""Pure protocol and route regressions; no listener or workload is started."""

import base64
import copy
import hashlib
import os
import socket
import struct
import subprocess
import threading
import unittest
from unittest import mock

import udp_matrix_ws as websocket
import udp_matrix_topology as topology
import udp_matrix_advanced as advanced

import udp_matrix as matrix
from run_udp_matrix import pion_application_route_proof


TRANSACTION = bytes(range(12))
BINDING_SUCCESS = bytes.fromhex(
    "0101000c2112a442000102030405060708090a0b"
    "002000080001a147e112a643"
)


class TLSProxyFixtureTest(unittest.TestCase):
    def test_proxy_loads_original_certificate_with_normalized_work_key(self):
        for kind in ("ws", "xhttp"):
            with self.subTest(kind=kind), \
                 mock.patch.object(topology.bench, "CERT_DIR", "/fixture/certs"), \
                 mock.patch.object(topology.subprocess, "run") as run, \
                 mock.patch.object(topology.ssl, "SSLContext") as ssl_context, \
                 mock.patch.object(topology.socket, "socket"), \
                 mock.patch.object(topology.threading, "Thread"):
                work = "/matrix/work"
                proxy = topology.TLSProxy(kind, work)
                proxy.__enter__()
                key_path = os.path.join(work, f"{kind}-proxy.key")
                run.assert_called_once_with(
                    ["openssl", "pkey", "-in", "/fixture/certs/example.key", "-out", key_path],
                    check=True, capture_output=True, timeout=10)
                ssl_context.return_value.load_cert_chain.assert_called_once_with(
                    "/fixture/certs/example.crt", key_path)

    def test_normalization_failure_aborts_before_loading_or_listening(self):
        for error in (subprocess.CalledProcessError(1, ["openssl", "pkey"]),
                      subprocess.TimeoutExpired(["openssl", "pkey"], 10)):
            with self.subTest(error=type(error).__name__), \
                 mock.patch.object(topology.subprocess, "run", side_effect=error), \
                 mock.patch.object(topology.ssl, "SSLContext") as ssl_context, \
                 mock.patch.object(topology.socket, "socket") as listener, \
                 mock.patch.object(topology.threading, "Thread") as thread:
                proxy = topology.TLSProxy("ws", "/matrix/work")
                with self.assertRaises(type(error)) as raised:
                    proxy.__enter__()
                self.assertIs(raised.exception, error)
                ssl_context.return_value.load_cert_chain.assert_not_called()
                listener.assert_not_called()
                thread.assert_not_called()


class BlockingProxySocket:
    def __init__(self):
        self.reading = threading.Event()
        self.released = threading.Event()
        self.closed = False

    def recv(self, _size):
        self.reading.set()
        if not self.released.wait(2):
            raise AssertionError("Blocked proxy read was not released")
        return b""

    def shutdown(self, how):
        assert how == socket.SHUT_RDWR
        self.released.set()

    def close(self):
        self.closed = True


class TLSProxyLifecycleTest(unittest.TestCase):
    def test_eof_and_receive_or_send_error_shutdown_both_directions(self):
        for mode in ("eof", "recv_error", "send_error"):
            with self.subTest(mode=mode):
                source, target = mock.Mock(), mock.Mock()
                source.recv.return_value = b"" if mode == "eof" else b"payload"
                error = OSError("pipe failed")
                if mode == "recv_error":
                    source.recv.side_effect = error
                elif mode == "send_error":
                    target.sendall.side_effect = error
                row, done = {"up_bytes": 0}, threading.Event()
                topology.TLSProxy._pipe(source, target, row, "up_bytes", done)
                self.assertTrue(done.is_set())
                source.shutdown.assert_called_once_with(socket.SHUT_RDWR)
                target.shutdown.assert_called_once_with(socket.SHUT_RDWR)
                self.assertEqual(row["up_bytes"], 0)
                if mode == "eof":
                    self.assertNotIn("up_bytes_error", row)
                else:
                    self.assertEqual(row["up_bytes_error"], repr(error))

    def test_eof_releases_opposite_blocked_read(self):
        blocked = BlockingProxySocket()
        eof = mock.Mock()
        eof.recv.side_effect = lambda _size: b"" if blocked.reading.wait(1) else self.fail("No read")
        done, row = threading.Event(), {"up_bytes": 0, "down_bytes": 0}
        worker = threading.Thread(target=topology.TLSProxy._pipe,
                                  args=(blocked, eof, row, "up_bytes", done))
        worker.start()
        topology.TLSProxy._pipe(eof, blocked, row, "down_bytes", done)
        worker.join(timeout=1)
        self.assertFalse(worker.is_alive())
        self.assertTrue(blocked.released.is_set())

    def test_explicit_stop_releases_both_blocked_reads(self):
        proxy = topology.TLSProxy("ws", "/matrix/work")
        first, second = BlockingProxySocket(), BlockingProxySocket()
        for sock in (first, second):
            proxy._register(sock)
        done, row = threading.Event(), {"up_bytes": 0, "down_bytes": 0}
        def forward(source, target, counter):
            try:
                proxy._pipe(source, target, row, counter, done)
            finally:
                proxy._release(source)
        proxy.threads = [threading.Thread(target=forward, args=(first, second, "up_bytes")),
                         threading.Thread(target=forward, args=(second, first, "down_bytes"))]
        proxy.listener, proxy.thread = mock.Mock(), mock.Mock()
        for worker in proxy.threads:
            worker.start()
        self.assertTrue(first.reading.wait(1))
        self.assertTrue(second.reading.wait(1))
        with mock.patch.object(topology, "save_json"):
            proxy.__exit__()
        self.assertTrue(proxy.stop.is_set())
        self.assertTrue(all(not worker.is_alive() for worker in proxy.threads))
        self.assertTrue(first.closed and second.closed)
        self.assertEqual(proxy.sockets, set())
        proxy.listener.close.assert_called_once()

    def test_registration_after_stop_closes_new_socket(self):
        proxy = topology.TLSProxy("ws", "/matrix/work")
        proxy.stop.set()
        sock = mock.Mock()
        with self.assertRaisesRegex(OSError, "TLS proxy stopped"):
            proxy._register(sock)
        sock.shutdown.assert_called_once_with(socket.SHUT_RDWR)
        sock.close.assert_called_once()
        self.assertEqual(proxy.sockets, set())

    def test_wrap_replaces_registered_socket_before_handshake_and_clears_timeout(self):
        for kind in ("ws", "xhttp"):
            with self.subTest(kind=kind):
                proxy = topology.TLSProxy(kind, "/matrix/work")
                for kwargs in ({"server_side": True}, {"server_hostname": "example.test"}):
                    raw, wrapped, context = mock.Mock(), mock.Mock(), mock.Mock()
                    proxy._register(raw)
                    def wrap(sock, **options):
                        self.assertTrue(proxy.socket_lock.locked())
                        self.assertIn(raw, proxy.sockets)
                        self.assertIs(sock, raw)
                        self.assertEqual(options, dict(kwargs, do_handshake_on_connect=False))
                        return wrapped
                    def handshake():
                        self.assertFalse(proxy.socket_lock.locked())
                        self.assertNotIn(raw, proxy.sockets)
                        self.assertIn(wrapped, proxy.sockets)
                    context.wrap_socket.side_effect = wrap
                    wrapped.do_handshake.side_effect = handshake
                    self.assertIs(proxy._wrap_socket(context, raw, **kwargs), wrapped)
                    self.assertEqual(wrapped.settimeout.call_args_list, [mock.call(5), mock.call(None)])
                    wrapped.do_handshake.assert_called_once()
                    proxy._release(wrapped)
                self.assertEqual(proxy.sockets, set())

    def test_failed_handshake_releases_wrapped_socket(self):
        proxy = topology.TLSProxy("ws", "/matrix/work")
        raw, wrapped, context = mock.Mock(), mock.Mock(), mock.Mock()
        proxy._register(raw)
        context.wrap_socket.return_value = wrapped
        wrapped.do_handshake.side_effect = socket.timeout("handshake deadline")
        with self.assertRaises(socket.timeout):
            proxy._wrap_socket(context, raw, server_side=True)
        wrapped.shutdown.assert_called_once_with(socket.SHUT_RDWR)
        wrapped.close.assert_called_once()
        self.assertEqual(proxy.sockets, set())


class STUNBindingOracleTest(unittest.TestCase):
    def test_known_ipv4_source_vector(self):
        self.assertEqual(matrix.parse_stun_binding_success(BINDING_SUCCESS, TRANSACTION),
                         ("192.0.2.1", 32853))
        self.assertEqual(matrix.stun_binding_success(TRANSACTION, ("192.0.2.1", 32853)),
                         BINDING_SUCCESS)

    def test_success_without_xor_mapped_address_is_rejected(self):
        response = bytes.fromhex("010100002112a442") + TRANSACTION
        with self.assertRaises(ValueError):
            matrix.parse_stun_binding_success(response, TRANSACTION)

    def test_invalid_headers_and_attributes_are_rejected(self):
        invalid = {}
        for name, offset, replacement in (
                ("declared_message_length", 2, b"\x00\x08"),
                ("unaligned_message_length", 2, b"\x00\x0b"),
                ("attribute_overrun", 22, b"\x00\x09"),
                ("attribute_size", 22, b"\x00\x07"),
                ("unsupported_family", 25, b"\x02"),
                ("reserved_byte", 24, b"\x01"),
                ("transaction_id", 8, b"\xff"),
                ("response_type", 0, b"\x00\x01"),
                ("magic_cookie", 4, b"\xff")):
            response = bytearray(BINDING_SUCCESS)
            response[offset:offset + len(replacement)] = replacement
            invalid[name] = bytes(response)
        invalid["truncated_header"] = BINDING_SUCCESS[:19]
        invalid["truncated_attribute"] = BINDING_SUCCESS[:-1]
        invalid["trailing_bytes"] = BINDING_SUCCESS + b"\x00"
        invalid["duplicate_mapped_address"] = (
            BINDING_SUCCESS[:2] + struct.pack("!H", 24) + BINDING_SUCCESS[4:] + BINDING_SUCCESS[20:])
        for name, response in invalid.items():
            with self.subTest(name=name), self.assertRaises(ValueError):
                matrix.parse_stun_binding_success(response, TRANSACTION)


class PionRouteOracleTest(unittest.TestCase):
    def result(self, mode):
        pair = {"local_type": mode, "remote_type": mode,
                "remote_address": "198.18.0.1", "remote_port": 34014}
        destination = "127.0.0.1:34012" if mode == "relay" else "198.18.0.1:34014"
        events = [{"at_unix_ns": 150, "dtls_application_data": True,
                   "direction": direction, "destination": destination} for direction in ("send", "recv")]
        return {"transfer_started_unix_ns": 100, "transfer_ended_unix_ns": 200,
                "selected_pair": pair, "selected_pair_b": pair,
                "turn_destination": "127.0.0.1:34012",
                "application_routes_a": events, "application_routes_b": copy.deepcopy(events)}

    def test_complete_expected_route_is_accepted(self):
        for mode in ("host", "relay"):
            for peer in ("a", "b"):
                with self.subTest(mode=mode, peer=peer):
                    self.assertTrue(pion_application_route_proof(self.result(mode), mode, peer))

    def test_one_foreign_destination_cannot_hide_behind_valid_traffic(self):
        for mode in ("host", "relay"):
            for peer in ("a", "b"):
                for direction in ("send", "recv"):
                    data = self.result(mode)
                    data[f"application_routes_{peer}"].append(
                        {"at_unix_ns": 175, "dtls_application_data": True,
                         "direction": direction, "destination": "192.0.2.100:55555"})
                    with self.subTest(mode=mode, peer=peer, direction=direction):
                        self.assertFalse(pion_application_route_proof(data, mode, peer))

    def test_missing_direction_or_outside_window_is_rejected(self):
        for mode in ("host", "relay"):
            data = self.result(mode)
            data["application_routes_a"] = data["application_routes_a"][:1]
            self.assertFalse(pion_application_route_proof(data, mode, "a"))
            data = self.result(mode)
            data["application_routes_a"][0]["at_unix_ns"] = 99
            self.assertFalse(pion_application_route_proof(data, mode, "a"))


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def monotonic_ns(self):
        return int(self.now * 1_000_000_000)

    def sleep(self, seconds):
        self.now += seconds


class AdvancedFixtureTest(unittest.TestCase):
    def client(self):
        client = mock.MagicMock()
        client.__enter__.return_value = client
        return client

    def test_reconnect_sends_under_fault_restores_and_keeps_same_association(self):
        client, channel, ledger = self.client(), self.client(), mock.Mock(flows=[])
        modes, observed = [], []
        def echo(current, host):
            self.assertIs(current, channel)
            observed.append(modes[-1] if modes else "pass")
            if observed[-1] == "reset_after:1":
                raise socket.timeout("reset packet")
            return True
        with mock.patch.object(advanced.bench, "Client", return_value=client), \
             mock.patch.object(advanced, "SocksUDP", return_value=channel) as association, \
             mock.patch.object(advanced, "Ledger", return_value=ledger), \
             mock.patch.object(advanced.bench, "chaos_stats", side_effect=[{200: [5]}, {200: [7]}]), \
             mock.patch.object(advanced.bench, "address_port", return_value=200), \
             mock.patch.object(advanced.bench, "chaos", side_effect=lambda _port, mode: modes.append(mode)), \
             mock.patch.object(advanced, "_echo", side_effect=echo), \
             mock.patch.object(advanced, "_source_access", return_value={}), \
             mock.patch.object(advanced, "_routed_starts", return_value=[{}, {}]) as starts, \
             mock.patch.object(advanced, "reconcile_flows", return_value={"ok": False, "actual": {
                 "udp_flows_unanswered": 0}}) as reconcile, \
             mock.patch.object(advanced, "save_json"):
            result = advanced.reconnect_flow(None, "fake-xray", "/matrix/work")
        self.assertEqual(observed, ["pass", "reset_after:1", "pass"])
        self.assertEqual(modes, ["reset_after:1", "pass"])
        association.assert_called_once()
        reconcile.assert_called_once_with(ledger.flows, client.final_report, "matrix-reality",
                                          routed_starts=starts.return_value)
        self.assertTrue(result["first_ok"] and result["recovered"])
        self.assertEqual(result["physical_connections"], 2)
        self.assertFalse(result["ok"])
        self.assertIn("reset packet", result["fault"]["error"])
        fault = result["fault"]
        self.assertLessEqual(fault["armed_ns"], fault["send_started_ns"])
        self.assertLessEqual(fault["attempt_ended_ns"], fault["restore_started_ns"])
        self.assertLessEqual(fault["restore_started_ns"], fault["restored_ns"])

    def test_unexpected_fault_packet_failure_still_restores_pass(self):
        with mock.patch.object(advanced.bench, "Client", return_value=self.client()), \
             mock.patch.object(advanced, "SocksUDP", return_value=self.client()), \
             mock.patch.object(advanced, "Ledger"), \
             mock.patch.object(advanced.bench, "chaos_stats", return_value={200: [5]}), \
             mock.patch.object(advanced.bench, "address_port", return_value=200), \
             mock.patch.object(advanced.bench, "chaos") as chaos, \
             mock.patch.object(advanced, "_echo", side_effect=[True, RuntimeError("unexpected")]):
            with self.assertRaisesRegex(RuntimeError, "unexpected"):
                advanced.reconnect_flow(None, "fake-xray", "/matrix/work")
        self.assertEqual(chaos.call_args_list, [mock.call("200", "reset_after:1"), mock.call("200", "pass")])

    def cross_window(self, missing=None):
        tag, reserve = "matrix-reality", "matrix-reality-reserve"
        client, channel, clock = self.client(), self.client(), FakeClock()
        initial = {"balancers": [{"balancer": "auto", "active": tag}]}
        selected = {"balancers": [{"balancer": "auto", "active": reserve}]}
        client.status.side_effect = [initial] + [initial if missing == "status" else selected] * 110
        client.events = []
        def drain():
            if client.drain.call_count > 1 and not client.events and missing != "decision":
                client.events.append({"type": "switch", "fields": {"reason": "pin", "from": tag, "to": reserve}})
        client.drain.side_effect = drain
        first = {"report_reason": "interval", "report_index": 1, "paths": {tag: {"udp_flows": 0}}}
        client.take_report.side_effect = lambda: None if missing == "report" or clock.now < 1 else first
        client.final_report = {"report_reason": "stop", "report_index": 2, "paths": {tag: {"udp_flows": 1}}}
        snapshot = {"report_reason": "snapshot", "report_index": 2}
        client.api.side_effect = lambda *args: {"ok": missing != "accepted"} if args[0] == "pin" else snapshot
        with mock.patch.object(advanced.bench, "Client", return_value=client) as constructor, \
             mock.patch.object(advanced.bench, "client_config", wraps=advanced.bench.client_config) as config, \
             mock.patch.object(advanced, "SocksUDP", return_value=channel) as association, \
             mock.patch.object(advanced, "Ledger"), \
             mock.patch.object(advanced, "_echo", return_value=True) as echo, \
             mock.patch.object(advanced, "_source_access", return_value={}), \
             mock.patch.object(advanced, "save_json"), \
             mock.patch.object(advanced.time, "monotonic", side_effect=clock.monotonic), \
             mock.patch.object(advanced.time, "sleep", side_effect=clock.sleep):
            result = advanced.cross_report_window(None, "fake-xray", "/matrix/work")
        config.assert_called_once_with([(tag, "reality", 0), (reserve, "reality", 1)], {tag: 1, reserve: 2},
                                       health={"report": {"quietShare": 1, "interval": "1s"}}, loglevel="info")
        association.assert_called_once()
        self.assertEqual(echo.call_args_list, [mock.call(channel, "127.0.0.1")] * 2)
        self.assertIn(mock.call("pin", "auto", reserve), client.api.call_args_list)
        return result, client

    def test_cross_window_reserve_decision_interval_and_old_association(self):
        result, client = self.cross_window()
        self.assertTrue(result["ok"])
        self.assertEqual((result["first_flows"], result["final_flows"]), (0, 1))
        self.assertEqual(result["snapshot"]["report_index"], 2)
        self.assertGreater(client.take_report.call_count, 1)

    def test_cross_window_missing_report_or_real_decision_remains_failure(self):
        for missing in ("report", "decision", "accepted", "status"):
            with self.subTest(missing=missing):
                result, client = self.cross_window(missing)
                self.assertFalse(result["ok"])
                self.assertIsNone(result["first_report"])
                self.assertIsNone(result["first_flows"])
                if missing != "report":
                    client.take_report.assert_not_called()

    def test_source_access_preserves_all_raw_lines_and_per_source_counts(self):
        lines = "time udp:127.0.0.1:4000 accepted udp:dest [socks -> old]\n" * 2
        lines += "time udp:127.0.0.1:4001 accepted udp:dest [socks -> reserve]\n"
        flows = [mock.Mock(source="127.0.0.1:4000"), mock.Mock(source="127.0.0.1:4001")]
        with mock.patch.object(advanced.os.path, "exists", return_value=True), \
             mock.patch("builtins.open", mock.mock_open(read_data=lines)):
            evidence = advanced._source_access("/matrix/access.log", flows)
        self.assertEqual(evidence["by_source"]["127.0.0.1:4000"],
                         {"count": 2, "lines": lines.splitlines()[:2]})
        self.assertEqual(evidence["by_source"]["127.0.0.1:4001"]["count"], 1)


class FollowUpOracleTest(unittest.TestCase):
    def flow(self, name, answered=False, controlled=False, port=40011):
        flow = matrix.Flow(name, "tested", "127.0.0.1:4000", f"127.0.0.1:{port}", 1,
                           closed_ns=4, controlled_no_reply=controlled)
        flow.sent = [{"bytes": 12, "when_ns": 2}]
        flow.received = [{"bytes": 12, "when_ns": 3}] if answered else []
        return flow

    def report(self, count, unanswered, writes=None, reads=0):
        return {"paths": {"tested": {"udp_flows": count, "udp_flows_unanswered": unanswered,
                 "udp_ports": {"other": {"flows": count, "unanswered": unanswered}},
                 "udp_writes": count if writes is None else writes, "udp_reads": reads,
                 "udp_bytes_up": 12 * (count if writes is None else writes), "udp_bytes_down": 12 * reads}}}

    def test_unread_application_socket_is_not_controlled_no_reply(self):
        flows = [self.flow("blackhole", controlled=True), self.flow("pion-register-a"),
                 self.flow("pion-register-b"), self.flow("answered", answered=True)]
        result = matrix.reconcile_flows(flows, self.report(4, 2, reads=1), "tested")
        self.assertTrue(result["ok"], result["issues"])
        self.assertEqual(result["unanswered_total_bounds"], [1, 3])
        self.assertFalse(flows[1].controlled_no_reply)

    def test_bounds_counts_buckets_numeric_fields_and_sums_fail_closed(self):
        flows = [self.flow("blackhole", controlled=True), self.flow("unread")]
        base = self.report(2, 1)
        variants = []
        for value in (0, 3):
            report = copy.deepcopy(base)
            report["paths"]["tested"]["udp_flows_unanswered"] = value
            report["paths"]["tested"]["udp_ports"]["other"]["unanswered"] = value
            variants.append(report)
        for name in ("udp_flows", "udp_flows_unanswered", "udp_writes", "udp_reads",
                     "udp_bytes_up", "udp_bytes_down"):
            for value in (None, -1, True, 1.0, "2"):
                report = copy.deepcopy(base)
                report["paths"]["tested"][name] = value
                variants.append(report)
            report = copy.deepcopy(base)
            del report["paths"]["tested"][name]
            variants.append(report)
        report = copy.deepcopy(base)
        report["paths"]["tested"]["udp_ports"] = {"dns": {"flows": 2, "unanswered": 1}}
        variants.append(report)
        report = copy.deepcopy(base)
        report["paths"]["tested"]["udp_ports"]["other"]["unanswered"] = 2
        variants.append(report)
        report = copy.deepcopy(base)
        del report["paths"]["tested"]["udp_ports"]["other"]["unanswered"]
        variants.append(report)
        for name in ("flows", "unanswered"):
            for value in (None, -1, True, 1.0, "2"):
                report = copy.deepcopy(base)
                report["paths"]["tested"]["udp_ports"]["other"][name] = value
                variants.append(report)
        variants.append(self.report(2, 1, writes=1))
        for i, report in enumerate(variants):
            with self.subTest(variant=i):
                self.assertFalse(matrix.reconcile_flows(flows, report, "tested")["ok"])

    def test_isolated_controls_detect_compensation_hidden_by_aggregate_bounds(self):
        blackhole, unread = self.flow("blackhole", controlled=True), self.flow("unread")
        self.assertTrue(matrix.reconcile_flows([blackhole, unread], self.report(2, 1), "tested")["ok"])
        self.assertFalse(matrix.reconcile_flows([blackhole], self.report(1, 0), "tested")["ok"])
        answered = self.flow("answered", answered=True)
        self.assertFalse(matrix.reconcile_flows([answered], self.report(1, 1, reads=1), "tested")["ok"])
        self.assertTrue(matrix.reconcile_flows([blackhole], self.report(1, 1), "tested")["ok"])
        self.assertTrue(matrix.reconcile_flows([answered], self.report(1, 0, reads=1), "tested")["ok"])

    def test_mux_requires_exact_udp_source_target_tag_routed_starts(self):
        flow = self.flow("mux", answered=True, port=matrix.HELPER_PORTS["echo"])
        flow.sent *= 3
        flow.received *= 2
        route = f"from udp:{flow.source} accepted udp:{flow.first_destination} [socks -> tested]"
        raw = ["time1 " + route, "time2 " + route,
               route.replace("udp:", "tcp:"), route.replace("tested", "foreign"),
               route.replace(flow.source, "127.0.0.1:4001"),
               route.replace(flow.first_destination, "127.0.0.1:55555")]
        access = {"by_source": {flow.source: {"lines": raw}}}
        starts = advanced._routed_starts(access, [flow], "tested")
        self.assertEqual(len(starts), 2)
        result = matrix.reconcile_flows([flow], self.report(2, 0, writes=3, reads=2), "tested", routed_starts=starts)
        self.assertTrue(result["ok"], result["issues"])
        self.assertEqual((result["association_count"], result["expected_flows"]), (1, 2))
        self.assertFalse(matrix.reconcile_flows([flow], self.report(1, 0, writes=3, reads=2),
                                               "tested", routed_starts=starts)["ok"])
        self.assertFalse(matrix.reconcile_flows([flow], self.report(2, 0, writes=3, reads=2),
                                               "tested", routed_starts=[])["ok"])

    def dns(self, responses):
        channel, clock = mock.Mock(last_source="1.1.1.1:53"), FakeClock()
        def receive(timeout):
            if not responses:
                clock.sleep(timeout)
                raise socket.timeout()
            delay, response = responses.pop(0)
            clock.sleep(delay)
            return response
        channel.receive.side_effect = receive
        with mock.patch.object(matrix.time, "monotonic", side_effect=clock.monotonic):
            result = matrix.dns_exchange(channel, "example.com", b"\x00\x02", 0, prior_ids={b"\x00\x01"})
        return result, channel, clock

    def test_dns_stale_id_then_valid_response_preserves_raw_and_deadline(self):
        header = b"\x81\x80\x00\x01\x00\x01\x00\x00\x00\x00"
        stale, valid = b"\x00\x01" + header, b"\x00\x02" + header
        result, channel, clock = self.dns([(1, stale), (1, valid)])
        self.assertTrue(result["valid"])
        self.assertEqual([r["classification"] for r in result["responses"]], ["stale_id", "valid"])
        self.assertEqual(result["responses"][0]["hex"], stale.hex())
        self.assertEqual(result["responses"][1]["sha256"], hashlib.sha256(valid).hexdigest())
        self.assertEqual(channel.receive.call_args_list, [mock.call(timeout=4), mock.call(timeout=3)])

    def test_dns_invalid_is_not_timeout_and_late_response_cannot_pass(self):
        invalid = b"\x00\x02\x01\x00" + bytes(8)
        result, channel, clock = self.dns([(1, invalid)])
        self.assertFalse(result["valid"])
        self.assertEqual(result["status"], "invalid_response")
        self.assertTrue(result["deadline_expired"])
        self.assertEqual(clock.now, 4)
        valid = b"\x00\x02\x81\x80\x00\x01\x00\x01" + bytes(4)
        result, channel, clock = self.dns([(4.1, valid)])
        self.assertFalse(result["valid"])
        self.assertEqual(result["responses"][0]["classification"], "late")
        self.assertEqual(channel.receive.call_count, 1)

    def test_dns_transport_error_is_preserved_and_no_reply_marker_is_scoped(self):
        channel = mock.Mock()
        channel.send.side_effect = OSError("send failed")
        result = matrix.dns_exchange(channel, "example.com", b"\x00\x01", 0)
        self.assertEqual(result["status"], "transport_error")
        self.assertIn("send failed", result["error"])
        self.assertFalse(result["deadline_expired"])
        channel.receive.assert_not_called()
        controlled = matrix.SocksUDP(mock.Mock(), "tested", "control", controlled_no_reply=True)
        for host, port in (("1.1.1.1", matrix.HELPER_PORTS["blackhole"]),
                           ("127.0.0.1", matrix.HELPER_PORTS["echo"])):
            with self.assertRaisesRegex(ValueError, "controlled blackhole"):
                controlled.send(host, port, b"payload")


class MatrixRunModeTest(unittest.TestCase):
    def test_default_full_contract_is_preserved(self):
        settings = matrix.run_settings()
        self.assertEqual((settings["mode"], settings["udp_seconds"], settings["udp_packets"],
                          settings["ws_ping_seconds"], settings["ws_idle_seconds"]),
                         ("full", 1200, 60000, 600, 600))
        self.assertEqual(sum(settings["removed_soak_seconds_per_path"].values()), 0)

    def test_functional_contract_names_removed_soak(self):
        settings = matrix.run_settings(True)
        self.assertEqual((settings["mode"], settings["udp_seconds"], settings["udp_rate_pps"],
                          settings["udp_packets"], settings["ws_ping_seconds"], settings["ws_idle_seconds"]),
                         ("functional", 60, 50, 3000, 60, 30))
        self.assertEqual(settings["timed_seconds_per_path"], 150)
        self.assertEqual(sum(settings["removed_soak_seconds_per_path"].values()) * 5, 11250)
        self.assertFalse(settings["performance_acceptance"])

    def stream_result(self, duration, required_duration):
        clock = FakeClock()
        channel = mock.MagicMock()
        channel.__enter__.return_value = channel
        def receive(timeout=None):
            if timeout:
                clock.sleep(timeout)
                raise socket.timeout()
            raise BlockingIOError()
        channel.receive.side_effect = receive
        with mock.patch.object(matrix, "SocksUDP", return_value=channel), \
             mock.patch.object(matrix.time, "monotonic", side_effect=clock.monotonic), \
             mock.patch.object(matrix.time, "monotonic_ns", side_effect=clock.monotonic_ns), \
             mock.patch.object(matrix.time, "sleep", side_effect=clock.sleep):
            result = matrix.stream_case(mock.Mock(), "route", "127.0.0.1", duration=duration,
                                        required_duration=required_duration)
        return result, channel.send.call_count

    def test_functional_stream_sends_all_3000_packets(self):
        result, sent = self.stream_result(60, 60)
        self.assertEqual((result["required_packets"], result["sent"], sent), (3000, 3000, 3000))
        self.assertTrue(result["meets_required_duration"])

    def test_shorter_stream_cannot_pass_required_window(self):
        result, _ = self.stream_result(30, 60)
        self.assertEqual(result["required_packets"], 3000)
        self.assertFalse(result["meets_required_duration"])

    def websocket_result(self, duration, required_duration, ping):
        clock = FakeClock()
        sock = mock.MagicMock()
        sock.__enter__.return_value = sock
        key = base64.b64encode(b"x" * 16)
        accept = base64.b64encode(hashlib.sha1(key + websocket.GUID).digest())
        header = b"HTTP/1.1 101 Switching Protocols\r\nSec-WebSocket-Accept: " + accept + b"\r\n\r\n"
        frames = []
        def send_frame(_sock, opcode, payload, _masked):
            frames.append((opcode, payload))
        def read_frame(_sock):
            opcode, payload = frames[-1]
            return (10, payload) if opcode == 9 else (8, struct.pack("!H", 1000))
        def select_ready(_read, _write, _error, timeout):
            clock.sleep(timeout)
            return [], [], []
        with mock.patch.object(websocket, "socks_tcp", return_value=sock), \
             mock.patch.object(websocket, "read_header", return_value=header), \
             mock.patch.object(websocket, "send_frame", side_effect=send_frame), \
             mock.patch.object(websocket, "read_frame", side_effect=read_frame), \
             mock.patch.object(websocket.os, "urandom", side_effect=lambda size: b"x" * size), \
             mock.patch.object(websocket.select, "select", side_effect=select_ready), \
             mock.patch.object(websocket.time, "monotonic", side_effect=clock.monotonic), \
             mock.patch.object(websocket.time, "monotonic_ns", side_effect=clock.monotonic_ns):
            return websocket.websocket_case("127.0.0.1", duration=duration, ping=ping,
                                             required_duration=required_duration)

    def test_functional_ping_keeps_three_cycles_and_close_boundary(self):
        result = self.websocket_result(60, 60, True)
        self.assertEqual((result["required_pongs"], result["pongs"], result["close_code"]), (3, 3, 1000))
        self.assertTrue(result["final_pong"])
        self.assertTrue(result["ok"])

    def test_functional_idle_keeps_final_liveness_and_close_boundary(self):
        result = self.websocket_result(30, 30, False)
        self.assertEqual((result["pongs"], result["close_code"]), (0, 1000))
        self.assertTrue(result["final_pong"])
        self.assertTrue(result["ok"])

    def test_shorter_websocket_cannot_pass_required_window(self):
        self.assertFalse(self.websocket_result(30, 60, True)["ok"])


if __name__ == "__main__":
    unittest.main()
