"""Pure protocol and route regressions; no listener or workload is started."""

import base64
import copy
import hashlib
import os
import socket
import struct
import subprocess
import unittest
from unittest import mock

import udp_matrix_ws as websocket
import udp_matrix_topology as topology

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
