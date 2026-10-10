"""Pure parser checks for native Go server TLS trace records."""

import json
import unittest

from group_scenarios import handshake_window, parse_server_handshakes


def record(ident, stamp, phase, name="", error=""):
    return json.dumps({"bench_tls_handshake": True, "ts_ns": stamp,
                       "id": ident, "remote": "127.0.0.1:40000",
                       "serverName": name, "phase": phase, "error": error})


class HandshakeTraceTest(unittest.TestCase):
    def test_late_sni_and_failed_overlap(self):
        lines = ["unrelated Xray log\n", record(1, 1_000_000_000, "begin"),
                 record(2, 1_500_000_000, "begin"),
                 record(1, 2_000_000_000, "end", "std.example.com"),
                 record(2, 2_500_000_000, "end", "std.example.com", "tls: client closed")]
        intervals, problems = parse_server_handshakes(lines)
        self.assertEqual(problems, [])
        self.assertEqual(handshake_window(intervals, 0.5, 3), {
            "count": 2, "failed": 1, "unfinished": 0, "open_at_window_end": 0,
            "unknown_sni": 0, "peak_by_sni": {"std.example.com": 2}})

    def test_other_sni_and_unfinished_unknown(self):
        intervals, problems = parse_server_handshakes([
            record(1, 1_000_000_000, "begin"), record(2, 1_100_000_000, "begin"),
            record(1, 2_000_000_000, "end", "std.example.com"),
            record(2, 2_000_000_000, "end", "ws.example.com")])
        self.assertEqual(problems, [])
        self.assertEqual(handshake_window(intervals, 0.5, 3)["peak_by_sni"],
                         {"std.example.com": 1, "ws.example.com": 1})
        intervals, problems = parse_server_handshakes([
            record(1, 1_000_000_000, "begin"), record(2, 1_100_000_000, "begin"),
            record(2, 2_000_000_000, "end", "std.example.com")])
        summary = handshake_window(intervals, 0.5, 3)
        self.assertEqual(summary["unfinished"], 1)
        self.assertEqual(summary["unknown_sni"], 1)
        self.assertEqual(summary["peak_by_sni"]["std.example.com"], 2)

    def test_ordering_missing_end_and_malformed_trace(self):
        lines = [record(1, 2_000_000_000, "end"),
                 record(2, 2_000_000_000, "begin"),
                 record(2, 1_000_000_000, "end", "std.example.com"),
                 record(2, 3_000_000_000, "begin"),
                 '{"bench_tls_handshake":true,\n']
        intervals, problems = parse_server_handshakes(lines)
        self.assertEqual(len(intervals), 1)
        self.assertEqual(len(problems), 4)
        self.assertTrue(any("end without begin" in p for p in problems))
        self.assertTrue(any("end before begin" in p for p in problems))
        self.assertTrue(any("duplicate begin" in p for p in problems))
        self.assertTrue(any("malformed trace JSON" in p for p in problems))


if __name__ == "__main__":
    unittest.main()
