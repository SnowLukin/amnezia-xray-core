"""Pure parser checks for native Go server TLS trace records."""

import json
import unittest

from group_scenarios import handshake_window, parse_server_handshakes, probe_windows, scheduled_server_names


def record(ident, stamp, phase, name="", error=""):
    return json.dumps({"bench_tls_handshake": True, "ts_ns": stamp,
                       "id": ident, "remote": "127.0.0.1:40000",
                       "serverName": name, "phase": phase, "error": error})


class HandshakeTraceTest(unittest.TestCase):
    def test_probe_overlap_uses_config_when_status_has_no_server_name(self):
        status = {"paths": [{"outbound": "relay-01", "server_name": ""},
                            {"outbound": "relay-02"}]}
        names = scheduled_server_names(status)
        events = [{"ts": 1000, "type": "probe_start", "fields": {"outbound": "relay-01"}},
                  {"ts": 1100, "type": "probe_start", "fields": {"outbound": "relay-02"}},
                  {"ts": 2000, "type": "probe_result", "fields": {"outbound": "relay-01"}},
                  {"ts": 2100, "type": "probe_result", "fields": {"outbound": "relay-02"}}]
        self.assertEqual(probe_windows(events, names)["std.example.com"], 2)
        status["paths"][1]["server_name"] = "actual.example.com"
        self.assertEqual(scheduled_server_names(status)["relay-02"], "actual.example.com")

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
