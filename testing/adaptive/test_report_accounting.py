import re
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import bench
import stage1_helpers as h


def report(index, events, **counters):
    result = dict.fromkeys((
        "switches", "exhausted_count", "group_down_count", "path_down_count",
        "path_suspect_count", "path_rescued_count", "pending_closed_count",
        "idle_closed_count", "probes", "probes_failed", "probe_bytes",
        "host_probe_requests", "host_probes", "return_attempts", "return_failures",
        "pinned_down_count", "network_changes", "network_known", "network_down_count",
        "provider_fetches", "provider_fetch_failures", "provider_retry_after",
        "provider_applied", "provider_rejected",
        "provider_expired", "provider_renewals", "provider_auth_failed",
        "provider_base_changed", "pins", "releases", "memory_low_count", "outbound_sleeps",
        "candidate_probes", "candidate_probes_failed", "probe_only_promoted_count", "fresh_path_down_count",
        "udp_probe_count", "udp_probe_ok_count", "udp_probe_lossy_count", "udp_probe_dead_count",
        "udp_probe_second_destination_count", "udp_suspect_count", "udp_suspect_confirmed_count",
    ), 0)
    result.update(events=events, report_index=index, report_reason="final",
                  switch_reasons={}, switch_history=[], provider_triggers={})
    result.update(counters)
    return result


class ReportAccountingTest(unittest.TestCase):
    def check_reports(self, missing=False, dropped=0, empty=False):
        client = object.__new__(bench.Client)
        client.process = SimpleNamespace(clock_offset=0)
        client.has_events = client.has_report = True
        client.dropped = 0
        client.events = [] if empty else [{"seq": 1, "ts": 1000, "type": "pin"}]
        client.reports = [] if empty else [report(1, 1, pins=1)]
        final = report(1, 0) if empty else report(2, 2, probes=1, probes_failed=1,
                                                probe_bytes=64, host_probes=1, releases=1)
        drained = [] if empty else [
            {"seq": 2, "ts": 2000, "type": "probe_result",
             "fields": {"is_ok": False, "bytes": 64, "reason": "host"}},
            {"seq": 3, "ts": 3000, "type": "release"},
        ]
        if missing:
            drained = drained[1:]
        snapshot = dict(final, report_reason="snapshot")
        client.api = mock.Mock(side_effect=[snapshot, final, None,
                                            {"events": drained, "dropped": dropped}])
        result = client._check_reports()
        self.assertEqual(client.api.call_args_list, [
            mock.call("report"), mock.call("report", "final"),
            mock.call("report", "final"), mock.call("events"),
        ])
        return result

    def test_complete_consistent_sequence_windows_pass(self):
        self.assertTrue(self.check_reports()["ok"])

    def test_missing_sequence_fails_instead_of_skipping_accounting(self):
        result = self.check_reports(missing=True)
        self.assertFalse(result["ok"])
        self.assertIn("report 2 missing 1 events", result["value"])

    def test_dropped_events_from_final_drain_fail_even_with_complete_windows(self):
        result = self.check_reports(dropped=1)
        self.assertFalse(result["ok"])
        self.assertIn("1 dropped events", result["value"])

    def test_zero_event_report_with_no_drops_passes(self):
        self.assertTrue(self.check_reports(empty=True)["ok"])



class NetworkStrengthAccountingTest(unittest.TestCase):
    def history(self, network_fields=None):
        fields = [
            ("switch", {"to": "a1"}),
            ("path_state", {"outbound": "a1", "state": "suspect", "is_strong": True}),
            ("path_state", {"outbound": "a1", "state": "down", "reason": "traffic", "quarantine_level": 1}),
            ("switch", {"to": "b1"}),
        ]
        if network_fields is not None:
            fields.append(("network_change", network_fields))
        fields.append(("path_state", {"outbound": "a1", "state": "down", "reason": "traffic", "quarantine_level": 1}))
        return [{"seq": seq, "ts": seq * 1000, "type": typ, "fields": values}
                for seq, (typ, values) in enumerate(fields, 1)]

    def rows(self, history, window, strong_count, index=1, weak_count=0):
        counters = {key: 0 for key in h.GLOBAL_FIELDS}
        path = {key: 0 for key in h.PATH_FIELDS}
        path.update(downs_strong=strong_count, downs_weak=weak_count, probe_steps=[])
        counters.update(events=len(window), report_index=index, window_ms=10000,
                        session_ms=10000 * index, is_quiet_sample=False, paths={"a1": path})
        context = {"strategy": {"confirmWithProbe": "never"}, "faults": [],
                   "penalty_seconds": 300, "present_actions": [], "tcp_only": True}
        return h.window_checks(counters, history, window, context, (0, 0), (10, 10), {"a1"})

    def test_accepted_network_change_counts_same_key_again(self):
        for fields in ({"generation": 1}, {"generation": 1, "is_ignored": False}):
            with self.subTest(network=fields):
                history = self.history(fields)
                self.assertEqual(h.strength_decisions(history, []), [
                    {"seq": 3, "outbound": "a1", "strong": True},
                    {"seq": 6, "outbound": "a1", "strong": True},
                ])
                self.assertTrue(all(row["ok"] for row in self.rows(history, history, 2)))
                wrong = next(row for row in self.rows(history, history, 1) if row["check"] == "a1.downs_strong")
                self.assertFalse(wrong["ok"])
                self.assertEqual(wrong["limit"], 2)

    def test_ignored_change_and_same_generation_keep_deduplication(self):
        for fields in (None, {"generation": 1, "is_ignored": True}):
            with self.subTest(network=fields):
                history = self.history(fields)
                self.assertEqual(h.strength_decisions(history, []), [
                    {"seq": 3, "outbound": "a1", "strong": True},
                ])
                self.assertTrue(all(row["ok"] for row in self.rows(history, history, 1)))
                wrong = next(row for row in self.rows(history, history, 2) if row["check"] == "a1.downs_strong")
                self.assertFalse(wrong["ok"])
                self.assertEqual(wrong["limit"], 1)

    def test_two_windows_count_own_down_with_shared_history(self):
        history = self.history({"generation": 1})
        for index, prefix, window in ((1, history[:4], history[:4]), (2, history, history[4:])):
            with self.subTest(window=index):
                self.assertTrue(all(row["ok"] for row in self.rows(prefix, window, 1, index)))
                for wrong_count in (0, 2):
                    wrong = next(row for row in self.rows(prefix, window, wrong_count, index)
                                 if row["check"] == "a1.downs_strong")
                    self.assertFalse(wrong["ok"])
                    self.assertEqual(wrong["limit"], 1)

    def test_reset_does_not_assume_unknown_traffic_strength(self):
        history = self.history({"generation": 1})
        history[1].update(type="probe_result", fields={"outbound": "a1", "reason": "lone", "is_ok": False})
        history[2]["fields"]["reason"] = "probe"
        history[3]["fields"]["to"] = "a1"
        self.assertEqual(len(h.strength_decisions(history[:4], [])), 1)
        with self.assertRaises(ValueError):
            h.strength_decisions(history, [])


    def test_old_weak_suspect_cannot_classify_new_inactive_down(self):
        history = self.history({"generation": 1})
        history[1]["fields"]["is_strong"] = False
        self.assertEqual(h.strength_decisions(history, []), [
            {"seq": 3, "outbound": "a1", "strong": False},
            {"seq": 6, "outbound": "a1", "strong": True},
        ])
        self.assertTrue(all(row["ok"] for row in self.rows(history, history, 1, weak_count=1)))
        for counts in ((0, 2), (2, 0)):
            rows = self.rows(history, history, counts[0], weak_count=counts[1])
            self.assertTrue(all(not row["ok"] for row in rows
                                if row["check"] in ("a1.downs_strong", "a1.downs_weak")))
        self.assertTrue(all(row["ok"] for row in self.rows(history[:4], history[:4], 0, weak_count=1)))
        self.assertTrue(all(row["ok"] for row in self.rows(history, history[4:], 1, index=2)))

    def test_active_or_unknown_selection_needs_new_evidence_after_reset(self):
        choices = [
            [("auto", "a1")],
            [],
            [("auto", None)],
            [("first", "b1"), ("second", "a1")],
            [("first", "b1"), ("second", None)],
        ]
        for selectors in choices:
            with self.subTest(selectors=selectors):
                history = self.history({"generation": 1})
                history[1]["fields"]["is_strong"] = False
                history = [e for e in history if e["type"] != "switch"]
                switches = [{"seq": 100 + index, "ts": 4500, "type": "switch",
                             "fields": {"balancer": balancer, "to": tag}}
                            for index, (balancer, tag) in enumerate(selectors)]
                history[2:2] = switches
                for seq, event in enumerate(history, 1):
                    event["seq"] = seq
                with self.assertRaises(ValueError):
                    h.strength_decisions(history, [])
                old_fault = {"at": 2.5, "ports": str(bench.address_port("reality", 0)), "mode": "refuse"}
                with self.assertRaises(ValueError):
                    h.strength_decisions(history, [old_fault])

    def test_every_observed_balancer_away_proves_inactive_traffic_strength(self):
        history = self.history({"generation": 1})
        history[1]["fields"]["is_strong"] = False
        history[0]["fields"]["balancer"] = "first"
        history[3]["fields"]["balancer"] = "first"
        history.insert(4, {"seq": 100, "ts": 4500, "type": "switch",
                           "fields": {"balancer": "second", "to": "c1"}})
        for seq, event in enumerate(history, 1):
            event["seq"] = seq
        self.assertIs(h.strength_decisions(history, [])[-1]["strong"], True)

    def test_old_probe_result_cannot_prove_down_after_reset(self):
        for reason in ("lone", "confirm"):
            with self.subTest(reason=reason):
                history = self.history({"generation": 1})
                history.insert(2, {"seq": 100, "ts": 2500, "type": "probe_result",
                                   "fields": {"outbound": "a1", "reason": reason, "is_ok": False}})
                history[-1]["fields"]["reason"] = "probe"
                for seq, event in enumerate(history, 1):
                    event["seq"] = seq
                with self.assertRaises(ValueError):
                    h.strength_decisions(history, [])

    def test_ignored_change_preserves_weak_suspect_and_probe_evidence(self):
        history = self.history({"generation": 1, "is_ignored": True})
        history[1]["fields"]["is_strong"] = False
        history[3]["fields"]["to"] = "a1"
        history[-1]["fields"]["quarantine_level"] = 2
        self.assertEqual([d["strong"] for d in h.strength_decisions(history, [])], [False, False])
        history.insert(2, {"seq": 100, "ts": 2500, "type": "probe_result",
                           "fields": {"outbound": "a1", "reason": "confirm", "is_ok": False}})
        history[-1]["fields"]["reason"] = "probe"
        for seq, event in enumerate(history, 1):
            event["seq"] = seq
        self.assertEqual([d["strong"] for d in h.strength_decisions(history, [])], [False, False])


    def test_exhausted_then_network_forget_proves_new_inactive_down(self):
        history = self.history({"generation": 1})
        history[1]["fields"]["is_strong"] = False
        history[0]["fields"]["balancer"] = "auto"
        history[3].update(type="exhausted", fields={"balancer": "auto", "from": "a1"})
        self.assertEqual(h.strength_decisions(history, []), [
            {"seq": 3, "outbound": "a1", "strong": False},
            {"seq": 6, "outbound": "a1", "strong": True},
        ])
        self.assertTrue(all(row["ok"] for row in self.rows(history, history, 1, weak_count=1)))
        wrong = self.rows(history, history, 0, weak_count=2)
        self.assertTrue(all(not row["ok"] for row in wrong
                            if row["check"] in ("a1.downs_strong", "a1.downs_weak")))
        self.assertTrue(all(row["ok"] for row in self.rows(history, history[4:], 1, index=2)))

    def test_exhausted_selection_is_known_empty_across_balancers(self):
        for second, expected in (("a1", None), ("", True), (None, None)):
            with self.subTest(second=second):
                history = self.history({"generation": 1})
                history[1]["fields"]["is_strong"] = False
                history[0]["fields"]["balancer"] = "first"
                history[3].update(type="exhausted", fields={"balancer": "first", "from": "a1"})
                history.insert(4, {"seq": 100, "ts": 4500,
                    "type": "exhausted" if second == "" else "switch",
                    "fields": {"balancer": "second", "from": "b1"} if second == ""
                              else {"balancer": "second", "to": second}})
                for seq, event in enumerate(history, 1):
                    event["seq"] = seq
                if expected is None:
                    with self.assertRaises(ValueError):
                        h.strength_decisions(history, [])
                else:
                    self.assertIs(h.strength_decisions(history, [])[-1]["strong"], expected)


class ReturnSpacingFixtureTest(unittest.TestCase):
    def test_positive_fixture_survives_go_default_and_preserves_return_bound(self):
        root = Path(__file__).resolve().parents[2]
        settings = (root / "app/observatory/health/settings.go").read_text()
        engine = (root / "app/observatory/health/engine.go").read_text()
        self.assertRegex(settings, r"func fnum\(v float64, def float64\) float64 \{\s*if v <= 0 \{\s*return def\s*\}\s*return v\s*\}")
        self.assertRegex(settings, r"func jitter\(v float64, def float64\) float64 \{\s*v = fnum\(v, def\)")
        self.assertRegex(settings, r"spacingJitter:\s*defaultSpacingJitter")
        self.assertIn("float64(d) * (1 + j*(2*e.rnd.Float64()-1))", engine)
        default = float(re.search(r"defaultSpacingJitter\s*=\s*([0-9.]+)", settings).group(1))
        maximum = float(re.search(r"maxJitter\s*=\s*([0-9.]+)", settings).group(1))
        self.assertEqual(default, 0.3)
        cfg = h.config()
        fraction = cfg["healthObservatory"]["schedule"]["probeJitterFraction"]
        spacing = float(cfg["routing"]["balancers"][0]["strategy"]["settings"]["returnToCheaper"]["probeInterval"][:-1])
        self.assertGreater(fraction, 0)
        self.assertLess(fraction, maximum)
        for configured, expected in ((0, default), (fraction, fraction)):
            with self.subTest(configured=configured):
                effective = default if configured <= 0 else configured
                self.assertEqual(effective, expected)
                low, high = (spacing * (1 + effective * (2 * draw - 1)) for draw in (0, 1))
                if configured == 0:
                    self.assertAlmostEqual(low, 14)
                    self.assertAlmostEqual(high, 26)
                    self.assertLess(low, 19.9)
                else:
                    self.assertAlmostEqual(low, 19.99998)
                    self.assertAlmostEqual(high, 20.00002)
                    self.assertGreaterEqual(low, 19.9)


if __name__ == "__main__":
    unittest.main()
