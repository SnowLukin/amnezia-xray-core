"""Негативные проверки provenance и отдельных окон. Xray и сеть не запускаются."""
import copy
import json
import os
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

import run_stage1_checks as runner
import unittest

import stage1_helpers as h
import stage1_probe_step_scenarios as steps
from run_stage1_checks import LEGACY_TIMINGS, LEGACY_HARNESS_FILES, legacy_proof_checks, legacy_reference_checks


def event(seq, typ, at, **fields):
    return {"seq": seq, "type": typ, "ts": int(at * 1000), "fields": fields}


def path_counts(**values):
    result = {key: 0 for key in h.PATH_FIELDS}
    result.update(probe_steps=[], **values)
    return result


class ReportOracleTest(unittest.TestCase):
    def setUp(self):
        self.history = [event(1, "switch", 1000, to="a1", reason="start"),
            event(2, "path_state", 1001.1, outbound="a1", state="suspect", is_strong=True),
            event(3, "path_state", 1001.2, outbound="a1", state="down", reason="traffic", quarantine_level=1),
            event(4, "switch", 1001.3, to="b1", reason="failover"),
            event(5, "probe_start", 1001.4, outbound="a1", reason="audit"),
            event(6, "probe_result", 1001.5, outbound="a1", reason="audit", is_ok=True, steps=[
                {"name": "transport", "is_passed": True}, {"name": "egress", "is_passed": False}]),
            event(7, "path_state", 1001.55, outbound="a1", state="up", reason="rescued", by="audit"),
            event(8, "path_state", 1001.6, outbound="a1", state="degraded", reason="egress"),
            event(9, "probe_start", 1001.7, outbound="b1", reason="wake"),
            event(10, "probe_result", 1001.8, outbound="b1", reason="wake", is_ok=True, steps=[
                {"name": "reserve", "is_passed": True}])]
        self.context = {"strategy": {"confirmWithProbe": "auto"}, "penalty_seconds": 300,
                        "faults": [], "present_actions": [], "tcp_only": True}
        self.report = {key: 0 for key in h.GLOBAL_FIELDS}
        self.report.update(is_quiet_sample=True, events=10, report_index=1, window_ms=2000, session_ms=2000,
            audit_count=1, audit_passed_count=1, wake_probe_count=1, path_degraded_count=1,
            paths={"a1": path_counts(audits=1, audits_passed=1, downs_strong=1, degraded_ms=400),
                   "b1": path_counts()})
        self.report["paths"]["a1"]["probe_steps"] = [{"name": "transport", "passed": 1, "failed": 0},
                                                     {"name": "egress", "passed": 0, "failed": 1}]
        self.report["paths"]["b1"]["probe_steps"] = [{"name": "reserve", "passed": 1, "failed": 0}]

    def rows(self, report=None, history=None, window=None, start=(1000, 1000), end=(1002, 1002)):
        history = self.history if history is None else history
        return h.window_checks(self.report if report is None else report, history,
                               history if window is None else window, self.context, start, end, {"a1", "b1"})

    def test_complete_new_contract_matches_events(self):
        self.assertTrue(all(row["ok"] for row in self.rows()))

    def test_every_new_global_is_required_even_when_zero(self):
        for key in h.GLOBAL_FIELDS:
            broken = copy.deepcopy(self.report)
            del broken[key]
            with self.subTest(field=key):
                self.assertTrue(any(not row["ok"] for row in self.rows(broken)))

    def test_every_new_path_counter_is_required_even_when_zero(self):
        for key in h.PATH_FIELDS + ("probe_steps",):
            broken = copy.deepcopy(self.report)
            del broken["paths"]["a1"][key]
            with self.subTest(field=key):
                self.assertTrue(any(not row["ok"] for row in self.rows(broken)))

    def test_boolean_rejected_for_every_global_and_path_counter(self):
        for key in h.GLOBAL_FIELDS:
            broken = copy.deepcopy(self.report)
            broken[key] = bool(broken[key])
            with self.subTest(field=key):
                self.assertTrue(any(not row["ok"] for row in self.rows(broken)))
        for key in h.PATH_FIELDS:
            broken = copy.deepcopy(self.report)
            broken["paths"]["a1"][key] = bool(broken["paths"]["a1"][key])
            with self.subTest(path_field=key):
                self.assertTrue(any(not row["ok"] for row in self.rows(broken)))

    def test_missing_paths_do_not_vacuously_pass(self):
        broken = copy.deepcopy(self.report)
        broken["paths"] = {}
        self.assertTrue(any(not row["ok"] for row in self.rows(broken)))

    def test_step_order_counts_and_boolean_are_exact(self):
        for mode in ("order", "count", "bool"):
            broken = copy.deepcopy(self.report)
            steps = broken["paths"]["a1"]["probe_steps"]
            if mode == "order":
                steps.reverse()
            elif mode == "count":
                steps[1]["failed"] = 2
            else:
                steps[0]["passed"] = True
            with self.subTest(mode=mode):
                self.assertTrue(any(not row["ok"] for row in self.rows(broken)))

    def test_unknown_strength_rejected_instead_of_assumed_zero(self):
        history = [e for e in self.history if e["seq"] != 2]
        with self.assertRaises(ValueError):
            self.rows(history=history)

    def test_network_change_ends_degraded_without_path_state(self):
        history = [event(1, "path_state", 1001.6, outbound="a1", state="degraded"),
                   event(2, "network_change", 1001.8, network="new", is_known=False)]
        lower, upper = h.degraded_range("a1", history, (1001.6, 1001.6), (1002, 1002))
        self.assertLessEqual(lower, 200)
        self.assertGreaterEqual(upper, 200)
        self.assertLess(upper, 220)

    def test_ignored_network_change_and_network_down_keep_degraded(self):
        for typ, fields in (("network_change", {"is_ignored": True}), ("network_down", {})):
            history = [event(1, "path_state", 1001.6, outbound="a1", state="degraded"),
                       event(2, typ, 1001.8, **fields)]
            with self.subTest(event=typ):
                lower, upper = h.degraded_range("a1", history, (1001.6, 1001.6), (1002, 1002))
                self.assertLessEqual(lower, 400)
                self.assertGreaterEqual(upper, 400)
                self.assertGreater(lower, 380)

    def test_degraded_after_network_change_starts_new_interval(self):
        history = [event(1, "path_state", 1001.6, outbound="a1", state="degraded"),
                   event(2, "network_change", 1001.8, network="new"),
                   event(3, "path_state", 1001.9, outbound="a1", state="degraded")]
        lower, upper = h.degraded_range("a1", history, (1001.6, 1001.6), (1002, 1002))
        self.assertLessEqual(lower, 300)
        self.assertGreaterEqual(upper, 300)
        self.assertLess(upper, 320)

    def test_lone_and_wake_down_have_different_strength_semantics(self):
        for reason, strong in (("lone", 1), ("wake", 0), ("network_change", 0), ("recheck", 0), ("failover", 0)):
            history = [event(1, "probe_result", 1001, outbound="a1", reason=reason, is_ok=False),
                       event(2, "path_state", 1001.1, outbound="a1", state="down", reason="probe")]
            with self.subTest(reason=reason):
                self.assertEqual(len(h.strength_decisions(history, [])), strong)

    def test_same_quarantine_does_not_repeat_strength_decision(self):
        history = self.history[:4] + [event(5, "path_state", 1001.4, outbound="a1", state="down", reason="traffic", quarantine_level=1)]
        self.assertEqual(len(h.strength_decisions(history, [])), 1)

    def test_next_window_rejects_counter_step_and_time_carryover(self):
        next_event = event(11, "path_state", 1002.1, outbound="a1", state="up", reason="network_change")
        history = self.history + [next_event]
        clean = {key: 0 for key in h.GLOBAL_FIELDS}
        clean.update(is_quiet_sample=True, events=1, report_index=2, window_ms=1500, session_ms=3500,
                     paths={"a1": path_counts(degraded_ms=100), "b1": path_counts()})
        self.assertTrue(all(row["ok"] for row in self.rows(clean, history, [next_event], (1002, 1002), (1003.5, 1003.5))))
        for mode in ("counter", "steps", "time"):
            broken = copy.deepcopy(clean)
            if mode == "counter":
                broken["audit_count"] = 1
            elif mode == "steps":
                broken["paths"]["a1"]["probe_steps"] = self.report["paths"]["a1"]["probe_steps"]
            else:
                broken["paths"]["a1"]["degraded_ms"] = 500
            with self.subTest(mode=mode):
                self.assertTrue(any(not row["ok"] for row in self.rows(broken, history, [next_event], (1002, 1002), (1003.5, 1003.5))))

    def test_skipped_is_proved_by_cancel_action_and_type_is_strict(self):
        history = self.history[:4] + [event(5, "network_change", 1001.35, generation=2)]
        self.assertEqual(h.audit_skips(history, self.context, 1002), [1001.35])
        self.context["present_actions"] = [{"at": 1001.7, "skip": True, "fresh": True, "proofs": [1001.6]}]
        broken = copy.deepcopy(self.report)
        broken["wake_skipped_count"] = True
        self.assertTrue(any(not row["ok"] for row in self.rows(broken)))


class LegacyProofTest(unittest.TestCase):
    def setUp(self):
        self.expected = {"binary_sha256": "before-sha", "commit": "8b3d839f", "source_manifest_sha256": "source-before",
                         "harness": {"run.py": "frozen-sha"}, "catalog": ["ordinary"],
                         "harness_root": "/frozen", "harness_source_commit": "8b3d839f",
                         "legacy_reference_sha256": "reference-sha", "defaults": {"BENCH_TLS": "unset -> false"},
                         "cleared_environment": ["BENCH_SLOW"]}
        self.data = [{"scenario": "ordinary", "seconds": 1, "checks": [h.check("real check", 1, True, 1)]}]
        self.meta = {"binary": {"path": "/frozen/before", "sha256": "before-sha", "commit": "8b3d839f"},
                     "source_manifest_sha256": "source-before", "report_sha256": "report-sha",
                     "harness": {"run.py": "frozen-sha"}, "catalog": ["ordinary"], "environment": {},
                     "legacy_reference_sha256": "reference-sha", "harness_source_commit": "8b3d839f",
                     "defaults": {"BENCH_TLS": "unset -> false"}, "cleared_environment": ["BENCH_SLOW"],
                     "timings": dict(LEGACY_TIMINGS), "command": ["python3", "/frozen/run.py", "/frozen/before", "ordinary"],
                     "report_path": "/frozen/report.json", "terminal": {
                         "checks_total": 1, "checks_failed": 0, "exit_code": 0, "scenario_count": 1}}
        self.terminal = "PASS ordinary real check 1 1\n1/1 checks passed, report /frozen/report.json\n"

    def rows(self, data=None, meta=None, terminal=None):
        return legacy_proof_checks(self.data if data is None else data, self.meta if meta is None else meta,
                                   self.expected, "report-sha", self.terminal if terminal is None else terminal)

    def test_valid_receipt_binds_all_inputs(self):
        self.assertTrue(all(row["ok"] for row in self.rows()))

    def test_after_report_cannot_impersonate_before_by_changing_seconds(self):
        fake = copy.deepcopy(self.meta)
        fake["binary"].update(sha256="after-sha", commit="after-commit")
        altered = copy.deepcopy(self.data)
        altered[0]["seconds"] = 2
        self.assertTrue(any(not row["ok"] for row in self.rows(altered, fake)))

    def test_report_source_harness_and_environment_tampering_fail(self):
        for field, value in (("report_sha256", "other-report"), ("source_manifest_sha256", "after-source"),
                             ("harness", {}), ("environment", {"BENCH_DEAD_SITE_SEC": "1"}),
                             ("catalog", []), ("command", [])):
            fake = copy.deepcopy(self.meta)
            fake[field] = value
            with self.subTest(field=field):
                self.assertTrue(any(not row["ok"] for row in self.rows(meta=fake)))

    def test_missing_terminal_count_and_boolean_exit_fail(self):
        for field in ("checks_total", "checks_failed", "exit_code", "scenario_count"):
            fake = copy.deepcopy(self.meta)
            del fake["terminal"][field]
            self.assertTrue(any(not row["ok"] for row in self.rows(meta=fake)))
            fake = copy.deepcopy(self.meta)
            fake["terminal"][field] = bool(fake["terminal"][field])
            self.assertTrue(any(not row["ok"] for row in self.rows(meta=fake)))

    def test_new_phase_harness_cannot_replace_original_reference(self):
        fake = copy.deepcopy(self.meta)
        fake["harness"].update({"run.py": "current-run-sha", "chaos.py": "current-chaos-sha",
                                 "phase_timeout_scenarios.py": "new-phase-sha"})
        self.assertTrue(any(not row["ok"] for row in self.rows(meta=fake)))
        fake = copy.deepcopy(self.meta)
        fake["command"][1] = "/current/testing/adaptive/run.py"
        self.assertTrue(any(not row["ok"] for row in self.rows(meta=fake)))

    def test_reference_sha_and_cleared_environment_are_bound(self):
        for field, value in (("legacy_reference_sha256", "current-reference"),
                             ("harness_source_commit", "after-commit"),
                             ("defaults", {}), ("cleared_environment", []),
                             ("environment", {"BENCH_SLOW": ""})):
            fake = copy.deepcopy(self.meta)
            fake[field] = value
            with self.subTest(field=field):
                self.assertTrue(any(not row["ok"] for row in self.rows(meta=fake)))

    def test_terminal_cannot_omit_or_invent_checks(self):
        self.assertTrue(any(not row["ok"] for row in self.rows(terminal="1/1 checks passed, report /frozen/report.json\n")))
        self.assertTrue(any(not row["ok"] for row in self.rows(terminal=self.terminal.replace("1/1", "2/2"))))

    def test_red_baseline_is_preserved_without_whitelist(self):
        data = copy.deepcopy(self.data)
        data[0]["checks"][0].update(ok=False, value=2.1, limit="2%")
        meta = copy.deepcopy(self.meta)
        meta["terminal"].update(checks_failed=1, exit_code=1)
        terminal = "FAIL ordinary real check 2.1 2%\n0/1 checks passed, report /frozen/report.json\n"
        rows = self.rows(data, meta, terminal)
        red = next(row for row in rows if row["check"] == "Все красные обычного стенда сохранены")
        self.assertFalse(red["ok"])
        self.assertEqual(red["value"], [("ordinary", "real check", 2.1, "2%")])


class OriginalReferenceTest(unittest.TestCase):
    def setUp(self):
        self.reference = {"source_commit": "8b3d839f1ee12ffa36a460bafd872af68c00261f",
            "harness_root": "/frozen/original8b", "harness": {name: "a" * 64 for name in LEGACY_HARNESS_FILES},
            "catalog": ["ordinary_%03d" % i for i in range(85)], "scenario_count": 85,
            "defaults": {"default_%s" % i: "unset" for i in range(8)},
            "cleared_environment": ["BENCH_%s" % i for i in range(19)]}

    def test_valid_original_nine_file_reference_has_no_phase_fixture(self):
        rows = legacy_reference_checks(self.reference, dict(self.reference["harness"]), False)
        self.assertTrue(all(row["ok"] for row in rows))

    def test_current_phase_modified_run_or_chaos_is_rejected(self):
        for name in ("run.py", "chaos.py"):
            changed = dict(self.reference["harness"])
            changed[name] = "b" * 64
            with self.subTest(file=name):
                self.assertTrue(any(not row["ok"] for row in legacy_reference_checks(self.reference, changed, False)))
        changed = dict(self.reference["harness"])
        changed["phase_timeout_scenarios.py"] = "b" * 64
        self.assertTrue(any(not row["ok"] for row in legacy_reference_checks(self.reference, changed, True)))

    def test_reference_cannot_add_phase_or_incomplete_catalog(self):
        for mode in ("phase", "missing"):
            changed = copy.deepcopy(self.reference)
            if mode == "phase":
                changed["harness"]["phase_timeout_scenarios.py"] = "b" * 64
            else:
                changed["catalog"].pop()
            self.assertTrue(any(not row["ok"] for row in legacy_reference_checks(changed, changed["harness"], False)))


class LegacyReuseScopeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.args = SimpleNamespace(legacy_after_manifest=None, legacy_reuse_disposition=None,
                                    legacy_after_report=None, legacy_after_meta=None)
        self.old = self.make_manifest("old", False)
        self.current = self.make_manifest("current", True)
        self.old_path = self.save("old-manifest.json", self.old)
        self.identity = {"current_binary_sha256": self.current["sha256"],
                         "current_source_manifest_sha256": self.current["source_manifest_sha256"],
                         "original_binary_sha256": self.old["sha256"],
                         "original_source_manifest_sha256": self.old["source_manifest_sha256"],
                         "legacy_reference_sha256": "a" * 64}
        self.args.legacy_after_report = self.save("old-report.json", [{"scenario": "historical"}])
        terminal = self.save("old-terminal.json", {"exit_code": 1})
        self.args.legacy_after_meta = self.save("old-meta.json", {
            "binary": {"sha256": self.old["sha256"]},
            "source_manifest_sha256": self.old["source_manifest_sha256"],
            "report_sha256": runner.sha(self.args.legacy_after_report), "terminal": {"path": terminal}})
        proof = self.save("source-delta.json", {"identities": self.identity, "added_lines": runner.EXPORT_DELTA})
        reason = self.save("reason-review.json", {"source_only_test_fixture": True})
        self.decision = {"schema": "stage1-legacy-reuse-disposition/v1", "root_address": runner.ROOT_ADDRESS,
            "scope": runner.REUSE_SCOPE, "current_measurement": False, "stage_acceptance": False,
            "raw_policy": "preserve-all-failures-zero-failed-gate", "rationale": "Тестовая source export delta",
            **self.identity, "pins": {key: {"path": path, "sha256": runner.sha(path)} for key, path in {
                "original_manifest": self.old_path, "original_report": self.args.legacy_after_report,
                "original_meta": self.args.legacy_after_meta, "original_terminal": terminal,
                "reason_review": reason, "source_delta_proof": proof}.items()}}

    def save(self, name, data):
        path = os.path.join(self.root, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as stream:
            json.dump(data, stream)
        return path

    def make_manifest(self, label, current):
        root = os.path.join(self.root, label)
        files = {}
        for path, additions in runner.EXPORT_DELTA.items():
            target = os.path.join(root, path)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with open(target, "w") as stream:
                stream.write("package health\n" + ("\n".join(additions) + "\n" if current else ""))
            files[path] = runner.sha(target)
        commit = label + "-commit"
        source = ({"source": root, "metadata": {"git_head": commit},
                   "files": [{"path": path, "sha256": digest} for path, digest in files.items()]}
                  if current else {"source_root": root, "commit": commit, "files": files})
        source_path = self.save(label + "-source.json", source)
        binary_path = self.save(label + "-binary.json", {"fixture": label})
        return {"commit": commit, "source_manifest": source_path, "source_manifest_sha256": runner.sha(source_path),
                "sha256": runner.sha(binary_path), "binary_path": binary_path}

    def enable(self):
        self.args.legacy_after_manifest = self.old_path
        self.args.legacy_reuse_disposition = self.save("disposition.json", self.decision)

    def scope(self):
        return runner.legacy_after_scope(self.args, self.current, self.current["sha256"],
                                         self.current["source_manifest_sha256"], "a" * 64)

    def test_cli_exposes_both_reuse_options(self):
        with patch("sys.argv", ["run_stage1_checks.py", "--legacy-after-manifest", "/old.json",
                                "--legacy-reuse-disposition", "/decision.json"]):
            parsed = runner.arguments()
        self.assertEqual(parsed.legacy_after_manifest, "/old.json")
        self.assertEqual(parsed.legacy_reuse_disposition, "/decision.json")
        self.assertFalse(parsed.go)

    def test_default_uses_current_identity_without_historical_label(self):
        identity, metadata = self.scope()
        self.assertEqual(identity["binary_sha256"], self.current["sha256"])
        self.assertEqual(metadata["scope"], "current-binary-measurement")
        self.assertNotIn("original", metadata)

    def test_each_unpaired_cli_option_is_rejected(self):
        for option in ("legacy_after_manifest", "legacy_reuse_disposition"):
            setattr(self.args, option, self.old_path)
            with self.subTest(option=option), self.assertRaises(ValueError):
                self.scope()
            setattr(self.args, option, None)

    def test_historical_identity_is_separate_from_current_measurement(self):
        self.enable()
        identity, metadata = self.scope()
        self.assertEqual(identity["binary_sha256"], self.old["sha256"])
        self.assertEqual(metadata["original"], identity)
        self.assertEqual(metadata["current"]["binary_sha256"], self.current["sha256"])
        self.assertEqual(metadata["scope"], runner.REUSE_SCOPE)
        self.assertIs(metadata["current_measurement"], False)
        self.assertIs(metadata["stage_acceptance"], False)

    def test_wrong_identity_scope_or_acceptance_claim_is_rejected(self):
        original = copy.deepcopy(self.decision)
        for key, value in [(key, "f" * 64) for key in self.identity] + [
                ("root_address", "other"), ("scope", "current-binary-measurement"),
                ("current_measurement", True), ("stage_acceptance", True), ("raw_policy", "whitelist"),
                ("rationale", "")]:
            self.decision = copy.deepcopy(original)
            self.decision[key] = value
            self.enable()
            with self.subTest(field=key), self.assertRaises(ValueError):
                self.scope()

    def test_each_provenance_pin_rejects_tampering(self):
        original = copy.deepcopy(self.decision)
        for key in original["pins"]:
            self.decision = copy.deepcopy(original)
            self.decision["pins"][key]["sha256"] = "f" * 64
            self.enable()
            with self.subTest(pin=key), self.assertRaises(ValueError):
                self.scope()

    def test_repinned_delta_proof_cannot_allow_other_source_change(self):
        proof_pin = self.decision["pins"]["source_delta_proof"]
        self.save("source-delta.json", {"identities": self.identity, "added_lines": {}})
        proof_pin["sha256"] = runner.sha(proof_pin["path"])
        self.enable()
        with self.assertRaises(ValueError):
            self.scope()

    def test_repinned_source_rejects_non_export_production_change(self):
        source_path = self.current["source_manifest"]
        with open(source_path) as stream:
            source = json.load(stream)
        target = os.path.join(source["source"], source["files"][0]["path"])
        with open(target, "a") as stream:
            stream.write("var unexpected = 1\n")
        source["files"][0]["sha256"] = runner.sha(target)
        self.save("current-source.json", source)
        self.current["source_manifest_sha256"] = runner.sha(source_path)
        self.identity["current_source_manifest_sha256"] = self.current["source_manifest_sha256"]
        self.decision.update(self.identity)
        proof_pin = self.decision["pins"]["source_delta_proof"]
        self.save("source-delta.json", {"identities": self.identity, "added_lines": runner.EXPORT_DELTA})
        proof_pin["sha256"] = runner.sha(proof_pin["path"])
        self.enable()
        with self.assertRaises(ValueError):
            self.scope()

    def test_both_manifest_shapes_validate_source_bytes_and_declared_commit(self):
        for manifest in (self.old, self.current):
            with self.subTest(commit=manifest["commit"]):
                self.assertEqual(runner.source_manifest(manifest), manifest["source_manifest_sha256"])
                invalid = dict(manifest, commit="other")
                with self.assertRaises(ValueError):
                    runner.source_manifest(invalid)
        with open(os.path.join(self.root, "current", next(iter(runner.EXPORT_DELTA))), "a") as stream:
            stream.write("changed\n")
        with self.assertRaises(ValueError):
            runner.source_manifest(self.current)

    def test_boolean_source_commit_cannot_be_used_as_identity(self):
        path = self.current["source_manifest"]
        with open(path) as stream:
            source = json.load(stream)
        source["metadata"]["git_head"] = True
        self.save("current-source.json", source)
        manifest = dict(self.current, commit=True, source_manifest_sha256=runner.sha(path))
        with self.assertRaises(ValueError):
            runner.source_manifest(manifest)

    def test_source_list_duplicate_and_parent_escape_are_rejected(self):
        path = self.current["source_manifest"]
        with open(path) as stream:
            original = json.load(stream)
        for mode in ("duplicate", "escape"):
            source = copy.deepcopy(original)
            if mode == "duplicate":
                source["files"].append(dict(source["files"][0]))
            else:
                source["files"][0]["path"] = "../outside.go"
            self.save("current-source.json", source)
            manifest = dict(self.current, source_manifest_sha256=runner.sha(path))
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                runner.source_manifest(manifest)



class HostStepProbeFixtureTest(unittest.TestCase):
    def response(self, result="ok", source="probe"):
        return {"id": 7, "run": "fake-run", "request": {"id": 7, "is_done": True,
                "outbounds": [{"outbound": steps.A, "source": source, "result": result}]}}

    def client(self, response):
        return SimpleNamespace(path="/tmp/client-fake.json", api=lambda *args, **kwargs: response)

    def events(self, request=7, reason="host", stale=False):
        return [event(1, "probe_start", 0, outbound=steps.A, request=request, reason=reason),
                event(2, "probe_result", 0, outbound=steps.A, request=request, reason=reason, is_stale=stale)]

    def run_probe(self, response, events):
        def select(client, typ, tag, reason, since=0):
            return [e for e in events if e["type"] == typ and e["fields"]["outbound"] == tag
                    and e["fields"]["reason"] == reason and e["ts"] / 1000 >= since]
        def wait(predicate, timeout):
            self.assertEqual(timeout, 5)
            value = predicate()
            if not value:
                raise RuntimeError("fake missing event")
            return value
        with patch.object(h, "events", side_effect=select), patch.object(h, "wait_for", side_effect=wait):
            return steps.probe(self.client(response))

    def test_zero_reuse_preserves_schedule_and_profiles(self):
        nodes = object.__new__(steps.Nodes)
        nodes.http = SimpleNamespace(port=12345)
        nodes.domain, nodes.name, nodes.ports = "probe.test", "fake", [12346, 12347]
        with tempfile.TemporaryDirectory() as work:
            cfg = nodes.client_config(SimpleNamespace(provider_answer=os.path.join(work, "answer.json")))
        health = cfg["healthObservatory"]
        self.assertEqual(health["schedule"]["hostProbes"], {"reuseWindow": "0s"})
        self.assertEqual(health["schedule"]["minProbeInterval"], "1s")
        self.assertEqual(health["schedule"]["idleAfter"], "1h")
        self.assertEqual(health["schedule"]["quarantine"]["maxDelay"], "30m")
        self.assertEqual(health["probes"], [nodes.profile()])

    def test_recent_rejected_before_wait(self):
        with patch.object(h, "save") as save, patch.object(h, "wait_for") as wait:
            with self.assertRaisesRegex(RuntimeError, "выполненная probe"):
                steps.probe(self.client(self.response(source="recent")))
        save.assert_called_once()
        wait.assert_not_called()

    def test_completed_ok_and_failed_host_request(self):
        for result in ("ok", "failed"):
            with self.subTest(result=result), patch.object(h, "save"):
                actual, response = self.run_probe(self.response(result), self.events())
                self.assertEqual(actual["fields"]["request"], response["id"])
                self.assertNotIn("probe_reason", response["request"]["outbounds"][0])

    def test_same_millisecond_event_matches_request_id(self):
        events = self.events()
        for item in events:
            item["ts"] = 1000123
        with patch.object(steps.time, "time", return_value=1000.1239), patch.object(h, "save"):
            actual, response = self.run_probe(self.response(), events)
        self.assertEqual(actual["ts"], 1000123)
        self.assertEqual(actual["fields"]["request"], response["id"])

    def test_wrong_request_reason_or_stale_rejected(self):
        for events in (self.events(request=8), self.events(reason="confirm"), self.events(stale=True)):
            with self.subTest(events=events), patch.object(h, "save"):
                with self.assertRaisesRegex(RuntimeError, "нет связанных"):
                    self.run_probe(self.response(), events)

    def test_missing_or_pending_request_rejected(self):
        responses = [dict(self.response(), request=None), self.response(), self.response()]
        responses[1]["request"]["is_done"] = False
        responses[2]["request"]["outbounds"] = []
        for response in responses:
            with self.subTest(response=response), patch.object(h, "save"), patch.object(h, "wait_for") as wait:
                with self.assertRaises(RuntimeError):
                    steps.probe(self.client(response))
                wait.assert_not_called()

    def test_response_saved_before_missing_event_without_credentials(self):
        response = self.response()
        response["credentials"] = "secret"
        response["request"]["outbounds"][0]["password"] = "secret"
        saved = []
        with patch.object(h, "save", side_effect=lambda *args: saved.append(args)):
            with self.assertRaisesRegex(RuntimeError, "нет связанных"):
                self.run_probe(response, [])
        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0][:2], ("/tmp", "client-fake-host-probe-request-7"))
        self.assertEqual(saved[0][2]["id"], 7)
        self.assertNotIn("secret", json.dumps(saved[0][2]))

if __name__ == "__main__":
    unittest.main()
