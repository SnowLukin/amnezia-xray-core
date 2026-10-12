#!/usr/bin/env python3
"""Planned synthetic comparator/config regressions; no real stand result.

Invoke only in an agreed short test window after GO. These invented fixtures
start no benchmark or network and do not establish quality invariance.
"""
import copy
from pathlib import Path
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True

from compare_events import compare, load_arm, read, semantics
from ordinary_common import (FLAG_PATH, RUNTIME_ENV_KEYS, file_sha256, quality_config, save,
                             value_sha256, without_flag)


def event(seq, typ="switch", **fields):
    return {"seq": seq, "ts": 1000 * seq, "type": typ,
            "fields": fields or {"from": "", "to": "a1", "reason": "start", "balancer": "auto"}}


def body(events):
    return {"dropped": 0, "events": events, "reports": []}


def write_invented_arm(root, mode, client_scenarios=("invented-a", "invented-b")):
    """Complete invented on/off artifacts for validator tests, never real results."""
    (root / "receipts").mkdir(parents=True)
    (root / "stand").mkdir()
    names = list(dict.fromkeys(client_scenarios))
    environment = {key: None for key in RUNTIME_ENV_KEYS}
    environment.update(PATH="/invented/no-execution/bin", BENCH_HOST="127.0.0.1", BENCH_LISTEN="127.0.0.1")
    save(root / "manifest.json", {"kind": "invented guard inputs, not a real stand result", "mode": mode,
         "forces_quietShare": False, "xray_sha256": "0" * 64,
         "bench_source_sha256": {"bench.py": "1" * 64, "run.py": "2" * 64},
         "helper_sha256": {"ordinary_runner.py": "3" * 64}, "environment": environment,
         "environment_sha256": value_sha256(environment), "port_offset": 24000,
         "planned_scenarios": names, "requested_scenarios": [], "slow_requested": False,
         "python": "invented fixture", "platform": "invented fixture"})
    save(root / "summary.json", {"original_exit_code": 0, "wrapper_exit_code": 0, "evidence_errors": []})
    (root / "terminal.log").write_text("invented synthetic check data; never an actual bench result\n")
    save(root / "stand/report.json", [{"scenario": name, "seconds": 0,
         "checks": [{"check": "invented synthetic input", "value": 1, "limit": "==1", "ok": True}]}
         for name in names])
    clients = []
    for ordinal, scenario in enumerate(client_scenarios, 1):
        receipt_id = f"receipt-{ordinal:06d}"
        original = {"healthObservatory": {"report": {"quietShare": 0.25}}, "log": {"loglevel": "warning"}}
        final, _, changed = quality_config(original, mode)
        row = {"id": receipt_id, "ordinal": ordinal, "scenario": scenario, "client_name": "reused-name",
               "mode": mode, "has_health": True, "profile": False, "exited": True,
               "flag_only_mutation_verified": True, "ledger_file": f"receipts/{receipt_id}.json",
               "original_config": f"receipts/{receipt_id}-original.json",
               "final_config": f"receipts/{receipt_id}-final.json",
               "client_log": f"receipts/{receipt_id}-client.log",
               "events_file": f"receipts/{receipt_id}-events.json",
               "normalized_final_sha256": value_sha256(without_flag(final)),
               "changed_leaf_paths": [list(path) for path in changed], "event_count": 1, "dropped": 0,
               "api_errors": [], "memory_errors": []}
        save(root / row["original_config"], original)
        save(root / row["final_config"], final)
        (root / row["client_log"]).write_text("invented occurrence log\n")
        save(root / row["events_file"], body([event(1)]))
        for role, key in (("original_config", "archived_original_config_sha256"),
                          ("final_config", "archived_final_config_sha256"),
                          ("client_log", "client_log_sha256"), ("events_file", "events_sha256")):
            row[key] = file_sha256(root / row[role])
        clients.append(row)
    rewrite_ledgers(root, clients)
    save(root / "scenario-receipts.json", [{"scenario": name, "client_receipt_ids":
         [row["id"] for row in clients if row["scenario"] == name]} for name in names])
    return clients


def rewrite_ledgers(root, clients):
    save(root / "client-receipts.json", clients)
    for row in clients:
        save(root / "receipts" / f"{row['id']}.json", row)


class OrdinaryGuards(unittest.TestCase):
    def test_only_quality_flag_changes_and_quietShare_is_retained(self):
        source = {"healthObservatory": {"report": {"quietShare": 0.25}}}
        frozen = copy.deepcopy(source)
        on, _, changes = quality_config(source, "on")
        off, _, _ = quality_config(source, "off")
        self.assertEqual(source, frozen)
        self.assertEqual(changes, [FLAG_PATH])
        self.assertEqual(on["healthObservatory"]["report"]["quietShare"], 0.25)
        self.assertEqual(value_sha256(without_flag(on)), value_sha256(without_flag(off)))
        plain, health, changes = quality_config({"routing": {"rules": []}}, "off")
        self.assertFalse(health)
        self.assertEqual(plain, {"routing": {"rules": []}})
        self.assertEqual(changes, [])

    def test_dropped_gap_empty_and_invalid_identity_are_rejected(self):
        malformed = [body([]), body([event(2)]), body([event(1), event(1)]),
                     {**body([event(1)]), "dropped": 1},
                     body([event(1, balancer="auto", **{"from": "", "to": None, "reason": "start"})])]
        for fixture in malformed:
            with self.subTest(fixture=fixture), self.assertRaises(ValueError):
                semantics(fixture)

    def test_group_absent_reason_and_quarantine_level_are_preserved(self):
        stream = semantics(body([
            event(1, "group_state", balancer="auto", group="relay", state="down",
                  members=3, failed=3, quarantine_ms=12000),
            event(2, "path_state", outbound="a1", state="down", reason="probe",
                  quarantine_level=2, quarantine_ms=12000)]))
        self.assertFalse(stream[0]["fields"]["reason"]["present"])
        self.assertEqual(stream[1]["fields"]["quarantine_level"]["value"], 2)

    def test_nonfinite_and_duplicate_json_are_rejected(self):
        with tempfile.TemporaryDirectory(prefix="ordinary-synthetic-guard-") as directory:
            path = Path(directory) / "malformed.json"
            for source in ('{"metric":1e999}', '{"x":1,"x":2}'):
                path.write_text(source)
                with self.subTest(source=source), self.assertRaises(ValueError):
                    read(path)

    def arm(self):
        events = body([event(1), event(2, "path_state", outbound="a1", state="suspect", reason="traffic")])
        receipt = {"id": "receipt-000001", "ordinal": 1, "scenario": "invented", "client_name": "invented",
                   "has_health": True, "profile": False, "events_file": "receipts/receipt-000001-events.json",
                   "ledger_file": "receipts/receipt-000001.json",
                   "original_config": "receipts/receipt-000001-original.json",
                   "final_config": "receipts/receipt-000001-final.json",
                   "client_log": "receipts/receipt-000001-client.log"}
        return {"manifest": {key: "invented-identical-input" for key in
                 ("xray_sha256", "bench_source_sha256", "helper_sha256", "environment", "environment_sha256", "port_offset",
                  "requested_scenarios", "planned_scenarios", "slow_requested", "python", "platform")},
                "event_files": [receipt["events_file"]], "report": [{"scenario": "invented"}],
                "artifact_files": [receipt[key] for key in
                  ("ledger_file", "original_config", "final_config", "client_log", "events_file")],
                "scenario_bindings": [{"scenario": "invented", "client_receipt_ids": [receipt["id"]]}],
                "clients": [{"receipt": receipt, "original_config": {}, "normalized_final_sha256": "invented",
                             "raw_events": events, "semantic_events": semantics(events)}]}

    def test_raw_time_difference_is_retained_without_an_equality_claim(self):
        on, off = self.arm(), self.arm()
        for row in off["clients"][0]["raw_events"]["events"]:
            row["ts"] += 1234
        off["clients"][0]["semantic_events"] = semantics(off["clients"][0]["raw_events"])
        result = compare(on, off)
        self.assertTrue(result["accepted"])
        self.assertEqual(result["pairs"][0]["raw_timing_differences"][0]["off_minus_on_ts_ms"], 1234)
        self.assertFalse(result["pairs"][0]["raw_timing_differences"][0]["timing_is_gate"])

    def test_semantic_mismatch_is_rejected(self):
        on, off = self.arm(), self.arm()
        off["clients"][0]["raw_events"]["events"][1]["fields"]["state"] = "up"
        off["clients"][0]["semantic_events"] = semantics(off["clients"][0]["raw_events"])
        self.assertFalse(compare(on, off)["accepted"])

    def test_complete_valid_invented_arms_pass_load_and_comparison(self):
        with tempfile.TemporaryDirectory(prefix="ordinary-invented-valid-") as directory:
            root = Path(directory)
            write_invented_arm(root / "on", "on")
            write_invented_arm(root / "off", "off")
            self.assertTrue(compare(load_arm(root / "on", "on"), load_arm(root / "off", "off"))["accepted"])

    def test_aliased_occurrence_archive_is_rejected_even_when_unused_file_is_removed(self):
        with tempfile.TemporaryDirectory(prefix="ordinary-invented-alias-") as directory:
            root = Path(directory)
            clients = write_invented_arm(root, "on")
            (root / clients[1]["original_config"]).unlink()
            clients[1]["original_config"] = clients[0]["original_config"]
            clients[1]["archived_original_config_sha256"] = clients[0]["archived_original_config_sha256"]
            rewrite_ledgers(root, clients)
            with self.assertRaisesRegex(ValueError, "noncanonical archive path"):
                load_arm(root, "on")

    def test_renamed_non_event_archive_is_rejected(self):
        for role in ("original_config", "final_config", "client_log"):
            with self.subTest(role=role), tempfile.TemporaryDirectory(prefix="ordinary-invented-rename-") as directory:
                root = Path(directory)
                clients = write_invented_arm(root, "on")
                replacement = "receipts/renamed-" + Path(clients[0][role]).name
                (root / clients[0][role]).rename(root / replacement)
                clients[0][role] = replacement
                rewrite_ledgers(root, clients)
                with self.assertRaisesRegex(ValueError, "noncanonical archive path"):
                    load_arm(root, "on")

    def test_complete_cross_arm_file_sets_are_a_gate(self):
        on, off = self.arm(), self.arm()
        off["artifact_files"][0] = "receipts/renamed-ledger.json"
        result = compare(on, off)
        self.assertFalse(result["accepted"])
        self.assertIn("on/off complete config/log/ledger/event file sets differ", result["errors"])

    def test_scenario_receipt_redistribution_is_rejected_with_same_flattened_ids(self):
        with tempfile.TemporaryDirectory(prefix="ordinary-invented-bindings-") as directory:
            root = Path(directory)
            clients = write_invented_arm(root, "on")
            rows = read(root / "scenario-receipts.json")
            rows[0]["client_receipt_ids"] = [row["id"] for row in clients]
            rows[1]["client_receipt_ids"] = []
            save(root / "scenario-receipts.json", rows)
            with self.assertRaisesRegex(ValueError, "ownership/order mismatch"):
                load_arm(root, "on")

    def test_scenario_receipt_order_is_a_gate(self):
        with tempfile.TemporaryDirectory(prefix="ordinary-invented-order-") as directory:
            root = Path(directory)
            write_invented_arm(root, "on", ("invented-a", "invented-a", "invented-b"))
            rows = read(root / "scenario-receipts.json")
            rows[0]["client_receipt_ids"].reverse()
            save(root / "scenario-receipts.json", rows)
            with self.assertRaisesRegex(ValueError, "ownership/order mismatch"):
                load_arm(root, "on")

    def test_cross_arm_scenario_bindings_are_a_gate(self):
        on, off = self.arm(), self.arm()
        off["scenario_bindings"][0]["client_receipt_ids"] = []
        result = compare(on, off)
        self.assertFalse(result["accepted"])
        self.assertIn("on/off per-scenario client receipt bindings differ", result["errors"])

    def test_changed_environment_requires_matching_recorded_digest(self):
        with tempfile.TemporaryDirectory(prefix="ordinary-invented-environment-") as directory:
            root = Path(directory)
            write_invented_arm(root, "on")
            manifest = read(root / "manifest.json")
            manifest["environment"]["PATH"] = "/invented/different/bin"
            save(root / "manifest.json", manifest)
            with self.assertRaisesRegex(ValueError, "environment hash mismatch"):
                load_arm(root, "on")

    def test_path_selector_is_required_even_with_updated_digest(self):
        with tempfile.TemporaryDirectory(prefix="ordinary-invented-no-path-") as directory:
            root = Path(directory)
            write_invented_arm(root, "on")
            manifest = read(root / "manifest.json")
            del manifest["environment"]["PATH"]
            manifest["environment_sha256"] = value_sha256(manifest["environment"])
            save(root / "manifest.json", manifest)
            with self.assertRaisesRegex(ValueError, "missing effective environment/runtime selectors"):
                load_arm(root, "on")

    def test_missing_environment_digest_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="ordinary-invented-no-env-hash-") as directory:
            root = Path(directory)
            write_invented_arm(root, "on")
            manifest = read(root / "manifest.json")
            del manifest["environment_sha256"]
            save(root / "manifest.json", manifest)
            with self.assertRaisesRegex(ValueError, "environment hashes"):
                load_arm(root, "on")

    def test_environment_digest_is_compared_across_valid_arms(self):
        with tempfile.TemporaryDirectory(prefix="ordinary-invented-env-pair-") as directory:
            root = Path(directory)
            write_invented_arm(root / "on", "on")
            write_invented_arm(root / "off", "off")
            manifest = read(root / "off/manifest.json")
            manifest["environment"]["PATH"] = "/invented/different/bin"
            manifest["environment_sha256"] = value_sha256(manifest["environment"])
            save(root / "off/manifest.json", manifest)
            result = compare(load_arm(root / "on", "on"), load_arm(root / "off", "off"))
            self.assertFalse(result["accepted"])
            self.assertIn("on/off execution input mismatch: environment_sha256", result["errors"])


if __name__ == "__main__":
    unittest.main()
