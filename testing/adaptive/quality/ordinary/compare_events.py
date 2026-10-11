#!/usr/bin/env python3
"""Compare existing ordinary on/off evidence without starting a stand.

Exact ordered decision/state semantics are gates. Raw wall times, elapsed times,
held_ms and quarantine_ms remain evidence only; no timing tolerance is invented.
"""
import argparse
import json
import math
from pathlib import Path
import sys
import traceback

sys.dont_write_bytecode = True

from ordinary_common import (RUNTIME_ENV_KEYS, archive_paths, file_sha256, quality_config, save, value_sha256,
                             without_flag)

SEMANTIC_KEYS = {
    "switch": ("from", "to", "reason", "balancer"),
    "path_state": ("outbound", "state", "reason", "quarantine_level", "is_provisional"),
    "group_state": ("balancer", "group", "state", "reason", "members", "failed"),
    "exhausted": ("balancer", "from", "candidates"),
    "pinned_down": ("balancer", "outbound", "is_start_pinned"),
    "return_failed": ("balancer", "outbound", "reason", "successes"),
}
REQUIRED_KEYS = {
    "switch": ("from", "to", "reason", "balancer"),
    "path_state": ("outbound", "state", "reason"),
    # group_state has no reason field in the legacy wire schema.
    "group_state": ("balancer", "group", "state", "members", "failed"),
    "exhausted": ("balancer", "from", "candidates"),
    "pinned_down": ("balancer", "outbound"),
    "return_failed": ("balancer", "outbound", "successes"),
}


def require(ok, message):
    if not ok:
        raise ValueError(message)


def read(path):
    require(path.is_file(), f"missing file: {path}")
    def real(text):
        value = float(text)
        require(math.isfinite(value), f"nonfinite JSON number: {text}")
        return value

    def constant(value):
        raise ValueError(f"nonfinite JSON constant: {value}")

    def unique_keys(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(path.read_text(encoding="utf-8"), parse_float=real,
                      parse_constant=constant, object_pairs_hook=unique_keys)


def contained(root, relative):
    require(isinstance(relative, str) and relative, "missing relative artifact path")
    value = Path(relative)
    require(not value.is_absolute(), f"absolute artifact reference: {relative}")
    path = (root / value).resolve()
    require(root in path.parents, f"artifact reference escaped run directory: {relative}")
    return path


def semantics(body):
    require(isinstance(body, dict), "event artifact must be an object")
    require(type(body.get("dropped")) is int and body["dropped"] == 0,
            "dropped must be a present integer zero")
    events = body.get("events")
    require(isinstance(events, list) and bool(events), "no data: expected nonempty health-client event stream")
    require(isinstance(body.get("reports"), list), "reports must be a list (empty is allowed)")
    stream = []
    for ordinal, event in enumerate(events, 1):
        require(isinstance(event, dict), f"event {ordinal} must be an object")
        require(type(event.get("seq")) is int and event["seq"] == ordinal,
                f"event sequence gap/duplicate/reorder at {ordinal}")
        ts = event.get("ts")
        require(isinstance(ts, (int, float)) and not isinstance(ts, bool)
                and math.isfinite(ts) and ts >= 0, f"invalid event timestamp at {ordinal}")
        typ, fields = event.get("type"), event.get("fields", {})
        require(isinstance(typ, str) and bool(typ) and isinstance(fields, dict),
                f"invalid event type/fields at {ordinal}")
        if typ not in SEMANTIC_KEYS:
            continue  # Entire raw event is retained; no claim of observational equality.
        require(all(key in fields for key in REQUIRED_KEYS[typ]), f"incomplete {typ} semantic fields at {ordinal}")
        if typ == "path_state":
            require(fields["state"] in ("up", "suspect", "down", "unknown"), "unknown path state")
            if fields["state"] == "down":
                require("quarantine_level" in fields and "quarantine_ms" in fields,
                        "down path lacks quarantine receipt")
        if typ == "group_state":
            require(fields["state"] in ("up", "down") and "quarantine_ms" in fields,
                    "group state lacks quarantine receipt")
        for key in ("from", "to", "reason", "balancer", "outbound", "group", "state"):
            if key in fields:
                require(isinstance(fields[key], str), f"nonstring {typ}.{key}")
        for key in ("balancer", "outbound", "group"):
            if key in fields:
                require(bool(fields[key]), f"empty {typ}.{key} identity")
        for key in ("members", "failed", "candidates", "successes", "quarantine_level", "quarantine_ms"):
            if key in fields:
                require(type(fields[key]) is int, f"noninteger {typ}.{key}")
        for key in ("is_provisional", "is_start_pinned"):
            if key in fields:
                require(type(fields[key]) is bool, f"nonboolean {typ}.{key}")
        # Exact key presence is part of semantics, including absent group reason.
        stream.append({"type": typ, "fields": {key: {"present": key in fields,
                       "value": fields.get(key)} for key in SEMANTIC_KEYS[typ]},
                       "raw_event_index": ordinal - 1, "seq": event["seq"], "ts": ts})
    return stream


def load_arm(root, mode):
    root = root.resolve(strict=True)
    require(root.is_dir(), f"not an evidence directory: {root}")
    manifest, summary = read(root / "manifest.json"), read(root / "summary.json")
    require(manifest.get("mode") == mode, f"expected {mode} manifest")
    require(type(summary.get("original_exit_code")) is int and summary["original_exit_code"] == 0,
            f"{mode}: original run.py failed; retain terminal/report FAIL evidence")
    require(summary.get("evidence_errors") == [], f"{mode}: runner/capture errors")
    require(type(summary.get("wrapper_exit_code")) is int and summary["wrapper_exit_code"] == 0,
            f"{mode}: wrapper evidence gate failed")
    require(manifest.get("forces_quietShare") is False, "wrapper must not force legacy quietShare")
    hashes = [manifest.get("xray_sha256"), manifest.get("environment_sha256")]
    for key in ("bench_source_sha256", "helper_sha256"):
        value = manifest.get(key)
        require(isinstance(value, dict) and bool(value), f"{mode}: missing {key}")
        hashes.extend(value.values())
    require(all(isinstance(value, str) and len(value) == 64 and
                all(char in "0123456789abcdef" for char in value) for value in hashes),
            f"{mode}: malformed source/binary/environment hashes")
    environment = manifest.get("environment")
    require(isinstance(environment, dict) and all(key in environment for key in RUNTIME_ENV_KEYS),
            f"{mode}: missing effective environment/runtime selectors")
    require(all(isinstance(key, str) and (value is None or isinstance(value, str))
                for key, value in environment.items()), f"{mode}: malformed effective environment")
    require(value_sha256(environment) == manifest["environment_sha256"],
            f"{mode}: effective environment hash mismatch")
    terminal = root / "terminal.log"
    require(terminal.is_file() and terminal.stat().st_size > 0, f"{mode}: missing/empty full terminal")
    reports = read(root / "stand/report.json")
    require(isinstance(reports, list) and bool(reports), f"{mode}: no original scenario report")
    names = [row.get("scenario") for row in reports if isinstance(row, dict)]
    require(len(names) == len(reports) and all(isinstance(name, str) and name for name in names)
            and len(names) == len(set(names)), f"{mode}: malformed/duplicate scenario set")
    planned = manifest.get("planned_scenarios")
    require(names == planned, f"{mode}: actual/planned scenario sets/order differ")
    for row in reports:
        checks = row.get("checks")
        require(isinstance(checks, list) and bool(checks), f"{mode}/{row['scenario']}: missing original checks")
        require(all(isinstance(check, dict) and check.get("ok") is True
                    and all(key in check for key in ("check", "value", "limit")) for check in checks),
                f"{mode}/{row['scenario']}: original acceptance FAIL or malformed check")
    scenario_rows = read(root / "scenario-receipts.json")
    require(isinstance(scenario_rows, list) and [row.get("scenario") for row in scenario_rows] == names,
            f"{mode}: missing/extra scenario receipts")
    require(all(not row.get("error") for row in scenario_rows), f"{mode}: original scenario raised an error")
    clients = read(root / "client-receipts.json")
    require(isinstance(clients, list) and bool(clients), f"{mode}: no client receipt data")
    arms, event_files, artifact_files = [], [], []
    claimed_relative, claimed_resolved = {}, {}

    def claim(relative, owner):
        require(relative not in claimed_relative, f"{mode}: archive reference reused by {owner}")
        path = contained(root, relative)
        require(path == root / relative, f"{mode}: noncanonical resolved archive path: {relative}")
        require(path not in claimed_resolved, f"{mode}: archive alias reused by {owner}")
        claimed_relative[relative], claimed_resolved[path] = owner, owner
        artifact_files.append(relative)  # Preserve one claim per reference, never de-duplicate.
        return path

    for ordinal, row in enumerate(clients, 1):
        require(isinstance(row, dict) and type(row.get("ordinal")) is int
                and row["ordinal"] == ordinal and row.get("id") == f"receipt-{ordinal:06d}",
                f"{mode}: missing/duplicate/out-of-order client receipt")
        require(row.get("scenario") in names and row.get("mode") == mode, f"{mode}: unbound client receipt")
        require(row.get("exited") is True and row.get("flag_only_mutation_verified") is True,
                f"{mode}/{row['id']}: incomplete final config/exit receipt")
        require(not any(row.get(key) for key in ("init_error", "exit_error", "event_capture_error", "log_capture_error",
                                                "api_errors", "memory_errors")),
                f"{mode}/{row['id']}: constructor/exit/capture/API/poll error")
        expected_paths = archive_paths(row["id"], row.get("has_health"))
        require(all(row.get(role) == relative for role, relative in expected_paths.items()),
                f"{mode}/{row['id']}: noncanonical archive path for occurrence/role")
        paths = {role: claim(relative, f"{row['id']}/{role}") for role, relative in expected_paths.items()
                 if relative is not None}
        single = read(paths["ledger_file"])
        require(single == row, f"{mode}/{row['id']}: final ledger differs from receipt")
        original_path, final_path = paths["original_config"], paths["final_config"]
        original, final = read(original_path), read(final_path)
        require(file_sha256(original_path) == row.get("archived_original_config_sha256")
                and file_sha256(final_path) == row.get("archived_final_config_sha256"),
                f"{mode}/{row['id']}: archived config hash mismatch")
        expected, has_health, changed = quality_config(original, mode)
        require(final == expected and row.get("has_health") is has_health
                and row.get("changed_leaf_paths") == [list(path) for path in changed],
                f"{mode}/{row['id']}: changes extend beyond the quality flag")
        normalized = value_sha256(without_flag(final))
        require(normalized == row.get("normalized_final_sha256"), f"{mode}/{row['id']}: normalized hash mismatch")
        log = paths["client_log"]
        require(file_sha256(log) == row.get("client_log_sha256"), f"{mode}/{row['id']}: client log hash mismatch")
        item = {"receipt": row, "original_config": original, "final_config": final,
                "normalized_final_sha256": normalized, "raw_events": None, "semantic_events": []}
        if has_health:
            events = paths["events_file"]
            event_files.append(str(events.relative_to(root)))
            require(file_sha256(events) == row.get("events_sha256"), f"{mode}/{row['id']}: raw event hash mismatch")
            body = read(events)
            item["raw_events"], item["semantic_events"] = body, semantics(body)
            require(row.get("event_count") == len(body["events"]) and row.get("dropped") == body["dropped"],
                    f"{mode}/{row['id']}: event count/drop receipt mismatch")
        else:
            require(row.get("events_file") is None, f"{mode}/{row['id']}: no-health client falsely has quality events")
        arms.append(item)
    actual_files = sorted(str(path.relative_to(root)) for path in (root / "receipts").glob("*-events.json"))
    require(actual_files == sorted(event_files), f"{mode}: missing/extra event file set")
    actual_artifacts = sorted(str(path.relative_to(root)) for path in (root / "receipts").iterdir())
    require(actual_artifacts == sorted(artifact_files), f"{mode}: missing/extra config/log/ledger/event receipt files")
    bindings = []
    for scenario in scenario_rows:
        ids = scenario.get("client_receipt_ids")
        expected_ids = [row["id"] for row in clients if row["scenario"] == scenario["scenario"]]
        require(isinstance(ids, list) and ids == expected_ids,
                f"{mode}/{scenario['scenario']}: scenario/client receipt ownership/order mismatch")
        bindings.append({"scenario": scenario["scenario"], "client_receipt_ids": ids})
    scenario_clients = [receipt_id for row in bindings for receipt_id in row["client_receipt_ids"]]
    require(scenario_clients == [row["id"] for row in clients], f"{mode}: scenario/client receipt sets differ")
    require(bool(event_files) and any(item["semantic_events"] for item in arms), f"{mode}: no semantic health behavior data")
    return {"root": str(root), "manifest": manifest, "summary": summary, "report": reports,
            "scenario_receipts": scenario_rows, "clients": arms, "event_files": sorted(event_files),
            "artifact_files": sorted(artifact_files), "scenario_bindings": bindings,
            "terminal_sha256": file_sha256(terminal)}


def compare(on, off):
    errors, pairs = [], []
    for key in ("xray_sha256", "bench_source_sha256", "helper_sha256", "environment", "environment_sha256", "port_offset",
                "requested_scenarios", "planned_scenarios", "slow_requested", "python", "platform"):
        if on["manifest"].get(key) != off["manifest"].get(key) or key not in on["manifest"] or key not in off["manifest"]:
            errors.append(f"on/off execution input mismatch: {key}")
    if on["event_files"] != off["event_files"]:
        errors.append("on/off event file sets differ")
    if on.get("artifact_files") != off.get("artifact_files") or "artifact_files" not in on or "artifact_files" not in off:
        errors.append("on/off complete config/log/ledger/event file sets differ")
    if on.get("scenario_bindings") != off.get("scenario_bindings") or "scenario_bindings" not in on or "scenario_bindings" not in off:
        errors.append("on/off per-scenario client receipt bindings differ")
    if [row["scenario"] for row in on["report"]] != [row["scenario"] for row in off["report"]]:
        errors.append("on/off actual scenario sets/order differ")
    if len(on["clients"]) != len(off["clients"]):
        errors.append("on/off client receipt counts differ")
    for index in range(max(len(on["clients"]), len(off["clients"]))):
        left = on["clients"][index] if index < len(on["clients"]) else None
        right = off["clients"][index] if index < len(off["clients"]) else None
        if left is None or right is None:
            pairs.append({"ordinal": index + 1, "on": left, "off": right, "semantic_equal": False})
            continue
        a, b = left["receipt"], right["receipt"]
        identity = ("id", "ordinal", "scenario", "client_name", "has_health", "profile",
                    "ledger_file", "original_config", "final_config", "client_log", "events_file")
        same_identity = all(a.get(key) == b.get(key) for key in identity)
        same_config = (value_sha256(left["original_config"]) == value_sha256(right["original_config"])
                       and left["normalized_final_sha256"] == right["normalized_final_sha256"])
        sa, sb = left["semantic_events"], right["semantic_events"]
        semantic_a = [{"type": item["type"], "fields": item["fields"]} for item in sa]
        semantic_b = [{"type": item["type"], "fields": item["fields"]} for item in sb]
        equal = same_identity and same_config and value_sha256(semantic_a) == value_sha256(semantic_b)
        if not equal:
            errors.append(f"receipt {index + 1}: semantic/client/config mismatch")
        timing = []
        for position in range(max(len(sa), len(sb))):
            ta = sa[position] if position < len(sa) else None
            tb = sb[position] if position < len(sb) else None
            timing.append({"semantic_position": position + 1, "on": ta, "off": tb,
                           "off_minus_on_ts_ms": tb["ts"] - ta["ts"] if ta and tb else None,
                           "on_elapsed_ms": ta["ts"] - sa[0]["ts"] if ta else None,
                           "off_elapsed_ms": tb["ts"] - sb[0]["ts"] if tb else None,
                           "timing_is_gate": False})
        pairs.append({"ordinal": index + 1, "scenario": a["scenario"], "client_name": a["client_name"],
                      "same_identity": same_identity, "flag_only_configs_equal": same_config,
                      "config_mismatch_note": None if same_config else
                      "Non-comparable exact input configs; dynamic initialAnswer timestamps/tags remain unnormalized and this is a failure.",
                      "semantic_equal": equal, "on": left, "off": right, "raw_timing_differences": timing})
    coverage = [{"scenario": row["scenario"], "observed_health_clients": sum(
        item["receipt"]["scenario"] == row["scenario"] and item["receipt"]["has_health"] for item in on["clients"]),
        "note": "zero means original config/check gates only; no live health behavior equality claim"}
        for row in on["report"]]
    return {"accepted": not errors, "errors": errors, "pairs": pairs, "coverage": coverage,
            "on_inputs": {key: value for key, value in on.items() if key != "clients"},
            "off_inputs": {key: value for key, value in off.items() if key != "clients"},
            "note": "Exact ordered selected decision/state semantics plus original acceptance gates. All raw events/timestamps retained. No timing equality, timing tolerance, tag or randomness normalization."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--on", type=Path, required=True, help="existing on evidence directory")
    parser.add_argument("--off", type=Path, required=True, help="existing off evidence directory")
    parser.add_argument("--output", type=Path, required=True, help="new comparison JSON file outside both input directories")
    args = parser.parse_args()
    args.output = args.output.resolve()
    if args.output.exists():
        parser.error("comparison output must not already exist")
    for root in (args.on.resolve(), args.off.resolve()):
        if root == args.output or root in args.output.parents:
            parser.error("comparison output must be outside both immutable input directories")
    result = {"accepted": False, "errors": [], "on": str(args.on), "off": str(args.off)}
    try:
        result = compare(load_arm(args.on, "on"), load_arm(args.off, "off"))
    except Exception as error:
        result["errors"].append(repr(error))
        result["exception_traceback"] = traceback.format_exc()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    save(args.output, result)
    print(json.dumps({"accepted": result["accepted"], "errors": result["errors"], "output": str(args.output)}, indent=2))
    return 0 if result["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
