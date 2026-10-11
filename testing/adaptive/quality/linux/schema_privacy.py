#!/usr/bin/env python3
"""Exact Stage3 quality JSON shapes and provider privacy, for captured artifacts.

No import-time I/O or execution. Status legacy `network` is host-local and may
contain the raw host ID. Report quality and provider bodies must not expose it.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import sys

FIELDS = ("bytes_down", "bytes_down_overflow", "ttfb_count", "ttfb_p50_ms", "ttfb_p90_ms",
          "dial_count", "dial_p50_ms", "dial_p90_ms", "tcp_count", "tcp_rtt_p50_ms",
          "tcp_rtt_p90_ms", "tcp_rtt_min_ms", "bulk_count", "bulk_kbps_p50", "bulk_kbps_p90",
          "bulk_kbps_max", "bulk_rtt_p50_ms", "bulk_spliced_count", "rx_out_of_order_fraction",
          "tx_retransmit_fraction")
CAMEL = {field: field.split("_")[0] + "".join(part.title() for part in field.split("_")[1:]) for field in FIELDS}
SEGMENT_FIELDS = {"network_type", "network_operator", "is_network_known", "duration_ms", "paths",
                  "witness", "paths_dropped", "samples_dropped"}


def has_measurement(view, camel=False):
    if not isinstance(view, dict):
        return False
    count_fields = {"ttfb_count", "dial_count", "tcp_count", "bulk_count"}
    for field in FIELDS:
        if field == "bytes_down_overflow":
            continue
        value = view.get(CAMEL[field] if camel else field)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
            # BulkSplicedCount is presence-bearing: measured nonspliced=0.
            if value >= 0 and (field not in count_fields or value > 0):
                return True
    return False


def item(name, value, ok, limit):
    return {"check": name, "value": value, "ok": bool(ok), "limit": limit}


def quality_views(value, tag, path="$"):
    """Only status paths[].outbound.quality or report quality[].paths[tag]."""
    result = []
    if not isinstance(value, dict):
        return result
    paths = value.get("paths")
    if isinstance(paths, list):
        for index, row in enumerate(paths):
            if isinstance(row, dict) and row.get("outbound") == tag and isinstance(row.get("quality"), dict):
                result.append({"json_path": f"{path}.paths[{index}].quality", "view": row["quality"]})
    segments = value.get("quality")
    if isinstance(segments, list):
        for index, segment in enumerate(segments):
            if isinstance(segment, dict) and isinstance(segment.get("paths"), dict) and isinstance(segment["paths"].get(tag), dict):
                result.append({"json_path": f"{path}.quality[{index}].paths.{tag}", "view": segment["paths"][tag]})
    return result


def view_checks(view, name, camel=False):
    allowed = set(CAMEL.values()) if camel else set(FIELDS)
    bool_field = CAMEL["bytes_down_overflow"] if camel else "bytes_down_overflow"
    unexpected = sorted(set(view) - allowed) if isinstance(view, dict) else ["not an object"]
    checks = [item(f"{name} quality keys", unexpected, isinstance(view, dict) and not unexpected,
                   "exact quality scalar names; no addresses, sites, raw network fields")]
    if not isinstance(view, dict):
        return checks
    invalid = []
    for key, value in view.items():
        valid = isinstance(value, bool) if key == bool_field else (
            isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0)
        if not valid:
            invalid.append(key)
    checks.append(item(f"{name} quality scalar types", invalid, not invalid,
                       "finite nonnegative numbers; bytes overflow boolean; unavailable fields omitted"))
    checks.append(item(f"{name} quality has measurements", sorted(view), has_measurement(view, camel),
                       "known numeric measurement, including valid zero; empty/zero-count/flag-only object is insufficient"))
    return checks


def identity(raw_network_id):
    if raw_network_id.startswith("cell:"):
        parts = raw_network_id.split(":", 2)
        if len(parts) == 3 and len(parts[1]) in (5, 6) and parts[1].isascii() and parts[1].isdigit() and parts[2]:
            return "cell", parts[1], parts[2]
    prefix, sep, fingerprint = raw_network_id.partition(":")
    if sep and fingerprint and prefix in ("wifi", "eth", "other"):
        return prefix, None, fingerprint
    return "unknown", None, raw_network_id


def report_status_checks(status, report, tag, raw_network_id=None):
    checks = []
    status_views = quality_views(status, tag)
    report_views = quality_views(report, tag)
    checks.append(item("status paths[].outbound.quality", [v["json_path"] for v in status_views],
                       bool(status_views), "measured tag's snake_case quality object"))
    checks.append(item("report quality[].paths[tag]", [v["json_path"] for v in report_views],
                       bool(report_views), "measured tag in segmented snake_case quality"))
    for row in status_views + report_views:
        checks.extend(view_checks(row["view"], row["json_path"]))
    if isinstance(status, dict) and "witness_quality" in status:
        checks.extend(view_checks(status["witness_quality"], "status.witness_quality"))
    segments = report.get("quality") if isinstance(report, dict) else None
    checks.append(item("report segment bound", len(segments) if isinstance(segments, list) else None,
                       isinstance(segments, list) and len(segments) <= 4, "at most4 segments"))
    for index, segment in enumerate(segments if isinstance(segments, list) else []):
        prefix = f"report.quality[{index}]"
        unknown = sorted(set(segment) - SEGMENT_FIELDS) if isinstance(segment, dict) else ["not object"]
        checks.append(item(prefix + " keys", unknown, not unknown, "known metadata/paths/witness/drop fields"))
        if not isinstance(segment, dict):
            continue
        paths = segment.get("paths", {})
        checks.append(item(prefix + " path bound", len(paths) if isinstance(paths, dict) else None,
                           isinstance(paths, dict) and len(paths) <= 4, "at most4 paths"))
        for path_tag, view in paths.items() if isinstance(paths, dict) else []:
            checks.extend(view_checks(view, prefix + ".paths." + path_tag))
        if "witness" in segment:
            checks.extend(view_checks(segment["witness"], prefix + ".witness"))
        checks.append(item(prefix + " network metadata", segment.get("network_type"),
                           segment.get("network_type") in ("wifi", "cell", "eth", "other", "unknown")
                           and isinstance(segment.get("is_network_known"), bool)
                           and isinstance(segment.get("duration_ms"), int) and not isinstance(segment.get("duration_ms"), bool)
                           and segment["duration_ms"] >= 0, "network_type/known/duration schema"))
    if raw_network_id:
        kind, operator, fingerprint = identity(raw_network_id)
        checks.append(item("status network_type", status.get("network_type"), status.get("network_type") == kind, kind))
        raw_report = json.dumps(report, sort_keys=True)
        leaks = [marker for marker in (raw_network_id, fingerprint) if marker and marker in raw_report]
        checks.append(item("report raw network privacy", leaks, not leaks, "no host ID/fingerprint anywhere in report"))
        measured = [s for s in segments if isinstance(s, dict) and tag in s.get("paths", {})] if isinstance(segments, list) else []
        for segment in measured:
            checks.append(item("quality segment network_type", segment.get("network_type"), segment.get("network_type") == kind, kind))
            checks.append(item("quality segment operator", segment.get("network_operator"),
                               segment.get("network_operator") == operator if operator else "network_operator" not in segment,
                               operator or "omitted"))
    return checks


def provider_hash(url, value):
    # Exact existing Go providerHash(url,value), not SHA256(value) or a nonce.
    return hashlib.sha256((url + "\0" + value).encode()).hexdigest()[:16]


def provider_checks(body, url, raw_network_id, raw_run=None, require_quality=True, expected_tag=None):
    checks = []
    if not isinstance(body, dict):
        return [item("provider JSON body", type(body).__name__, False, "object required")]
    kind, operator, fingerprint = identity(raw_network_id)
    checks.append(item("provider networkType", body.get("networkType"), body.get("networkType") == kind, kind))
    checks.append(item("provider networkOperator", body.get("networkOperator"),
                       body.get("networkOperator") == operator if operator else "networkOperator" not in body,
                       operator or "omitted"))
    network = body.get("network")
    hashed = network.get("id") if isinstance(network, dict) else None
    expected = provider_hash(url, raw_network_id)
    checks.append(item("provider network.id hash", hashed, hashed == expected,
                       "sha256(providerURL + NUL + complete host ID).hex[:16]"))
    if raw_run:
        checks.append(item("provider run hash", body.get("run"), body.get("run") == provider_hash(url, raw_run),
                           "per-provider salted hash of report health_run"))
    payload = json.dumps(body, sort_keys=True)
    leaks = [marker for marker in (raw_network_id, fingerprint, raw_run) if marker and marker in payload]
    checks.append(item("provider raw identity privacy", leaks, not leaks,
                       "no host ID, fingerprint, raw health_run in POST body"))
    outbounds = body.get("outbounds")
    measured = []
    selected = []
    if isinstance(outbounds, list):
        for index, outbound in enumerate(outbounds):
            if isinstance(outbound, dict) and "quality" in outbound:
                measured.append(outbound.get("tag"))
                checks.extend(view_checks(outbound["quality"], f"provider.outbounds[{index}].quality", camel=True))
                if outbound.get("tag") == expected_tag and has_measurement(outbound["quality"], camel=True):
                    selected.append(index)
    checks.append(item("provider expected measured outbound quality", {"expected_tag": expected_tag,
                       "measured_tags": measured, "selected_rows": selected},
                       not require_quality or (expected_tag is not None and len(selected) == 1),
                       "exactly one meaningful camelCase quality object for the fixture's expected display tag"))
    if "witnessQuality" in body:
        checks.extend(view_checks(body["witnessQuality"], "provider.witnessQuality", camel=True))
    return checks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--tag", default="a1")
    parser.add_argument("--raw-network-id", required=True)
    parser.add_argument("--provider-bodies", type=Path)
    parser.add_argument("--provider-url")
    parser.add_argument("--provider-tag", help="Exact provider display tag of the measured fixture path")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.provider_bodies and (not args.provider_url or not args.provider_tag):
        parser.error("provider bodies require exact --provider-url and --provider-tag used by the fixture")
    status, report = json.loads(args.status.read_text()), json.loads(args.report.read_text())
    checks = report_status_checks(status, report, args.tag, args.raw_network_id)
    if args.provider_bodies:
        rows = [json.loads(line) for line in args.provider_bodies.read_text().splitlines() if line.strip()]
        checks.append(item("provider captures present", len(rows), bool(rows), "at leastone POST after measured traffic"))
        for row in rows:
            # The bench capture wrapper includes Authorization; validate only
            # the outbound POST body, not locally retained capture metadata.
            checks.extend(provider_checks(row.get("body"), args.provider_url, args.raw_network_id,
                                          report.get("health_run"), expected_tag=args.provider_tag))
    result = {"checks": checks, "ok": all(row["ok"] for row in checks),
              "provider_capture_checked": args.provider_bodies is not None,
              "note": "Status legacy raw network remains host-local; report/provider checks concern exported identities"}
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
