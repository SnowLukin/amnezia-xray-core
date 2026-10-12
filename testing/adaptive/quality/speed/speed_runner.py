#!/usr/bin/env python3
"""Quality-signals section 5 speed fixture; execute only after execution GO.

Source preparation is safe to inspect: bench import, subprocesses and network
operations happen only from main(). Requires the same worktree's integrated
quietShare/quality exports, existing bench certificates, and its optional
BENCH_CHAOS_SCRIPT override. Never builds binaries or changes testing sources.

The fixed fixture is a1/tcp_tls at 2,000,000 bit/s, 8 MiB of plain HTTP, followed
by 16 MiB through an explicitly configured direct witness. The same fixture is
repeated with shouldSkipQuality=true. CPU samples are diagnostic labels, not
the separate <=2% CPU-cost acceptance benchmark. Linux direct splice can make
the enabled case not_comparable; it must never become a speed-comparison pass.
"""
import argparse
import datetime
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
import traceback
import urllib.request
import uuid

sys.dont_write_bytecode = True

# Installed at <repo>/testing/adaptive/quality/speed/speed_runner.py.
WORKTREE = Path(__file__).resolve().parents[4]
BENCH_DIR = WORKTREE / "testing" / "adaptive"
RATE_PROXY = Path(__file__).resolve().with_name("quality_rate_proxy.py")
MIB = 1024 * 1024
RATE_BITS_PER_SECOND = 2_000_000
PATH_BYTES = 8 * MIB
WITNESS_BYTES = 16 * MIB
PATH_HOST = "t1.test"
WITNESS_HOST = "witness.test"
PATH_KBPS_MIN, PATH_KBPS_MAX = 1800, 2200  # Fixed ±10%; never tuned after a run.
WITNESS_MIN_KBPS, WITNESS_MIN_RATIO = 20_000, 10
TRANSFER_TIMEOUT = 90
FINISHED_TIMEOUT = 8
SPEED_BOUNDS = (128, 256, 512, 1000, 2000, 3000, 5000, 8000, 12000,
                20000, 30000, 50000, 80000, 120000, 200000, 300000, 500000)
MEASUREMENT_KEYS = {
    "bytes_down", "bytes_down_overflow", "ttfb_count", "ttfb_p50_ms", "ttfb_p90_ms",
    "dial_count", "dial_p50_ms", "dial_p90_ms", "tcp_count", "tcp_rtt_p50_ms",
    "tcp_rtt_p90_ms", "tcp_rtt_min_ms", "bulk_count", "bulk_kbps_p50", "bulk_kbps_p90",
    "bulk_kbps_max", "bulk_rtt_p50_ms", "bulk_spliced_count",
    "rx_out_of_order_fraction", "tx_retransmit_fraction",
}


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def stamp():
    return {"wall_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns()}


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(MIB), b""):
            digest.update(chunk)
    return digest.hexdigest()


def check(name, value, ok, limit):
    return {"check": name, "value": value, "ok": bool(ok), "limit": limit}


def numeric(value, key):
    candidate = value.get(key) if isinstance(value, dict) else None
    if isinstance(candidate, (int, float)) and not isinstance(candidate, bool) and math.isfinite(candidate):
        return candidate
    return None


def at_least(value, key, minimum):
    candidate = numeric(value, key)
    return candidate is not None and candidate >= minimum


def status_path(status):
    return next((item for item in status.get("paths", []) if item.get("outbound") == "a1"), {})


def status_quality(status):
    return status_path(status).get("quality", {}), status.get("witness_quality", {})


def report_quality(report):
    segments = report.get("quality", []) if isinstance(report, dict) else []
    if not isinstance(segments, list):
        raise RuntimeError("report quality must be a segment list")
    return [(segment.get("paths", {}).get("a1", {}), segment.get("witness", {}))
            for segment in segments]


def measured_keys(view):
    return sorted(MEASUREMENT_KEYS & view.keys()) if isinstance(view, dict) else []


def absent_measurements(status, report):
    views = [item.get("quality", {}) for item in status.get("paths", [])]
    views.append(status.get("witness_quality", {}))
    for segment in report.get("quality", []) if isinstance(report, dict) else []:
        views.extend(segment.get("paths", {}).values())
        views.append(segment.get("witness", {}))
    return [measured_keys(view) for view in views if measured_keys(view)]


def speed_bucket(raw_kbps):
    return next((bound for bound in SPEED_BOUNDS if raw_kbps <= bound), SPEED_BOUNDS[-1])


def bulk_finished(view, body_bytes, allow_splice=False):
    if not at_least(view, "bytes_down", body_bytes):
        return False
    if at_least(view, "bulk_count", 1) and numeric(view, "bulk_kbps_max") is not None:
        return True
    return allow_splice and at_least(view, "bulk_spliced_count", 1)


def preflight(args):
    if platform.system() not in ("Darwin", "Linux"):
        raise RuntimeError("this local fixture supports macOS and Linux only")
    if os.environ.get("BENCH_CLIENT") or os.environ.get("BENCH_TLS") == "1":
        raise RuntimeError("local client and a plain HTTP target are required")
    if os.environ.get("BENCH_HOST", "127.0.0.1") != "127.0.0.1":
        raise RuntimeError("BENCH_HOST must be 127.0.0.1 for this local fixture")
    if os.environ.get("BENCH_LISTEN", "127.0.0.1") != "127.0.0.1":
        raise RuntimeError("BENCH_LISTEN must be 127.0.0.1")
    if not 1000 <= args.port_offset <= 44000:
        raise RuntimeError("choose a separate --port-offset in [1000, 44000]")
    required = ["bench.py", "chaos.py", "target.py", "provider.py", "certs/example.crt",
                "certs/example.key", "certs/target.crt", "certs/target.key"]
    missing = [name for name in required if not (BENCH_DIR / name).is_file()]
    if missing or not RATE_PROXY.is_file():
        raise RuntimeError(f"existing bench/certs/shaper required; missing={missing}, shaper={RATE_PROXY.is_file()}")
    feature_sources = {
        "BENCH_CHAOS_SCRIPT": BENCH_DIR / "bench.py",
        'json:"quietShare"': WORKTREE / "infra/conf/health.go",
        'json:"witness_quality': WORKTREE / "app/observatory/health/status.go",
        'json:"quality': WORKTREE / "app/observatory/health/report.go",
    }
    absent = [token for token, path in feature_sources.items() if token not in path.read_text()]
    if absent:
        raise RuntimeError(f"pending source integration; stand will not start: {absent}")
    for binary in (args.xray, args.server_xray):
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise RuntimeError(f"existing executable binary required: {binary}")
    # Module constants are read at import time, before Stand or Client exists.
    os.environ["BENCH_PORT_OFFSET"] = str(args.port_offset)
    os.environ["BENCH_TLS"] = "0"
    os.environ["BENCH_HOST"] = "127.0.0.1"
    os.environ["BENCH_LISTEN"] = "127.0.0.1"
    os.environ["BENCH_SERVER_BIN"] = str(args.server_xray)
    os.environ["BENCH_CHAOS_SCRIPT"] = str(RATE_PROXY)
    sys.path.insert(0, str(BENCH_DIR))
    bench = importlib.import_module("bench")
    if Path(bench.__file__).resolve() != BENCH_DIR / "bench.py":
        raise RuntimeError("imported bench must be from this same worktree")
    chaos_script = getattr(bench, "CHAOS_SCRIPT", None)
    if not chaos_script or Path(chaos_script).resolve() != RATE_PROXY:
        raise RuntimeError("bench.CHAOS_SCRIPT must honor BENCH_CHAOS_SCRIPT before Stand starts")
    return bench


def modes_and_counters(bench, output, label):
    raw = urllib.request.urlopen(f"http://127.0.0.1:{bench.CHAOS_CONTROL}/set", timeout=3).read().decode()
    (output / f"server-{label}.txt").write_text(raw)
    modes = {}
    for line in raw.strip().splitlines():
        port, mode, *_ = line.split()
        modes[int(port)] = mode
    result = {"label": label, **stamp(), "modes": modes,
              "counters": bench.chaos_stats(), "raw_file": f"server-{label}.txt"}
    save(output / f"server-{label}.json", result)
    return result


def cpu_sample(client):
    # Keep the unmodified coarse ps text alongside the helper's seconds value.
    proc = subprocess.run(["ps", "-o", "time=", "-p", str(client.process.proc.pid)],
                          capture_output=True, text=True, timeout=5)
    value = None
    error = None
    try:
        value = client.cpu_seconds()
    except Exception as exc:
        error = repr(exc)
    return {**stamp(), "pid": client.process.proc.pid, "cpu_seconds": value,
            "source": "bench.LocalProcess.cpu_seconds: ps cumulative process CPU time",
            "raw_ps_time": proc.stdout, "raw_ps_stderr": proc.stderr, "ps_returncode": proc.returncode,
            "error": error, "precision_label": "ps display is coarse; diagnostic only",
            "acceptance_label": "not a CPU-cost benchmark; no <=2% assertion"}


def transfer(bench, client, output, label, host, want):
    before_cpu = cpu_sample(client)
    before = stamp()
    socket = bench.open_stream(host, TRANSFER_TIMEOUT, tls=False)
    if socket is None:
        raise RuntimeError(f"SOCKS refused {label}")
    try:
        request = f"GET /bytes/{want} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
        socket.sendall(request.encode("ascii"))
        head, body = bench.read_answer(socket, time.time() + TRANSFER_TIMEOUT)
    finally:
        socket.close()  # Observer Finished/end aggregation follows asynchronously.
    after = stamp()
    after_cpu = cpu_sample(client)
    seconds = (after["monotonic_ns"] - before["monotonic_ns"]) / 1e9
    ok = head.startswith(b"HTTP/1.1 200") and body == want
    record = {"label": label, "host": host, "path": f"/bytes/{want}", "tls": False,
              "before": before, "after": after, "body_bytes": body, "expected_body_bytes": want,
              "response_headers": head.decode("ascii", errors="replace"), "ok": ok,
              "seconds_including_dial": seconds, "body_kbps_including_dial": body * 8 / seconds / 1000,
              "cpu_before": before_cpu, "cpu_after": after_cpu,
              "cpu_delta_seconds": after_cpu["cpu_seconds"] - before_cpu["cpu_seconds"]
              if before_cpu["cpu_seconds"] is not None and after_cpu["cpu_seconds"] is not None else None,
              "server_after": modes_and_counters(bench, output, label)}
    save(output / f"transfer-{label}.json", record)
    if not ok:
        raise RuntimeError(f"incomplete {label}: received {body}/{want}, head={head[:100]!r}")
    return record


def no_influence_checks(bench, client, stand, since, result):
    events = sorted(client.events, key=lambda item: item["seq"])
    counts, decided = bench.fold_report(events)
    probes = [event for event in events if event["type"] in ("probe_start", "probe_result", "probe_request")]
    downs = [event for event in events if event["type"] == "path_state"
             and event.get("fields", {}).get("state") in ("down", "suspect")]
    switches = [{key: event["fields"].get(key) for key in ("balancer", "from", "to", "reason", "cost")}
                for event in events if event["type"] == "switch"]
    probe_hits = stand.probe_hits(since)
    result["decision_signature"] = {"decided": decided, "switches": switches, "downs": downs,
                                    "probes": probes, "active": result.get("status_after", {}).get("balancers", [])}
    result["checks"] += [
        check("no routing decisions", decided, not decided, "false; start switch is not a decision"),
        check("no path down/suspect", downs, not downs, "none"),
        check("no probe events", probes, not probes, "none; user traffic supplies all speed samples"),
        check("no probe requests at target", probe_hits, not probe_hits, "none"),
        check("no event loss", client.dropped, client.dropped == 0, "0"),
    ]
    result["event_counts"] = counts
    return events


def validate_enabled(result):
    path, witness = status_quality(result["status_after"])
    result["path_quality"], result["witness_quality"] = path, witness
    raw = numeric(path, "bulk_kbps_max")
    spliced = numeric(witness, "bulk_spliced_count")
    witness_rate = numeric(witness, "bulk_kbps_max")
    witness_has_no_rate = all(numeric(witness, key) is None
                              for key in ("bulk_kbps_max", "bulk_kbps_p50", "bulk_kbps_p90"))
    linux_splice = (platform.system() == "Linux" and spliced is not None and spliced > 0
                    and witness_has_no_rate and numeric(witness, "bulk_count") in (None, 0))
    result["checks"] += [
        check("path end bulk sample", path, bulk_finished(path, PATH_BYTES), "bytes_down>=8 MiB, bulk_count>=1, raw max present"),
        check("path has one completed bulk connection", numeric(path, "bulk_count"),
              numeric(path, "bulk_count") == 1, "one 8 MiB request"),
        check("one path bulk sample per completed transfer", numeric(path, "bulk_count"),
              numeric(path, "bulk_count") == 1, "1"),
        check("path raw bulk_kbps_max near fixed 2 Mbit/s", raw,
              raw is not None and PATH_KBPS_MIN <= raw <= PATH_KBPS_MAX, "1800..2200 Kbit/s fixed upfront"),
        check("path bulk sample unspliced", numeric(path, "bulk_spliced_count"),
              numeric(path, "bulk_spliced_count") == 0, "present observed zero"),
        check("path p90 equals its raw sample's upper bucket", numeric(path, "bulk_kbps_p90"),
              raw is not None and numeric(path, "bulk_kbps_p90") == speed_bucket(raw),
              "one sample; 2000 or 3000 can both be correct around 2 Mbit/s"),
    ]
    snapshots = report_quality(result["snapshot_after"])
    result["checks"].append(check("snapshot includes same completed path and witness", snapshots,
        any(p == path and w == witness for p, w in snapshots), "scalar quality matches current network Status"))
    if linux_splice:
        result["comparison"] = "not_comparable"
        result["comparison_reason"] = "Linux direct witness bulk was spliced: bytes observed, speed absent"
        result["checks"].append(check("Linux witness splice is explicitly incomparable", witness,
            bulk_finished(witness, WITNESS_BYTES, allow_splice=True)
            and numeric(witness, "bulk_spliced_count") == 1,
            "bytes_down>=16 MiB and one spliced bulk; no synthetic rate"))
    else:
        result["comparison"] = "comparable"
        result["checks"] += [
            check("witness end bulk sample", witness, bulk_finished(witness, WITNESS_BYTES), "bytes_down>=16 MiB and raw max present"),
            check("witness has one completed bulk connection", numeric(witness, "bulk_count"),
                  numeric(witness, "bulk_count") == 1, "one 16 MiB request"),
            check("one witness bulk sample per completed transfer", numeric(witness, "bulk_count"),
                  numeric(witness, "bulk_count") == 1, "1"),
            check("witness bulk unspliced", spliced, spliced == 0, "present observed zero"),
            check("direct witness much faster", {"path_kbps": raw, "witness_kbps": witness_rate},
                  raw is not None and witness_rate is not None and witness_rate >= WITNESS_MIN_KBPS
                  and witness_rate >= WITNESS_MIN_RATIO * raw, ">=20000 Kbit/s and >=10x path raw max"),
        ]


def run_case(bench, stand, args, output, name, skip):
    output.mkdir()
    stand.reset()
    port = bench.address_port("tcp_tls", 0)
    # All addresses pass before shaping, then the a1 address is capped. The
    # wrapper gives each direction an independent budget; upload is just HTTP.
    # This precedes Client.__enter__, hence precedes its first TLS connection.
    bench.chaos(str(port), f"rate:{RATE_BITS_PER_SECOND}")
    configured = modes_and_counters(bench, output, "before-client")
    expected_modes = {p: f"rate:{RATE_BITS_PER_SECOND}" if p == port else "pass"
                      for p in bench.all_address_ports()}
    modes_ok = all(configured["modes"].get(p) == mode for p, mode in expected_modes.items())
    if not modes_ok:
        raise RuntimeError("a1 rate and all other pass modes must be set before Client starts")
    config = bench.client_config([("a1", "tcp_tls", 0)], {"a1": 1},
        health={"passive": {"shouldSkipQuality": skip}, "connectivity": {"witnesses": ["direct"]},
                "report": {"interval": "0s", "quietShare": 1}},
        strategy={"startOutbound": "a1", "shouldProbeBeforeSwitch": False}, loglevel="debug")
    config["dns"]["hosts"][WITNESS_HOST] = "127.0.0.1"
    config["routing"]["rules"].insert(0, {"inboundTag": ["socks"], "domain": [f"full:{WITNESS_HOST}"],
                                        "outboundTag": "direct"})
    save(output / "config.json", config)
    result = {"case": name, "should_skip_quality": skip, "checks": [], "transfers": [], "error": None,
              "configured_modes": configured, "status_after": {}, "snapshot_after": None,
              "comparison": "disabled_control" if skip else "pending"}
    client = bench.Client(str(args.xray), config, str(output), name)
    since = time.time()
    try:
        with client:
            save(output / "client-process.json", {"pid": client.process.proc.pid, **stamp()})
            save(output / "status-before.json", client.status())
            save(output / "snapshot-before.json", client.api("report"))
            result["transfers"].append(transfer(bench, client, output, "a1", PATH_HOST, PATH_BYTES))
            result["transfers"].append(transfer(bench, client, output, "direct-witness", WITNESS_HOST, WITNESS_BYTES))
            body_rate = result["transfers"][0]["body_kbps_including_dial"]
            result["checks"].append(check("same fixed shaped transfer also observed by client", body_rate,
                1600 <= body_rate <= PATH_KBPS_MAX, "1600..2200 Kbit/s including TLS dial; fixed upfront"))
            polls = []
            deadline = time.monotonic() + FINISHED_TIMEOUT
            while True:
                status = client.status()
                snapshot = client.api("report")
                direct = client.outbound_bytes("direct")
                path, witness = status_quality(status)
                ordinary_done = at_least(status_path(status), "bytes_down", PATH_BYTES) and direct[1] >= WITNESS_BYTES
                ready = ordinary_done and (skip or (bulk_finished(path, PATH_BYTES)
                    and bulk_finished(witness, WITNESS_BYTES, allow_splice=platform.system() == "Linux")))
                polls.append({**stamp(), "ready": ready, "ordinary_done": ordinary_done,
                              "status": status, "snapshot": snapshot, "direct_outbound_bytes": direct})
                if ready or time.monotonic() >= deadline:
                    break
                time.sleep(0.2)
            save(output / "finished-polls.json", polls)
            result["status_after"], result["snapshot_after"] = status, snapshot
            result["direct_outbound_bytes"] = direct
            save(output / "status-after.json", status)
            save(output / "snapshot-after.json", snapshot)
            # Existing API is due-only. With interval=0s, null is valid and
            # DOES NOT flush a window. Save it, then let Client exit take final.
            taken = client.take_report()
            result["take_report"] = taken
            result["take_did_close_window"] = taken is not None
            save(output / "take-report.json", taken)
            after_take = client.status()
            save(output / "status-after-take.json", after_take)
            save(output / "snapshot-after-take.json", client.api("report"))
            result["checks"] += [
                check("both transfers reached Finished evidence", ready, ready, f"bounded {FINISHED_TIMEOUT}s after socket closure"),
                check("ordinary a1 transfer bytes", status_path(status), ordinary_done, "a1>=8 MiB and direct outbound>=16 MiB"),
                check("snapshot available for quiet run", snapshot is not None, snapshot is not None, "stage1 quietShare=1"),
                check("a1 remained selected and healthy", {"path_state": status_path(status).get("state"),
                      "balancers": status.get("balancers", [])}, status_path(status).get("state") == "up"
                      and any(item.get("active") == "a1" and item.get("is_exhausted") is False
                              for item in status.get("balancers", [])), "up and active a1, no exhaustion"),
                check("take leaves current-network quality intact", status_quality(after_take),
                      status_quality(after_take) == status_quality(status), "same scalar path and witness quality"),
            ]
            if skip:
                present = absent_measurements(status, snapshot)
                result["checks"].append(check("shouldSkipQuality omits all quality measurements", present,
                    not present, "absent/empty quality allowed; measured zero still forbidden"))
            else:
                validate_enabled(result)
            client.drain()
    except Exception as exc:
        result["error"] = repr(exc)
        (output / "exception.txt").write_text(traceback.format_exc())
        result["checks"].append(check("case completed", repr(exc), False, "transfers and evidence complete"))
        # A failed __enter__ never reaches __exit__; stop only our own child.
        if hasattr(client.process, "proc") and client.process.proc.poll() is None:
            client.process.stop()
    result["server_after"] = modes_and_counters(bench, output, "after-client")
    before_counters, after_counters = configured["counters"], result["server_after"]["counters"]
    deltas = {p: [after_counters[p][i] - before_counters[p][i] for i in range(5)]
              for p in bench.all_address_ports()}
    result["server_counter_deltas"] = deltas
    result["checks"] += [
        check("a1 proxy carried complete shaped download", deltas[port],
              deltas[port][0] >= 1 and deltas[port][2] >= PATH_BYTES, "connections>=1 and wire down>=8 MiB"),
        check("no traffic through other chaos addresses", {p: v for p, v in deltas.items() if p != port},
              all(not any(v) for p, v in deltas.items() if p != port), "all counter deltas zero; direct bypasses proxy"),
    ]
    result["events"] = no_influence_checks(bench, client, stand, since, result)
    result["reports"], result["final_report"] = client.reports, client.final_report
    save(output / "events.json", {"dropped": client.dropped, "events": result["events"]})
    save(output / "reports.json", client.reports)
    save(output / "final-report.json", client.final_report)
    result["checks"] += bench.pop_report_checks()
    result["checks"].append(check("quiet final selected", client.final_report,
        isinstance(client.final_report, dict) and client.final_report.get("is_quiet_sample") is True,
        "stage1 is_quiet_sample=true with no routing decisions"))
    if skip:
        present = absent_measurements(result["status_after"], client.final_report)
        result["checks"].append(check("skip final omits quality measurements", present, not present, "none"))
    else:
        final_quality = report_quality(client.final_report)
        path, witness = status_quality(result["status_after"])
        result["checks"].append(check("quiet final retains both completed quality samples", final_quality,
            any(p == path and w == witness for p, w in final_quality), "same scalar path and witness as Status"))
    (output / "target.log").write_text(Path(stand.target_log).read_text())
    result["checks"].append(check("all server modes unchanged through case", result["server_after"]["modes"],
        all(result["server_after"]["modes"].get(p) == mode for p, mode in expected_modes.items()), "a1 rate, every other port pass"))
    result["outcome"] = "failed" if any(not item["ok"] for item in result["checks"]) else (
        "not_comparable" if result["comparison"] == "not_comparable" else "passed")
    save(output / "result.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--xray", type=Path, required=True, help="already built candidate client binary")
    parser.add_argument("--server-xray", type=Path, help="existing server binary; defaults to --xray")
    parser.add_argument("--source-commit", required=True, help="source identity label; binary hashes are also saved")
    parser.add_argument("--port-offset", type=int, required=True, help="separate bench namespace, 1000..44000")
    parser.add_argument("--output", type=Path, required=True, help="parent of a new unique evidence directory")
    args = parser.parse_args()
    args.xray = args.xray.resolve()
    args.server_xray = (args.server_xray or args.xray).resolve()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:12]
    output = args.output / ("quality-speed-" + run_id)
    output.mkdir()
    metadata = {"run_id": run_id, **stamp(), "platform": platform.platform(), "machine": platform.machine(),
                "worktree": str(WORKTREE), "bench_dir": str(BENCH_DIR), "source_commit_label": args.source_commit,
                "client_binary": str(args.xray), "server_binary": str(args.server_xray),
                "port_offset": args.port_offset, "rate_bits_per_second": RATE_BITS_PER_SECOND,
                "path_body_bytes": PATH_BYTES, "witness_body_bytes": WITNESS_BYTES,
                "path_max_tolerance_kbps": [PATH_KBPS_MIN, PATH_KBPS_MAX],
                "witness_min_kbps": WITNESS_MIN_KBPS, "witness_min_ratio": WITNESS_MIN_RATIO,
                "rate_tuning": "none; all thresholds fixed in source before running",
                "cpu_acceptance": "diagnostic only; separate CPU-cost benchmark required"}
    save(output / "metadata.json", metadata)
    try:
        bench = preflight(args)
        metadata["file_sha256"] = {str(path): sha256(path) for path in
                                   (args.xray, args.server_xray, BENCH_DIR / "bench.py",
                                    BENCH_DIR / "chaos.py", BENCH_DIR / "target.py",
                                    RATE_PROXY, Path(__file__).resolve())}
        save(output / "metadata.json", metadata)
        results = []
        with bench.Stand(str(args.server_xray), str(output)) as stand:
            results.append(run_case(bench, stand, args, output / "enabled", "enabled", False))
            results.append(run_case(bench, stand, args, output / "skip-control", "skip-control", True))
        signatures = [item["decision_signature"] for item in results]
        # Compare choices/events, excluding elapsed status durations.
        signature = lambda value: {"decided": value["decided"], "switches": value["switches"],
                                    "downs": value["downs"], "probes": value["probes"]}
        same = signature(signatures[0]) == signature(signatures[1])
        comparison = check("enabled/skip fixture has identical routing decisions", signatures, same,
                           "same start switch, no down/suspect, no probes, no later switch")
        failed = not same or any(item["outcome"] == "failed" for item in results)
        outcome = "failed" if failed else ("not_comparable" if results[0]["outcome"] == "not_comparable" else "passed")
        save(output / "summary.json", {"outcome": outcome, "checks": [comparison], "cases": results,
                                      "finished": stamp()})
        print(json.dumps({"outcome": outcome, "output": str(output)}))
        return 1 if failed else (2 if outcome == "not_comparable" else 0)
    except Exception as exc:
        (output / "exception.txt").write_text(traceback.format_exc())
        save(output / "summary.json", {"outcome": "failed", "error": repr(exc), "finished": stamp()})
        print(json.dumps({"outcome": "failed", "error": repr(exc), "output": str(output)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
