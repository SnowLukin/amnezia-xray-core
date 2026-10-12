#!/usr/bin/env python3
"""Untraced native Linux throughput, fixed 10 blocks x 4 attempts per transport.

Acceptance uses the legacy best-of-two wall-time winner, then the unchanged
wall and CPU <=2% gates. Raw attempts and monotonic results are always retained.
Comparisons are separate: plain/observed Stage0; quality off/on Stage4.
No Docker/build/network configuration is performed by this runner.
"""
import argparse
import copy
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time
import traceback
import uuid

sys.dont_write_bytecode = True
from cpu_clock import cpu_clock_sample
from schema_privacy import has_measurement, quality_views

ORDERS = ("POPO",) * 5 + ("OPOP",) * 5
ARM_NAMES = {"plain-observed": {"P": "plain", "O": "observed"},
             "quality-off-on": {"P": "quality_off", "O": "quality_on"}}


def save(path, item):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(item, indent=2, sort_keys=True) + "\n")
    tmp.replace(path)


def sha256(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            value.update(chunk)
    return value.hexdigest()


def proc_cpu(pid):
    raw = Path(f"/proc/{pid}/stat").read_text()
    fields = raw[raw.rfind(")") + 2:].split()
    ticks = os.sysconf("SC_CLK_TCK")
    return {"pid": pid, "raw_stat": raw, "utime_ticks": int(fields[11]),
            "stime_ticks": int(fields[12]), "starttime_ticks": int(fields[19]),
            "clock_ticks_per_second": ticks,
            "cpu_seconds": (int(fields[11]) + int(fields[12])) / ticks,
            "sample_wall_ns": time.time_ns(), "sample_monotonic_ns": time.monotonic_ns()}


def checked_linux():
    if sys.platform != "linux" or platform.machine() not in ("aarch64", "arm64"):
        raise RuntimeError("Untraced native Linux arm64 is required")
    if not Path("/.dockerenv").exists() or os.environ.get("XRAY_QUALITY_ISOLATED") != "1":
        raise RuntimeError("Use the dedicated Docker --network none container")
    links = json.loads(subprocess.run(["ip", "-j", "link"], check=True,
                                    capture_output=True, text=True).stdout)
    if [entry["ifname"] for entry in links] != ["lo"]:
        raise RuntimeError("Cost runner requires fresh none namespace with only lo")
    if links[0].get("mtu") != 65536:
        raise RuntimeError("Use a fresh namespace with default lo MTU; netem preflight namespace cannot be reused")
    qdiscs = subprocess.run(["tc", "qdisc", "show"], check=True, capture_output=True, text=True).stdout
    if "netem" in qdiscs or "tbf" in qdiscs or "clsact" in qdiscs:
        raise RuntimeError("Do not measure CPU in a shaped namespace")


def checked_bench(path):
    required = ("bench.py", "scenarios.py", "target.py", "chaos.py", "provider.py",
                "certs/example.crt", "certs/example.key", "certs/target.crt", "certs/target.key")
    missing = [name for name in required if not (path / name).is_file()]
    if missing:
        raise RuntimeError(f"Frozen bench/certs incomplete: {missing}")
    mounts = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields = line.split()
        root = Path(fields[4].replace("\\040", " "))
        if path == root or root in path.parents:
            mounts.append((len(root.parts), fields[5].split(",")))
    if not mounts or "ro" not in max(mounts)[1]:
        raise RuntimeError("--bench-dir requires a read-only frozen source bind mount")


def checked_arm64_elf(path):
    with Path(path).open("rb") as f:
        header = f.read(20)
    if len(header) < 20 or header[:4] != b"\x7fELF" or header[4:6] != bytes((2, 1)) or int.from_bytes(header[18:20], "little") != 183:
        raise RuntimeError(f"Native 64-bit little-endian AArch64 ELF required: {path}")


def command_metadata(argv):
    proc = subprocess.run(argv, capture_output=True, text=True, timeout=20)
    return {"argv": argv, "returncode": proc.returncode,
            "stdout": proc.stdout, "stderr": proc.stderr}


def config_pair(bench, tag, kind, comparison):
    common = bench.client_config([(tag, kind, 0)], {tag: 1},
                                 loglevel="debug" if kind == "reality" else bench.LOGLEVEL)
    if comparison == "plain-observed":
        return {"P": bench.plain(copy.deepcopy(common), tag), "O": common}, None
    common["healthObservatory"].setdefault("report", {})["quietShare"] = 1
    pair = {label: copy.deepcopy(common) for label in ("P", "O")}
    for label in ("P", "O"):
        pair[label]["healthObservatory"].setdefault("passive", {})["shouldSkipQuality"] = label == "P"
    normalized = []
    for label in ("P", "O"):
        cfg = copy.deepcopy(pair[label])
        del cfg["healthObservatory"]["passive"]["shouldSkipQuality"]
        normalized.append(cfg)
    if normalized[0] != normalized[1]:
        raise RuntimeError("quality comparison configs differ beyond passive.shouldSkipQuality")
    digest = hashlib.sha256(json.dumps(normalized[0], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return pair, digest


def block_checks(attempts, order, block_id, cpu_valid=True, comparison="plain-observed"):
    winners = {}
    for label in ("P", "O"):
        eligible = [a for a in attempts if a["label"] == label and a.get("complete")]
        if len(eligible) != 2:
            return {"block_id": block_id, "order": order, "comparison": comparison,
                    "arm_names": ARM_NAMES[comparison], "accepted": False,
                    "error": "Not exactly two completed attempts in each arm", "attempt_ids": [a["id"] for a in attempts]}
        winners[label] = min(eligible, key=lambda a: a["legacy_wall_seconds"])
    p, o = winners["P"], winners["O"]
    wall = (o["legacy_wall_seconds"] / p["legacy_wall_seconds"] - 1) * 100
    monotonic = (o["monotonic_seconds"] / p["monotonic_seconds"] - 1) * 100
    cpu = (o["cpu_seconds"] / p["cpu_seconds"] - 1) * 100 if cpu_valid and p["cpu_seconds"] is not None and p["cpu_seconds"] > 0 else None
    checks = [{"check": "no wall/monotonic clock discontinuity", "value": [a["clock_drift_seconds"] for a in attempts],
               "limit": "absolute drift <=0.03s", "ok": all(abs(a["clock_drift_seconds"]) <= 0.03 for a in attempts)}]
    if cpu_valid:
        increase_name = "quality enabled" if comparison == "quality-off-on" else "health enabled"
        checks[:0] = [{"check": f"legacy download slower with {increase_name}, %", "value": wall, "limit": "<=2", "ok": wall <= 2},
                      {"check": f"client CPU higher with {increase_name}, %", "value": cpu, "limit": "<=2",
                       "ok": cpu is not None and cpu <= 2}]
    if p["transport"] == "throughput_spliced":
        checks.append({"check": f"spliced {ARM_NAMES[comparison]['P']} vs {ARM_NAMES[comparison]['O']}", "value": [p["splices"], o["splices"]],
                       "limit": "1 vs 1", "ok": p["splices"] == o["splices"] == 1})
    checks.extend(check for a in attempts for check in a.get("report_checks", []))
    return {"block_id": block_id, "order": order, "comparison": comparison,
            "arm_names": ARM_NAMES[comparison], "attempt_ids": [a["id"] for a in attempts],
            "legacy_wall_winner_ids": {label: a["id"] for label, a in winners.items()},
            "legacy_wall_increase_percent": wall, "monotonic_increase_same_winners_percent": monotonic,
            "cpu_increase_percent": cpu, "cpu_valid": cpu_valid, "performance_gates_applied": cpu_valid,
            "checks": checks, "accepted": all(check["ok"] for check in checks)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bench-dir", type=Path, default=Path("/source/testing/adaptive"))
    parser.add_argument("--xray", type=Path, default=Path("/binaries/xray-base-linux-arm64"))
    parser.add_argument("--server-xray", type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--transport", choices=("throughput", "throughput_spliced", "both"), default="both")
    parser.add_argument("--size-mib", type=int, default=1024)
    parser.add_argument("--mode", choices=("acceptance", "poll-parity", "poll-off", "splice-proof"), default="acceptance")
    parser.add_argument("--comparison", choices=("plain-observed", "quality-off-on"), default="plain-observed")
    parser.add_argument("--actual-xray", type=Path, help="Wrapper's actual client binary; required for splice-proof")
    args = parser.parse_args()
    if args.size_mib != 1024 and args.mode != "splice-proof":
        parser.error("Standard acceptance/diagnostic series requires 1024 MiB; use splice-proof for16MiB")
    if args.mode == "splice-proof" and (args.size_mib != 16 or args.actual_xray is None):
        parser.error("splice-proof requires --size-mib16 and --actual-xray actual binary")
    checked_linux()
    args.bench_dir = args.bench_dir.resolve()
    checked_bench(args.bench_dir)
    args.xray = args.xray.resolve()
    server = (args.server_xray or args.actual_xray or args.xray).resolve()
    checked_arm64_elf(server)
    checked_arm64_elf(args.actual_xray if args.mode == "splice-proof" else args.xray)
    os.environ["BENCH_SERVER_BIN"] = str(server)
    os.environ["BENCH_PORT_OFFSET"] = "0"
    sys.path.insert(0, str(args.bench_dir.resolve()))
    import bench
    # Public foreign-PID process CPU clock, not jiffies or a tracer's CPU.
    # Keep /proc stat independently as raw legacy evidence below.
    bench.LocalProcess.cpu_seconds = lambda self: cpu_clock_sample(self.proc.pid)["cpu_seconds"]
    if args.mode == "poll-parity":
        def poll_parity(self):
            while not self._stop.wait(1.0):
                try:
                    heap, gor = self.memory()
                    self.mem.append((time.time(), heap, gor))
                    if self.has_health:
                        self.drain()
                    else:
                        self.api("events")  # Same API launch cadence; service may reject for plain.
                except Exception:
                    pass
        bench.Client._poll = poll_parity
    elif args.mode == "poll-off":
        bench.Client._poll = lambda self: self._stop.wait()
    run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:10]
    output = args.output.resolve() / run_id
    output.mkdir(parents=True, exist_ok=False)
    stand_dir = output / "stand"
    stand_dir.mkdir()
    cpu_valid = args.mode != "splice-proof"
    selected = ["throughput", "throughput_spliced"] if args.transport == "both" else [args.transport]
    if args.mode == "splice-proof":
        selected = ["throughput_spliced"]
    schedule = []
    for transport in selected:
        for block_number, order in enumerate(ORDERS if cpu_valid else ("POPO",), 1):
            for attempt_number, label in enumerate(order, 1):
                arm = ARM_NAMES[args.comparison][label]
                # Keep legacy Stage0 attempt IDs; Stage4 IDs are explicit and separate.
                name_suffix = label if args.comparison == "plain-observed" else arm
                schedule.append({"id": f"{transport}-block{block_number:02d}-attempt{attempt_number}-{name_suffix}",
                                 "transport": transport, "block": block_number,
                                 "order": order, "attempt_number": attempt_number, "label": label,
                                 "arm": arm, "comparison": args.comparison})
    manifest = {"run_id": run_id, "output_runtime_path": str(output), "mode": args.mode, "acceptance_series": args.mode == "acceptance",
                "comparison": args.comparison, "arm_names": ARM_NAMES[args.comparison],
                "quality_only_config_difference": "healthObservatory.passive.shouldSkipQuality" if args.comparison == "quality-off-on" else None,
                "cpu_valid": cpu_valid, "size_bytes": args.size_mib * (1 << 20),
                "source_commit": args.source_commit, "bench_dir": str(args.bench_dir),
                "xray": str(args.xray), "xray_sha256": sha256(args.xray),
                "actual_xray": str(args.actual_xray) if args.actual_xray else None,
                "actual_xray_sha256": sha256(args.actual_xray) if args.actual_xray else None,
                "server_xray": str(server), "server_xray_sha256": sha256(server),
                "bench_source_hashes": {name: sha256(args.bench_dir / name) for name in
                                        ("bench.py", "scenarios.py", "target.py", "chaos.py", "provider.py")},
                "native_machine": platform.machine(), "kernel": platform.release(),
                "python": sys.version, "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
                "primary_cpu_method": "libc clock_getcpuclockid(clientPID) + clock_gettime; all process threads",
                "parallel_legacy_cpu_method": "/proc/clientPID/stat utime+stime / SC_CLK_TCK; not acceptance precision",
                "measurement_helper_sha256": {name: sha256(Path(__file__).with_name(name)) for name in
                                              ("throughput_runner.py", "cpu_clock.py", "schema_privacy.py")},
                "cpu_count": os.cpu_count(), "environment": {key: os.environ.get(key) for key in
                    ("BENCH_TLS", "BENCH_LOGLEVEL", "BENCH_CONCURRENT", "BENCH_SERVER_BIN", "BENCH_PORT_OFFSET", "GODEBUG")},
                "fixed_schedule": schedule, "winner_rule": "minimum legacy wall seconds within each P/O pair; CPU from same winner",
                "metadata": [command_metadata(argv) for argv in ([str(server), "version"], ["openssl", "version"],
                    ["tc", "-V"], ["ip", "-d", "link", "show", "lo"], ["ethtool", "-k", "lo"], ["tc", "qdisc", "show"])],
                "started_wall_ns": time.time_ns(), "started_monotonic_ns": time.monotonic_ns()}
    save(output / "manifest.json", manifest)
    print(f"output {output}", flush=True)
    attempts, blocks = [], []
    fatal = None
    try:
        with bench.Stand(str(server), str(stand_dir)) as stand:
            for plan in schedule:
                stand.reset()
                attempt_dir = output / plan["id"]
                attempt_dir.mkdir()
                record = dict(plan, complete=False, cpu_valid=cpu_valid, size_bytes=manifest["size_bytes"],
                              started_wall_ns=time.time_ns(), started_monotonic_ns=time.monotonic_ns())
                kind = "tcp_tls" if plan["transport"] == "throughput" else "reality"
                tag = "b1" if kind == "tcp_tls" else "a1"
                pair, normalized_digest = config_pair(bench, tag, kind, args.comparison)
                cfg = pair[plan["label"]]
                record["config_sha256"] = hashlib.sha256(json.dumps(cfg, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                if args.comparison == "quality-off-on":
                    record["config_equal_except_skip_quality_verified"] = True
                    record["normalized_config_without_skip_quality_sha256"] = normalized_digest
                    record["should_skip_quality"] = plan["label"] == "P"
                client = bench.Client(str(args.xray), copy.deepcopy(cfg), str(attempt_dir), plan["id"])
                # Preserve the exact container/runtime argv path. The proof may
                # later be evaluated on the host through a different mount path.
                record["client_config_runtime_path"] = str(Path(client.path).resolve())
                status_after = None
                try:
                    with client:
                        record["pid"] = client.process.proc.pid
                        record["cpu_legacy_start"] = proc_cpu(record["pid"]) if cpu_valid else None
                        record["cpu_start"] = cpu_clock_sample(record["pid"]) if cpu_valid else None
                        wall0, mono0 = time.time_ns(), time.monotonic_ns()
                        got = bench.download("t1.test", f"/bytes/{manifest['size_bytes']}", timeout=240,
                                             tls=(kind == "reality" or bench.TLS))
                        wall1, mono1 = time.time_ns(), time.monotonic_ns()
                        record["cpu_end"] = cpu_clock_sample(record["pid"]) if cpu_valid else None
                        record["cpu_legacy_end"] = proc_cpu(record["pid"]) if cpu_valid else None
                        record.update(download_started_wall_ns=wall0, download_ended_wall_ns=wall1,
                                      download_started_monotonic_ns=mono0, download_ended_monotonic_ns=mono1,
                                      legacy_wall_seconds=(wall1-wall0)/1e9, monotonic_seconds=(mono1-mono0)/1e9,
                                      clock_drift_seconds=((wall1-wall0)-(mono1-mono0))/1e9, transferred_bytes=got)
                        if cpu_valid:
                            if record["cpu_legacy_start"]["starttime_ticks"] != record["cpu_legacy_end"]["starttime_ticks"]:
                                raise RuntimeError("PID reuse during CPU sample")
                            record["cpu_nanoseconds"] = record["cpu_end"]["cpu_nanoseconds"] - record["cpu_start"]["cpu_nanoseconds"]
                            if record["cpu_nanoseconds"] <= 0:
                                raise RuntimeError("Primary process CPU delta is not positive")
                            record["cpu_seconds"] = record["cpu_nanoseconds"] / 1e9
                            record["cpu_legacy_seconds"] = record["cpu_legacy_end"]["cpu_seconds"] - record["cpu_legacy_start"]["cpu_seconds"]
                            record["cpu_resolution_nanoseconds"] = max(record["cpu_start"]["clock_getres_nanoseconds"], record["cpu_end"]["clock_getres_nanoseconds"])
                        else:
                            record["cpu_seconds"] = None
                        if got != manifest["size_bytes"]:
                            raise RuntimeError(f"downloaded {got} of {manifest['size_bytes']}")
                        # Post-measurement snapshots: never included in measured wall/CPU interval.
                        if client.has_health:
                            status_after = client.status()
                            save(attempt_dir / "status.json", status_after)
                    record["splices"] = client.splices()
                    record["direct_bytes"] = client.direct_bytes
                    record["poll_memory_samples"] = client.mem
                    if args.comparison == "quality-off-on":
                        views = [view for snapshot in [status_after, *client.reports]
                                 for view in quality_views(snapshot, tag)]
                        present = any(has_measurement(view["view"]) for view in views)
                        has_object = bool(views)
                        expected_present = plan["label"] == "O"
                        record["quality_observation_present"] = present
                        record["quality_object_present"] = has_object
                        record["quality_snapshot_paths"] = [view["json_path"] for view in views]
                        record["comparison_checks"] = [{"check": f"{plan['arm']} measurement switch honored",
                            "value": {"has_object": has_object, "has_measurement": present},
                            "limit": "meaningful measurement" if expected_present else "no quality object",
                            "ok": present if expected_present else not has_object}]
                    record["complete"] = True
                except BaseException as error:
                    record["error"] = repr(error)
                    record["traceback"] = traceback.format_exc()
                    proc = getattr(client.process, "proc", None)
                    if proc is not None and proc.poll() is None:
                        client.process.stop()
                    if isinstance(error, (KeyboardInterrupt, SystemExit)):
                        raise
                finally:
                    for path in (Path(stand.target_log), Path(stand.provider_log), Path(stand.provider_bodies_log)):
                        if path.exists():
                            shutil.copyfile(path, attempt_dir / path.name)
                    record["report_checks"] = bench.pop_report_checks() + record.get("comparison_checks", [])
                    record["ended_wall_ns"] = time.time_ns()
                    record["ended_monotonic_ns"] = time.monotonic_ns()
                    save(attempt_dir / "attempt.json", record)
                    attempts.append(record)
                    save(output / "attempts.json", attempts)
                print(f"attempt {plan['id']} complete={record['complete']} wall={record.get('legacy_wall_seconds')} cpu={record.get('cpu_seconds')}", flush=True)
                if plan["attempt_number"] == 4:
                    block = block_checks(attempts[-4:], plan["order"], f"{plan['transport']}-{plan['block']:02d}", cpu_valid, args.comparison)
                    blocks.append(block)
                    save(output / "blocks.json", blocks)
                    print(f"block {block['block_id']} accepted={block['accepted']}", flush=True)
    except BaseException as error:
        fatal = {"error": repr(error), "traceback": traceback.format_exc()}
    finished = {"run_id": run_id, "mode": args.mode, "acceptance_series": args.mode == "acceptance",
                "comparison": args.comparison, "arm_names": ARM_NAMES[args.comparison],
                "cpu_valid": cpu_valid, "attempts_planned": len(schedule), "attempts_finished": len(attempts),
                "blocks_planned": len(schedule)//4, "blocks_finished": len(blocks),
                "all_blocks_passed": len(blocks) == len(schedule)//4 and all(b["accepted"] for b in blocks),
                "failed_block_ids": [b["block_id"] for b in blocks if not b["accepted"]], "fatal": fatal,
                "finished_wall_ns": time.time_ns(), "finished_monotonic_ns": time.monotonic_ns()}
    save(output / "summary.json", finished)
    print(json.dumps(finished, indent=2), flush=True)
    if args.mode == "splice-proof":
        # The proof is evaluated by splice_proof.py; CPU gates cannot be valid under strace.
        return 0 if fatal is None and len(attempts) == len(schedule) and all(a["complete"] for a in attempts) else 1
    return 0 if fatal is None and finished["all_blocks_passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
