#!/usr/bin/env python3
"""Stage4 quality evidence runner; run only after an explicit execution GO.

Requires a native arm64 Docker --network none namespace, CAP_NET_ADMIN and
XRAY_QUALITY_ISOLATED=1. Imports a frozen read-only bench via --bench-dir;
only --output receives configs, raw snapshots and reports. No execution or
network mutation happens at import time. This is not a CPU cost benchmark.
"""
import argparse
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import platform
import re
import socket
import statistics
import struct
import subprocess
import sys
import threading
import time
import traceback
import uuid

sys.dont_write_bytecode = True
from duplex_target import create_server
from schema_privacy import quality_views, report_status_checks

MIB = 1024 * 1024
TIME_BOUNDS = [10, 20, 30, 40, 50, 60, 80, 100, 125, 150, 200, 250,
               300, 400, 500, 750, 1000, 1500, 2000, 3000, 5000, 8000]
QUALITY_KEYS = {"dial_count", "tcp_count", "bulk_count", "tcp_rtt_min_ms",
                "rx_out_of_order_fraction", "tx_retransmit_fraction"}


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(MIB), b""):
            digest.update(chunk)
    return digest.hexdigest()


def native_binary(path):
    with Path(path).open("rb") as f:
        head = f.read(64)
    if len(head) < 20 or head[:6] != b"\x7fELF\x02\x01" or struct.unpack_from("<H", head, 18)[0] != 183:
        raise RuntimeError(f"native AArch64 ELF binary required: {path}")


def check(name, value, ok, limit):
    return {"check": name, "value": value, "ok": bool(ok), "limit": limit}


def command(argv, output, timeout=30):
    started = time.monotonic_ns()
    proc = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    record = {"argv": argv, "returncode": proc.returncode, "stdout": proc.stdout,
              "stderr": proc.stderr, "monotonic_ns": started, "wall_ns": time.time_ns()}
    with (output / "commands.jsonl").open("a") as f:
        f.write(json.dumps(record) + "\n")
    if proc.returncode:
        raise RuntimeError(f"command failed {argv!r}: {proc.stderr.strip()}")
    return proc.stdout


def tc(args, verb, output, profile=None):
    argv = [sys.executable, str(args.tc_helper), verb, "--port", str(args.port),
            "--output", str(output)]
    if profile is not None:
        argv += ["--profile", profile]
    return command(argv, output)


def load_bench(args):
    if platform.system() != "Linux" or platform.machine() not in ("aarch64", "arm64"):
        raise RuntimeError("native Linux arm64 is required")
    if not Path("/.dockerenv").exists() or os.environ.get("XRAY_QUALITY_ISOLATED") != "1":
        raise RuntimeError("dedicated isolated Docker namespace is required")
    if os.environ.get("BENCH_TLS") == "1" or os.environ.get("BENCH_CLIENT"):
        raise RuntimeError("this runner requires a local client and a plain target")
    args.bench_dir = args.bench_dir.resolve()
    # Refuse the cert generator's writable fallback before importing bench.
    required = ["bench.py", "target.py", "chaos.py", "provider.py", "certs/example.crt",
                "certs/example.key", "certs/target.crt", "certs/target.key"]
    missing = [name for name in required if not (args.bench_dir / name).is_file()]
    if missing:
        raise RuntimeError(f"frozen bench/certs incomplete: {missing}")
    mounts = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields = line.split()
        root = Path(fields[4].replace("\\040", " "))
        if args.bench_dir == root or root in args.bench_dir.parents:
            mounts.append((len(root.parts), fields[5].split(",")))
    if not mounts or "ro" not in max(mounts)[1]:
        raise RuntimeError("--bench-dir must be on a read-only bind mount")
    sys.path.insert(0, str(args.bench_dir))
    bench = importlib.import_module("bench")
    if bench.address_port("reality", 0) != args.port:
        raise RuntimeError("--port must match frozen bench REALITY address index0")
    return bench


def parse_ss(raw, pid, port):
    """Keep only the client process endpoint whose remote port is 21002.

    Missing optional ss fields remain None. ss commonly omits zero counters;
    absence is never interpreted as an observed zero or as a valid delta.
    """
    blocks = []
    for line in raw.splitlines():
        if line and not line[0].isspace() and not line.startswith("State"):
            blocks.append([line])
        elif blocks and line.strip():
            blocks[-1].append(line)
    records = []
    for block in blocks:
        joined = " ".join(block)
        cols = block[0].split()
        if len(cols) < 5 or not re.search(rf"\bpid={pid}\b", joined):
            continue
        local, remote = cols[3], cols[4]
        if not remote.endswith(f":{port}"):
            continue
        item = {"pid": pid, "local": local, "remote": remote, "raw": "\n".join(block)}
        for field in ("bytes_sent", "bytes_retrans", "data_segs_in", "rcv_ooopack",
                      "minrtt", "rcv_rtt"):
            match = re.search(rf"\b{field}:([0-9.]+)", joined)
            item[field] = float(match[1]) if match else None
        match = re.search(r"\brtt:([0-9.]+)/", joined)
        item["rtt"] = float(match[1]) if match else None
        records.append(item)
    return records


class Sampler:
    def __init__(self, args, output, pid):
        self.args, self.output, self.pid = args, output, pid
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.records = []
        self.errors = []
        self.sequence = 0

    def snapshot(self, label):
        with self.lock:
            self.sequence += 1
            name = f"ss-{self.sequence:05d}-{label}.txt"
            raw = command(["ss", "-tinep", f"( dport = :{self.args.port} )"], self.output)
            (self.output / name).write_text(raw)
            item = {"label": label, "raw_file": name, "wall_ns": time.time_ns(),
                    "monotonic_ns": time.monotonic_ns(),
                    "sockets": parse_ss(raw, self.pid, self.args.port)}
            self.records.append(item)
            save(self.output / "socket-mapping.json", self.records)
            return item

    def loop(self):
        next_tc = 0
        while not self.stop_event.is_set():
            try:
                self.snapshot("periodic")
                if time.monotonic() >= next_tc:
                    tc(self.args, "dump", self.output)
                    next_tc = time.monotonic() + 2
            except Exception as exc:
                self.errors.append(repr(exc))
            self.stop_event.wait(0.5)

    def __enter__(self):
        self.thread = threading.Thread(target=self.loop, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop_event.set()
        self.thread.join(timeout=35)
        if self.thread.is_alive():
            self.errors.append("sampler thread did not stop")
        save(self.output / "sampler-errors.json", self.errors)


def receive_exact(sock, size):
    data = bytearray()
    while len(data) < size:
        part = sock.recv(size - len(data))
        if not part:
            raise EOFError("SOCKS handshake ended early")
        data.extend(part)
    return bytes(data)


def open_plain_stream(bench, target_port, timeout):
    sock = socket.create_connection(("127.0.0.1", bench.SOCKS_PORT), timeout=timeout)
    try:
        sock.sendall(b"\x05\x01\x00")
        if receive_exact(sock, 2) != b"\x05\x00":
            raise RuntimeError("SOCKS authentication rejected")
        host = b"t1.test"
        sock.sendall(b"\x05\x01\x00\x03" + bytes([len(host)]) + host + struct.pack("!H", target_port))
        reply = receive_exact(sock, 4)
        if reply[:3] != b"\x05\x00\x00":
            raise RuntimeError(f"SOCKS connect rejected: {reply.hex()}")
        address_size = {1: 4, 4: 16}.get(reply[3])
        if reply[3] == 3:
            address_size = receive_exact(sock, 1)[0]
        if address_size is None:
            raise RuntimeError("unknown SOCKS address type")
        receive_exact(sock, address_size + 2)
        return sock
    except BaseException:
        sock.close()
        raise


def transfer(bench, args, sampler, target_port, upload, download, index):
    token = f"{sampler.output.name}-{index}-{uuid.uuid4().hex}"
    started = time.monotonic()
    deadline = started + args.timeout
    with open_plain_stream(bench, target_port, args.timeout) as sock:
        request = (f"POST /duplex?download={download} HTTP/1.1\r\nHost: t1.test\r\n"
                   f"Content-Length: {upload + 1}\r\nX-Experiment-Token: {token}\r\n"
                   "Connection: close\r\n\r\n").encode()
        sock.sendall(request)
        initial = bytearray()
        while b"\r\n\r\n" not in initial or not initial.split(b"\r\n\r\n", 1)[1]:
            sock.settimeout(max(0.001, deadline - time.monotonic()))
            chunk = sock.recv(65536)
            if not chunk:
                raise EOFError("target closed before initial response byte")
            initial.extend(chunk)
            if len(initial) > 65536:
                raise RuntimeError("oversized initial response")
        head, body = bytes(initial).split(b"\r\n\r\n", 1)
        if not head.startswith(b"HTTP/1.1 200 ") or body != b"!":
            raise RuntimeError("target must send exactly one initial byte before upload")
        length = next((int(line.split(b":", 1)[1]) for line in head.split(b"\r\n")
                       if line.lower().startswith(b"content-length:")), None)
        if length != download:
            raise RuntimeError("target response Content-Length mismatch")
        initial_at = time.monotonic()
        first = sampler.snapshot(f"transfer{index}-initial-byte")
        # The first byte was delivered through the client's observer read
        # before upload starts. A short pause also allows its write callback.
        time.sleep(0.05)
        sock.sendall(b"A")
        sent = 0
        chunk = b"U" * 65536
        while sent < upload:
            sock.settimeout(max(0.001, deadline - time.monotonic()))
            payload = chunk[:min(len(chunk), upload - sent)]
            sock.sendall(payload)
            sent += len(payload)
        uploaded_at = time.monotonic()
        received = len(body)
        while received < download:
            sock.settimeout(max(0.001, deadline - time.monotonic()))
            data = sock.recv(min(65536, download - received))
            if not data:
                raise EOFError(f"download ended at {received}/{download}")
            received += len(data)
        finished_at = time.monotonic()
        late = sampler.snapshot(f"transfer{index}-download-complete")
        return {"token": token, "upload_bytes": sent, "download_bytes": received,
                "response_header": head.decode("ascii"), "seconds": finished_at - started,
                "initial_byte_seconds": initial_at - started,
                "upload_complete_seconds": uploaded_at - started,
                "initial_socket_snapshot": first, "late_socket_snapshot": late}


def numeric(view, key):
    value = view.get(key)
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else None


def filter_evidence(output):
    files = sorted(output.glob("dump-*/filters.txt"))
    qdisc_files = sorted(output.glob("dump-*/qdisc-all.txt"))
    counts = {"ifbqup": [], "ifbqdown": []}
    drops = {"ifbqup": None, "ifbqdown": None}
    backlog = None
    for path in files:
        raw = path.read_text()
        for dev in counts:
            matches = re.findall(rf"to device {dev}\b.*?Sent \d+ bytes (\d+) pkt", raw, re.S)
            if matches:
                counts[dev].append(max(map(int, matches)))
    for path in qdisc_files:
        for block in re.split(r"(?=^qdisc )", path.read_text(), flags=re.M):
            for dev in drops:
                if re.search(rf"\bdev {dev}\b", block):
                    match = re.search(r"\(dropped (\d+),", block)
                    if match:
                        drops[dev] = max(drops[dev] or 0, int(match[1]))
            if re.search(r"\bdev ifbqdown\b", block):
                match = re.search(r"backlog ([0-9.]+)([KMG]?)(?:b|B)\b", block)
                if match:
                    scale = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3}[match[2]]
                    backlog = max(backlog or 0, float(match[1]) * scale)
    start = {dev: values[0] if values else None for dev, values in counts.items()}
    end = {dev: values[-1] if values else None for dev, values in counts.items()}
    delta = {dev: end[dev] - start[dev] if start[dev] is not None and end[dev] is not None
             and end[dev] >= start[dev] else None for dev in counts}
    return {"redirect_packets": delta, "redirect_packets_before": start, "redirect_packets_after": end,
            "drops": drops, "max_down_backlog_bytes": backlog,
            "filter_files": [str(p.relative_to(output)) for p in files],
            "qdisc_files": [str(p.relative_to(output)) for p in qdisc_files]}


def run_case(bench, stand, args, name, profile, upload, download, count):
    output = args.output / name
    output.mkdir()
    tc(args, "profile", output, profile)
    stand.reset()
    config = bench.client_config([("a1", "reality", 0)], {"a1": 1},
                                health={"passive": {"shouldSkipQuality": False}, "report": {"quietShare": 1}},
                                strategy={"startPinnedOutbound": "a1", "shouldProbeBeforeSwitch": False},
                                loglevel="debug")
    result = {"case": name, "profile": profile, "checks": [], "transfers": [], "error": None}
    client = bench.Client(str(args.xray), config, str(output), name)
    sampler = None
    try:
        with client:
            client.network(args.raw_network_id)
            save(output / "status-before.json", client.status())
            save(output / "report-before.json", client.api("report"))
            with Sampler(args, output, client.process.proc.pid) as sampler:
                save(output / "client-process.json", {"pid": client.process.proc.pid,
                     "cmdline": Path(f"/proc/{client.process.proc.pid}/cmdline").read_bytes().decode().split("\x00")})
                for index in range(count):
                    item = transfer(bench, args, sampler, args.target_port, upload, download, index)
                    result["transfers"].append(item)
                    save(output / "transfers.json", result["transfers"])
                # Closing the SOCKS stream causes observer end aggregation.
                # Poll a bounded interval; snapshots themselves do not reset it.
                deadline = time.monotonic() + 5
                while True:
                    status, report = client.status(), client.api("report")
                    views = quality_views(status, "a1") + quality_views(report, "a1")
                    ready = any(numeric(item["view"], "dial_count") is not None
                                and numeric(item["view"], "dial_count") >= count
                                and (download < MIB or (numeric(item["view"], "bulk_count") is not None
                                     and numeric(item["view"], "bulk_count") >= count))
                                for item in views)
                    if ready or time.monotonic() >= deadline:
                        break
                    time.sleep(0.2)
                save(output / "status-after.json", status)
                save(output / "report-after.json", report)
                save(output / "quality-views.json", views)
                result["checks"].extend(report_status_checks(status, report, "a1", args.raw_network_id))
                result["quality_candidates"] = views
                # Prefer a view with complete end samples and measured bytes.
                selected = max(views, key=lambda item: (
                    numeric(item["view"], "bulk_count") or -1,
                    numeric(item["view"], "bytes_down") or -1), default=None)
                result["quality"] = selected["view"] if selected else {}
                result["quality_json_path"] = selected["json_path"] if selected else None
                result["checks"].append(check("per-path quality present", selected, selected is not None,
                                               "measured a1 view required; missing is failure"))
                tc(args, "dump", output)
    except Exception as exc:
        result["error"] = repr(exc)
        (output / "exception.txt").write_text(traceback.format_exc())
        result["checks"].append(check("case ran", repr(exc), False, "complete transfer/evidence"))
        # __enter__ failures are outside a context manager's __exit__ path.
        if hasattr(client.process, "proc") and client.process.proc.poll() is None:
            client.process.stop()
    if sampler is not None:
        result["socket_samples"] = sampler.records
        result["checks"].append(check("raw sampler succeeded", sampler.errors, not sampler.errors, "no errors"))
    result["reports"] = client.reports
    result["final_report"] = client.final_report
    result["events_dropped"] = client.dropped
    result["splices_debug_count"] = client.splices() if Path(client.log_path).exists() else None
    result["checks"].append(check("plain target stayed unspliced", result["splices_debug_count"],
                                   result["splices_debug_count"] == 0, "== 0"))
    result["checks"] += bench.pop_report_checks()
    result["tc"] = filter_evidence(output)
    for direction, value in result["tc"]["redirect_packets"].items():
        result["checks"].append(check(f"{direction} filter matched", value, value is not None and value > 0,
                                       "> 0 packets; missing is failure"))
    save(output / "result.json", result)
    return result


def all_sockets(result):
    return [sock for sample in result.get("socket_samples", []) for sock in sample["sockets"]]


def raw_median_min(result):
    values = [sock["minrtt"] for item in result["transfers"]
              for sock in item["initial_socket_snapshot"]["sockets"] if sock["minrtt"] is not None]
    return statistics.median(values) if values else None


def bucket_interval(value):
    if value not in TIME_BOUNDS:
        return None
    index = TIME_BOUNDS.index(value)
    # The final finite bound also represents an overflow bucket: ambiguous.
    if index == len(TIME_BOUNDS) - 1:
        return None
    return (0 if index == 0 else TIME_BOUNDS[index - 1], value)


def compare_rtt(clean, delayed):
    baseline, impaired = raw_median_min(clean), raw_median_min(delayed)
    delta = impaired - baseline if baseline is not None and impaired is not None else None
    checks = [check("raw client TCP minrtt increase", {"clean_ms": baseline, "rtt_ms": impaired,
                                                    "delta_ms": delta},
                    delta is not None and 70 <= delta <= 130, "+100 ms, tolerance ±30 ms")]
    clean_min = numeric(clean.get("quality", {}), "tcp_rtt_min_ms")
    delayed_min = numeric(delayed.get("quality", {}), "tcp_rtt_min_ms")
    exported_delta = delayed_min - clean_min if clean_min is not None and delayed_min is not None else None
    checks.append(check("exported TCP min RTT increase", {"clean_ms": clean_min, "rtt_ms": delayed_min,
                        "delta_ms": exported_delta}, exported_delta is not None and 70 <= exported_delta <= 130,
                        "+100 ms, tolerance ±30 ms; missing exported metric is failure"))
    clean_p50 = bucket_interval(numeric(clean.get("quality", {}), "tcp_rtt_p50_ms"))
    delayed_p50 = bucket_interval(numeric(delayed.get("quality", {}), "tcp_rtt_p50_ms"))
    p50_delta = (delayed_p50[0] - clean_p50[1], delayed_p50[1] - clean_p50[0]) if clean_p50 and delayed_p50 else None
    checks.append(check("exported TCP p50 bucket supports increase", {"clean_interval_ms": clean_p50,
                        "rtt_interval_ms": delayed_p50, "delta_interval_ms": p50_delta},
                        p50_delta is not None and p50_delta[0] <= 130 and p50_delta[1] >= 70,
                        "bucket delta overlaps +100±30 ms; missing exported metric is failure"))
    for result in (clean, delayed):
        expected_count = len(result["transfers"])
        measured_count = numeric(result.get("quality", {}), "tcp_count")
        checks.append(check(f"{result['case']} exported S1 TCP count", measured_count,
                            measured_count is not None and measured_count >= expected_count,
                            f">={expected_count} measured client sockets"))
    left = bucket_interval(numeric(clean.get("quality", {}), "dial_p50_ms"))
    right = bucket_interval(numeric(delayed.get("quality", {}), "dial_p50_ms"))
    bounds = (right[0] - left[1], right[1] - left[0]) if left and right else None
    checks.append(check("REALITY dial bucket supports increase", {"clean_interval_ms": left,
                    "rtt_interval_ms": right, "delta_interval_ms": bounds},
                    bounds is not None and bounds[0] <= 230 and bounds[1] >= 170,
                    "bucket delta overlaps +200±30 ms; bucket is not exact raw dial time"))
    for result in (clean, delayed):
        for dev, drops in result["tc"]["drops"].items():
            checks.append(check(f"{result['case']} {dev} drops", drops, drops == 0, "== 0; present raw counter"))
    return checks


def loss_checks(control, impaired):
    checks = []
    for key in ("tx_retransmit_fraction", "rx_out_of_order_fraction"):
        value = numeric(impaired.get("quality", {}), key)
        checks.append(check(f"loss quality {key}", value, value is not None and value > 0,
                            "> 0; missing is failure"))
        baseline = numeric(control.get("quality", {}), key)
        checks.append(check(f"same10ms control {key}", baseline,
                            baseline is not None and baseline <= 0.001, "present and <= 0.001"))
    # A probe can share the same PID and remote port. Select one socket that
    # actually carried the 4 MiB payload, then require every raw counter on
    # that same endpoint rather than collecting independent maxima.
    uploads = [sock for sock in all_sockets(impaired)
               if sock["bytes_sent"] is not None and sock["bytes_sent"] >= 4 * MIB]
    selected = max(uploads, key=lambda sock: sock["data_segs_in"]
                   if sock["data_segs_in"] is not None else -1, default=None)
    impaired["raw_duplex_socket"] = selected
    checks.append(check("raw socket carried full upload", selected,
                        selected is not None, "same live client socket bytes_sent >=4 MiB"))
    for key in ("bytes_sent", "bytes_retrans", "data_segs_in"):
        value = selected[key] if selected is not None else None
        checks.append(check(f"live client raw ss {key}", value, value is not None and value > 0,
                            "> 0 on PID-mapped remote:21002 socket; missing is failure"))
    values = [selected["rcv_ooopack"]] if selected is not None and selected["rcv_ooopack"] is not None else []
    impaired["raw_rx_ooo_evidence"] = {
        "value": max(values) if values else None, "known": bool(values),
        "note": "ss can omit this kernel field; absence is unknown, never observed zero. "
                "RX acceptance requires nonzero quality fraction, raw data_segs_in and tc drops. "
                "A missing raw OOO numerator remains an independent evidence gap."}
    if values:
        checks.append(check("live client raw ss rcv_ooopack", max(values), max(values) > 0,
                            "> 0 when ss exposes the field"))
    for dev, value in impaired["tc"]["drops"].items():
        checks.append(check(f"loss {dev} drops", value, value is not None and value > 0,
                            "> 0; present raw tc counter"))
    return checks


def queue_checks(result):
    quality = result.get("quality", {})
    minimum, bulk = numeric(quality, "tcp_rtt_min_ms"), numeric(quality, "bulk_rtt_p50_ms")
    checks = [check("bulk receiver RTT exceeds initial min", {"min_ms": minimum, "bulk_bucket_ms": bulk,
                    "delta_ms": bulk - minimum if minimum is not None and bulk is not None else None,
                    "ratio": bulk / minimum if minimum is not None and minimum > 0 and bulk is not None else None},
                    minimum is not None and bulk is not None and bulk > minimum,
                    "bulk bucket > min; preserve magnitude for manager assessment")]
    backlog = result["tc"]["max_down_backlog_bytes"]
    checks.append(check("downlink TBF had backlog", backlog, backlog is not None and backlog > 0,
                        "> 0 bytes during 64 MiB download"))
    early = [sock["minrtt"] for item in result["transfers"]
             for sock in item["initial_socket_snapshot"]["sockets"] if sock["minrtt"] is not None]
    late = [sock["rcv_rtt"] for sock in all_sockets(result) if sock["rcv_rtt"] is not None]
    low, high = min(early) if early else None, max(late) if late else None
    checks.append(check("raw client receiver RTT rises under queue", {"initial_minrtt_ms": low, "max_rcv_rtt_ms": high},
                        low is not None and high is not None and high > low,
                        "raw rcv_rtt > initial minrtt; missing is failure; magnitude remains reported"))
    result["queue_raw_rtt_delta_ms"] = high - low if high is not None and low is not None else None
    result["queue_raw_rtt_ratio"] = high / low if high is not None and low is not None and low > 0 else None
    result["queue_acceptance_note"] = "These checks prove an increase. Its magnitude and the bucketed " \
                                      "bulk RTT need manager assessment before final acceptance."
    return checks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bench-dir", type=Path, required=True)
    parser.add_argument("--xray", type=Path, required=True)
    parser.add_argument("--server-xray", type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--raw-network-id", default="cell:25001:quality-synthetic-" + uuid.uuid4().hex,
                        help="Synthetic host ID for network type/operator and export privacy checks")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--tc-helper", type=Path, default=Path("/runner/tc_netem.py"))
    parser.add_argument("--port", type=int, default=21002)
    parser.add_argument("--target-port", type=int, default=18996)
    parser.add_argument("--scenario", choices=("all", "rtt", "loss", "queue"), default="all")
    parser.add_argument("--timeout", type=float, default=240)
    parser.add_argument("--short-count", type=int, default=12)
    args = parser.parse_args()
    if args.timeout < 180 or args.short_count < 8:
        parser.error("timeout must be >=180 seconds and short-count >=8")
    args.output = args.output.resolve()
    args.xray = args.xray.resolve()
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("output must be a fresh empty directory")
    args.output.mkdir(parents=True, exist_ok=True)
    if args.server_xray is not None:
        os.environ["BENCH_SERVER_BIN"] = str(args.server_xray.resolve())
    bench = load_bench(args)
    native_binary(args.xray)
    native_binary(bench.SERVER_BIN or args.xray)
    args.target_port += bench.OFFSET
    save(args.output / "runner-manifest.json", {"argv": sys.argv, "kernel": platform.release(),
         "native_machine": platform.machine(), "python": sys.version, "bench_dir": str(args.bench_dir),
         "source_commit": args.source_commit, "xray": str(args.xray), "xray_sha256": sha256(args.xray),
         "server_xray": str(Path(bench.SERVER_BIN).resolve()) if bench.SERVER_BIN else str(args.xray),
         "server_xray_sha256": sha256(bench.SERVER_BIN or args.xray),
         "bench_file_sha256": {str(path.relative_to(args.bench_dir)): sha256(path)
                               for path in sorted(args.bench_dir.rglob("*")) if path.is_file()},
         "helper_sha256": {str(path): sha256(path) for path in
                           (Path(__file__), Path(__file__).with_name("duplex_target.py"),
                            Path(__file__).with_name("schema_privacy.py"), args.tc_helper)},
         "target_port": args.target_port, "timeout_seconds": args.timeout,
         "raw_network_id": args.raw_network_id, "report_config": {"quietShare": 1},
         "provider_capture_checked": False,
         "inner_tls": False, "created_wall_ns": time.time_ns(), "created_monotonic_ns": time.monotonic_ns(),
         "note": "Prepared helper execution; CPU cost results are outside this runner"})
    summary = {"cases": [], "checks": [], "errors": [], "measurement_notes": []}
    summary["measurement_notes"].append("Provider body capture/hash/privacy is a separate post-traffic diagnostic; this runner configures no provider and does not claim a provider check.")
    server = None
    setup_started = False
    try:
        setup_started = True
        tc(args, "setup", args.output)
        server = create_server(args.target_port, args.output / "duplex-target.jsonl", args.timeout, end_hold=2)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        stand_dir = args.output / "stand"
        stand_dir.mkdir()
        with bench.Stand(str(args.xray), str(stand_dir)) as stand:
            if args.scenario in ("all", "rtt"):
                clean = run_case(bench, stand, args, "rtt-clean", "clean", 0, 4096, args.short_count)
                delayed = run_case(bench, stand, args, "rtt-plus100ms", "rtt", 0, 4096, args.short_count)
                summary["cases"] += [clean, delayed]
                summary["checks"] += compare_rtt(clean, delayed)
            if args.scenario in ("all", "loss"):
                control = run_case(bench, stand, args, "loss-control10ms", "baseline", 4 * MIB, 16 * MIB, 1)
                loss = run_case(bench, stand, args, "loss2percent", "loss", 4 * MIB, 16 * MIB, 1)
                summary["cases"] += [control, loss]
                summary["checks"] += loss_checks(control, loss)
            if args.scenario in ("all", "queue"):
                queue = run_case(bench, stand, args, "queue5Mbit", "queue", 0, 64 * MIB, 1)
                summary["cases"].append(queue)
                summary["checks"] += queue_checks(queue)
    except Exception as exc:
        summary["errors"].append(repr(exc))
        (args.output / "exception.txt").write_text(traceback.format_exc())
    finally:
        if server is not None:
            server.shutdown()
            server.server_close()
        if setup_started:
            try:
                tc(args, "cleanup", args.output)
            except Exception as exc:
                summary["errors"].append("cleanup: " + repr(exc))
        summary["checks"] += [c for case in summary["cases"] for c in case["checks"]]
        for case in summary["cases"]:
            save(args.output / case["case"] / "result.json", case)
            if case.get("queue_acceptance_note"):
                summary["measurement_notes"].append(case["queue_acceptance_note"])
            raw_rx = case.get("raw_rx_ooo_evidence")
            if raw_rx is not None and not raw_rx["known"]:
                summary["measurement_notes"].append(raw_rx["note"])
        summary["ok"] = bool(summary["checks"]) and not summary["errors"] and all(c["ok"] for c in summary["checks"])
        save(args.output / "stage4-result.json", summary)
    for item in summary["checks"]:
        print(f"{'PASS' if item['ok'] else 'FAIL'} {item['check']}: {item['value']}", flush=True)
    print(f"stage4 evidence: {args.output / 'stage4-result.json'}", flush=True)
    raise SystemExit(0 if summary["ok"] else 1)


if __name__ == "__main__":
    main()
