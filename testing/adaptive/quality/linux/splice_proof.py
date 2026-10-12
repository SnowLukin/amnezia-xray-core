#!/usr/bin/env python3
"""Evaluate positive kernel splice evidence from one isolated proof run.

The syscall tracer is used only for proof; its CPU timings are invalid.
No trace line containing only EAGAIN/EOF counts as transferred bytes.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True, help="Unique throughput runner run directory")
    parser.add_argument("--trace-dir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=21002)
    args = parser.parse_args()
    attempts = json.loads((args.run_dir / "attempts.json").read_text())
    manifests = json.loads((args.run_dir / "manifest.json").read_text())
    if manifests.get("mode") != "splice-proof" or manifests.get("cpu_valid") is not False:
        parser.error("Input must be a declared CPU-invalid splice-proof run")
    planned = [row["id"] for row in manifests.get("fixed_schedule", [])]
    if not planned or [row["id"] for row in attempts] != planned:
        parser.error("proof must contain exactly every planned attempt from this run")
    runtime_dir = manifests.get("output_runtime_path")
    actual_binary = manifests.get("actual_xray")
    if not runtime_dir or not actual_binary:
        parser.error("exact runtime output directory and actual binary receipts are required")
    traces = [(path, path.read_text(errors="replace")) for path in sorted(args.trace_dir.glob("client-splice-*.trace"))]
    results = []
    positive_pattern = re.compile(r"\bsplice\(.*\)\s+=\s+([1-9][0-9]*)(?:\s|$)")
    resumed_pattern = re.compile(r"<\.\.\. splice resumed>.*\)\s+=\s+([1-9][0-9]*)(?:\s|$)")
    for attempt in attempts:
        # IDs/basenames repeat between runs. Match the complete runtime config
        # argv in this run's actual binary execve, even when read on the host.
        config = str(Path(runtime_dir) / attempt["id"] / f"client-{attempt['id']}.json")
        if attempt.get("client_config_runtime_path") != config:
            parser.error("attempt config path does not belong to this unique run")
        config_arg = re.compile(r'"-c"\s*,\s*' + re.escape(json.dumps(config)))
        selected = [(path, body) for path, body in traces if any(
            "execve(" + json.dumps(actual_binary) + "," in line and config_arg.search(line)
            for line in body.splitlines())]
        evidence = []
        transferred = 0
        socket_input_bytes = 0
        for path, body in selected:
            pending = {}
            for line_number, line in enumerate(body.splitlines(), 1):
                match = positive_pattern.search(line)
                source = line
                if match is None and "splice(" in line and "<unfinished ...>" in line:
                    pid = line.split(None, 1)[0]
                    pending[pid] = (line_number, line)
                    continue
                if match is None:
                    match = resumed_pattern.search(line)
                    if match:
                        pid = line.split(None, 1)[0]
                        previous = pending.pop(pid, None)
                        source = (previous[1] + " " + line) if previous else line
                if match is None:
                    continue
                count = int(match.group(1))
                transferred += count
                # fd -yy annotation must show socket local ephemeral -> chaos port.
                # Only source FD before first ', NULL,' counts as the outbound read leg.
                fd_source = source.split("splice(", 1)[-1].split(", NULL,", 1)[0]
                outbound_socket = "->127.0.0.1:" + str(args.port) in fd_source
                if outbound_socket:
                    socket_input_bytes += count
                evidence.append({"trace": str(path), "line": line_number, "bytes": count,
                                 "outbound_socket_source": outbound_socket, "text": source})
        complete = attempt.get("complete") and attempt.get("transferred_bytes") == 16 * (1 << 20)
        results.append({"attempt_id": attempt["id"], "label": attempt["label"],
                        "client_config_runtime_path": config,
                        "trace_files": [str(path) for path, _ in selected],
                        "trace_sha256": {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path, _ in selected},
                        "application_transferred_bytes": attempt.get("transferred_bytes"),
                        "sum_positive_splice_bytes_all_legs": transferred,
                        "sum_positive_splice_bytes_client_socket_input": socket_input_bytes,
                        "positive_splice_evidence": evidence,
                        "accepted": bool(complete and selected and socket_input_bytes > 0)})
    summary = {"mode": "syscall-splice-proof", "cpu_valid": False,
               "port": args.port, "attempts": results,
               "accepted": bool(results) and all(row["accepted"] for row in results),
               "note": "all-legs byte sum may double count socket->pipe->socket; input-leg bytes alone prove actual read transfer"}
    output = args.run_dir / "splice-proof.json"
    output.write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({"output": str(output), "accepted": summary["accepted"],
                      "attempts": [{key: row[key] for key in ("attempt_id", "sum_positive_splice_bytes_client_socket_input", "accepted")} for row in results]}, indent=2))
    return 0 if summary["accepted"] else 1


if __name__ == "__main__":
    sys.exit(main())
