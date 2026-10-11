"""Run the full stage 1 UDP matrix after the shared stand has been released.

BENCH_PORT_OFFSET=18000 python3 testing/adaptive/run_udp_matrix.py XrayBinary PionBinary
"""

import argparse
import contextlib
import csv
import os
import subprocess
import sys
import tempfile
import time
import traceback

import bench
import udp_matrix as matrix
import udp_matrix_advanced as advanced
import udp_matrix_topology as topology
import udp_matrix_ws as websocket


def access_routes(path, tag):
    if not os.path.exists(path):
        return {"tag": tag, "count": 0, "evidence": [], "error": "access log missing"}
    with open(path, errors="replace") as stream:
        lines = [line.strip() for line in stream if f"[socks -> {tag}]" in line]
    return {"tag": tag, "count": len(lines), "evidence": lines[:20]}


def pion_access_proof(path, tag, pion_result):
    with open(path, errors="replace") as stream:
        routed = [line for line in stream if f"[socks -> {tag}]" in line]
    proof = {}
    for peer in ("a", "b"):
        ports = sorted({event["source_port"] for event in pion_result[f"application_routes_{peer}"]})
        proof[peer] = {str(port): any(f":{port} " in line for line in routed) for port in ports}
    return {"ok": all(proof[peer] and all(proof[peer].values()) for peer in ("a", "b")),
            "by_peer": proof}


def flow_access_proof(path, tag, flows):
    with open(path, errors="replace") as stream:
        routed = [line for line in stream if f"[socks -> {tag}]" in line]
    proof = {flow.name: any(f":{flow.source.rsplit(':', 1)[1]} " in line for line in routed)
             for flow in flows if flow.tag == tag}
    return {"ok": bool(proof) and all(proof.values()), "by_flow": proof}


def line_count(path):
    if not os.path.exists(path):
        return 0
    with open(path, errors="replace") as stream:
        return sum(1 for _ in stream)


def process_resources(client):
    heap, goroutines = client.memory()
    pid = getattr(client.process.proc, "pid", None)
    rss_kb = None
    fds = None
    if pid is not None and not bench.ADB_SERIAL:
        result = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            rss_kb = int(result.stdout.strip())
        result = subprocess.run(["lsof", "-p", str(pid), "-Fn"], capture_output=True, text=True)
        if result.returncode == 0:
            fds = sum(line.startswith("f") for line in result.stdout.splitlines())
    return {"at_ns": time.monotonic_ns(), "heap_bytes": heap, "goroutines": goroutines,
            "rss_kb": rss_kb, "fds": fds, "cpu_seconds": client.cpu_seconds()}


def pion_application_route_proof(data, mode, peer):
    started, ended = data["transfer_started_unix_ns"], data["transfer_ended_unix_ns"]
    pair = data["selected_pair" if peer == "a" else "selected_pair_b"]
    if pair.get("local_type") != mode or pair.get("remote_type") != mode:
        return False
    destination = (data["turn_destination"] if mode == "relay" else
                   f"{pair['remote_address']}:{pair['remote_port']}")
    events = data.get(f"application_routes_{peer}", [])
    if not events or any(not event.get("dtls_application_data") or
                         not started <= event["at_unix_ns"] <= ended or
                         event.get("direction") not in ("send", "recv") or
                         event.get("destination") != destination for event in events):
        return False
    return all(any(event["direction"] == direction for event in events) for direction in ("send", "recv"))


def pion_case(binary, mode, path, output):
    command = [binary, "--mode", mode, "--socks", f"127.0.0.1:{bench.SOCKS_PORT}",
               "--target", topology.destination_host(path), "--hub-a", str(matrix.HELPER_PORTS["rtc_a"]),
               "--hub-b", str(matrix.HELPER_PORTS["rtc_b"]),
               "--turn", str(matrix.HELPER_PORTS["turn"]), "--output", output]
    result = subprocess.run(command, capture_output=True, text=True, timeout=240)
    if result.returncode:
        raise RuntimeError(f"Pion {mode} failed: {result.stderr[-1500:]}")
    with open(output) as stream:
        import json
        data = json.load(stream)
    if data.get("mode") != mode:
        raise RuntimeError("Pion result mode mismatch")
    required_sequences = set(range(500))
    started, ended = data["transfer_started_unix_ns"], data["transfer_ended_unix_ns"]
    for direction in ("a_to_b", "b_to_a"):
        messages = data[direction]
        records = messages["records"]
        sequences = {record["sequence"] for record in records}
        if (messages.get("sent") != 500 or messages.get("received_unique") != 500 or
                messages.get("invalid") != 0 or sequences != required_sequences or
                not messages.get("ok") or
                any(not started <= record["sent_unix_ns"] <= record["received_unix_ns"] <= ended
                    for record in records)):
            raise RuntimeError(f"Pion {mode} {direction}: all 500 unique messages not delivered in transfer window")
    samples = data.get("selected_pairs_during_transfer", [])
    if len(samples) < 2 or samples[0]["at_unix_ns"] < started or samples[-1]["at_unix_ns"] > ended:
        raise RuntimeError("Pion selected pairs lack transfer window evidence")
    sample_times = [sample["at_unix_ns"] for sample in samples]
    if (sample_times != sorted(sample_times) or sample_times[0] - started > 1_000_000_000 or
            ended - sample_times[-1] > 1_000_000_000 or
            any(right - left > 1_000_000_000 for left, right in zip(sample_times, sample_times[1:]))):
        raise RuntimeError("Pion pair sampling does not cover application transfer")
    for peer in ("a", "b"):
        pair = data["selected_pair" if peer == "a" else "selected_pair_b"]
        for sample in samples:
            selected = sample[peer]
            if (selected.get("local_type") != mode or selected.get("remote_type") != mode or
                    selected.get("pair") != pair.get("pair")):
                raise RuntimeError(f"Pion {mode} peer {peer} pair changed during transfer")
        if not pion_application_route_proof(data, mode, peer):
            raise RuntimeError(f"Pion {mode} peer {peer}: application traffic escaped selected route or is missing")
    if not data.get("application_route_proved") or (mode == "relay" and not data.get("turn_route_proved")):
        raise RuntimeError("Pion application or TURN route not proved")
    return data


def run_path(path, xray, pion, work, targets, proxies, settings=None):
    settings = matrix.run_settings() if settings is None else settings
    config, tag = topology.matrix_config(path)
    config["log"]["access"] = os.path.join(work, f"client-{path}-access.log")
    ledger = matrix.Ledger(os.path.join(work, f"{path}-ledger.json"))
    host = topology.destination_host(path)
    row = {"path": path, "tag": tag, "mode": settings["mode"], "timing_contract": settings,
           "path_label": ("local TLS reverse proxy, CDN approximation" if "proxy" in path else
                          "two real Xray VLESS hops" if path == "relay" else path),
           "checks": [], "resources": []}
    target_start = {name: len(target.receipts) for name, target in targets.items()}
    relay_before = line_count(os.path.join(work, "relay-access.log"))
    server_before = line_count(os.path.join(work, "server-access.log"))
    proxy_before = {name: len(proxy.records) for name, proxy in proxies.items()}
    with bench.Client(xray, config, work, path) as client:
        row["resources"].append(process_resources(client))
        row["association_cases"] = matrix.association_cases(ledger, tag, host)
        row["stun"] = matrix.stun_case(ledger, tag, host)
        row["dns"] = matrix.dns_case(ledger, tag)
        row["mixed_53_19302"] = matrix.mixed_port_case(ledger, tag, host)
        row["mixed_19302_53"] = matrix.mixed_port_case(ledger, tag, host, reverse=True)
        row["stream"] = matrix.stream_case(ledger, tag, host, duration=settings["udp_seconds"],
                                            rate=settings["udp_rate_pps"], required_duration=settings["udp_seconds"])
        row["resources"].append(process_resources(client))
        for mode in ("host", "relay"):
            row[f"pion_{mode}"] = pion_case(pion, mode, path, os.path.join(work, f"{path}-pion-{mode}.json"))
            row[f"pion_{mode}_flows"] = matrix.import_pion_flows(ledger, tag, mode, row[f"pion_{mode}"])
        row["pre_ws_snapshot"] = client.api("report")
        row["ws_ping"] = websocket.websocket_case(host, duration=settings["ws_ping_seconds"], ping=True,
                                                    required_duration=settings["ws_ping_seconds"])
        row["ws_no_ping"] = websocket.websocket_case(host, duration=settings["ws_idle_seconds"], ping=False,
                                                       required_duration=settings["ws_idle_seconds"])
        row["resources"].append(process_resources(client))
        row["post_ws_snapshot"] = client.api("report")
        row["snapshot_before_final"] = client.api("report")
    row["final_report"] = client.final_report
    row["dropped_events"] = client.dropped
    row["checks"].append({"name": "no dropped health events",
                          "value": row["dropped_events"], "limit": "== 0",
                          "ok": row["dropped_events"] == 0})
    row["resource_peak"] = {"heap_bytes": max((sample[1] for sample in client.mem), default=None),
                            "goroutines": max((sample[2] for sample in client.mem), default=None)}
    row["access_route"] = access_routes(config["log"]["access"], tag)
    row["flow_route_proof"] = flow_access_proof(config["log"]["access"], tag, ledger.flows)
    row["target_receipts"] = {name: len(target.receipts) - target_start[name]
                              for name, target in targets.items()}
    if path == "relay":
        row["relay_hops"] = {"entry_access": line_count(os.path.join(work, "relay-access.log")) - relay_before,
                             "exit_access": line_count(os.path.join(work, "server-access.log")) - server_before}
        row["checks"].append({"name": "both real Xray relay hops saw traffic",
                              "ok": min(row["relay_hops"].values()) > 0})
    if path in ("ws_local_proxy", "xhttp_local_proxy"):
        kind = "ws" if path.startswith("ws") else "xhttp"
        rows = proxies[kind].records[proxy_before[kind]:]
        accepted_alpn = (None, "http/1.1") if kind == "ws" else ("h2",)
        row["local_proxy"] = {"connections": len(rows),
                              "forwarded_up": sum(item["up_bytes"] for item in rows),
                              "forwarded_down": sum(item["down_bytes"] for item in rows),
                              "alpn": [item.get("client_alpn") for item in rows],
                              "label": "local TLS reverse proxy; not a deployed CDN"}
        row["checks"].append({"name": f"{kind} local TLS reverse proxy carried bidirectional traffic",
                              "ok": bool(rows) and row["local_proxy"]["forwarded_up"] > 0 and
                                    row["local_proxy"]["forwarded_down"] > 0 and
                                    all(item.get("client_alpn") in accepted_alpn for item in rows)})
    row["stream_target_proof"] = matrix.stream_target_proof(
        targets["echo"].receipts[target_start["echo"]:], row["stream"])
    row["stream_target_receipts"] = row["stream_target_proof"]["received_unique"]
    row["stream"]["lost_before_target"] = len(row["stream_target_proof"]["missing_before_target"])
    row["stream"]["lost_after_target"] = len(row["stream_target_proof"]["missing_after_target"])
    row["checks"].append({"name": "stream target identity, sequence, hash and loss boundaries",
                          "ok": row["stream_target_proof"]["ok"]})
    row["checks"].append({"name": "selected path appears in access log",
                          "ok": row["access_route"]["count"] > 0})
    row["checks"].append({"name": "every UDP association source routed to selected tag",
                          "ok": row["flow_route_proof"]["ok"]})
    row["checks"].append({"name": "controlled STUN/echo targets received routed packets",
                          "ok": row["target_receipts"]["stun"] >= 5 and
                                row["stream_target_receipts"] > 0})
    for mode in ("host", "relay"):
        row[f"pion_{mode}_route_proof"] = pion_access_proof(config["log"]["access"], tag,
                                                             row[f"pion_{mode}"])
        row["checks"].append({"name": f"Pion {mode}: both peer source sockets routed to selected tag",
                              "ok": row[f"pion_{mode}_route_proof"]["ok"]})
    row["checks"].append({"name": "Pion host forwarder carried both directions",
                          "ok": row["pion_host"]["host_forwarder"]["forwarded_to_a"] > 0 and
                                row["pion_host"]["host_forwarder"]["forwarded_to_b"] > 0})
    row["checks"].append({"name": ("stream sent full 20 minutes at 50 pps" if settings["mode"] == "full" else
                                   "stream sent the complete functional window at 50 pps"),
                          "ok": row["stream"]["meets_required_duration"] and
                                row["stream"]["sent"] == settings["udp_packets"]})
    row["checks"].append({"name": ("WebSocket ran both full 10-minute modes" if settings["mode"] == "full" else
                                   "WebSocket ran both complete functional modes"),
                          "ok": all(row[k]["ok"] and row[k]["configured_seconds"] == row[k]["required_seconds"] ==
                                    settings["ws_ping_seconds" if k == "ws_ping" else "ws_idle_seconds"] and
                                    row[k]["survived_seconds"] >= row[k]["required_seconds"] and
                                    "remote_closed_ns" not in row[k] and row[k]["final_pong"]
                                    for k in ("ws_ping", "ws_no_ping"))})
    if settings["mode"] == "functional":
        row["checks"].append({"name": "WebSocket functional boundaries completed normal close",
                              "ok": all(row[k]["close_code"] == 1000 for k in ("ws_ping", "ws_no_ping"))})
    before_ws = row["pre_ws_snapshot"]["paths"][tag]
    after_ws = row["post_ws_snapshot"]["paths"][tag]
    row["checks"].append({"name": "WebSocket alone added no UDP flow or packet",
                          "ok": all(before_ws[field] == after_ws[field]
                                    for field in ("udp_flows", "udp_writes", "udp_reads"))})
    row["checks"].append({"name": "all STUN transaction IDs answered",
                          "ok": row["stun"]["valid"] == 5})
    row["checks"].append({"name": "answered/unanswered/two-source association controls",
                          "ok": row["association_cases"]["answered_once"] and
                                row["association_cases"]["answered_five"] == 5 and
                                row["association_cases"]["unanswered_timeout"] and
                                len(set(row["association_cases"]["parallel_sources"])) == 2})
    row["checks"].append({"name": "DNS answers from 1.1.1.1:53",
                          "ok": row["dns"]["valid"] == len(row["dns"]["records"])})
    if row["final_report"] is None:
        row["checks"].append({"name": "quietShare=1 final report", "ok": False})
    else:
        row["udp_reconciliation"] = matrix.reconcile_flows(ledger.flows, row["final_report"], tag)
        row["checks"].append({"name": "UDP report agrees with routed association ledger",
                              "ok": row["udp_reconciliation"]["ok"]})
        row["checks"].append({"name": "quiet sample emitted", "ok":
                              bool(row["final_report"].get("is_quiet_sample"))})
    ledger.flush()
    return row


def collect_report_checks(row):
    checks = bench.pop_report_checks()
    row["report_checks"] = checks
    row["checks"].append({"name": "Client snapshot/final/second-final and report/event checks present",
                          "ok": bool(checks)})
    row["checks"].extend({"name": check["check"], "ok": bool(check["ok"]),
                          "value": check.get("value"), "limit": check.get("limit")}
                         for check in checks)


def write_resource_csv(path, results):
    with open(path, "w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=["path", "at_ns", "heap_bytes", "goroutines",
                                                        "rss_kb", "fds", "cpu_seconds"])
        writer.writeheader()
        for result in results:
            for sample in result.get("resources", []):
                writer.writerow({"path": result["path"], **sample})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("xray", help="prebuilt Xray binary; this runner does not build")
    parser.add_argument("pion", help="prebuilt Pion two-peer helper; mandatory")
    parser.add_argument("--output", help="artifact directory outside Git")
    parser.add_argument("--functional", action="store_true",
                        help="all eleven functional cases; excludes long soak/performance windows")
    arguments = parser.parse_args()
    settings = matrix.run_settings(arguments.functional)
    if bench.ADB_SERIAL:
        parser.error("SOCKS UDP via adb is unsupported; run this matrix locally")
    xray, pion = map(os.path.abspath, (arguments.xray, arguments.pion))
    for executable in (xray, pion):
        if not os.path.isfile(executable) or not os.access(executable, os.X_OK):
            parser.error(f"missing executable: {executable}")
    matrix.assert_ports_available()
    work = os.path.abspath(arguments.output or tempfile.mkdtemp(prefix="xray-stage1-udp-matrix-"))
    if os.path.commonpath((work, os.path.dirname(__file__))) == os.path.dirname(__file__):
        parser.error("output must be outside testing/adaptive")
    os.makedirs(work, exist_ok=True)
    matrix.save_json(os.path.join(work, "run-metadata.json"),
                     {"timing_contract": settings, "paths": list(matrix.PATHS), "catalog_size": 11,
                      "full_200_minute_soak": not arguments.functional,
                      "removed_total_soak_seconds": sum(settings["removed_soak_seconds_per_path"].values()) * len(matrix.PATHS),
                      "performance_acceptance": False if arguments.functional else None,
                      "stage1_accepted": False})
    print(f"mode {settings['mode']}; artifacts {work}", flush=True)
    results = []
    original_server = bench.server_config
    original_awg = bench.awg_server_config
    bench.server_config = lambda: topology.local_server_config(work, original_server)
    bench.awg_server_config = lambda: topology.local_awg_server_config(work, original_awg)
    try:
        stand = bench.Stand(xray, work)
        with contextlib.ExitStack() as stack:
            targets = {
                "stun": stack.enter_context(matrix.UDPOracle(matrix.HELPER_PORTS["stun"], "stun",
                                                             os.path.join(work, "stun-target.json"))),
                "echo": stack.enter_context(matrix.UDPOracle(matrix.HELPER_PORTS["echo"], "echo",
                                                             os.path.join(work, "echo-target.json"))),
                "blackhole": stack.enter_context(matrix.UDPOracle(matrix.HELPER_PORTS["blackhole"], "blackhole",
                                                                  os.path.join(work, "blackhole-target.json"))),
            }
            stack.enter_context(websocket.WebSocketEcho(work))
            stack.enter_context(stand)
            stack.enter_context(topology.RelayHop(xray, work))
            proxies = {kind: stack.enter_context(topology.TLSProxy(kind, work))
                       for kind in ("ws", "xhttp")}
            for path in matrix.PATHS:
                stand.reset()
                try:
                    row = run_path(path, xray, pion, work, targets, proxies, settings=settings)
                except Exception as error:
                    traceback.print_exc()
                    row = {"path": path, "error": repr(error), "checks": [{"name": "matrix path ran", "ok": False}]}
                row["mode"] = settings["mode"]
                collect_report_checks(row)
                results.append(row)
                matrix.save_json(os.path.join(work, "matrix.json"), results)
                print(f"{path}: {'PASS' if all(c['ok'] for c in row['checks']) else 'FAIL'}", flush=True)
            for name, action in (("mux-two-flows", advanced.mux_two_flows),
                                 ("mux-reconnect", advanced.reconnect_flow),
                                 ("path-switch-old-flow", advanced.path_switch_old_flow),
                                 ("cross-report-window", advanced.cross_report_window),
                                 ("awg-cold-answer", lambda s, x, w: advanced.awg_cold_case(s, x, w, True)),
                                 ("awg-cold-unanswered", lambda s, x, w: advanced.awg_cold_case(s, x, w, False))):
                stand.reset()
                try:
                    details = action(stand, xray, work)
                    row = {"path": name, "details": details,
                           "checks": [{"name": name, "ok": bool(details["ok"])}]}
                except Exception as error:
                    traceback.print_exc()
                    row = {"path": name, "error": repr(error),
                           "checks": [{"name": name, "ok": False}]}
                row["mode"] = settings["mode"]
                collect_report_checks(row)
                results.append(row)
                matrix.save_json(os.path.join(work, "matrix.json"), results)
                print(f"{name}: {'PASS' if all(c['ok'] for c in row['checks']) else 'FAIL'}", flush=True)
    finally:
        bench.server_config = original_server
        bench.awg_server_config = original_awg
    write_resource_csv(os.path.join(work, "resources.csv"), results)
    failed = sum(not check["ok"] for row in results for check in row.get("checks", []))
    print(f"failed checks: {failed}; artifacts: {work}", flush=True)
    return int(bool(failed))


if __name__ == "__main__":
    sys.exit(main())
