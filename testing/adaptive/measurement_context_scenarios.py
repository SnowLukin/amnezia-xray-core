"""Focused local whoami / host API / reserve probe / policy comparison stand.

python3 measurement_context_scenarios.py <xray binary>
Uses the ordinary bench topology and unmodified controller thresholds. Network
retry and hourly time boundaries are checked with the core's fake-clock tests;
this runner never changes the real 10-minute retry delay.
"""
import http.server
import json
import os
import sys
import threading
import time

import bench
import scenarios


class Whoami(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        with self.server.lock:
            self.server.requests += 1
            mode = self.server.mode
        if mode == "blocked":
            self.server.release.wait(10)
        if mode == "failure":
            self.send_response(503)
        else:
            self.send_response(204)
            self.send_header("X-Network-Asn", "8359")
            self.send_header("X-Network-Country", "RU")
            self.send_header("X-Network-Region", "RU-MOW")
            self.send_header("X-Unlisted", "203.0.113.42")
        self.end_headers()

    def log_message(self, *args):
        pass


def wait_for(fn, seconds=10):
    end = time.time() + seconds
    while not fn():
        if time.time() >= end:
            raise AssertionError("condition timed out")
        time.sleep(0.05)


def checks_main(xray, work):
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Whoami)
    server.mode, server.requests = "ok", 0
    server.lock, server.release = threading.Lock(), threading.Event()
    threading.Thread(target=server.serve_forever, daemon=True).start()
    endpoint = f"http://127.0.0.1:{server.server_port}/whoami"
    results = []

    def check(name, value, ok):
        results.append({"check": name, "value": value, "ok": bool(ok)})
        print(f"{'PASS' if ok else 'FAIL'} {name}: {value}", flush=True)

    def config(enabled):
        cfg = bench.api_config(bench.client_config(
            scenarios.THREE, {"a1": 1, "a2": 2, "b1": 3},
            health={"report": {"quietShare": 1}},
            strategy={"networkChangeAction": "forget", "minHoldTime": "0s",
                      "returnToCheaper": {"interval": "1s"}}))
        health = cfg["healthObservatory"]
        health["telemetry"]["probeMeasurements"] = {}
        # The same local target serves the three reserve paths. Allow the
        # focused fixture to exercise real returns without spending a minute
        # waiting on the shared SNI quota. Core timing constants stay intact.
        health["probes"]["limits"].update(perServerNamePerMinute=10, perMinute=60)
        if enabled:
            health["network"]["labelRequest"] = {
                "outbound": "direct", "destinations": [endpoint], "timeout": "1s",
                "labels": {"asn": "X-Network-Asn", "country": "X-Network-Country", "region": "X-Network-Region"}}
            health["network"]["hostLabels"] = {}
            health["telemetry"]["pathLabels"] = {}
            health["telemetry"]["report"]["sessionLabels"] = {
                "labels": {"cohort": "stand"}}
            for outbound in cfg["outbounds"]:
                if "selection" in outbound:
                    outbound["selection"]["labels"] = {"node": outbound["tag"], "location": "NL"}
        return cfg

    try:
        with bench.Stand(xray, work) as stand:
            with bench.Client(xray, config(True), work, "measurement-cache") as client:
                client.api("network", "home", "--label", "type=wifi", "--label", "is_expensive=false")
                time.sleep(0.25)
                check("no whoami while idle", server.requests, server.requests == 0)
                traffic = bench.Traffic(rate=4, big=1, timeout=2).start()
                try:
                    wait_for(lambda: client.status().get("network", {}).get("ip", {}).get("asn") == "8359")
                    client.api("network", "mobile", "--label", "type=cellular")
                    wait_for(lambda: server.requests == 2 and client.status().get("network", {}).get("ip", {}).get("asn") == "8359")
                    client.api("network", "home", "--label", "type=wifi")
                    time.sleep(0.5)
                    check("new network queries / return caches", server.requests, server.requests == 2)
                    client.api("probe", "auto", "a2")
                    report = client.api("report")
                    paths = [path for segment in report.get("quality", []) for path in segment.get("paths", {}).values()]
                    check("reserve probe counters in segment", sum(p.get("probe_count", 0) for p in paths), any(p.get("probe_reasons", {}).get("host") for p in paths))
                    check("session and path labels", report.get("labels"), report.get("labels") == {"cohort": "stand"} and report.get("path_labels", {}).get("a2", {}).get("node") == "a2")
                    check("network host/IP in segment", [s.get("network") for s in report.get("quality", [])], any(s.get("network", {}).get("host", {}).get("type") == "cellular" and s.get("network", {}).get("ip", {}).get("asn") == "8359" for s in report.get("quality", [])))
                    check("unlisted headers excluded", "203.0.113.42" in json.dumps(report), "203.0.113.42" not in json.dumps(report))
                    server.mode = "failure"
                    client.api("network", "broken", "--label", "type=wifi")
                    wait_for(lambda: server.requests == 3)
                    time.sleep(0.2)
                    check("whoami failure leaves no IP labels", client.status().get("network"), not client.status().get("network", {}).get("ip"))
                    time.sleep(0.3)
                    check("no immediate failed request retry", server.requests, server.requests == 3)
                    client.api("lost")
                    before = server.requests
                    time.sleep(0.3)
                    check("no request without network", server.requests, server.requests == before)
                finally:
                    traffic.stop()
                with open(os.path.join(work, "measurement-cache-report.json"), "w") as output:
                    json.dump(report, output, indent=2)

            # Same fault/thresholds and actual transports, one normal arm and
            # one with whoami blocked. Compare state choices; real-time timing
            # uses the ordinary failover threshold instead of exact equality.
            traces = []
            for enabled in (False, True):
                stand.reset()
                server.mode = "blocked"
                server.release.clear()
                requests_before = server.requests
                cfg = config(enabled)
                with bench.Client(xray, cfg, work, f"measurement-failover-{enabled}") as client:
                    client.api("network", "failover", "--label", "type=wifi") if enabled else client.network("failover")
                    tr = bench.Traffic(rate=4, big=1, timeout=2).start()
                    try:
                        wait_for(lambda: client.status()["paths"][0]["state"] == "up")
                        if enabled:
                            wait_for(lambda: server.requests > requests_before)
                        fault = time.time()
                        bench.chaos(str(scenarios.A0), "refuse")
                        wait_for(lambda: client.status()["balancers"][0]["active"] == "a2", seconds=12)
                        time.sleep(0.25)
                    finally:
                        tr.stop()
                    client.drain()
                    sw = bench.switches(client.events, fault)
                    down = [e for e in client.events if e["ts"] >= fault * 1000 and e["type"] == "path_state" and e["fields"]["state"] == "down"]
                    trace = {"enabled": enabled, "switches": [(e["fields"]["from"], e["fields"]["to"], e["fields"]["reason"]) for e in sw], "downs": [(e["fields"]["outbound"], e["fields"]["state"]) for e in down], "switch_after_s": (sw[0]["ts"] / 1000 - fault) if sw else None}
                    traces.append(trace)
                    report = client.api("report")
                    check(f"failover reserve probes enabled={enabled}", [s.get("paths") for s in report.get("quality", [])], any(p.get("probe_reasons", {}).get("failover") for s in report.get("quality", []) for p in s.get("paths", {}).values()))
                    bench.chaos(str(scenarios.A0), "normal")
                    return_traffic = bench.Traffic(rate=4, big=1, timeout=2).start()
                    try:
                        wait_for(lambda: client.status()["balancers"][0]["active"] == "a1", seconds=210)
                    finally:
                        return_traffic.stop()
                    returned = client.api("report")
                    check(f"return reserve probes enabled={enabled}", [s.get("paths") for s in returned.get("quality", [])], any(p.get("probe_reasons", {}).get("return") for s in returned.get("quality", []) for p in s.get("paths", {}).values()))
                server.release.set()
            check("same switches and down with/without labels", traces, traces[0]["switches"] == traces[1]["switches"] and traces[0]["downs"] == traces[1]["downs"])
            check("whoami does not delay failover", [t["switch_after_s"] for t in traces], all(t["switch_after_s"] is not None and t["switch_after_s"] <= 12 for t in traces))
            with open(os.path.join(work, "measurement-failover-pair.json"), "w") as output:
                json.dump(traces, output, indent=2)
    finally:
        server.release.set()
        server.shutdown()
        server.server_close()
    results.extend(bench.pop_report_checks())
    with open(os.path.join(work, "measurement-context-checks.json"), "w") as output:
        json.dump(results, output, indent=2)
    return all(result["ok"] for result in results)


if __name__ == "__main__":
    work = bench.workdir()
    print(f"workdir {work}", flush=True)
    sys.exit(0 if checks_main(os.path.abspath(sys.argv[1]), work) else 1)
