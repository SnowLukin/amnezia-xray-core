"""Warm reserve against a slot issued on request, on one failure of the entry.

    python3 reserve_ab.py <xray binary> <out dir> <run> [<run> ...]

A run is scheme:backend:road:fault:delay:repeat[:net], for example
B1:1:entries:blackhole:0s:3. net "delayN" delays every chunk through the
entries by N ms each way, closer to a mobile network than loopback; the
backend itself stays behind the entries or direct as the road says.

Schemes:
  A    the chosen slot s1 on entry 1, a warm slot w2 on entry 2, a probe-only
       credential q3 on entry 3; the backend replaces a dead slot in the
       background (minHealthy 2).
  B0   one slot s1, probe-only q2 and q3 on entries 2 and 3; an exhausted
       balancer asks the backend for a new slot.
  B1   B0 with probeOnlyRoads, the fault 20 s after the start, while the
       check of the probe-only credentials is fresh.
  B1s  B1 with the fault at 80 s, when the check is older, as it mostly is in
       the field (the credentials are rechecked every 15 minutes). The run of
       14.10.2026 also sent selection.provenBy, which let a slot skip its probe
       after a check younger than 60 s; B1 and B1s came out the same, an
       exhausted balancer takes a new slot without waiting for its probe
       anyway, and the field was removed from the core.
Backend: seconds before the answer, "overload" (503 with a protocol body and a
random Retry-After of 30-90 s for the first 60 s after the fault, then 1 s) or
"down" (never answers).
Road: "direct" (via auto, direct) or "entries" (via auto only: the backend is
reachable through the entries alone; in B1 through the probe-only roads).
Fault on entry 1: blackhole, stall (after 16 KB) or reset.
Delay: triggers.exhausted.delay, "0s" or "default" (0s..10s).

Every user entry and probe-only credential is a REALITY address of the one
bench server; the chaos proxy fails each address alone. The probe-only
credential reaches only the probe target and the backend.
"""
import copy
import json
import os
import random
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import bench
from api_v2_recipe import provider_answer

BACKEND_PORT = 19060 + bench.OFFSET
OBSERVE_AFTER_FAULT = 90
FAULT_AT = 20
ENTRY = {"s1": 0, "w2": 1, "q2": 1, "q3": 2}


def patch_server_config():
    """The probe-only credential may reach the backend, as it will in production."""
    original = bench.server_config

    def server_config(probe_credentials=False, access_log=None, udp_observation=False):
        cfg = original(probe_credentials, access_log, udp_observation)
        if probe_credentials:
            cfg["routing"]["rules"].insert(0, {"user": [bench.PROBE_EMAIL], "ip": ["127.0.0.1"],
                                               "port": str(BACKEND_PORT), "outboundTag": "allow"})
        return cfg
    bench.server_config = server_config


def outbound(tag, entry, probe_only, proven_by=None):
    o = bench.member(tag, "reality", entry, bench.PIN)
    o["path"] = {"balancer": "auto", "cost": 1, "probe": "default", "isProbeOnly": False}
    bench.set_probe_only(o, probe_only)
    if proven_by:
        o["path"]["provenBy"] = proven_by
    return o


class Backend:
    """Issues slots from a pool on the entry of a live probe-only credential."""

    def __init__(self):
        self.lock = threading.Lock()
        self.log = []
        self.reset("1", "B0")

    def reset(self, mode, scheme, fault_at=None):
        with self.lock:
            self.mode, self.scheme, self.fault_at = mode, scheme, fault_at
            self.sets = {}
            self.log = []

    def initial(self, scheme):
        if scheme == "A":
            return [outbound("s1", 0, False), outbound("w2", 1, False), outbound("q3", 2, True)]
        return [outbound("s1", 0, False), outbound("q2", 1, True), outbound("q3", 2, True)]

    def answer(self, key, body):
        """The set for one client after its POST, or None when nothing changed."""
        scheme = self.scheme
        current = self.sets.setdefault(key, {"n": 1, "outbounds": self.initial(scheme)})
        inventory = {o.get("tag"): o for o in body.get("outbounds", [])}
        dead = {t for t, o in inventory.items() if not o.get("isProbeOnly") and o.get("state") in ("down", "suspect")}
        kept = [o for o in current["outbounds"] if o["tag"] not in dead]
        real = [o for o in kept if not o["path"]["isProbeOnly"]]
        capacity = 2 if scheme == "A" else 1
        used = {o["settings"]["vnext"][0]["port"] for o in real}
        probes = [o for o in kept if o["path"]["isProbeOnly"] and o["settings"]["vnext"][0]["port"] not in used]
        live = [o for o in probes if inventory.get(o["tag"], {}).get("state") == "up"
                or inventory.get(o["tag"], {}).get("probe", {}).get("isPassed") is True]
        issued = []
        while len(real) < capacity and (live or probes):
            source = (live or probes).pop(0)
            if source in probes:
                probes.remove(source)
            if source in live:
                live.remove(source)
            current["n"] += 1
            tag = f"s{current['n']}"
            entry = (source["settings"]["vnext"][0]["port"] - bench.address_port("reality", 0)) // 10
            proven = None
            slot = outbound(tag, entry, False)
            kept.append(slot)
            real.append(slot)
            issued.append({"tag": tag, "entry": entry, "provenBy": proven})
        current["outbounds"] = kept
        return {"protocol": 1, "version": f"v{current['n']}-{len(kept)}", "outbounds": copy.deepcopy(kept)}, issued

    def handler(self):
        backend = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def reply(self, code, body, headers=None):
                self.send_response(code)
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                at = time.time()
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                try:
                    body = json.loads(raw)
                except ValueError:
                    body = {}
                key = self.headers.get("Authorization") or "-"
                mode = backend.mode
                balancers = body.get("balancers") or [body.get("balancer") or {}]
                row = {"at": at, "key": key, "trigger": body.get("trigger"), "mode": mode,
                       "isExhausted": bool(balancers and balancers[0].get("isExhausted")),
                       "lastRefresh": body.get("lastRefresh"), "header": self.headers.get("Xray-Adaptive")}
                if mode == "down":
                    with backend.lock:
                        backend.log.append(dict(row, result="hang"))
                    time.sleep(600)
                    return
                if mode == "overload" and backend.fault_at and at < backend.fault_at + 60:
                    retry = random.randint(30, 90)
                    with backend.lock:
                        backend.log.append(dict(row, result="503", retryAfter=retry))
                    self.reply(503, json.dumps({"protocol": 1, "code": "overloaded", "message": "bench"}).encode(),
                               {"Retry-After": str(retry)})
                    return
                time.sleep(1.0 if mode == "overload" else float(mode))
                with backend.lock:
                    answer, issued = backend.answer(key, body)
                    backend.log.append(dict(row, result="200", issued=issued, answeredAt=time.time()))
                self.reply(200, json.dumps(provider_answer(answer)).encode())

        return Handler

    def serve(self):
        ThreadingHTTPServer.daemon_threads = True
        self.server = ThreadingHTTPServer(("127.0.0.1", BACKEND_PORT), self.handler())
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        return self


def client_config(scheme, road, delay, key="Bearer bench"):
    via = ["auto", "direct"] if road == "direct" else ["auto"]
    triggers = {"exhausted": {"isEnabled": True} if delay == "default" else {"isEnabled": True, "delay": delay}}
    if scheme == "A":
        triggers["minHealthyOutbounds"] = 2
        if delay == "default":
            # The recipe keeps the historical immediate trigger; this one
            # takes the default spread of the core.
            triggers["minHealthy"] = {"isEnabled": True, "outbounds": 2}
    prov = bench.provider_config(via, triggers=triggers)
    prov["url"] = f"http://{bench.HOST}:{BACKEND_PORT}/set"
    prov["timeout"] = "8s"
    prov["headers"] = {"Authorization": key}
    prov["limits"] = {}
    if scheme.startswith("B1"):
        prov["probeOnlyRoads"] = {}
    cfg = bench.client_config([], {}, providers=[prov], loglevel="warning", strategy={
        "startOutbound": "p-s1", "shouldSkipDownAudit": True},
        health={"events": {"maxEvents": 4096, "maxBytes": 262144}})
    for o in Backend().initial(scheme):
        item = copy.deepcopy(o)
        item["tag"] = "p-" + item["tag"]
        cfg["outbounds"].insert(-1, item)
    return cfg


FAULTS = {"blackhole": "blackhole", "stall": "stall_after:16384", "reset": "reset"}


def one_run(stand, backend, xray, out, spec):
    scheme, mode, road, fault, delay, repeat, *net = spec.split(":")
    name = spec.replace(":", "_")
    stand.reset()
    backend.reset(mode, scheme)
    cfg = client_config(scheme, road, delay)
    marks = {}
    entries = [bench.address_port("reality", i) for i in range(3)]
    if net and net[0].startswith("delay"):
        bench.chaos(",".join(map(str, entries)), "delay:" + net[0][len("delay"):])
    with bench.Client(xray, cfg, stand.workdir, name) as client:
        traffic = bench.Traffic(rate=2, big=0.5).start()
        epoch = time.time()
        time.sleep(max(0, epoch + (80 if scheme == "B1s" else FAULT_AT) - time.time()))
        client.drain()
        marks["before"] = client.status()
        marks["fault"] = time.time()
        backend.fault_at = marks["fault"]
        bench.chaos(bench.address_port("reality", 0), FAULTS[fault])
        time.sleep(OBSERVE_AFTER_FAULT)
        client.drain()
        marks["after"] = client.status()
        traffic.stop()
        events = client.events
    rec, fails = bench.recovery(traffic.records, marks["fault"])
    fault_at = marks["fault"]
    urgent = [r for r in backend.log if r["at"] >= fault_at]
    switches = [e for e in events if e["type"] == "switch" and e["ts"] / 1000 >= fault_at]
    first_down = next((e["ts"] / 1000 - fault_at for e in events if e["type"] == "path_state"
                       and e["ts"] / 1000 >= fault_at and e["fields"].get("outbound") == "p-s1"
                       and e["fields"].get("state") == "down"), None)
    exhausted = next((e["ts"] / 1000 - fault_at for e in events if e["type"] == "exhausted"
                      and e["ts"] / 1000 >= fault_at), None)
    fetches = [e["fields"] for e in events if e["type"] == "provider_fetch" and e["ts"] / 1000 >= fault_at]
    proven = [e for e in events if e["type"] == "provider_proven_by"]
    result = {
        "spec": spec, "net": net[0] if net else "", "scheme": scheme, "backend": mode, "road": road, "fault": fault, "delay": delay,
        "repeat": int(repeat), "recovery": rec and round(rec, 2), "failedRequests": fails,
        "s1DownAfter": first_down and round(first_down, 2), "exhaustedAfter": exhausted and round(exhausted, 2),
        "firstBackendRequestAfter": urgent and round(urgent[0]["at"] - fault_at, 2),
        "backendRequests": len(urgent), "backendTriggers": [r["trigger"] for r in urgent],
        "fetches": [{k: f.get(k) for k in ("via", "outbound", "is_ok", "status", "ms", "error")} for f in fetches],
        "switches": [{"t": round(e["ts"] / 1000 - fault_at, 2), "from": e["fields"].get("from"),
                      "to": e["fields"].get("to"), "reason": e["fields"].get("reason")} for e in switches],
        "provenBy": len(proven), "issued": [i for r in urgent for i in r.get("issued", [])],
    }
    with open(os.path.join(out, name + ".json"), "w") as f:
        json.dump({"result": result, "backend": backend.log, "events": events,
                   "traffic": sorted(traffic.records), "marks": {k: v for k, v in marks.items() if k == "fault"}},
                  f, indent=1)
    return result


def main():
    xray = os.path.abspath(sys.argv[1])
    out = os.path.abspath(sys.argv[2])
    specs = sys.argv[3:]
    os.makedirs(out, exist_ok=True)
    patch_server_config()
    backend = Backend().serve()
    work = bench.workdir()
    print(f"workdir {work}", flush=True)
    with bench.Stand(xray, work, probe_credentials=True) as stand:
        for spec in specs:
            try:
                r = one_run(stand, backend, xray, out, spec)
            except Exception as exc:
                r = {"spec": spec, "error": repr(exc)}
            with open(os.path.join(out, "results.jsonl"), "a") as f:
                f.write(json.dumps(r) + "\n")
            print(json.dumps({k: r.get(k) for k in ("spec", "recovery", "s1DownAfter", "exhaustedAfter",
                                                    "firstBackendRequestAfter", "backendRequests", "provenBy",
                                                    "error")}), flush=True)


if __name__ == "__main__":
    main()
