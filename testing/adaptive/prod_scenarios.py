"""End-to-end scenarios on the config the backend generates.

    python3 run.py <xray binary> --prod [scenario ...]

The client runs the base config of the backend with the profile of the first
rollout: two leastCost balancers (auto for the user, roads to the backend with
no routing rule), a provider with via auto, roads, direct, probeOnlyRoads and
an initialAnswer, keepVerified 3, connectivity and witnesses through direct,
events, report, quality, path labels and returnToCheaper on auto. The backend
is strict_provider.Backend on its own port: one user slot (-s), -p on every
entry, -r on every entry without the slot, and every request checked against
provider-protocol.md. The four entries are the ws addresses of the bench.

Stand-made differences from production, all on loopback: the provider URL is
plain http (isPlainHttpAllowed), DNS has hosts only, the geoip:private rule
and the http inbound are left out, and direct refuses the backend port unless
a scenario lets it through, as in a network that blocks the backend.

Importing the module opens no sockets.
"""
import datetime
import json
import os
import threading
import time

import bench
import probe_only_scenarios
import scenarios
import strict_provider
from scenarios import Marks, check, wait_for

MOCK_PORT = 19051 + bench.OFFSET
API_HOST = "api.test"
URL = f"http://{API_HOST}:{MOCK_PORT}{strict_provider.PATH}"
PROBE_URL = f"http://probe.test:{bench.TARGET_PORT}/bytes/65536"
PREFIX = "p-"
WARMUP = 40
URGENT = {"host", "exhausted", "expiry"}

ALL = []


def prod(fn):
    ALL.append(scenarios.Scenario(fn.__name__, fn))
    return fn


def with_api(config):
    """The Xray server lets the service credential reach the backend too."""
    config["dns"]["hosts"][API_HOST] = "127.0.0.1"
    config["routing"]["rules"].insert(0, {"user": [bench.PROBE_EMAIL], "domain": [f"full:{API_HOST}"],
                                          "port": str(MOCK_PORT), "outboundTag": "allow"})
    return config


def with_awg_api(config):
    config["dns"]["hosts"][API_HOST] = bench.TUNNEL_TARGET
    return config


class ProdStand(bench.Stand):
    """The bench with the strict backend; the scenario hands it a Backend."""

    def __init__(self, xray, workdir):
        super().__init__(xray, workdir, probe_credentials=True)
        self.backend = None
        self.server = None

    def __enter__(self):
        if bench.port_open(MOCK_PORT):
            raise RuntimeError(f"port {MOCK_PORT} already in use, another bench running?")
        server_config, awg_config = bench.server_config, bench.awg_server_config
        bench.server_config = lambda *a, **k: with_api(server_config(*a, **k))
        bench.awg_server_config = lambda *a, **k: with_awg_api(awg_config(*a, **k))
        try:
            super().__enter__()
        finally:
            bench.server_config, bench.awg_server_config = server_config, awg_config
        try:
            self.server = strict_provider.Server(MOCK_PORT, self).__enter__()
        except BaseException:
            super().__exit__()
            raise
        return self

    def __exit__(self, *exc):
        if self.server is not None:
            self.server.__exit__()
            self.server = None
        super().__exit__(*exc)

    def reset(self):
        super().reset()
        self.backend = None

    def serve(self, backend):
        self.backend = backend
        return backend


def prod_config(backend, connectivity=True, returns=None, direct=False):
    """The base config of the backend with the rollout profile."""
    hosts = {h: bench.HOST for h in bench.TARGETS + ["probe.test", "dead.test", API_HOST]}
    final = [{"action": "allow"}] if direct else [{"action": "block", "port": str(MOCK_PORT)}, {"action": "allow"}]
    sniffing = {"enabled": True, "destOverride": ["tls", "http"], "routeOnly": True}
    recheck = {"networkChanges": {"action": "recheck"}}
    returns = {"interval": "30m", "minHoldTime": "2m"} if returns is None else returns

    def profile(name):
        return {"name": name, "steps": [{"name": "payload", "destinations": [PROBE_URL], "method": "GET",
                                         "expectedStatuses": [200], "minBytes": 65536, "timeout": "8s",
                                         "progressTimeout": "4s", "isOptional": False}]}

    provider = {
        "tag": backend.expect.provider, "urls": [URL], "method": "POST", "via": list(backend.expect.via),
        "probeOnlyRoads": {}, "timeout": "10s", "isPlainHttpAllowed": True,
        "triggers": {"exhausted": {"delay": "0s..2s"}, "leaseRenewal": {}, "backendSchedule": {},
                     "whenCostAbove": {"cost": 1.5, "after": "15m..25m"}},
        "limits": {"minInterval": "5m", "urgentMinInterval": "30s", "maxRefreshesPerHour": 6},
        "apply": {"balancers": list(backend.expect.balancers), "tagPrefix": PREFIX,
                  "allowedProtocols": ["vless", "wireguard"], "drainTimeout": "5m", "maxOutbounds": 64,
                  "expiredLeaseFallback": {}},
        "baseVersion": backend.expect.base_version,
        "initialAnswer": backend.initial_answer(),
    }
    health = {
        "host": {"view": {"balancer": "auto"}},
        "probes": {"candidates": {"keepVerified": 3}, "profiles": [profile("full"), profile("candidate")]},
        "telemetry": {"events": {"maxEvents": 500}, "report": {}, "quality": {}, "pathLabels": {}},
        "providers": [provider],
    }
    if connectivity:
        health["network"] = {
            "connectivity": {"outbound": "direct", "destinations": [f"http://{bench.HOST}:{bench.CONNECTIVITY_PORT}/204"],
                             "timeout": "4s"},
            "witnesses": {"outbounds": ["direct"]}}
    return {
        "log": {"loglevel": "warning"},
        "api": {"tag": "api", "listen": f"127.0.0.1:{bench.API_PORT}",
                "services": ["HealthService", "RoutingService", "StatsService", "ObservatoryService"]},
        "metrics": {"tag": "metrics", "listen": f"127.0.0.1:{bench.METRICS_PORT}"},
        "stats": {},
        "dns": {"hosts": hosts, "queryStrategy": "UseIPv4"},
        "inbounds": [{"tag": "socks-in", "listen": "127.0.0.1", "port": bench.SOCKS_PORT, "protocol": "socks",
                      "settings": {"auth": "noauth", "udp": True}, "sniffing": sniffing}],
        "outbounds": [{"tag": "direct", "protocol": "freedom",
                       "settings": {"domainStrategy": "UseIPv4", "finalRules": final}},
                      {"tag": "block", "protocol": "blackhole"}],
        "routing": {
            "domainStrategy": "IPIfNonMatch",
            "rules": [{"type": "field", "network": "udp", "port": "443", "outboundTag": "block"},
                      {"type": "field", "network": "udp", "port": "80", "outboundTag": "block"},
                      {"type": "field", "port": "53", "network": "tcp,udp", "balancerTag": "auto"},
                      {"type": "field", "ip": ["::/0"], "outboundTag": "block"},
                      {"type": "field", "network": "tcp,udp", "balancerTag": "auto"}],
            "balancers": [
                {"tag": "auto", "fallbackTag": "block",
                 "strategy": {"type": "leastCost", "settings": recheck | ({"returnToCheaper": returns} if returns else {})}},
                {"tag": "roads", "fallbackTag": "block", "strategy": {"type": "leastCost", "settings": dict(recheck)}}]},
        "policy": {"levels": {"8": {"bufferSize": 3, "connIdle": 300, "downlinkOnly": 4, "handshake": 3,
                                    "statsUserDownlink": False, "statsUserOnline": False, "statsUserUplink": False,
                                    "uplinkOnly": 2}},
                   "system": {"statsInboundDownlink": False, "statsInboundUplink": False,
                              "statsOutboundDownlink": False, "statsOutboundUplink": False}},
        "healthObservatory": health,
    }


# ---- helpers ------------------------------------------------------------------

def entry_ports(*entries):
    return ",".join(str(bench.address_port("ws", e)) for e in entries)


def full(backend, entry, role):
    return PREFIX + backend.tag(entry, role)


class Clock:
    """Seconds since the client started, for timelines written in absolute seconds."""

    def __init__(self):
        self.start = time.time()

    def until(self, seconds):
        left = self.start + seconds - time.time()
        if left > 0:
            time.sleep(left)

    def __call__(self):
        return time.time() - self.start


def recovered(tr, at, streak=5):
    return bench.recovery(tr.records, at, streak)[0] is not None


def wait_recovery(tr, at, timeout, streak=5):
    wait_for(lambda: recovered(tr, at, streak), timeout)
    return bench.recovery(tr.records, at, streak)


def fields(events, typ, since=0):
    return [e["fields"] for e in bench.events_of(events, typ, since)]


def refresh_starts(events):
    """(start, trigger, refresh) of every refresh, from its first attempt."""
    starts = {}
    for e in events:
        f = e.get("fields", {})
        if e["type"] == "provider_fetch" and isinstance(f.get("refresh"), int):
            t = e["ts"] / 1000
            if f["refresh"] not in starts or t < starts[f["refresh"]][0]:
                starts[f["refresh"]] = (t, f.get("trigger"), f["refresh"])
    return [starts[n] for n in sorted(starts)]


def limits_check(events, per_hour=6):
    starts = refresh_starts(events)
    bad = []
    for (t0, _, n0), (t1, trigger, n1) in zip(starts, starts[1:]):
        floor = 30 if trigger in URGENT else 300
        if t1 - t0 < floor - 0.5:
            bad.append(f"refresh {n1} ({trigger}) {round(t1 - t0, 1)} s after {n0}")
    for t, _, _ in starts:
        within = [s for s in starts if t <= s[0] < t + 3600]
        if len(within) > per_hour:
            bad.append(f"{len(within)} refreshes in an hour")
            break
    value = bad or [(n, trigger) for _, trigger, n in starts]
    return check("refreshes keep the 30 s, 5 m and 6 per hour limits", value, not bad, "urgent >= 30 s, other >= 300 s")


def credential_check(stand, since):
    """The service credential reaches nothing but the probe target and the backend."""
    access = probe_only_scenarios.credential_access(stand.server_access_log, since)
    allowed = {f"tcp:probe.test:{bench.TARGET_PORT}", f"tcp:{API_HOST}:{MOCK_PORT}"}
    leaked = [a["destination"] for a in access if a["email"] == bench.PROBE_EMAIL and a["destination"] not in allowed]
    return check("service credential reached only probes and the backend", sorted(set(leaked)), not leaked, "[]")


def protocol_checks(stand, c, backend, since):
    rules = sorted({v["rule"] for v in backend.violations})
    return [check("backend saw no protocol violation", rules[:8], not rules, "[]"),
            limits_check(c.events), credential_check(stand, since)]


def save(work, name, **raw):
    with open(os.path.join(work, name + "-raw.json"), "w") as f:
        json.dump(raw, f, indent=1, default=str)


def first_switch_to(events, tag, since=0):
    return next((e for e in bench.switches(events, since) if e["fields"].get("to") == tag), None)


def strip(tag):
    return tag[len(PREFIX):] if isinstance(tag, str) and tag.startswith(PREFIX) else tag


def road_of(fetch):
    """How an attempt reached the backend: (via, outbound without the prefix)."""
    return fetch.get("via"), strip(fetch.get("outbound"))


def routed(c, since, until=None):
    """User connections by outbound from the access log, as Client.routed for
    the inbound tag of production."""
    count = {}
    with open(c.log_path, errors="replace") as f:
        for line in f:
            if " accepted " not in line or "[socks-in -> " not in line:
                continue
            stamp = datetime.datetime.strptime(line[:26], "%Y/%m/%d %H:%M:%S.%f").timestamp()
            if stamp >= since and (until is None or stamp <= until):
                tag = line.rsplit("[socks-in -> ", 1)[1].split("]")[0]
                count[tag] = count.get(tag, 0) + 1
    return count


def failed(tr, since, until):
    return sum(1 for r in tr.records if since <= r[0] < until and not r[1])


def incident_checks(report, frm, kind, outcome, total=None):
    """The incident of the final report that starts at frm."""
    incidents = (report or {}).get("incidents", [])
    inc = next((i for i in incidents if i.get("from") == frm), None)
    keys = ["detect_ms", "total_ms"] + (["switch_ms", "recover_ms"] if outcome == "recovered" else [])
    value = inc and {k: inc.get(k) for k in ["kind", "to", "outcome", "backend_ms"] + keys}
    ok = (inc is not None and inc.get("kind") == kind and inc.get("outcome") == outcome
          and all(isinstance(inc.get(k), int) for k in keys))
    checks = [check("final report incident from the slot", value or incidents, ok,
                    f"kind {kind}, outcome {outcome}, {', '.join(keys)}")]
    if ok and total is not None:
        checks.append(check("incident total_ms against the bench clock, s", round(inc["total_ms"] / 1000, 1),
                            abs(inc["total_ms"] / 1000 - total) <= 10, f"{round(total, 1)} +- 10"))
    return checks


class Screen:
    """Polls View once a second, as the screen of an application does."""

    def __init__(self, client):
        self.client, self.views, self.errors = client, [], []
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while not self._stop.wait(1):
            try:
                self.views.append(self.client.api("view"))
            except Exception as e:
                self.errors.append(repr(e))

    def stop(self):
        self._stop.set()
        self.thread.join(timeout=15)

    def states(self):
        """The states in the order seen, each run of one state once, without
        the ready before the first connection."""
        out = []
        for v in self.views:
            if v["state"] != "ready" and (not out or out[-1] != v["state"]):
                out.append(v["state"])
        return out

    def samples(self):
        """The first view of every state."""
        out = {}
        for v in self.views:
            out.setdefault(v["state"], v)
        return out


def slot_after_move(backend, events, since):
    """The new slot the backend gave after since, and the switch to it."""
    tag = full(backend, backend.slot, "s")
    return tag, first_switch_to(events, tag, since)


# ---- scenarios ------------------------------------------------------------------

@prod
def prod_cold_start(stand, xray, work, name):
    """The core starts on the initialAnswer: user traffic goes through the slot
    at once, keepVerified probe credentials get verified and no more, and a
    refresh from the host carries a body the protocol accepts."""
    backend = stand.serve(strict_provider.Backend())
    began = time.time()
    m, st = Marks(), {}
    with bench.Client(xray, prod_config(backend), work, name) as c:
        clock = Clock()
        tr = bench.Traffic(rate=2, big=0.5, timeout=5).start()
        clock.until(60)
        st["before"] = c.status()
        m["refresh"] = time.time()
        st["refresh"] = c.api("refresh", backend.expect.provider)
        wait_for(lambda: backend.requests, 10)
        clock.until(66)
        st["after"] = c.status()
        tr.stop()
    events = c.events
    slot = full(backend, 0, "s")
    applies = fields(events, "provider_apply")
    first = next(iter(bench.switches(events)), None)
    reqs = backend.requests
    used = routed(c, clock.start, m["refresh"])
    passes = {}
    for e in events:
        f = e["fields"]
        if e["type"] == "probe_result" and f.get("reason") == "candidate" and f.get("is_ok") is True:
            passes.setdefault(f["outbound"], e["ts"] / 1000 - clock.start)
    starts = {e["fields"]["outbound"] for e in events if e["type"] == "probe_start"
              and e["fields"].get("reason") == "candidate"}
    third = sorted(passes.values())[2] if len(passes) >= 3 else None
    provider = (st["before"].get("providers") or [{}])[0]
    host = [f for f in applies if f.get("trigger") == "host"]
    n = len(backend.expect.known)
    save(work, name, events=events, statuses=st, requests=reqs, violations=backend.violations)
    return [
        check("initial set applied (trigger, added, clock)", applies and [applies[0].get(k) for k in ("trigger", "added", "clock")],
              bool(applies) and applies[0].get("trigger") == "initial" and applies[0].get("added") == n
              and applies[0].get("clock") == "server", f"[initial, {n}, server]"),
        check("first switch to the slot before any request", first and (first["fields"]["to"], first["fields"]["reason"]),
              first is not None and first["fields"]["to"] == slot and (not reqs or first["ts"] / 1000 < reqs[0]["at"]),
              "(slot, start)"),
        check("user connections by outbound", used, set(used) == {slot} and used[slot] >= 20, "slot only"),
        check("user requests failed", failed(tr, clock.start, time.time()), failed(tr, clock.start, time.time()) == 0, "== 0"),
        check("probe credentials verified (Status, probed, passed)",
              (provider.get("probe_only_verified"), len(starts), len(passes)),
              provider.get("probe_only_verified") == 3 and len(starts) == 3 and len(passes) == 3, "(3, 3, 3) of 4"),
        check("third credential verified after the start, s", third and round(third, 1),
              third is not None and third <= WARMUP, f"<= {WARMUP}"),
        check("host refresh: one request, trigger host, set unchanged",
              [(r["body"] or {}).get("trigger") for r in reqs] + [(f.get("is_ok"), f.get("is_changed")) for f in host],
              len(reqs) == 1 and (reqs[0]["body"] or {}).get("trigger") == "host"
              and [(f.get("is_ok"), f.get("is_changed")) for f in host] == [(True, False)], "[host, (True, False)]"),
    ] + protocol_checks(stand, c, backend, began)


def slot_dead(stand, xray, work, name, script=(), limit=30, mode="blackhole"):
    """The entry of the slot dies, the backend is reachable only over the roads.
    Returns the client, the backend, the traffic and the marks."""
    backend = stand.serve(strict_provider.Backend())
    backend.script = list(script)
    m = Marks()
    with bench.Client(xray, prod_config(backend), work, name) as c:
        clock = Clock()
        tr = bench.Traffic(rate=2, big=0.5, timeout=5).start()
        clock.until(WARMUP)
        m["fault"] = time.time()
        backend.kill(0)
        bench.chaos(entry_ports(0), mode)
        wait_recovery(tr, m["fault"], limit + 60)
        time.sleep(5)
        m["status"] = c.status()
        tr.stop()
    return c, backend, tr, m


def road_checks(backend, events, m):
    old = full(backend, 0, "s")
    new, switched = slot_after_move(backend, events, m["fault"])
    exhausted = [f for f in fields(events, "exhausted", m["fault"]) if f.get("balancer") == "auto"]
    reached = [f for f in fields(events, "provider_fetch", m["fault"]) if f.get("is_ok") is True]
    roads = [road_of(f) for f in reached]
    service = all((via == "roads" and str(out).endswith("-r")) or (via == "auto" and str(out).endswith("-p"))
                  for via, out in roads)
    applied = [f for f in fields(events, "provider_apply", m["fault"]) if f.get("is_changed") is True]
    return [
        check("auto exhausted after the slot died (from, held)", [(strip(f.get("from"))[-2:], strip(f.get("held"))[-2:]
                                                                   if f.get("held") else None) for f in exhausted],
              bool(exhausted) and exhausted[0].get("from") == old and exhausted[0].get("held") == old,
              "from and held the slot"),
        check("backend reached over a service credential (via, outbound)", roads,
              bool(roads) and service and not any(str(out).startswith(strict_provider.ENTRY_IDS[0]) for _, out in roads),
              "roads/-r or auto/-p of a live entry"),
        check("new slot applied on another entry", applied and [(f.get("added"), f.get("removed"), f.get("was_exhausted"))
                                                                for f in applied],
              bool(applied) and backend.slot != 0 and applied[0].get("was_exhausted") is True, "was_exhausted"),
        check("auto switched to the new slot", switched and (switched["fields"]["to"][:8], switched["fields"]["reason"]),
              switched is not None, new[:8]),
    ]


@prod
def prod_slot_entry_dead(stand, xray, work, name):
    """The entry of the slot goes silent and direct cannot reach the backend:
    the core asks over a road, gets a slot on another entry, and traffic comes
    back. The final report keeps the incident with its durations."""
    began = time.time()
    c, backend, tr, m = slot_dead(stand, xray, work, name)
    rec, fails = bench.recovery(tr.records, m["fault"], 5)
    save(work, name, events=c.events, requests=backend.requests, violations=backend.violations,
         report=c.final_report, marks=m, traffic=sorted(tr.records))
    return road_checks(backend, c.events, m) + [
        check("user outage after the entry died, s (failed requests)", rec and (round(rec, 1), fails),
              rec is not None and rec <= 30, "<= 30"),
    ] + incident_checks(c.final_report, full(backend, 0, "s"), "exhausted", "recovered") + [
        check("incident backend_ms (exhausted fixed by the provider)",
              next((i.get("backend_ms") for i in (c.final_report or {}).get("incidents", [])), None),
              any(isinstance(i.get("backend_ms"), int) for i in (c.final_report or {}).get("incidents", [])), "present"),
    ] + protocol_checks(stand, c, backend, began)


@prod
def prod_slot_dead_retry_after(stand, xray, work, name):
    """As prod_slot_entry_dead, but the backend first answers 503 with
    Retry-After 20 and the error body: the core stops that refresh, asks
    again after the pause and urgentMinInterval, without a grown backoff,
    and gets the slot."""
    began = time.time()
    c, backend, tr, m = slot_dead(stand, xray, work, name, script=[("busy", 20)], limit=60)
    rec, fails = bench.recovery(tr.records, m["fault"], 5)
    reqs = [r for r in backend.requests if r["at"] >= m["fault"]]
    fetches = fields(c.events, "provider_fetch", m["fault"])
    busy = [f for f in fetches if f.get("status") == 503]
    after_busy = [f for f in fetches[fetches.index(busy[0]) + 1:] if f.get("refresh") == busy[0].get("refresh")] \
        if busy else []
    gap = reqs[1]["at"] - reqs[0]["at"] if len(reqs) >= 2 else None
    second = (reqs[1]["body"] or {}) if len(reqs) >= 2 else {}
    save(work, name, events=c.events, requests=backend.requests, violations=backend.violations,
         report=c.final_report, marks=m, traffic=sorted(tr.records))
    return [
        check("503 seen by the core (status, code, retry_after_ms)",
              busy and [busy[0].get(k) for k in ("status", "code", "retry_after_ms")],
              bool(busy) and busy[0].get("code") == "unavailable" and busy[0].get("retry_after_ms") == 20000,
              "[503, unavailable, 20000]"),
        check("attempts of that refresh after the 503", len(after_busy), not after_busy, "== 0"),
        check("next request after the 503, s", gap and round(gap, 1), gap is not None and 29.5 <= gap <= 36,
              "30 .. 36: Retry-After 20..26, then urgentMinInterval 30"),
        check("next request: trigger, retryLevel", (second.get("trigger"), second.get("retryLevel")),
              second.get("trigger") == "exhausted" and second.get("retryLevel") == 0, "(exhausted, 0)"),
    ] + road_checks(backend, c.events, m)[2:] + [
        check("user outage after the entry died, s (failed requests)", rec and (round(rec, 1), fails),
              rec is not None and rec <= 60, "<= 60"),
    ] + protocol_checks(stand, c, backend, began)


@prod
def prod_all_entries_dead(stand, xray, work, name):
    """Every entry goes silent, then one comes back: the core leaves the
    exhaustion through that entry and a slot the backend puts on it."""
    backend = stand.serve(strict_provider.Backend())
    began = time.time()
    m = Marks()
    with bench.Client(xray, prod_config(backend), work, name) as c:
        clock = Clock()
        tr = bench.Traffic(rate=2, big=0.5, timeout=5).start()
        screen = Screen(c)
        clock.until(WARMUP)
        m["fault"] = time.time()
        backend.kill(0, 1, 2, 3)
        bench.chaos(entry_ports(0, 1, 2, 3), "blackhole")
        clock.until(WARMUP + 60)
        m["revive"] = time.time()
        backend.revive(2)
        bench.chaos(entry_ports(2), "pass")
        wait_recovery(tr, m["revive"], 240)
        time.sleep(5)
        m["status"] = c.status()
        screen.stop()
        tr.stop()
    events = c.events
    rec, fails = bench.recovery(tr.records, m["revive"], 5)
    during = [(e["fields"].get("refresh"), e["fields"].get("via"), strip(e["fields"].get("outbound")),
               e["fields"].get("error") or e["fields"].get("code"))
              for e in bench.events_of(events, "provider_fetch", m["fault"]) if e["ts"] / 1000 < m["revive"]]
    reached = [r for r in backend.requests if r["at"] >= m["revive"]]
    new = full(backend, 2, "s")
    switched = first_switch_to(events, new, m["revive"])
    first = [via for refresh, via, _, _ in during if during and refresh == during[0][0]]
    states = screen.states()
    save(work, name, events=events, requests=backend.requests, violations=backend.violations,
         report=c.final_report, marks=m, traffic=sorted(tr.records), views=screen.samples())
    return [
        check("view states on the screen (errors)", (states, screen.errors[:3]),
              not screen.errors and states[:1] == ["ok"] and states[-1:] == ["ok"]
              and bool({"searching", "blocked"} & set(states[1:-1])), "ok, searching or blocked, ok"),
        check("attempts while every entry was dead (refresh, via, outbound, error)", during[:12], True, "info"),
        check("roads of the first refresh after the entries died", first,
              {"auto", "roads", "direct"} <= set(first), "every via, direct among them"),
        check("first request at the backend after the revival, s",
              reached and round(reached[0]["at"] - m["revive"], 1), bool(reached), "info, must come"),
        check("slot moved to the revived entry and auto switched to it",
              (backend.slot, switched and switched["fields"]["reason"]), backend.slot == 2 and switched is not None,
              "(2, switch)"),
        check("user outage after one entry came back, s (failed requests)", rec and (round(rec, 1), fails),
              rec is not None and rec <= 90, "<= 90: one backoff step of 60 s +30% and a request"),
    ] + incident_checks(c.final_report, full(backend, 0, "s"), "exhausted", "recovered") \
        + protocol_checks(stand, c, backend, began)


def network_gone(stand, xray, work, name, connectivity):
    backend = stand.serve(strict_provider.Backend())
    began = time.time()
    m = Marks()
    everything = entry_ports(0, 1, 2, 3) + f",{bench.CONNECTIVITY_PORT}"
    with bench.Client(xray, prod_config(backend, connectivity=connectivity), work, name) as c:
        clock = Clock()
        tr = bench.Traffic(rate=2, big=0.5, timeout=5).start()
        clock.until(WARMUP)
        m["down"] = time.time()
        bench.chaos(everything, "blackhole")
        clock.until(WARMUP + 40)
        m["status_down"] = c.status()
        m["up"] = time.time()
        bench.chaos(everything, "pass")
        wait_recovery(tr, m["up"], 150)
        time.sleep(5)
        m["status"] = c.status()
        tr.stop()
    events = c.events
    slot = full(backend, 0, "s")
    rec, fails = bench.recovery(tr.records, m["up"], 5)
    downs = [f for f in fields(events, "path_state", m["down"]) if f.get("state") == "down"]
    lasting = [(strip(f["outbound"])[-2:], f.get("quarantine_level"), f.get("reason")) for f in downs
               if not f.get("is_provisional")]
    path = next((p for p in m["status"].get("paths", []) if p["outbound"] == slot), {})
    exhausted = fields(events, "exhausted", m["down"])
    reached = [r for r in backend.requests if r["at"] >= m["down"]]
    save(work, name, events=events, requests=backend.requests, violations=backend.violations,
         report=c.final_report, marks=m, traffic=sorted(tr.records))
    checks = [
        check("user waits after the network returned, s (failed requests)", rec and (round(rec, 1), fails),
              rec is not None and rec <= 10, "<= 10"),
        check("paths ruled down for good while the network was gone", lasting, not lasting,
              "none: provisional at most"),
        check("slot after the return (state, quarantine level)", (path.get("state"), path.get("quarantine_level")),
              path.get("state") in ("up", "unknown") and not path.get("quarantine_level"), "(up, none)"),
        check("auto exhausted while the network was gone (held)", [strip(f.get("held"))[-2:] if f.get("held") else None
                                                                   for f in exhausted], True, "info"),
        check("requests that reached the backend after the loss", [(r["body"] or {}).get("trigger") for r in reached],
              True, "info"),
    ]
    if connectivity:
        down = fields(events, "network_down", m["down"])
        checks.append(check("connectivity paused decisions (network_down)", [f.get("error") for f in down],
                            bool(down), ">= 1"))
    return checks + protocol_checks(stand, c, backend, began)


@prod
def prod_network_gone_checked(stand, xray, work, name):
    """The whole network disappears for 40 s with no word from the host, then
    returns; the config has the connectivity check of the rollout profile."""
    return network_gone(stand, xray, work, name, True)


@prod
def prod_network_gone_unchecked(stand, xray, work, name):
    """As prod_network_gone_checked on the base config of the backend: no
    connectivity check, no witnesses."""
    return network_gone(stand, xray, work, name, False)


@prod
def prod_host_network_signals(stand, xray, work, name):
    """The host reports the loss of the network and, 20 s later, a new network
    in which everything works. Nothing is ruled down meanwhile, traffic comes
    back right after the change, and the next request names the network by
    hash only."""
    backend = stand.serve(strict_provider.Backend())
    backend.expect.secrets.add("lte-2")
    began = time.time()
    m = Marks()
    everything = entry_ports(0, 1, 2, 3) + f",{bench.CONNECTIVITY_PORT}"
    with bench.Client(xray, prod_config(backend), work, name) as c:
        clock = Clock()
        tr = bench.Traffic(rate=2, big=0.5, timeout=5).start()
        clock.until(WARMUP)
        m["lost"] = time.time()
        bench.chaos(everything, "blackhole")
        c.api("lost")
        clock.until(WARMUP + 20)
        m["status_lost"] = c.status()
        bench.chaos(everything, "pass")
        m["change"] = time.time()
        c.network("lte-2")
        wait_recovery(tr, m["change"], 60)
        clock.until(WARMUP + 55)
        m["refresh"] = time.time()
        c.api("refresh", backend.expect.provider)
        wait_for(lambda: [r for r in backend.requests if r["at"] >= m["refresh"]], 15)
        m["status"] = c.status()
        tr.stop()
    events = c.events
    rec, fails = bench.recovery(tr.records, m["change"], 5)
    down = fields(events, "network_down", m["lost"])
    changes = fields(events, "network_change", m["lost"])
    ruled = [(strip(e["fields"]["outbound"])[-2:], e["fields"]["state"])
             for e in bench.events_of(events, "path_state", m["lost"])
             if e["fields"].get("state") in ("down", "suspect") and e["ts"] / 1000 < m["change"]]
    exhausted = [e for e in bench.events_of(events, "exhausted", m["lost"]) if e["ts"] / 1000 < m["change"]]
    body = next((r["body"] for r in backend.requests if r["at"] >= m["refresh"] and r["body"]), {})
    net = body.get("network", {})
    save(work, name, events=events, requests=backend.requests, violations=backend.violations,
         report=c.final_report, marks=m, traffic=sorted(tr.records))
    return [
        check("network_down from the host", [f.get("error") for f in down], [f.get("error") for f in down][:1] == ["host"],
              "[host]"),
        check("Status while the network is lost", m["status_lost"].get("is_network_up"),
              m["status_lost"].get("is_network_up") is False, "False"),
        check("paths ruled down or suspect while lost", ruled, not ruled, "none"),
        check("exhausted while lost", len(exhausted), not exhausted, "== 0"),
        check("network_change after the host reported it (known, generation)",
              changes and [(f.get("is_known"), f.get("generation")) for f in changes], len(changes) == 1, "one"),
        check("user waits after the network change, s (failed requests)", rec and (round(rec, 1), fails),
              rec is not None and rec <= 10, "<= 10"),
        check("network of the next request (id hashed, isUp)", (net.get("id"), net.get("isUp")),
              isinstance(net.get("id"), str) and net.get("id") != "lte-2" and net.get("isUp") is True,
              "(16 hex, True)"),
    ] + protocol_checks(stand, c, backend, began)


@prod
def prod_labels_change(stand, xray, work, name):
    """The backend relabels the slot and keeps its outbound: Status shows the
    new labels and a download through the slot goes on."""
    backend = stand.serve(strict_provider.Backend(slot_labels={"country": "nl", "node": "e1"}))
    new_labels = {"country": "de", "node": "e1b"}
    began = time.time()
    m, st, got = Marks(), {}, {}
    slot = full(backend, 0, "s")
    with bench.Client(xray, prod_config(backend), work, name) as c:
        clock = Clock()
        tr = bench.Traffic(rate=2, big=0.5, timeout=5).start()
        clock.until(20)
        st["before"] = c.status()

        def download():
            got["bytes"] = bench.download(bench.TARGETS[0], "/slow/4194304/16", 40, tls=False)
            got["end"] = time.time()

        worker = threading.Thread(target=download, daemon=True)
        worker.start()
        clock.until(24)
        backend.script = [("labels", new_labels)]
        m["refresh"] = time.time()
        c.api("refresh", backend.expect.provider)
        wait_for(lambda: fields(c.events, "provider_apply", m["refresh"]), 15)
        time.sleep(2)
        st["after"] = c.status()
        worker.join(40)
        tr.stop()
    events = c.events

    def labels(status):
        return next((p.get("labels") for p in status.get("paths", []) if p["outbound"] == slot), None)
    apply = fields(events, "provider_apply", m["refresh"])
    switched = bench.switches(events, m["refresh"])
    save(work, name, events=events, statuses=st, requests=backend.requests, violations=backend.violations)
    return [
        check("labels in Status before", labels(st["before"]), labels(st["before"]) == {"country": "nl", "node": "e1"},
              "initial labels"),
        check("relabelled set applied (changed, added, replaced, removed)",
              apply and [apply[0].get(k) for k in ("is_changed", "added", "replaced", "removed")],
              bool(apply) and apply[0].get("is_ok") is True and not apply[0].get("added")
              and not apply[0].get("replaced") and not apply[0].get("removed"), "nothing added, replaced, removed"),
        check("labels in Status after", labels(st["after"]), labels(st["after"]) == new_labels, new_labels),
        check("download through the slot completed", got.get("bytes"), got.get("bytes") == 4194304, "4194304"),
        check("switches after the relabel", [(e["fields"]["to"], e["fields"]["reason"]) for e in switched],
              not switched, "none"),
        check("user requests failed", failed(tr, m["refresh"], time.time()), failed(tr, m["refresh"], time.time()) == 0,
              "== 0"),
    ] + protocol_checks(stand, c, backend, began)


@prod
def prod_auth_refused(stand, xray, work, name):
    """The backend answers 401 with the error body: the host hears of it, the
    core holds every refresh, and new headers from the host end the pause."""
    backend = stand.serve(strict_provider.Backend())
    backend.script = [("unauthorized",)]
    token = "Bearer host-2"
    began = time.time()
    m, st = Marks(), {}
    with bench.Client(xray, prod_config(backend), work, name) as c:
        clock = Clock()
        tr = bench.Traffic(rate=1, big=0, timeout=5).start()
        clock.until(10)
        m["first"] = time.time()
        c.api("refresh", backend.expect.provider)
        wait_for(lambda: fields(c.events, "provider_auth_failed"), 15)
        st["refused"] = c.status()
        clock.until(45)
        m["held"] = time.time()
        st["held_answer"] = c.api("refresh", backend.expect.provider)
        clock.until(50)
        m["headers"] = time.time()
        c.api("headers", backend.expect.provider, stdin=json.dumps({"Authorization": token}))
        st["new_answer"] = c.api("refresh", backend.expect.provider)
        wait_for(lambda: [r for r in backend.requests if r["at"] >= m["headers"]], 15)
        time.sleep(2)
        st["after"] = c.status()
        tr.stop()
    events = c.events
    refused = fields(events, "provider_auth_failed")
    provider = (st["refused"].get("providers") or [{}])[0]
    skips = [f.get("reason") for f in fields(events, "provider_skip", m["held"])]
    held = [r for r in backend.requests if m["held"] <= r["at"] < m["headers"]]
    after = [r for r in backend.requests if r["at"] >= m["headers"]]
    seen = json.dumps([events, c.reports, st])
    save(work, name, events=events, statuses=st, requests=backend.requests, violations=backend.violations)
    return [
        check("provider_auth_failed (status, code)", [(f.get("status"), f.get("code")) for f in refused],
              [(f.get("status"), f.get("code")) for f in refused] == [(401, "unauthorized")], "[(401, unauthorized)]"),
        check("Status after the refusal (is_auth_refused, auth_retry_in_ms)",
              (provider.get("is_auth_refused"), provider.get("auth_retry_in_ms")),
              provider.get("is_auth_refused") is True and (provider.get("auth_retry_in_ms") or 0) >= 2400000,
              "(True, >= 40 min)"),
        check("host refresh during the pause (answer, skip, requests)",
              (st["held_answer"], skips, len(held)),
              isinstance(st["held_answer"], dict) and st["held_answer"].get("reason") == "auth" and not held,
              "reason auth, no request"),
        check("request with the new headers after SetProviderHeaders, s",
              after and (round(after[0]["at"] - m["headers"], 1), after[0]["auth"]),
              bool(after) and after[0]["at"] - m["headers"] <= 3 and after[0]["auth"] == token, f"<= 3, {token}"),
        check("token in events, Status, reports", token in seen, token not in seen, "False"),
    ] + protocol_checks(stand, c, backend, began)


@prod
def prod_base_changed(stand, xray, work, name):
    """The backend names another base config: the host hears of it once per
    value, the set still applies and every request keeps the base of the core."""
    backend = stand.serve(strict_provider.Backend())
    backend.script = [("base", "base-5")]
    began = time.time()
    m, st = Marks(), {}
    with bench.Client(xray, prod_config(backend), work, name) as c:
        clock = Clock()
        tr = bench.Traffic(rate=1, big=0, timeout=5).start()
        clock.until(5)
        m["first"] = time.time()
        c.api("refresh", backend.expect.provider)
        clock.until(37)
        c.api("refresh", backend.expect.provider)
        wait_for(lambda: len(backend.requests) >= 2, 15)
        time.sleep(2)
        st["after"] = c.status()
        tr.stop()
    events = c.events
    told = [(f.get("from"), f.get("to")) for f in fields(events, "provider_base_changed")]
    applies = [(f.get("is_ok"), f.get("base_version")) for f in fields(events, "provider_apply", m["first"])]
    provider = (st["after"].get("providers") or [{}])[0]
    report = c.final_report or {}
    save(work, name, events=events, statuses=st, requests=backend.requests, violations=backend.violations, report=report)
    return [
        check("provider_base_changed (from, to)", told, told == [("base-4", "base-5")], "[(base-4, base-5)] once"),
        check("answers applied (ok, base_version)", applies, applies == [(True, "base-5")] * 2, "twice base-5"),
        check("base in the requests", [(b or {}).get("baseVersion") for b in backend.bodies()],
              len(backend.requests) == 2 and all((b or {}).get("baseVersion") == "base-4" for b in backend.bodies()),
              "base-4 each"),
        check("Status (base, answer base, changed)",
              (provider.get("base_version"), provider.get("answer_base_version"), provider.get("is_base_changed")),
              (provider.get("base_version"), provider.get("answer_base_version"), provider.get("is_base_changed"))
              == ("base-4", "base-5", True), "(base-4, base-5, True)"),
        check("final report provider_base_changed", report.get("provider_base_changed"),
              report.get("provider_base_changed") == 1, "== 1"),
    ] + protocol_checks(stand, c, backend, began)


@prod
def prod_lease_expiry(stand, xray, work, name):
    """The slot has a lease of 40 s. The core asks at half of it and the
    backend renews; at 80% of the new lease it asks again and the backend does
    not renew. The lease ends, the slot stays the last resort, traffic goes on."""
    lease = 40
    backend = stand.serve(strict_provider.Backend(leases={"s": lease}))
    backend.script = [("same",), ("hold",)]
    began = time.time()
    m, st = Marks(), {}
    slot = full(backend, 0, "s")
    with bench.Client(xray, prod_config(backend), work, name) as c:
        clock = Clock()
        tr = bench.Traffic(rate=2, big=0.5, timeout=5).start()
        wait_for(lambda: len(backend.requests) >= 2, 70)
        wait_for(lambda: fields(c.events, "provider_expired"), lease + 10)
        m["expired"] = time.time()
        time.sleep(10)
        st["after"] = c.status()
        tr.stop()
    events = c.events
    reqs = backend.requests
    at = [round(r["at"] - began, 1) for r in reqs]
    trig = [(r["body"] or {}).get("trigger") for r in reqs]
    applies = fields(events, "provider_apply", began)
    applies = [f for f in applies if f.get("trigger") != "initial"]
    expired = fields(events, "provider_expired")
    path = next((p for p in st["after"].get("paths", []) if p["outbound"] == slot), {})
    active = next((b.get("active") for b in st["after"].get("balancers", []) if b["balancer"] == "auto"), None)
    save(work, name, events=events, statuses=st, requests=reqs, violations=backend.violations, marks=m,
         traffic=sorted(tr.records))
    return [
        check("first request: trigger, time after the start, s", (trig[:1], at[:1]),
              trig[:1] == ["expiry"] and bool(at) and 0.4 * lease - 3 <= at[0] <= 0.6 * lease + 3,
              f"expiry at {0.45 * lease} .. {0.55 * lease}"),
        check("renewal applied (is_changed, renewed)", applies[:1] and [(f.get("is_changed"), f.get("renewed")) for f in applies[:1]],
              bool(applies) and applies[0].get("is_changed") is False and (applies[0].get("renewed") or 0) >= 1,
              "(False, >= 1)"),
        check("second request after the first, s", len(at) >= 2 and round(at[1] - at[0], 1),
              len(at) >= 2 and trig[1] == "expiry" and 0.8 * lease - 3 <= at[1] - at[0] <= 0.8 * lease + 4,
              f"expiry at 80% of the new lease, {0.8 * lease} s"),
        check("requests about the refused lease", len(reqs), len(reqs) == 2, "== 2"),
        check("provider_expired (outbound, renewal, was_active)",
              [(strip(f.get("outbound"))[-2:], f.get("renewal"), f.get("was_active")) for f in expired],
              [(f.get("outbound"), f.get("renewal"), f.get("was_active")) for f in expired] == [(slot, "refused", True)],
              "[(-s, refused, True)]"),
        check("slot after the end of its lease (active, is_expired)", (active == slot, path.get("is_expired")),
              active == slot and path.get("is_expired") is True, "(True, True)"),
        check("user requests failed", failed(tr, began, time.time()), failed(tr, began, time.time()) == 0, "== 0"),
    ] + protocol_checks(stand, c, backend, began)


@prod
def prod_awg_pair(stand, xray, work, name):
    """The entry of the slot carries Xray (-s, cost 1) and AmneziaWG (-a,
    cost 2). Xray goes silent: auto moves to AmneziaWG without exhaustion.
    Xray comes back: returnToCheaper moves auto back. The stand shortens the
    return to 1 m with a hold of 20 s; production has 30 m and 2 m."""
    backend = stand.serve(strict_provider.Backend(awg=True))
    began = time.time()
    m = Marks()
    slot, awg = full(backend, 0, "s"), full(backend, 0, "a")
    with bench.Client(xray, prod_config(backend, returns={"interval": "1m", "minHoldTime": "20s"}), work, name) as c:
        clock = Clock()
        tr = bench.Traffic(rate=2, big=0.5, timeout=5).start()
        clock.until(WARMUP)
        m["fault"] = time.time()
        bench.chaos(entry_ports(0), "blackhole")
        wait_recovery(tr, m["fault"], 60)
        clock.until(WARMUP + 40)
        m["back"] = time.time()
        bench.chaos(entry_ports(0), "pass")
        wait_for(lambda: first_switch_to(c.events, slot, m["back"]), 180)
        time.sleep(3)
        m["status"] = c.status()
        tr.stop()
    events = c.events
    rec, fails = bench.recovery(tr.records, m["fault"], 5)
    away = first_switch_to(events, awg, m["fault"])
    back = first_switch_to(events, slot, m["back"])
    exhausted = [e for e in bench.events_of(events, "exhausted", m["fault"]) if e["ts"] / 1000 < m["back"]]
    returns = [f for f in fields(events, "probe_result", m["back"]) if f.get("reason") == "return"]
    save(work, name, events=events, requests=backend.requests, violations=backend.violations,
         report=c.final_report, marks=m, traffic=sorted(tr.records))
    return [
        check("switch to AmneziaWG after Xray died (reason)", away and away["fields"]["reason"],
              away is not None and away["fields"]["reason"] == "failover", "failover"),
        check("exhausted on the way", len(exhausted), not exhausted, "== 0"),
        check("user outage after Xray died, s (failed requests)", rec and (round(rec, 1), fails),
              rec is not None and rec <= 20, "<= 20"),
        check("return to Xray after it came back, s (reason)",
              back and (round(back["ts"] / 1000 - m["back"], 1), back["fields"]["reason"]),
              back is not None and back["fields"]["reason"] == "return" and back["ts"] / 1000 - m["back"] <= 150,
              "return <= 150: interval 1m + three return probes"),
        check("return probes of the slot", [(strip(f["outbound"])[-2:], f.get("is_ok")) for f in returns],
              bool(returns) and all(strip(f["outbound"]).endswith("-s") for f in returns), "on -s"),
        check("requests at the backend", len(backend.requests), not backend.requests, "== 0, nothing to ask"),
    ] + protocol_checks(stand, c, backend, began)


@prod
def prod_stop_open_incident(stand, xray, work, name):
    """The entry of the slot dies and the backend answers 503 with a long
    Retry-After; the user stops the core while exhausted. The final report
    carries the open incident with its durations."""
    backend = stand.serve(strict_provider.Backend())
    backend.default = ("busy", 600)
    began = time.time()
    m = Marks()
    with bench.Client(xray, prod_config(backend), work, name) as c:
        clock = Clock()
        tr = bench.Traffic(rate=2, big=0.5, timeout=5).start()
        clock.until(WARMUP)
        m["fault"] = time.time()
        backend.kill(0)
        bench.chaos(entry_ports(0), "blackhole")
        clock.until(WARMUP + 40)
        tr.stop()
        m["stop"] = time.time()
    report = c.final_report or {}
    save(work, name, events=c.events, requests=backend.requests, violations=backend.violations, report=report, marks=m)
    return [
        check("final report (reason, exhausted_count, is_balancer_exhausted, retry_after)",
              [report.get(k) for k in ("report_reason", "exhausted_count", "is_balancer_exhausted", "provider_retry_after")],
              report.get("report_reason") == "stop" and report.get("exhausted_count", 0) >= 1
              and report.get("is_balancer_exhausted") is True and report.get("provider_retry_after", 0) >= 1,
              "[stop, >= 1, True, >= 1]"),
        check("final report recent failures", (report.get("recent") or {}).get("failed"),
              (report.get("recent") or {}).get("failed", 0) > 0, "> 0"),
    ] + incident_checks(report, full(backend, 0, "s"), "exhausted", "open", total=m["stop"] - m["fault"]) \
        + protocol_checks(stand, c, backend, began)
