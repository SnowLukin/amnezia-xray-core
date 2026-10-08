"""Bench scenarios. Each one returns checks: name, measured value, limit, ok.

Long scenarios read their length in minutes from the environment so a quick
pass and the full acceptance run use the same code:
BENCH_IDLE_MIN (default 10), BENCH_FLAP_MIN (10), BENCH_SOAK_MIN (120).
"""
import json
import os
import subprocess
import time

import bench
from bench import address_port

A0 = address_port("reality", 0)
A1 = address_port("reality", 1)
B0 = address_port("tcp_tls", 0)
IMPOSTOR = bench.server_port("tcp_tls")

THREE = [("a1", "reality", 0), ("a2", "reality", 1), ("b1", "tcp_tls", 0)]
THREE_COSTS = {"a": 1, "b": 3}


def minutes(name, default):
    return float(os.environ.get(name, default))


def check(name, value, ok, limit):
    return {"check": name, "value": value, "limit": limit, "ok": bool(ok)}


class Scenario:
    def __init__(self, name, fn, slow=False):
        self.name = name
        self.fn = fn
        self.slow = slow

    def run(self, stand, xray, work):
        return self.fn(stand, xray, work, self.name)


ALL = []


def scenario(slow=False):
    def wrap(fn):
        ALL.append(Scenario(fn.__name__, fn, slow))
        return fn
    return wrap


class Marks(dict):
    def at(self, key, fn=None):
        def go():
            self[key] = time.time()
            if fn:
                fn()
        return go


def run_timeline(traffic, actions, duration):
    """actions: (second, callable). Returns the absolute start time."""
    start = time.time()
    if traffic:
        traffic.start()
    pending = sorted(actions, key=lambda a: a[0])
    while time.time() - start < duration:
        while pending and time.time() - start >= pending[0][0]:
            pending.pop(0)[1]()
        time.sleep(0.05)
    if traffic:
        traffic.stop()
    return start


def conns_on(port, before, after):
    return after.get(port, 0) - before.get(port, 0)


def probe_checks(events, since=0):
    """Budget checks over probe starts: one at a time, spacing, per server name."""
    starts = [e for e in events if e["type"] == "probe_start" and e["ts"] / 1000 >= since]
    results = [e for e in events if e["type"] == "probe_result" and e["ts"] / 1000 >= since]
    # Overlap: a probe starts while another one has not reported yet.
    timeline = sorted([(e["ts"], 1) for e in starts] + [(e["ts"], -1) for e in results])
    running, peak = 0, 0
    for _, d in timeline:
        running += d
        peak = max(peak, running)
    gaps = [(b["ts"] - a["ts"]) / 1000 for a, b in zip(starts, starts[1:])]
    return peak, (min(gaps) if gaps else None), len(starts)


# ---- 1. failover ------------------------------------------------------------

def failover(stand, xray, work, name, port, mode, limit, members=THREE, costs=THREE_COSTS, expect_to=None,
             traffic=None, duration=40):
    cfg = bench.client_config(members, costs)
    m = Marks()
    with bench.Client(xray, cfg, work, name, profile=True) as c, bench.ConnSampler() as cs:
        tr = traffic or bench.Traffic(rate=2, big=0.5)
        mech = []
        sample = [(10 + s, lambda s=s: mech.append(c.mechanism_heap(f"{s}"))) for s in (1, 5, 8, 10, 12, 15)]
        run_timeline(tr, [(10, m.at("fault", lambda: bench.chaos(port, mode)))] + sample, duration)
        end = time.time()
        peak = max((h for t, h, _ in c.mem if t >= m["fault"]), default=0)
        base = max((h for t, h, _ in c.mem if t < m["fault"]), default=0)
    rec, fails = bench.recovery(tr.records, m["fault"])
    sw = bench.switches(c.events, m["fault"])
    checks = [
        check("recovery, s", rec and round(rec, 1), rec is not None and rec <= limit, f"<= {limit}"),
        check("switches", len(sw), len(sw) == 1, "== 1"),
    ]
    if expect_to:
        to = sw[0]["fields"]["to"] if sw else None
        checks.append(check("switched to", to, to == expect_to, expect_to))
    if sw:
        # After the switch nothing new may reach the dead address. VLESS retries
        # the dial of a connection that started before the switch for up to 2 s,
        # so the window starts later.
        t_sw = sw[0]["ts"] / 1000 + 3
        n = cs.between(port, t_sw, end)
        checks.append(check("connections to dead address after switch", n, n == 0, "== 0"))
        recheck = [e for e in c.events if e["type"] == "probe_start" and e["ts"] / 1000 > t_sw]
        checks.append(check("probes after switch", len(recheck), len(recheck) == 0, "== 0"))
    # The event log alone must tell the story: when the path became suspect or
    # down, with what evidence, and when traffic moved.
    ev = [e for e in c.events if e["ts"] / 1000 >= m["fault"]]
    noticed = next((e for e in ev if e["type"] == "path_state" and e["fields"].get("state") in ("suspect", "down")), None)
    if sw and rec is not None:
        gap = abs(sw[0]["ts"] / 1000 - (m["fault"] + rec))
        checks.append(check("switch in events vs recovery on stand, s", round(gap, 2), gap <= 1, "<= 1"))
    has_evidence = bool(noticed) and ("kinds" in noticed["fields"] or noticed["fields"].get("reason") in ("probe", "traffic"))
    checks.append(check("decision has reason and evidence", noticed and noticed["fields"].get("reason"),
                        has_evidence and bool(sw) and bool(sw[0]["fields"].get("reason")), "present"))
    mpeak = max(mech, default=0)
    checks.append(check("mechanism heap peak, MB", round(mpeak / 2**20, 2), mpeak <= 2 * 2**20, "<= 2"))
    checks.append(check("whole heap growth incl. user conns, MB", round((peak - base) / 2**20, 2), True, "info"))
    return checks


@scenario()
def failover_blackhole(stand, xray, work, name):
    return failover(stand, xray, work, name, A0, "blackhole", 10, expect_to="a2")


@scenario()
def failover_reset(stand, xray, work, name):
    return failover(stand, xray, work, name, A0, "reset", 3, expect_to="a2")


@scenario()
def failover_freeze(stand, xray, work, name):
    return failover(stand, xray, work, name, A0, "stall_after:16384", 15, expect_to="a2")


@scenario()
def failover_impostor(stand, xray, work, name):
    return failover(stand, xray, work, name, A0, f"redirect:{IMPOSTOR}", 5, expect_to="a2")


def per_transport(kind):
    def run(stand, xray, work, name):
        members = [("x1", kind, 0), ("x2", kind, 1)]
        return failover(stand, xray, work, name, address_port(kind, 0), "blackhole", 10,
                        members=members, costs={"x": 1}, expect_to="x2")
    return run


for _kind in ("tcp_tls", "ws", "xhttp", "grpc"):
    ALL.append(Scenario(f"failover_blackhole_{_kind}", per_transport(_kind)))


def plain_heap(kind):
    """Heap growth of the same fault and traffic without the mechanism, for comparison."""
    def run(stand, xray, work, name):
        cfg = bench.plain(bench.client_config([("x1", kind, 0)], {"x": 1}), "x1")
        m = Marks()
        with bench.Client(xray, cfg, work, name) as c:
            tr = bench.Traffic(rate=2, big=0.5)
            run_timeline(tr, [(10, m.at("fault", lambda: bench.chaos(address_port(kind, 0), "blackhole")))], 25)
            peak = max((h for t, h, _ in c.mem if t >= m["fault"]), default=0)
            base = max((h for t, h, _ in c.mem if t < m["fault"]), default=0)
        return [check("heap growth without the mechanism, MB", round((peak - base) / 2**20, 2), True, "info")]
    return run


for _kind in ("xhttp", "grpc"):
    ALL.append(Scenario(f"plain_heap_{_kind}", plain_heap(_kind), slow=True))


# ---- false alarms -------------------------------------------------------------

@scenario()
def one_dead_site(stand, xray, work, name):
    plan = [("t1.test", "/bytes/65536", 65536), ("dead.test", "/hang", 1), ("t2.test", "/204", 0),
            ("dead.test", "/hang", 1), ("t3.test", "/bytes/65536", 65536)]
    cfg = bench.client_config(THREE, THREE_COSTS)
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, plan=plan, timeout=20)
        start = run_timeline(tr, [], 120)
    sw = bench.switches(c.events, start + 1)
    pr = bench.probes(c.events, start)
    return [check("switches", len(sw), len(sw) == 0, "== 0"),
            check("probes", len(pr), len(pr) == 0, "== 0")]


@scenario()
def short_glitch(stand, xray, work, name):
    cfg = bench.client_config(THREE, THREE_COSTS)
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(10, m.at("fault", lambda: bench.chaos(A0, "pause"))),
                          (13, lambda: bench.chaos(A0, "pass"))], 40)
    sw = bench.switches(c.events, m["fault"])
    return [check("switches after 3 s pause", len(sw), len(sw) == 0, "== 0")]


@scenario(slow=True)
def idle_no_probes(stand, xray, work, name):
    mins = minutes("BENCH_IDLE_MIN", 10)
    cfg = bench.client_config(THREE, THREE_COSTS)
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        tr.start()
        time.sleep(15)
        tr.stop()
        _, _, g0 = c.mem[-1] if c.mem else (0, 0, 0)
        quiet_from = time.time()
        time.sleep(mins * 60)
        heap, gor = c.gc_memory()
    hits = stand.probe_hits(quiet_from)
    pr = bench.probes(c.events, quiet_from)
    return [check(f"probe requests in {mins:g} min idle", len(hits), len(hits) == 0, "== 0"),
            check("probe events while idle", len(pr), len(pr) == 0, "== 0"),
            check("goroutines idle vs busy", f"{gor} vs {g0}", gor <= g0, "idle <= busy")]


@scenario(slow=True)
def flapping(stand, xray, work, name):
    mins = minutes("BENCH_FLAP_MIN", 10)
    cfg = bench.client_config([("a1", "reality", 0), ("b1", "tcp_tls", 0)], {"a": 1, "b": 3},
                              strategy={"minHold": "30s", "upgrade": {"interval": "1m", "spacing": "10s"}})
    actions = []
    t = 10
    while t < mins * 60:
        actions.append((t, lambda: bench.chaos(A0, "blackhole")))
        actions.append((t + 20, lambda: bench.chaos(A0, "pass")))
        t += 40
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        start = run_timeline(tr, actions, mins * 60)
    sw = bench.switches(c.events, start + 1)
    ok = sum(1 for r in tr.records if r[1]) / max(1, len(tr.records))
    return [check(f"switches in {mins:g} min", len(sw), len(sw) <= 3 * mins / 10, f"<= {3 * mins / 10:g}"),
            check("requests ok, %", round(100 * ok, 1), True, "info")]


# ---- network and exhaustion ---------------------------------------------------

@scenario()
def network_down(stand, xray, work, name):
    cfg = bench.client_config(THREE, THREE_COSTS)
    ports = ",".join(str(p) for p in bench.all_address_ports())
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(10, m.at("down", lambda: bench.chaos(ports, "blackhole"))),
                          (40, m.at("up", lambda: bench.chaos(ports, "pass")))], 60)
        st = c.status()
    sw = bench.switches(c.events, 0)
    probes = bench.probes(c.events, m["down"], m["up"])
    rec, _ = bench.recovery(tr.records, m["up"])
    active = st["balancers"][0]["active"]
    return [check("switches", len(sw) - 1, len(sw) == 1, "== 0 after the start"),
            check("outbound probes while network is down", len(probes), not probes, "== 0"),
            check("active after network returns", active, active == "a1", "a1"),
            check("recovery after network returns, s", rec and round(rec, 1), rec is not None and rec <= 10, "<= 10")]


@scenario()
def long_outage(stand, xray, work, name):
    """The network is gone longer than maxPause: outbounds ruled out meanwhile come back with it."""
    cfg = bench.client_config(THREE, THREE_COSTS)
    ports = ",".join(str(p) for p in bench.all_address_ports())
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(10, m.at("down", lambda: bench.chaos(ports, "blackhole"))),
                          (110, m.at("up", lambda: bench.chaos(ports, "pass")))], 160)
        st = c.status()
    paused = bench.probes(c.events, m["down"], m["down"] + 60)
    during = bench.probes(c.events, m["down"], m["up"])
    rec, _ = bench.recovery(tr.records, m["up"])
    active = st["balancers"][0]["active"]
    a1 = next(p for p in st["paths"] if p["outbound"] == "a1")
    return [check("outbound probes during the first maxPause (60 s)", len(paused), not paused, "== 0"),
            check("outbound probes during 100 s without network", len(during), len(during) <= 6, "<= 6"),
            check("active after network returns", active, active == "a1", "a1"),
            check("a1 after network returns", a1["state"], a1["state"] != "down", "not down"),
            check("recovery after network returns, s", rec and round(rec, 1), rec is not None and rec <= 35,
                  "<= recheck max 30 + 5")]


@scenario()
def check_unreachable(stand, xray, work, name):
    """The network check cannot reach its destination, yet outbounds other than the dead one work."""
    cfg = bench.client_config(THREE, THREE_COSTS, health={"connectivity": {"maxPause": "20s"}})
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(5, lambda: bench.chaos(str(bench.CONNECTIVITY_PORT), "blackhole")),
                          (10, m.at("down", lambda: bench.chaos(str(A0), "blackhole")))], 70)
        st = c.status()
    rec, _ = bench.recovery(tr.records, m["down"])
    ignored = [e for e in c.events if e["type"] == "connectivity_ignored"]
    return [check("recovery, s", rec and round(rec, 1), rec is not None and rec <= 50,
                  "<= suspicion 8 + maxPause 20 + recheck 16 + failover 6"),
            check("check ignored after an outbound worked", len(ignored), len(ignored) == 1, "== 1"),
            check("connectivity state", st.get("connectivity"), st.get("connectivity") == "ignored", "ignored")]


@scenario()
def check_unreachable_witness(stand, xray, work, name):
    """As check_unreachable, but sites outside the tunnel work and prove that the network is there."""
    cfg = bench.client_config(THREE, THREE_COSTS,
                              health={"connectivity": {"maxPause": "20s", "witnesses": ["direct"]}})
    cfg["dns"] = {"hosts": {"w.test": "127.0.0.1"}}
    cfg["routing"]["rules"].insert(0, {"domain": ["full:w.test"], "outboundTag": "direct"})
    next(o for o in cfg["outbounds"] if o["tag"] == "direct")["settings"]["domainStrategy"] = "UseIPv4"
    tunnel = bench.TARGETS[:3]
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=3, big=1, targets=tunnel + ["w.test"])
        run_timeline(tr, [(5, lambda: bench.chaos(str(bench.CONNECTIVITY_PORT), "blackhole")),
                          (10, m.at("down", lambda: bench.chaos(str(A0), "blackhole")))], 50)
        st = c.status()
    rec, _ = bench.recovery(tr.records, m["down"], hosts=tunnel)
    direct_ok = sum(1 for r in tr.records if r[4] == "w.test" and r[1])
    return [check("recovery, s", rec and round(rec, 1), rec is not None and rec <= 25,
                  "<= suspicion 8 + two checks 4 + witness 3 + failover 6"),
            check("requests outside the tunnel ok", direct_ok, direct_ok > 0, "> 0"),
            check("connectivity state", st.get("connectivity"), st.get("connectivity") == "ignored", "ignored")]


@scenario()
def exhausted(stand, xray, work, name):
    cfg = bench.client_config(THREE, THREE_COSTS)
    member_ports = ",".join(str(p) for p in (A0, A1, B0))
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(10, m.at("down", lambda: bench.chaos(member_ports, "blackhole"))),
                          (70, m.at("leak_check")),
                          (80, m.at("up", lambda: bench.chaos(str(A1), "pass")))], 280)
    ex = [e for e in c.events if e["type"] == "exhausted" and e["ts"] / 1000 >= m["down"]]
    t_ex = ex[0]["ts"] / 1000 - m["down"] if ex else None
    # Leak: a user request reaching the target while every server is dead went around the tunnel.
    leaked = []
    with open(stand.target_log) as f:
        for line in f:
            ts, host, _ = line.split(" ", 2)
            if host.startswith("t") and m["down"] + 15 <= float(ts) <= m["up"]:
                leaked.append(line)
    rec, _ = bench.recovery(tr.records, m["up"])
    peak, gap, n = probe_checks(c.events, m["down"])
    return [check("exhausted after all paths died, s", t_ex and round(t_ex, 1), t_ex is not None and t_ex <= 20,
                  "<= 20"),
            check("requests leaked around the tunnel", len(leaked), not leaked, "== 0"),
            # A dead outbound rests 2 min +-30% before a recheck: a censor keeps a
            # frozen server frozen for that long and longer after new handshakes.
            check("recovery after one path returned, s", rec and round(rec, 1), rec is not None and rec <= 180,
                  "<= 180"),
            check("probes at once", peak, peak <= 1, "<= 1"),
            check("min pause between probes, s", gap and round(gap, 2), gap is None or gap >= 1.4, ">= 1.4"),
            check("probes", n, True, "info")]


# ---- memory and cost ----------------------------------------------------------

@scenario()
def memory_idle_members(stand, xray, work, name):
    one = bench.plain(bench.client_config([("a1", "reality", 0)], {"a": 1}), "a1")
    many = bench.client_config([(f"m{i:02d}", "reality", i % bench.ADDRESSES) for i in range(20)], {"m": 1})
    res = {}
    for label, cfg in (("one", one), ("many", many)):
        with bench.Client(xray, cfg, work, f"{name}-{label}") as c:
            tr = bench.Traffic(rate=2, big=0.5)
            run_timeline(tr, [], 10)
            time.sleep(3)
            res[label] = c.gc_memory()[0]
    diff = (res["many"] - res["one"]) / 2**20
    return [check("heap: 20 members with health vs 1 plain, MB", round(diff, 2), diff <= 1, "<= 1")]


@scenario()
def throughput(stand, xray, work, name):
    size = 1 << 30
    plain = bench.plain(bench.client_config([("b1", "tcp_tls", 0)], {"b": 1}), "b1")
    observed = bench.client_config([("b1", "tcp_tls", 0)], {"b": 1})
    res = {}
    for label, cfg in (("plain", plain), ("observed", observed)) * 2:
        with bench.Client(xray, cfg, work, f"{name}-{label}") as c:
            cpu0 = cpu_seconds(c.proc.pid)
            t0 = time.time()
            got = bench.download("t1.test", f"/bytes/{size}")
            dt = time.time() - t0
            cpu = cpu_seconds(c.proc.pid) - cpu0
        assert got == size, f"downloaded {got} of {size}"
        best = res.get(label)
        if best is None or dt < best[0]:
            res[label] = (dt, cpu)
    slower = (res["observed"][0] / res["plain"][0] - 1) * 100
    cpu = (res["observed"][1] / res["plain"][1] - 1) * 100
    return [check("download 1 GB slower with health, %", round(slower, 1), slower <= 2, "<= 2"),
            check("client CPU higher with health, %", round(cpu, 1), cpu <= 2, "<= 2")]


def cpu_seconds(pid):
    out = subprocess.run(["ps", "-o", "time=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    parts = [float(x) for x in out.replace("-", ":").split(":")]
    total = 0.0
    for p in parts:
        total = total * 60 + p
    return total


# ---- 2. return to cheaper outbounds and network changes ------------------------

CHEAP_EXPENSIVE = [("a1", "reality", 0), ("b1", "tcp_tls", 0)]
CHEAP_EXPENSIVE_COSTS = {"a": 1, "b": 3}


def switches_to(events, to, since=0):
    return [e for e in bench.switches(events, since) if e["fields"]["to"] == to]


@scenario()
def upgrade_after_network_change(stand, xray, work, name):
    strategy = {"minHold": "30s", "upgrade": {"spacing": "20s"}}
    cfg = bench.client_config(CHEAP_EXPENSIVE, CHEAP_EXPENSIVE_COSTS, strategy=strategy)
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(10, m.at("down", lambda: bench.chaos(A0, "blackhole"))),
                          (50, m.at("fixed", lambda: bench.chaos(A0, "pass"))),
                          (60, m.at("net", lambda: c.network("cafe")))], 150)
    back = switches_to(c.events, "a1", m["net"])
    first_to_b = switches_to(c.events, "b1", m["down"])
    took = back[0]["ts"] / 1000 - m["net"] if back else None
    held = (back[0]["ts"] - first_to_b[0]["ts"]) / 1000 if back and first_to_b else None
    upgrades = [e for e in c.events if e["type"] == "probe_start" and e["fields"].get("reason") == "upgrade"
                and e["ts"] / 1000 >= m["net"]]
    return [check("return after network change, s", took and round(took, 1), took is not None and took <= 70,
                  "<= 3 x 20 + 10"),
            check("time on the expensive path before return, s", held and round(held, 1),
                  held is not None and held >= 30, ">= minHold 30"),
            check("probes before the return", len(upgrades), len(upgrades) == 3, "== 3")]


@scenario()
def home_lte_home(stand, xray, work, name):
    """At home the cheap path works; on LTE it is blocked; back home it works again."""
    strategy = {"minHold": "30s"}
    cfg = bench.client_config(CHEAP_EXPENSIVE, CHEAP_EXPENSIVE_COSTS, strategy=strategy)
    m = Marks()

    def to_lte():
        c.network("lte")
        bench.chaos(A0, "blackhole")

    def to_home():
        bench.chaos(A0, "pass")
        c.network("home")

    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(2, lambda: c.network("home")),
                          (20, m.at("lte", to_lte)),
                          (80, m.at("home", to_home))], 110)
        st = c.status()
    back = switches_to(c.events, "a1", m["home"])
    took = back[0]["ts"] / 1000 - m["home"] if back else None
    probes_home = bench.probes(c.events, m["home"])
    active = st["balancers"][0]["active"]
    return [check("on LTE moved to the expensive path", bool(switches_to(c.events, "b1", m["lte"])),
                  bool(switches_to(c.events, "b1", m["lte"])), "yes"),
            check("back home: on the cheap path again", active, active == "a1", "a1"),
            check("back home: return took, s", took and round(took, 1), took is not None and took <= 10, "<= 10"),
            check("back home: probes before return", len(probes_home), len(probes_home) <= 1, "<= 1")]


@scenario()
def return_penalty(stand, xray, work, name):
    """A cheap path that fails right after the return waits twice as long next time."""
    strategy = {"minHold": "20s", "upgrade": {"interval": "40s", "randomPhase": False, "spacing": "5s",
                                              "successes": 2, "penaltyWindow": "2m"}}
    health = {"schedule": {"quarantine": {"initial": "10s", "jitter": 0.1}}}
    cfg = bench.client_config(CHEAP_EXPENSIVE, CHEAP_EXPENSIVE_COSTS, strategy=strategy, health=health)
    m = Marks()
    state = {}

    def break_after_return():
        # Wait for the first return to a1, then break it at once.
        state["armed"] = True

    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        start = time.time()
        tr.start()
        time.sleep(5)
        bench.chaos(A0, "blackhole")
        time.sleep(15)
        bench.chaos(A0, "pass")
        t_fix = time.time()
        while time.time() - t_fix < 120 and not switches_to(c.events, "a1", t_fix):
            time.sleep(0.5)
        first_return = switches_to(c.events, "a1", t_fix)
        bench.chaos(A0, "blackhole")
        time.sleep(15)
        bench.chaos(A0, "pass")
        t_fix2 = time.time()
        while time.time() - t_fix2 < 240 and not switches_to(c.events, "a1", t_fix2):
            time.sleep(0.5)
        second_return = switches_to(c.events, "a1", t_fix2)
        tr.stop()
    penalties = [e for e in c.events if e["type"] == "upgrade_penalty"]
    delayed = [e for e in c.events if e["type"] == "upgrade_delayed"]
    gap1 = first_return[0]["ts"] / 1000 - t_fix if first_return else None
    gap2 = second_return[0]["ts"] / 1000 - t_fix2 if second_return else None
    return [check("penalty after a failed return", len(penalties), len(penalties) >= 1, ">= 1"),
            check("next return delayed", len(delayed), len(delayed) >= 1, ">= 1"),
            check("first return after fix, s", gap1 and round(gap1, 1), gap1 is not None, "info"),
            check("second return after fix, s", gap2 and round(gap2, 1),
                  gap1 is not None and gap2 is not None and gap2 > gap1, "> first")]


@scenario()
def cold_start(stand, xray, work, name):
    """A restart starts on the outbound the host passes, without probing."""
    cfg = bench.client_config(CHEAP_EXPENSIVE, CHEAP_EXPENSIVE_COSTS, strategy={"startWith": "b1"})
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        start = run_timeline(tr, [], 15)
    first = bench.switches(c.events)
    pr = bench.probes(c.events, start - 5)
    to = first[0]["fields"]["to"] if first else None
    return [check("started on the passed outbound", to, to == "b1", "b1"),
            check("probes on start", len(pr), len(pr) == 0, "== 0")]


@scenario(slow=True)
def healthy_no_probes(stand, xray, work, name):
    mins = minutes("BENCH_HEALTHY_MIN", 30)
    cfg = bench.client_config(CHEAP_EXPENSIVE, CHEAP_EXPENSIVE_COSTS)
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        start = run_timeline(tr, [], mins * 60)
    hits = stand.probe_hits(start)
    pr = bench.probes(c.events, start)
    return [check(f"probes in {mins:g} min on the cheapest working path", len(pr), len(pr) == 0, "== 0"),
            check("probe requests at the target", len(hits), len(hits) == 0, "== 0")]


@scenario(slow=True)
def expensive_probe_budget(stand, xray, work, name):
    """The cheap path stays dead: what checking for a return costs per hour."""
    mins = minutes("BENCH_EXPENSIVE_MIN", 30)
    cfg = bench.client_config(CHEAP_EXPENSIVE, CHEAP_EXPENSIVE_COSTS)
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        start = run_timeline(tr, [(5, lambda: bench.chaos(A0, "blackhole"))], mins * 60)
    results = [e for e in c.events if e["type"] == "probe_result" and e["ts"] / 1000 >= start]
    total = sum(e["fields"].get("bytes", 0) for e in results)
    per_hour = total * 60 / mins
    to_dead = [e for e in results if e["fields"]["outbound"] == "a1"]
    return [check("probe traffic per hour, KB", round(per_hour / 1024, 1), per_hour <= 200 * 1024, "<= 200"),
            check("probes of the dead path per hour", round(len(to_dead) * 60 / mins, 1),
                  len(to_dead) * 60 / mins <= 6, "<= 6")]


@scenario(slow=True)
def soak(stand, xray, work, name):
    """Hours of traffic with a fault every five minutes: memory must not grow."""
    mins = minutes("BENCH_SOAK_MIN", 120)
    members = [("a1", "reality", 0), ("a2", "reality", 1), ("b1", "tcp_tls", 0), ("c1", "xhttp", 0)]
    cfg = bench.client_config(members, {"a": 1, "b": 3, "c": 10},
                              strategy={"minHold": "1m", "upgrade": {"interval": "3m", "spacing": "10s"}})
    faults = [(A0, "blackhole"), (A0, "stall_after:16384"), (A1, "reset"), (B0, "blackhole"),
              (address_port("xhttp", 0), "blackhole")]
    actions = []
    t, i = 300, 0
    while t + 120 < mins * 60:
        port, mode = faults[i % len(faults)]
        actions.append((t, lambda p=port, md=mode: bench.chaos(p, md)))
        actions.append((t + 90, lambda p=port: bench.chaos(p, "pass")))
        t += 300
        i += 1
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        tr.start()
        time.sleep(60)
        tr.paused.set()
        time.sleep(15)
        heap0, gor0 = c.gc_memory()
        tr.paused.clear()
        start = time.time()
        pending = sorted(actions, key=lambda a: a[0])
        while time.time() - start < mins * 60:
            while pending and time.time() - start >= pending[0][0]:
                pending.pop(0)[1]()
            time.sleep(0.5)
        bench.chaos(",".join(str(p) for p in bench.all_address_ports()), "pass")
        tr.paused.set()
        time.sleep(30)
        heap1, gor1 = c.gc_memory()
        tr.stop()
    ok = sum(1 for r in tr.records if r[1]) / max(1, len(tr.records))
    return [check(f"heap after {mins:g} min vs start, MB", round((heap1 - heap0) / 2**20, 2),
                  heap1 - heap0 <= 2**20, "<= 1"),
            check("goroutines after vs start", gor1 - gor0, gor1 - gor0 <= 5, "<= 5"),
            check("events dropped", c.dropped, c.dropped == 0, "== 0"),
            check("requests ok, %", round(100 * ok, 1), True, "info")]


@scenario()
def burst_baseline(stand, xray, work, name):
    """The stock mechanism still behaves as before when the new sections are absent."""
    cfg = bench.client_config(
        [("proxy", "reality", 0), ("fallback", "reality", 1)], {},
        burst={"fallback": "fallback", "subjects": ["proxy"],
               "ping": {"destination": f"http://probe.test:{bench.TARGET_PORT}/204", "interval": "30s",
                        "sampling": 2, "timeout": "5s"}})
    cfg["routing"]["balancers"][0]["selector"] = ["proxy"]
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(10, m.at("fault", lambda: bench.chaos(A0, "blackhole")))], 80)
    rec, fails = bench.recovery(tr.records, m["fault"])
    return [check("stock burst recovery, s (was 36)", rec and round(rec, 1), rec is not None and 20 <= rec <= 70,
                  "20..70")]
