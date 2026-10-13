"""Bench scenarios. Each one returns checks: name, measured value, limit, ok.

Long scenarios read their length from the environment so a quick pass and the
full acceptance run use the same code: BENCH_IDLE_MIN (default 10),
BENCH_FLAP_MIN (10), BENCH_HEALTHY_MIN (30), BENCH_EXPENSIVE_MIN (60),
BENCH_SOAK_MIN (120), BENCH_DEAD_SITE_SEC (300) and BENCH_AWG_WAKES (6);
BENCH_AWG_DELAY delays the datagrams of w1 by that many ms on the wakes of
awg_wake_by_probe that lose nothing. A shorter BENCH_EXPENSIVE_MIN
judges the cost of one probe instead of a whole hour; BENCH_EXPENSIVE_RETURN
(a duration such as 1m) sets returnToCheaper.interval there, so that a short run sees
probes at all.
"""
import json
import os
import threading
import time

import bench
from bench import address_port

A0 = address_port("reality", 0)
A1 = address_port("reality", 1)
B0 = address_port("tcp_tls", 0)
IMPOSTOR = bench.server_port("tcp_tls")

THREE = [("a1", "reality", 0), ("a2", "reality", 1), ("b1", "tcp_tls", 0)]
THREE_COSTS = {"a1": 1, "a2": 1, "b1": 3}


def minutes(name, default):
    return float(os.environ.get(name, default))


def seconds(name, default):
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
    """Budget checks over probe starts: probes at once, spacing, per server name.
    The spacing is the shortest time in which one start more than the probes
    allowed at once happened: the pause between two starts with one probe at a time."""
    starts = [e for e in events if e["type"] == "probe_start" and e["ts"] / 1000 >= since]
    results = [e for e in events if e["type"] == "probe_result" and e["ts"] / 1000 >= since]
    # Overlap: a probe starts while another one has not reported yet.
    timeline = sorted([(e["ts"], 1) for e in starts] + [(e["ts"], -1) for e in results])
    running, peak = 0, 0
    for _, d in timeline:
        running += d
        peak = max(peak, running)
    n = bench.CONCURRENT
    gaps = [(b["ts"] - a["ts"]) / 1000 for a, b in zip(starts, starts[n:])]
    return peak, (min(gaps) if gaps else None), len(starts)


def at_once_check(peak):
    return check("probes at once", peak, peak <= bench.CONCURRENT, f"<= {bench.CONCURRENT}")


def spacing_check(gap, need=False):
    n = bench.CONCURRENT
    name = "min pause between probes, s" if n == 1 else f"min time for {n + 1} probe starts, s"
    return check(name, gap and round(gap, 2), gap >= 1.4 if gap is not None else not need, ">= 1.4")


def leak_check(c):
    """Nothing may go around the tunnel. direct is the default outbound of the
    client and carries no check of the network, so every byte in its counters
    belongs to a user connection that the tunnel failed to hold."""
    got = c.direct_bytes
    value = "unreadable" if got is None else f"{got[0]} up, {got[1]} down"
    return check("bytes through direct (around the tunnel)", value, got == (0, 0), "== 0")


def failover_evidence(events, since=0):
    """Every failover switch needs the story before it: while the left outbound
    was chosen, a suspect path_state with failures and kinds, or a failed probe
    of that outbound. Returns the number of such switches and the bad ones."""
    chosen = {}
    bad = []
    total = 0
    for e in sorted(events, key=lambda e: e["ts"]):
        if e["type"] != "switch":
            continue
        f = e["fields"]
        if e["ts"] / 1000 >= since and f.get("reason") == "failover":
            total += 1
            left, t = f["from"], e["ts"]
            from_t = chosen.get(left, 0)
            proven = any(
                from_t <= x["ts"] <= t and (
                    (x["type"] == "path_state" and x["fields"].get("outbound") == left
                     and x["fields"].get("state") == "suspect" and x["fields"].get("failures", 0) > 0
                     and bool(x["fields"].get("kinds")))
                    or (x["type"] == "probe_result" and x["fields"].get("outbound") == left
                        and x["fields"].get("is_ok") is False))
                for x in events)
            if not proven:
                bad.append(f"{left}->{f['to']}")
        chosen[f["to"]] = e["ts"]
    return total, bad


def evidence_check(events, since=0, need=True):
    """need: the scenario must have had a failover to judge."""
    total, bad = failover_evidence(events, since)
    return check("failover switches with evidence before them", f"{total - len(bad)}/{total}",
                 (total > 0 or not need) and not bad, "all" + (", at least one" if need else ""))


def suspect_kinds(events, tag, since=0):
    """Failure kinds in the suspicions of an outbound, summed over its path_state events."""
    seen = {}
    for e in events:
        f = e["fields"]
        if (e["type"] == "path_state" and f.get("outbound") == tag and f.get("state") == "suspect"
                and e["ts"] / 1000 >= since):
            for k, n in (f.get("kinds") or {}).items():
                seen[k] = seen.get(k, 0) + n
    return seen


def confirm_chain(events, tag, since=0):
    """A confirming probe of the outbound that failed, and the path_state down
    that followed it: (probe_start, probe_result, path_state), each None if missing."""
    mine = [e for e in events if e["ts"] / 1000 >= since and e["fields"].get("outbound") == tag]
    started = next((e for e in mine if e["type"] == "probe_start" and e["fields"].get("reason") == "confirm"), None)
    failed = next((e for e in mine if started and e["type"] == "probe_result" and e["ts"] >= started["ts"]
                   and e["fields"].get("reason") == "confirm" and e["fields"].get("is_ok") is False), None)
    down = next((e for e in mine if failed and e["type"] == "path_state" and e["ts"] >= failed["ts"]
                 and e["fields"].get("state") == "down" and e["fields"].get("reason") == "probe"), None)
    return started, failed, down


def probe_conn_match(cs, events, ports, since=0):
    """The core says which outbounds it probed, the proxy says which connections
    it got. For each outbound the window runs from half a second before its
    first probe to half a second after its last one (probes are at least a
    second apart), on a port that no user traffic uses in that window.
    ports maps an outbound to the chaos port of its address."""
    parts, conns, starts = [], 0, 0
    for tag, port in ports.items():
        ts = sorted(e["ts"] / 1000 for e in events if e["type"] == "probe_start" and e["fields"]["outbound"] == tag
                    and e["ts"] / 1000 >= since)
        if not ts:
            continue
        n = cs.between(port, ts[0] - 0.5, ts[-1] + 0.5) or 0
        parts.append(f"{tag} {n}/{len(ts)}")
        conns += n
        starts += len(ts)
    return conns, starts, ", ".join(parts)


def probe_conn_check(cs, events, ports, since=0):
    conns, starts, detail = probe_conn_match(cs, events, ports, since)
    return check("probe connections at the proxy vs probe_start events", f"{conns}/{starts} ({detail})",
                 conns == starts and starts > 0, "equal, not zero")


def connectivity_conn_check(cs, events, since=0):
    """Checks of the network are connections of the core too: the proxy sees them on its own port."""
    ts = sorted(e["ts"] / 1000 for e in events if e["type"] == "connectivity" and e["ts"] / 1000 >= since)
    if not ts:
        return check("network check connections at the proxy vs events", "0/0", True, "equal")
    n = cs.between(bench.CONNECTIVITY_PORT, ts[0] - 0.5, ts[-1] + 0.5) or 0
    return check("network check connections at the proxy vs events", f"{n}/{len(ts)}", n == len(ts), "equal")


def server_names(status):
    """Outbound -> TLS or REALITY server name, as the core grouped them."""
    return {p["outbound"]: p.get("server_name") or p["outbound"] for p in status["paths"]}


# Events carry the wall clock in whole milliseconds, while the core keeps its limits per
# minute on the monotonic clock. A start the core let through 60 s after another may carry a
# stamp some milliseconds earlier: spans of 59.993 s were seen. A window of the limits is
# counted short by this much; a start one probe spacing early still counts.
LIMIT_CLOCK_SLACK = 0.05


def per_name_peak(events, names, since=0, window=60.0):
    """The most probe starts of one server name inside any 60 s, and that name."""
    by = {}
    for e in events:
        if e["type"] == "probe_start" and e["ts"] / 1000 >= since:
            by.setdefault(names.get(e["fields"]["outbound"], e["fields"]["outbound"]), []).append(e["ts"] / 1000)
    worst, who = 0, "-"
    for name, ts in by.items():
        ts.sort()
        for i, t in enumerate(ts):
            n = sum(1 for u in ts[i:] if u < t + window - LIMIT_CLOCK_SLACK)
            if n > worst:
                worst, who = n, name
    return worst, who


def per_name_check(events, names, since=0):
    worst, who = per_name_peak(events, names, since)
    return check("most probe starts of one server name in 60 s", f"{worst} ({who})", worst <= 3, "<= 3")


def jitter_check(events, since=0, spacing=2.0):
    """Probes are spaced by a jittered pause. Gaps near the spacing are the
    ones the pause set (a probe that ran longer sets the gap itself); they must
    not all be the same, and none may be shorter than the jitter allows."""
    ts = sorted(e["ts"] / 1000 for e in events if e["type"] == "probe_start" and e["ts"] / 1000 >= since)
    gaps = [round(b - a, 2) for a, b in zip(ts, ts[1:]) if b - a < 2 * spacing]
    if len(gaps) < 4:
        return check("pauses between probes vary", f"{len(gaps)} gaps", False, ">= 4 gaps")
    return check("pauses between probes vary, s", f"{min(gaps)}..{max(gaps)} of {len(gaps)}",
                 max(gaps) - min(gaps) >= 0.1 and min(gaps) >= 0.7 * spacing - 0.05, "spread >= 0.1, min >= 1.4")


# ---- 1. failover ------------------------------------------------------------

def handshakes_within(seconds):
    """The most handshakes an AmneziaWG device that gets no answer sends in a window."""
    return int(seconds // bench.AWG_REKEY_TIMEOUT) + 1


def wake_handshakes_within(seconds):
    """The same for a device woken for a connection: a second after the first handshake the
    connection has it repeat that one at once, and the device goes on from there."""
    return handshakes_within(max(seconds - 1, 0)) + 1


def failover(stand, xray, work, name, port, mode, limit, members=THREE, costs=THREE_COSTS, expect_to=None,
             traffic=None, duration=40, tag="a1", count_conns=True, kinds=None, confirm=False, immediate=False,
             redials=False, actions=(), more=None):
    """One fault on the address of the chosen outbound, or on a list of addresses at once.

    kinds: one of these failure kinds must be in the suspicion of the outbound.
    confirm: the evidence is weak, so a confirming probe must fail and rule the
    outbound out. immediate: the evidence is strong and rules it out without a
    probe. count_conns is off for a mode that the proxy cannot count (refuse). redials: the
    transport dials the address again by itself (gRPC does after a failure, a WireGuard outbound
    wakes for a probe). mode may be a callable that sets the fault itself. actions are more
    (second, callable) of the timeline, the fault comes at 10 s; more(c, cs, sw, marks, end)
    returns more checks once the client stopped."""
    cfg = bench.client_config(members, costs)
    ports = port if isinstance(port, list) else [port]
    m = Marks()
    with bench.Client(xray, cfg, work, name, profile=True) as c, bench.ConnSampler() as cs:
        tr = traffic or bench.Traffic(rate=2, big=0.5)
        mech = []
        sample = [(10 + s, lambda s=s: mech.append(c.mechanism_heap(f"{s}"))) for s in (1, 5, 8, 10, 12, 15)]
        fault = m.at("fault", mode if callable(mode) else lambda: bench.chaos(",".join(map(str, ports)), mode))
        run_timeline(tr, [(10, fault)] + sample + list(actions), duration)
        end = time.time()
        peak = max((h for t, h, _ in c.mem if t >= m["fault"]), default=0)
        base = max((h for t, h, _ in c.mem if t < m["fault"]), default=0)
    tr.save(os.path.join(work, f"{name}-traffic.json"))
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
        # After the switch nothing new may go to the dead outbound. VLESS retries the dial
        # of a connection that started before the switch for up to 2 s, so the window
        # starts later.
        t_sw = sw[0]["ts"] / 1000 + 3
        routed = c.routed(t_sw)
        if routed is not None:
            checks.append(check("user connections to the dead outbound after switch", routed.get(tag, 0),
                                routed.get(tag, 0) == 0, "== 0"))
        if count_conns:
            # The proxy also sees what a transport dials by itself, such as a gRPC client
            # reconnecting in the background; with the access log at hand that is no verdict.
            n = cs.between(ports, t_sw, end)
            judged = routed is None or not redials
            checks.append(check("connections at the dead address after switch", n, n == 0 or not judged,
                                "== 0" if judged else "info (the transport redials)"))
            if set(ports) & set(bench.UDP_PORTS):
                # A UDP address counts sockets as connections. Left by the balancer, a dead
                # AmneziaWG outbound sleeps once its connections end; until then its device
                # repeats the handshake, as it does for each probe that wakes it. The bound
                # follows from the window and the protocol, not from the speed of the
                # machine: one handshake series over the window, and one over each probe of
                # the outbound in it.
                up, _ = cs.packets(ports, t_sw, end)
                woken = [e for e in bench.probes(c.events, t_sw) if e["fields"].get("outbound") == tag]
                limit = bench.AWG_HANDSHAKE_DATAGRAMS * (
                    handshakes_within(end - t_sw) + len(woken) * wake_handshakes_within(bench.PROBE_TIMEOUT))
                checks.append(check("datagrams to the dead address after switch", up,
                                    up is not None and up <= limit, f"<= {limit}"))
                slept = [e for e in bench.events_of(c.events, "outbound_sleep", sw[0]["ts"] / 1000)
                         if e["fields"].get("outbound") == tag]
                took = slept and round(slept[0]["ts"] / 1000 - sw[0]["ts"] / 1000, 1)
                checks.append(check("dead outbound asleep after switch: reason, s",
                                    slept and f"{slept[0]['fields'].get('reason')} {took}",
                                    bool(slept) and slept[0]["fields"].get("reason") == "down", "down"))
        recheck = [e for e in c.events if e["type"] == "probe_start" and e["ts"] / 1000 > t_sw]
        checks.append(check("probes after switch", len(recheck), len(recheck) == 0, "== 0"))
    # The event log alone must tell the story: when the path became suspect or
    # down, with what evidence, and when traffic moved.
    ev = [e for e in c.events if e["ts"] / 1000 >= m["fault"]]
    noticed = next((e for e in ev if e["type"] == "path_state" and e["fields"].get("outbound") == tag
                    and e["fields"].get("state") in ("suspect", "down")), None)
    if sw and rec is not None:
        gap = abs(sw[0]["ts"] / 1000 - (m["fault"] + rec))
        checks.append(check("switch in events vs recovery on stand, s", round(gap, 2), gap <= 1, "<= 1"))
    checks.append(check("the first decision names its reason", noticed and noticed["fields"].get("reason"),
                        bool(noticed) and bool(sw) and bool(sw[0]["fields"].get("reason")), "present"))
    checks.append(evidence_check(c.events, m["fault"]))
    seen = suspect_kinds(c.events, tag, m["fault"])
    if kinds:
        checks.append(check("evidence names the failure kind", ",".join(f"{k}:{v}" for k, v in sorted(seen.items())),
                            any(k in seen for k in kinds), " or ".join(kinds)))
    else:
        checks.append(check("evidence kinds", ",".join(f"{k}:{v}" for k, v in sorted(seen.items())) or "-", True,
                            "info"))
    if immediate:
        suspects = [e for e in c.events if e["type"] == "path_state" and e["fields"].get("outbound") == tag
                    and e["fields"].get("state") == "suspect" and e["ts"] / 1000 >= m["fault"]]
        confirming = [e for e in c.events if e["type"] == "probe_start" and e["fields"].get("outbound") == tag
                      and e["fields"].get("reason") == "confirm" and e["ts"] / 1000 >= m["fault"]]
        strong = bool(suspects) and all(e["fields"].get("is_strong") for e in suspects)
        checks.append(check("ruled out on strong evidence, no confirming probe",
                            f"strong={strong}, confirming probes {len(confirming)}", strong and not confirming,
                            "strong, 0 probes"))
    if confirm:
        started, failed, down = confirm_chain(c.events, tag, m["fault"])
        chain = (f"probe {'ok=false ' + str(failed['fields'].get('error')) if failed else 'not failed'}"
                 if started else "no confirming probe")
        checks.append(check("confirming probe failed, then the path is down", chain + (", down" if down else ""),
                            bool(started and failed and down), "start, ok=false, down"))
    mpeak = max(mech, default=0)
    checks.append(check("mechanism heap peak, MB", round(mpeak / 2**20, 2), mpeak <= 2 * 2**20, "<= 2"))
    checks.append(check("whole heap growth incl. user conns, MB", round((peak - base) / 2**20, 2), True, "info"))
    checks.append(leak_check(c))
    if c.splices() is not None:
        checks.append(check("spliced connections", c.splices(), True, "info"))
    if more:
        checks += more(c, cs, sw, m, end)
    return checks


@scenario()
def failover_blackhole(stand, xray, work, name):
    return failover(stand, xray, work, name, A0, "blackhole", 10, expect_to="a2")


@scenario()
def report_interval(stand, xray, work, name):
    """A window with a failover is reported once it is report.interval long,
    not before and not again without a new decision; the stop brings the
    last report of the run."""
    interval = 20
    cfg = bench.client_config(THREE, THREE_COSTS, health={"report": {"interval": f"{interval}s"}})
    with bench.Client(xray, cfg, work, name) as c:
        polls = [(s, c.take_report) for s in range(1, 3 * interval)]
        run_timeline(bench.Traffic(rate=2, big=0.5), [(5, lambda: bench.chaos(A0, "blackhole"))] + polls,
                     3 * interval)
    first = c.reports[0] if c.reports else {}
    final = c.final_report or {}
    window = first.get("window_ms", 0) / 1000
    return [
        check("first report", f"{first.get('report_reason')} after {window} s",
              first.get("report_reason") == "interval" and interval <= window <= interval + 3
              and first.get("switch_reasons", {}).get("failover") == 1, f"interval after {interval}-{interval + 3} s"),
        check("reports while the client ran", len(c.reports) - 1, len(c.reports) == 2, "== 1"),
        check("final report", f"{final.get('report_reason')} #{final.get('report_index')}",
              final.get("report_reason") == "stop" and final.get("report_index") == 2, "stop #2"),
    ]


@scenario()
def failover_reset(stand, xray, work, name):
    return failover(stand, xray, work, name, A0, "reset", 3, expect_to="a2")


@scenario()
def failover_refuse(stand, xray, work, name):
    """The address answers the SYN with RST: nothing listens there."""
    return failover(stand, xray, work, name, A0, "refuse", 3, expect_to="a2", count_conns=False)


@scenario()
def failover_freeze(stand, xray, work, name):
    return failover(stand, xray, work, name, A0, "stall_after:16384", 15, expect_to="a2", confirm=True)


@scenario()
def failover_impostor(stand, xray, work, name):
    """Another server answers on the address. Its certificate is not trusted by the client, so the
    REALITY handshake ends in an x509 error, which the core reports as a failed dial. The kind
    server_unverified needs an impostor whose certificate the system trusts, such as a real decoy
    site; the stand cannot make one on macOS, where Go takes the trust store from the system."""
    return failover(stand, xray, work, name, A0, f"redirect:{IMPOSTOR}", 5, expect_to="a2",
                    kinds=("server_unverified", "dial_failed"), immediate=True)


@scenario()
def failover_two_dead(stand, xray, work, name):
    """The chosen outbound and the next one as cheap die at once; the dearer b1 works."""
    return failover(stand, xray, work, name, [A0, A1], "blackhole", 15, expect_to="b1")


CHAIN = [("a1", "reality", 0), ("b1", "tcp_tls", 0), ("w1", "ws", 0), ("g1", "grpc", 0)]


@scenario()
def failover_chain(stand, xray, work, name):
    """Three outbounds behind different server names die at once and the dearest one works:
    probed one at a time, every dead candidate costs its probe timeout in turn."""
    return failover(stand, xray, work, name, [A0, B0, address_port("ws", 0)], "blackhole", 18, members=CHAIN,
                    costs={"a1": 1, "b1": 2, "w1": 3, "g1": 4}, expect_to="g1")


# What each fault is, with the recovery it must meet on every transport.
FAULTS = {"blackhole": ("blackhole", 10), "reset": ("reset", 3), "freeze": ("stall_after:16384", 15)}


def per_transport(kind, fault):
    mode, limit = FAULTS[fault]
    if kind == "xhttp" and fault == "reset":
        # xhttp opens its stream lazily, so a reset shows only as connections
        # that end without an answer: weak evidence, which waits for a
        # confirming probe and the probe spacing before the switch.
        limit = 5

    def run(stand, xray, work, name):
        members = [("x1", kind, 0), ("x2", kind, 1)]
        # xhttp opens its stream lazily: the dial succeeds and the failure shows at the first byte.
        want = ("no_response",) if kind == "xhttp" and fault != "reset" else ("closed",) if kind == "xhttp" else None
        return failover(stand, xray, work, name, address_port(kind, 0), mode, limit, members=members,
                        costs={"x1": 1, "x2": 1}, expect_to="x2", tag="x1", kinds=want, confirm=fault == "freeze",
                        redials=kind == "grpc")
    return run


for _kind in ("tcp_tls", "ws", "xhttp", "grpc"):
    for _fault in FAULTS:
        ALL.append(Scenario(f"failover_{_fault}_{_kind}", per_transport(_kind, _fault)))


def plain_heap(kind):
    """Heap growth of the same fault and traffic without the mechanism, for comparison."""
    def run(stand, xray, work, name):
        cfg = bench.plain(bench.client_config([("x1", kind, 0)], {"x1": 1}), "x1")
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
    length = seconds("BENCH_DEAD_SITE_SEC", 300)
    plan = [("t1.test", "/bytes/65536", 65536), ("dead.test", "/hang", 1), ("t2.test", "/204", 0),
            ("dead.test", "/hang", 1), ("t3.test", "/bytes/65536", 65536)]
    cfg = bench.client_config(THREE, THREE_COSTS)
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, plan=plan, timeout=20)
        start = run_timeline(tr, [], length)
    sw = bench.switches(c.events, start + 1)
    pr = bench.probes(c.events, start)
    hung = sum(1 for r in tr.records if r[4] == "dead.test" and not r[1])
    return [check(f"switches in {length:g} s", len(sw), len(sw) == 0, "== 0"),
            check("probes", len(pr), len(pr) == 0, "== 0"),
            check("requests to the dead site that failed", hung, hung > 0, "> 0, or the site was not dead"),
            leak_check(c)]


def glitch(stand, xray, work, name, mode, members=THREE, costs=THREE_COSTS, port=A0):
    """The address of the chosen outbound is gone for 3 s, then back."""
    cfg = bench.client_config(members, costs)
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(10, m.at("fault", lambda: bench.chaos(port, mode))),
                          (13, lambda: bench.chaos(port, "pass"))], 40)
    sw = bench.switches(c.events, m["fault"])
    downs = [e for e in c.events if e["type"] == "path_state" and e["fields"].get("state") == "down"
             and e["ts"] / 1000 >= m["fault"]]
    pr = bench.probes(c.events, m["fault"])
    return [check(f"switches after 3 s of {mode}", len(sw), len(sw) == 0, "== 0"),
            check("paths ruled out", len(downs), len(downs) == 0, "== 0"),
            check("probes", len(pr), True, "info"),
            leak_check(c)]


@scenario()
def short_glitch(stand, xray, work, name):
    """Data held back and then delivered, as TCP retransmits do."""
    return glitch(stand, xray, work, name, "pause")


@scenario()
def short_glitch_blackhole(stand, xray, work, name):
    """Packets lost for 3 s: connections that began meanwhile never get an answer."""
    return glitch(stand, xray, work, name, "blackhole")


# The probe of false_down_rescue: 256 KB in four chunks a second apart, so that it runs
# for three seconds while it keeps making progress.
RESCUE_PROBE = 3


def hang(host):
    """A request through the client that its target never answers: no first byte, a
    failure that may be about that one destination. The client ends it when it rules
    the outbound out."""
    def run():
        try:
            s = bench.open_stream(host, 30, tls=False)
            if s is not None:
                s.sendall(f"GET /hang HTTP/1.1\r\nHost: {host}\r\n\r\n".encode())
                s.recv(1)
        except OSError:
            pass
    threading.Thread(target=run, daemon=True).start()


@scenario()
def false_down_rescue(stand, xray, work, name):
    """The hiccup of a phone on Wi-Fi under load. Three requests through a1 get no answer
    from three sites: weak evidence, so after the network check a probe confirms it. While
    that probe runs, a1 refuses new connections for 1.5 s: their dials fail, strong evidence
    that rules a1 out at once. The probe, on its connection from before, still passes after
    the down: it rescues a1, which is back within the probe instead of resting its quarantine,
    and its connections that went silent are kept. The failover the down started still
    moves the balancer to b1, which returns to a1 once minHoldTime ends, long before
    returnToCheaper.interval."""
    hold, spacing, successes = 15, 3, 2
    probe = {"destination": f"http://probe.test:{bench.TARGET_PORT}/slow/262144/{RESCUE_PROBE + 1}",
             "minBytes": 262144, "expectedStatuses": [200], "timeout": "10s", "progressTimeout": "3s"}
    strategy = {"minHoldTime": f"{hold}s", "returnToCheaper": {"interval": "10m", "hasRandomPhase": False,
                                                   "probesToReturn": successes, "probeInterval": f"{spacing}s"}}
    cfg = bench.client_config(CHEAP_EXPENSIVE, CHEAP_EXPENSIVE_COSTS, health={"probes": [probe]}, strategy=strategy)
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        # Empty answers only: a download of successBytes would prove a1 and clear the evidence.
        tr = bench.Traffic(rate=5, big=0)
        tr.start()
        time.sleep(5)
        m["weak"] = time.time()
        for host in bench.TARGETS[3:6]:
            hang(host)
        # The suspicion comes firstByteTimeout later; its confirming probe is the first
        # request of probe.test at the target.
        hits = wait_for(lambda: stand.probe_hits(m["weak"]), 20, step=0.05)
        if hits:
            m["probe"] = hits[0]
            m["refuse"] = time.time()
            bench.chaos(A0, "refuse")
            time.sleep(1.5)
            bench.chaos(A0, "pass")
            wait_for(lambda: [e for e in switches_to(c.events, "a1", m["refuse"])
                              if e["fields"]["reason"] == "return"], 60)
        time.sleep(2)
        st = c.status()
        tr.stop()
    tr.save(os.path.join(work, f"{name}-traffic.json"))
    if not hits:
        return [check("confirming probe of the weak evidence at the target", "none in 20 s", False, "one"),
                leak_check(c)]
    a1 = [e for e in c.events if e["ts"] / 1000 >= m["weak"] and e["fields"].get("outbound") == "a1"]
    story = [("weak suspicion", first_event(a1, "path_state", state="suspect", is_strong=False)),
             ("confirm start", first_event(a1, "probe_start", reason="confirm")),
             ("down by traffic", first_event(a1, "path_state", state="down", reason="traffic")),
             ("confirm ok", first_event(a1, "probe_result", reason="confirm", is_ok=True)),
             ("rescued", first_event(a1, "path_state", state="up", reason="rescued"))]
    missing = [label for label, e in story if e is None]
    seqs = [e["seq"] for _, e in story if e is not None]
    down, rescued = story[2][1], story[4][1]
    checks = [check("story of a1", f"missing {', '.join(missing)}" if missing else " > ".join(l for l, _ in story),
                    not missing and seqs == sorted(seqs), "in this order")]
    if down and rescued:
        out = (rescued["ts"] - down["ts"]) / 1000
        rest = down["fields"].get("quarantine_ms", 0) / 1000
        checks.append(check("a1 out, s (of its quarantine)", f"{round(out, 1)} ({round(rest)})",
                            out <= RESCUE_PROBE + 1, f"<= the probe {RESCUE_PROBE} + 1"))
    silent = [e for e in bench.events_of(c.events, "pending_closed", m["weak"])
              if e["fields"].get("outbound") == "a1" and e["fields"].get("is_silent")]
    checks.append(check("silent connections of a1 ended", sum(e["fields"]["connections"] for e in silent),
                        not silent, "== 0"))
    sw = bench.switches(c.events, m["refuse"])
    path = [f"{e['fields']['from']}>{e['fields']['to']}:{e['fields']['reason']}" for e in sw]
    checks.append(check("switches", path, path == ["a1>b1:failover", "b1>a1:return"],
                        "a1>b1:failover, b1>a1:return"))
    if len(sw) == 2:
        took = (sw[1]["ts"] - sw[0]["ts"]) / 1000
        limit = hold + successes * RESCUE_PROBE + (successes - 1) * spacing * 1.3 + 5
        checks.append(check("return to a1 after the failover, s", round(took, 1), hold <= took <= limit,
                            f"{hold} .. {round(limit)}"))
    report = c.final_report or {}
    checks.append(check("report path_rescued_count", report.get("path_rescued_count"), report.get("path_rescued_count") == 1, "== 1"))
    a1_now = next((p for p in st["paths"] if p["outbound"] == "a1"), {})
    checks.append(check("a1 at the end, active", f"{a1_now.get('state')}, {st['balancers'][0]['active']}",
                        a1_now.get("state") == "up" and st["balancers"][0]["active"] == "a1", "up, a1"))
    ok, n = requests_ok(tr, m["weak"], m["refuse"])
    checks.append(check("requests answered before the hiccup", f"{ok}/{n}", ok == n and n > 0, "all"))
    ok, n = requests_ok(tr, m["refuse"] + 3, time.time())
    checks.append(check("requests answered after it", f"{ok}/{n}", n > 0 and ok == n, "all"))
    checks.append(leak_check(c))
    return checks


@scenario()
def splice_keepalive(stand, xray, work, name):
    """A TLS connection that stays open after its answers. On Linux Vision
    splices the answers past the counters; requests sent after that must not
    look unanswered when the connection then idles."""
    cfg = bench.client_config(THREE, THREE_COSTS, loglevel="debug")
    pad = "x" * 300
    with bench.Client(xray, cfg, work, name) as c:
        s = bench.open_stream("t1.test", 30, tls=True)
        answers = []
        for path in ("/bytes/65536", "/204", "/204"):
            s.sendall(f"GET {path} HTTP/1.1\r\nHost: t1.test\r\nX-Pad: {pad}\r\n\r\n".encode())
            answers.append(bench.read_answer(s, time.time() + 30)[1])
            time.sleep(2)
        # Longer than stallTimeout with the connection open and idle.
        time.sleep(12)
        st = c.status()
        sweeping = c.goroutines("sweepLoop")
        s.close()
    a1 = {p["outbound"]: p for p in st["paths"]}["a1"]
    kinds = a1.get("failures_by_kind") or {}
    checks = [check("answers", answers, answers == [65536, 0, 0], "[65536, 0, 0]"),
              check("failures on the idle connection", kinds or 0, not kinds, "== 0"),
              check("sweep goroutines while it idles", sweeping, sweeping == 0, "== 0"),
              leak_check(c)]
    if c.splices() is not None:
        checks.append(check("spliced connections", c.splices(), c.splices() == 1, "== 1"))
    return checks


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
    sw = bench.switches(c.events, quiet_from)
    return [check(f"probe requests in {mins:g} min idle", len(hits), len(hits) == 0, "== 0"),
            check("probe events while idle", len(pr), len(pr) == 0, "== 0"),
            check("switches while idle", len(sw), len(sw) == 0, "== 0"),
            check("goroutines idle vs busy", f"{gor} vs {g0}", gor <= g0, "idle <= busy"),
            leak_check(c)]


@scenario(slow=True)
def flapping(stand, xray, work, name):
    mins = minutes("BENCH_FLAP_MIN", 10)
    cfg = bench.client_config([("a1", "reality", 0), ("b1", "tcp_tls", 0)], {"a1": 1, "b1": 3},
                              strategy={"minHoldTime": "30s", "returnToCheaper": {"interval": "1m", "probeInterval": "10s"}})
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
            evidence_check(c.events, start),
            check("requests ok, %", round(100 * ok, 1), True, "info"),
            leak_check(c)]


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
            check("recovery after network returns, s", rec and round(rec, 1), rec is not None and rec <= 10, "<= 10"),
            leak_check(c)]


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
                  "<= recheck max 30 + 5"),
            leak_check(c)]


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
            check("connectivity state", st.get("connectivity"), st.get("connectivity") == "ignored", "ignored"),
            evidence_check(c.events, m["down"]),
            leak_check(c)]


@scenario()
def check_unreachable_witness(stand, xray, work, name):
    """As check_unreachable, but sites outside the tunnel work and prove that the network is there."""
    cfg = bench.client_config(THREE, THREE_COSTS,
                              health={"connectivity": {"maxPause": "20s", "witnesses": ["direct"]}})
    cfg["dns"]["hosts"]["w.test"] = bench.HOST
    cfg["routing"]["rules"].insert(0, {"domain": ["full:w.test"], "outboundTag": "direct"})
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
            check("connectivity state", st.get("connectivity"), st.get("connectivity") == "ignored", "ignored"),
            evidence_check(c.events, m["down"])]


@scenario()
def exhausted(stand, xray, work, name):
    cfg = bench.client_config(THREE, THREE_COSTS)
    member_ports = ",".join(str(p) for p in (A0, A1, B0))
    m = Marks()
    mid = {}

    def look():
        m["mid"] = time.time()
        mid["status"] = c.status()

    with bench.Client(xray, cfg, work, name) as c, bench.ConnSampler() as cs:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(10, m.at("down", lambda: bench.chaos(member_ports, "blackhole"))),
                          (70, look),
                          (80, m.at("up", lambda: bench.chaos(str(A1), "pass")))], 280)
        names = server_names(c.status())
    ex = [e for e in c.events if e["type"] == "exhausted" and e["ts"] / 1000 >= m["down"]]
    t_ex = ex[0]["ts"] / 1000 - m["down"] if ex else None
    t_ex_abs = ex[0]["ts"] / 1000 if ex else m["down"]
    # Where traffic went. direct has no part in it (the leak check), and a request that reached
    # the target in this window went around the tunnel too, since every server is dead.
    leaked = []
    with open(stand.target_log) as f:
        for line in f:
            ts, host, _ = line.split(" ", 2)
            if host.startswith("t") and t_ex_abs + 3 <= float(ts) <= m["up"]:
                leaked.append(line)
    # What block does: it refuses at once, where a dead server would keep a request waiting.
    refused = sorted(r[2] for r in tr.records if t_ex_abs + 3 <= r[0] <= m["up"] and not r[1])
    served = sum(1 for r in tr.records if t_ex_abs + 3 <= r[0] <= m["up"] and r[1])
    median = refused[len(refused) // 2] if refused else None
    st = mid["status"]
    balancer = st["balancers"][0]
    rec, _ = bench.recovery(tr.records, m["up"])
    recovered = next((e for e in bench.switches(c.events, m["down"]) if e["fields"].get("reason") == "recovered"), None)
    until = recovered["ts"] / 1000 - 1.0 if recovered else None
    peak, gap, n = probe_checks(c.events, m["down"])
    ports = {"a1": A0, "a2": A1, "b1": B0}
    probe_events = [e for e in c.events if until is None or e["type"] != "probe_start" or e["ts"] / 1000 <= until]
    return [check("exhausted after all paths died, s", t_ex and round(t_ex, 1), t_ex is not None and t_ex <= 20,
                  "<= 20"),
            check("balancer while exhausted: exhausted, active", f"{balancer['is_exhausted']}, {balancer['active']!r}",
                  balancer["is_exhausted"] is True and balancer["active"] == "", "True, ''"),
            check("requests that reached the target while exhausted", len(leaked) + served,
                  not leaked and not served, "== 0"),
            check("failed requests answered at once (block), median s", median and round(median, 2),
                  median is not None and median < 2, "< 2"),
            leak_check(c),
            # A dead outbound rests 2 min +-30% before a recheck: a censor keeps a
            # frozen server frozen for that long and longer after new handshakes.
            check("recovery after one path returned, s", rec and round(rec, 1), rec is not None and rec <= 180,
                  "<= 180"),
            at_once_check(peak),
            spacing_check(gap),
            probe_conn_check(cs, probe_events, ports, t_ex_abs + 3),
            connectivity_conn_check(cs, c.events, m["down"]),
            per_name_check(c.events, names, m["down"]),
            check("probes", n, True, "info")]


@scenario()
def leak_detector(stand, xray, work, name):
    """The leak check itself: traffic that is sent to direct on purpose has to show in its
    counters, or a zero there would prove nothing."""
    cfg = bench.client_config(THREE, THREE_COSTS)
    cfg["routing"]["rules"] = [{"inboundTag": ["socks"], "outboundTag": "direct"}]
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [], 8)
    got = c.direct_bytes
    ok = sum(1 for r in tr.records if r[1])
    return [check("requests that went around the tunnel and were answered", f"{ok}/{len(tr.records)}", ok > 0, "> 0"),
            check("bytes through direct, down", got and got[1], bool(got) and got[1] >= 64 * 1024, ">= 64 KB"),
            check("the leak check says so", leak_check(c)["ok"], not leak_check(c)["ok"], "False")]


# ---- memory and cost ----------------------------------------------------------

@scenario()
def memory_idle_members(stand, xray, work, name):
    one = bench.plain(bench.client_config([("a1", "reality", 0)], {"a1": 1}), "a1")
    many = bench.client_config([(f"m{i:02d}", "reality", i % bench.ADDRESSES) for i in range(20)], {f"m{i:02d}": 1 for i in range(20)})
    res = {}
    for label, cfg in (("one", one), ("many", many)):
        with bench.Client(xray, cfg, work, f"{name}-{label}") as c:
            tr = bench.Traffic(rate=2, big=0.5)
            run_timeline(tr, [], 10)
            time.sleep(3)
            res[label] = c.gc_memory()[0]
    diff = (res["many"] - res["one"]) / 2**20
    return [check("heap: 20 members with health vs 1 plain, MB", round(diff, 2), diff <= 1, "<= 1")]


def throughput_run(stand, xray, work, name, members, tls, splice=False):
    """Downloads through the same outbound with and without the mechanism,
    best of two each. BENCH_THROUGHPUT_MB sets the size (default 1024). With
    splice the client logs at debug level to count spliced connections."""
    size = int(float(os.environ.get("BENCH_THROUGHPUT_MB", "1024")) * (1 << 20))
    loglevel = "debug" if splice else bench.LOGLEVEL
    tag = members[0][0]
    plain = bench.plain(bench.client_config(members, {tag: 1}, loglevel=loglevel), tag)
    observed = bench.client_config(members, {tag: 1}, loglevel=loglevel)
    res = {}
    splices = {}
    for label, cfg in (("plain", plain), ("observed", observed)) * 2:
        with bench.Client(xray, cfg, work, f"{name}-{label}") as c:
            cpu0 = c.cpu_seconds()
            t0 = time.time()
            got = bench.download("t1.test", f"/bytes/{size}", tls=tls)
            dt = time.time() - t0
            cpu = c.cpu_seconds() - cpu0
        assert got == size, f"downloaded {got} of {size}"
        splices[label] = c.splices()
        best = res.get(label)
        if best is None or dt < best[0]:
            res[label] = (dt, cpu)
    slower = (res["observed"][0] / res["plain"][0] - 1) * 100
    cpu = (res["observed"][1] / res["plain"][1] - 1) * 100
    mb = size / (1 << 20)
    checks = [check(f"download {mb:g} MB slower with health, %", round(slower, 1), slower <= 2, "<= 2"),
              check("client CPU higher with health, %", round(cpu, 1), cpu <= 2, "<= 2"),
              check("MB/s plain vs observed", f"{mb / res['plain'][0]:.0f} vs {mb / res['observed'][0]:.0f}", True,
                    "info"),
              check("client CPU s plain vs observed", f"{res['plain'][1]:.2f} vs {res['observed'][1]:.2f}", True,
                    "info")]
    if splice and splices["observed"] is not None:
        # The mechanism must not keep the connection from splicing.
        checks.append(check("spliced plain vs observed", f"{splices['plain']} vs {splices['observed']}",
                            splices["plain"] == splices["observed"] == 1, "1 vs 1"))
    return checks


@scenario()
def throughput(stand, xray, work, name):
    return throughput_run(stand, xray, work, name, [("b1", "tcp_tls", 0)], bench.TLS)


@scenario()
def throughput_spliced(stand, xray, work, name):
    """REALITY with Vision and TLS inside: on Linux the download is spliced,
    which the debug log shows."""
    return throughput_run(stand, xray, work, name, [("a1", "reality", 0)], True, splice=True)


# ---- 2. return to cheaper outbounds and network changes ------------------------

CHEAP_EXPENSIVE = [("a1", "reality", 0), ("b1", "tcp_tls", 0)]
CHEAP_EXPENSIVE_COSTS = {"a1": 1, "b1": 3}


def switches_to(events, to, since=0):
    return [e for e in bench.switches(events, since) if e["fields"]["to"] == to]


def wait_for(fn, timeout, step=0.5):
    """Polls fn until it gives something, for at most timeout seconds."""
    end = time.time() + timeout
    while time.time() < end:
        got = fn()
        if got:
            return got
        time.sleep(step)
    return None


@scenario()
def return_after_network_change(stand, xray, work, name):
    spacing, successes = 20, 3
    limit = successes * spacing + 10
    strategy = {"minHoldTime": "30s", "returnToCheaper": {"probeInterval": f"{spacing}s", "probesToReturn": successes}}
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
    returns = [e for e in c.events if e["type"] == "probe_start" and e["fields"].get("reason") == "return"
                and e["ts"] / 1000 >= m["net"]]
    return [check("return after network change, s", took and round(took, 1), took is not None and took <= limit,
                  f"<= {successes} x {spacing} + 10"),
            check("time on the expensive path before return, s", held and round(held, 1),
                  held is not None and held >= 30, ">= minHoldTime 30"),
            check("probes before the return", len(returns), len(returns) == successes, f"== {successes}"),
            leak_check(c)]


@scenario()
def return_inside_minhold(stand, xray, work, name):
    """The cheap path is fixed and the network changes while the expensive one is still
    inside minHoldTime: the return must wait for minHoldTime to end."""
    hold, spacing, successes = 30, 5, 3
    strategy = {"minHoldTime": f"{hold}s", "returnToCheaper": {"probeInterval": f"{spacing}s", "probesToReturn": successes}}
    cfg = bench.client_config(CHEAP_EXPENSIVE, CHEAP_EXPENSIVE_COSTS, strategy=strategy)
    m = Marks()
    t_b = None
    back = None
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        tr.start()
        time.sleep(5)
        m["down"] = time.time()
        bench.chaos(A0, "blackhole")
        to_b = wait_for(lambda: switches_to(c.events, "b1", m["down"]), 40)
        if to_b:
            t_b = to_b[0]["ts"] / 1000
            time.sleep(max(0, t_b + 6 - time.time()))
            bench.chaos(A0, "pass")
            time.sleep(max(0, t_b + 10 - time.time()))
            m["net"] = time.time()
            c.network("cafe")
            back = wait_for(lambda: switches_to(c.events, "a1", m["net"]), hold + successes * spacing + 40)
        tr.stop()
    if t_b is None or not back:
        return [check("moved to the expensive path and came back", f"b1 {t_b is not None}, a1 {bool(back)}", False,
                      "both")]
    t_back = back[0]["ts"] / 1000
    starts = [e["ts"] / 1000 for e in c.events if e["type"] == "probe_start" and e["fields"].get("reason") == "return"
              and e["ts"] / 1000 >= m["net"]]
    after_hold = t_back - (t_b + hold)
    return [check("network change came inside minHoldTime, s after the switch", round(m["net"] - t_b, 1),
                  m["net"] - t_b < hold, f"< {hold}"),
            check("first probe for the return, s after the switch", starts and round(starts[0] - t_b, 1),
                  bool(starts) and starts[0] - t_b >= hold, f">= minHoldTime {hold}"),
            check("held on the expensive path at the return, s", round(back[0]["fields"]["held_ms"] / 1000, 1),
                  back[0]["fields"]["held_ms"] >= hold * 1000, f">= minHoldTime {hold}"),
            check("return after minHoldTime ended, s", round(after_hold, 1),
                  0 <= after_hold <= successes * spacing + 10, f"0 .. {successes} x {spacing} + 10"),
            check("probes before the return", len(starts), len(starts) == successes, f"== {successes}"),
            leak_check(c)]


@scenario()
def home_lte_home(stand, xray, work, name):
    """At home the cheap path works; on LTE it is blocked; back home it works again."""
    strategy = {"minHoldTime": "30s"}
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
            check("back home: probes before return", len(probes_home), len(probes_home) <= 1, "<= 1"),
            leak_check(c)]


@scenario()
def home_lte_home_expensive(stand, xray, work, name):
    """At home the cheap path is blocked and the expensive one works; on LTE the cheap path is
    open. Back home the first choice is the path that last worked there, found with one probe,
    not the cheapest one."""
    spacing = 5
    strategy = {"minHoldTime": "20s", "returnToCheaper": {"probeInterval": f"{spacing}s"}}
    cfg = bench.client_config(CHEAP_EXPENSIVE, CHEAP_EXPENSIVE_COSTS, strategy=strategy)
    bench.chaos(A0, "blackhole")
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        tr.start()
        time.sleep(2)
        m["first_home"] = time.time()
        c.network("home")
        at_home = wait_for(lambda: switches_to(c.events, "b1", m["first_home"]), 45)
        time.sleep(10)
        m["lte"] = time.time()
        bench.chaos(A0, "pass")
        c.network("lte")
        on_lte = wait_for(lambda: switches_to(c.events, "a1", m["lte"]), 90)
        time.sleep(5)
        m["home"] = time.time()
        bench.chaos(A0, "blackhole")
        c.network("home")
        wait_for(lambda: bench.switches(c.events, m["home"]), 30)
        time.sleep(8)
        st = c.status()
        tr.stop()
    first = next(iter(bench.switches(c.events, m["home"])), None)
    t_first = first["ts"] / 1000 if first else None
    before = [e for e in bench.probes(c.events, m["home"]) if t_first is None or e["ts"] / 1000 <= t_first]
    rec, _ = bench.recovery(tr.records, m["home"])
    chosen = f"{first['fields']['to']} ({first['fields']['reason']})" if first else None
    return [check("at home moved to the expensive path", bool(at_home), bool(at_home), "yes"),
            check("on LTE came back to the cheap path", bool(on_lte), bool(on_lte), "yes"),
            check("back home: first choice", chosen, chosen == "b1 (remembered)", "b1 (remembered)"),
            check("back home: probes before it", [f"{e['fields']['outbound']} {e['fields']['reason']}" for e in before],
                  len(before) == 1 and before[0]["fields"]["outbound"] == "b1"
                  and before[0]["fields"]["reason"] == "remembered", "one: b1 remembered"),
            check("back home: recovery, s", rec and round(rec, 1), rec is not None and rec <= 10, "<= 10"),
            check("back home: stays on the expensive path", st["balancers"][0]["active"],
                  st["balancers"][0]["active"] == "b1", "b1"),
            leak_check(c)]


@scenario()
def return_penalty(stand, xray, work, name):
    """A cheap path that fails right after the return waits twice as long next time."""
    interval = 40
    strategy = {"minHoldTime": "20s", "returnToCheaper": {"interval": f"{interval}s", "hasRandomPhase": False, "probeInterval": "5s",
                                              "probesToReturn": 2, "penaltyWindow": "2m"}}
    health = {"schedule": {"quarantine": {"initialDelay": "10s", "jitterFraction": 0.1}}}
    cfg = bench.client_config(CHEAP_EXPENSIVE, CHEAP_EXPENSIVE_COSTS, strategy=strategy, health=health)
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        tr.start()
        time.sleep(5)
        t_fault = time.time()
        bench.chaos(A0, "blackhole")
        time.sleep(15)
        bench.chaos(A0, "pass")
        t_fix = time.time()
        first_return = wait_for(lambda: switches_to(c.events, "a1", t_fix), 120)
        # The return worked; break the cheap path at once.
        bench.chaos(A0, "blackhole")
        time.sleep(15)
        bench.chaos(A0, "pass")
        t_fix2 = time.time()
        second_return = wait_for(lambda: switches_to(c.events, "a1", t_fix2), 240)
        tr.stop()
    to_b = switches_to(c.events, "b1", t_fault)
    t_b = to_b[0]["ts"] / 1000 if to_b else None
    penalties = [e for e in c.events if e["type"] == "return_penalty" and first_return
                 and e["ts"] >= first_return[0]["ts"]]
    delayed = [e for e in c.events if e["type"] == "return_delayed"]
    starts = [e["ts"] / 1000 for e in c.events if e["type"] == "return_start" and e["fields"]["to"] == "a1"]
    # The usual wait runs from the switch to the expensive path; the doubled one from the failed return.
    t_failed = penalties[0]["ts"] / 1000 if penalties else None
    first_start = next((t for t in starts if t_b and t >= t_b), None)
    second_start = next((t for t in starts if t_failed and t > t_failed), None)
    w1 = first_start - t_b if first_start else None
    w2 = second_start - t_failed if second_start else None
    gap1 = first_return[0]["ts"] / 1000 - t_fix if first_return else None
    gap2 = second_return[0]["ts"] / 1000 - t_fix2 if second_return else None
    return [check("penalty after a failed return", len(penalties), len(penalties) >= 1, ">= 1"),
            check("next return delayed by, s", delayed and round(delayed[0]["fields"]["delay_ms"] / 1000, 1),
                  bool(delayed) and delayed[0]["fields"]["delay_ms"] >= interval * 1000 - 100,
                  f">= the usual wait {interval}"),
            check("first wait, switch to the expensive path to the first try, s", w1 and round(w1, 1), w1 is not None,
                  "info"),
            check("second wait, failed return to the next try, s", w2 and round(w2, 1),
                  w1 is not None and w2 is not None and w2 >= 2 * w1 - 0.2, ">= 2 x first"),
            check("first return after fix, s", gap1 and round(gap1, 1), gap1 is not None, "info"),
            check("second return happened", gap2 and round(gap2, 1), gap2 is not None, "yes"),
            leak_check(c)]


@scenario()
def cold_start(stand, xray, work, name):
    """A restart starts on the outbound the host passes, without probing."""
    cfg = bench.client_config(CHEAP_EXPENSIVE, CHEAP_EXPENSIVE_COSTS, strategy={"startOutbound": "b1"})
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        start = run_timeline(tr, [], 15)
    first = bench.switches(c.events)
    pr = bench.probes(c.events, start - 5)
    to = first[0]["fields"]["to"] if first else None
    return [check("started on the passed outbound", to, to == "b1", "b1"),
            check("probes on start", len(pr), len(pr) == 0, "== 0"),
            leak_check(c)]


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
            check("probe requests at the target", len(hits), len(hits) == 0, "== 0"),
            leak_check(c)]


def dead_probe_bound(mins):
    """The most probes a dead path may get in a run of this many minutes when its
    rests are 2, 4, 8, 16 and then 30 minutes, each cut by the 30% jitter."""
    n, t, rest = 0, 0.0, 2.0
    while True:
        t += rest * 0.7
        if t > mins:
            return n
        n += 1
        rest = min(rest * 2, 30)


def expensive_budget(stand, xray, work, name, mode):
    """The cheap path is dead (blackhole) or passes the handshake and freezes, the traffic
    sits on the expensive one: what checking for a return costs per hour. The cost is read
    from the events (bytes above TLS) and from the proxy (everything on the wire, handshakes
    included). BENCH_EXPENSIVE_MIN is the length, an hour by default; a shorter run judges
    the cost of one probe instead. BENCH_EXPENSIVE_RETURN sets returnToCheaper.interval so that a short
    run sees probes at all; it takes the count of probes out of the judgement."""
    mins = minutes("BENCH_EXPENSIVE_MIN", 60)
    knob = os.environ.get("BENCH_EXPENSIVE_RETURN")
    cfg = bench.client_config(CHEAP_EXPENSIVE, CHEAP_EXPENSIVE_COSTS,
                              strategy={"returnToCheaper": {"interval": knob}} if knob else None)
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c, bench.ConnSampler() as cs:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(5, m.at("fault", lambda: bench.chaos(A0, mode)))], mins * 60)
        end = time.time()
    to_b = switches_to(c.events, "b1", m["fault"])
    t_b = to_b[0]["ts"] / 1000 if to_b else m["fault"]
    results = [e for e in c.events if e["type"] == "probe_result" and e["ts"] / 1000 >= m["fault"]]
    to_dead = [e for e in results if e["fields"]["outbound"] == "a1"]
    # Past t_b nothing but probes uses the address of the dead path.
    up, down = cs.traffic(A0, t_b + 5, end)
    wire = up + down
    events_bytes = sum(e["fields"].get("bytes", 0) for e in results)
    full = mins >= 60
    hours = mins / 60
    kb = 1024
    n = len(to_dead)
    checks = [check("probe results of the dead path", f"{n}, all failed" if all(not e["fields"]["is_ok"] for e in to_dead)
                    else f"{n}, some ok", all(not e["fields"]["is_ok"] for e in to_dead), "all failed")]
    if full:
        checks += [check("probe traffic per hour from events, KB", round(events_bytes / hours / kb, 1),
                         events_bytes / hours <= 200 * kb, "<= 200"),
                   check("probe traffic per hour at the proxy, KB", round(wire / hours / kb, 1),
                         wire / hours <= 200 * kb, "<= 200")]
    else:
        # One probe at most six times an hour has to fit into 200 KB.
        per_events = sum(e["fields"].get("bytes", 0) for e in to_dead) / n if n else 0
        per_wire = wire / n if n else 0
        checks += [check("traffic of one probe from events, KB (6 an hour <= 200)", round(per_events / kb, 1),
                         per_events * 6 <= 200 * kb, "<= 33"),
                   check("traffic of one probe at the proxy, KB (6 an hour <= 200)", round(per_wire / kb, 1),
                         per_wire * 6 <= 200 * kb, "<= 33")]
    limit = 6 if full else dead_probe_bound(mins)
    if knob:
        checks.append(check(f"probes of the dead path in {mins:g} min", n, True, "info (interval set)"))
    else:
        checks.append(check(f"probes of the dead path in {mins:g} min", n, n <= limit, f"<= {limit}"))
    # The probe that confirms the fault runs while users are still on a1; the later ones are alone there.
    conns, starts, detail = probe_conn_match(cs, c.events, {"a1": A0}, t_b + 1)
    ok = conns == starts and (starts > 0 or not full)
    checks.append(check("probe connections at the proxy vs probe_start events", f"{conns}/{starts} ({detail})", ok,
                        "equal" + (", not zero" if full else "")))
    checks.append(leak_check(c))
    return checks


@scenario(slow=True)
def expensive_probe_budget(stand, xray, work, name):
    """The cheap path stays in a black hole."""
    return expensive_budget(stand, xray, work, name, "blackhole")


@scenario(slow=True)
def expensive_probe_budget_freeze(stand, xray, work, name):
    """The worst case: the cheap path passes the handshake and freezes after 16 KB, so every
    probe pays for the handshake and the first kilobytes before it gives up."""
    return expensive_budget(stand, xray, work, name, "stall_after:16384")


# ---- 3. probe budget ----------------------------------------------------------

@scenario()
def probe_limits(stand, xray, work, name):
    """Six paths of one server name and two of others are all down: probes keep a jittered
    distance, one at a time, and at most three of a server name start in a minute."""
    members = [(f"r{i}", "reality", i % 4) for i in range(1, 7)] + [("t1", "tcp_tls", 0), ("w1", "ws", 0)]
    cfg = bench.client_config(members, {**{f"r{i}": 1 for i in range(1, 7)}, "t1": 2, "w1": 3})
    ports = ",".join(str(address_port(k, i)) for _, k, i in members)
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(10, m.at("down", lambda: bench.chaos(ports, "refuse")))], 130)
        # The core names the server of an outbound once a probe of it waits for the budget
        # or a failover weighs it. r1 failed by traffic, so it gets its name only with its
        # recheck when its quarantine of 2 min +-30% ends, which may be while the client
        # stops. So the names come from a status after the run, and only the probes that
        # started before it count: a later one would show under its tag.
        m["names"] = time.time()
        names = server_names(c.status())
    named = [e for e in c.events if e["ts"] / 1000 <= m["names"]]
    ex = [e for e in c.events if e["type"] == "exhausted" and e["ts"] / 1000 >= m["down"]]
    peak, gap, n = probe_checks(c.events, m["down"])
    probed = {e["fields"]["outbound"] for e in named if e["type"] == "probe_start"}
    seen_names = sorted({names[o] for o in probed})
    return [check("probes", n, n >= 6, ">= 6"),
            at_once_check(peak),
            spacing_check(gap, need=True),
            per_name_check(named, names, m["down"]),
            check("server names of the probed paths", seen_names,
                  seen_names == ["example.com", "std.example.com", "ws.example.com"], "example.com, std., ws."),
            check("exhausted at the end", len(ex), len(ex) == 1, "== 1"),
            evidence_check(c.events, m["down"], need=False),
            leak_check(c)]


@scenario()
def probe_spacing(stand, xray, work, name):
    """A return needs eight good probes in a row. A good probe takes milliseconds, so the
    distance between them is the pause itself: spacing with its jitter, one probe at a time.
    (A failed probe holds the slot for about two seconds while the dial is retried.)"""
    successes = 8
    strategy = {"minHoldTime": "10s", "returnToCheaper": {"probesToReturn": successes, "probeInterval": "2s"}}
    # Eight probes of one server name would meet the budget of three a minute; that is not what
    # this scenario looks at.
    health = {"schedule": {"probesPerServerNamePerMinute": 30, "probesPerMinute": 60}}
    cfg = bench.client_config(CHEAP_EXPENSIVE, CHEAP_EXPENSIVE_COSTS, strategy=strategy, health=health)
    m = Marks()
    back = None
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        tr.start()
        time.sleep(5)
        m["down"] = time.time()
        bench.chaos(A0, "blackhole")
        to_b = wait_for(lambda: switches_to(c.events, "b1", m["down"]), 40)
        if to_b:
            bench.chaos(A0, "pass")
            time.sleep(2)
            m["net"] = time.time()
            c.network("cafe")
            back = wait_for(lambda: switches_to(c.events, "a1", m["net"]), 80)
        tr.stop()
    if not back:
        return [check("moved to the expensive path and came back", f"b1 {bool(to_b)}, a1 {bool(back)}", False, "both")]
    returns = [e for e in c.events if e["type"] == "probe_start" and e["fields"].get("reason") == "return"
                and e["ts"] / 1000 >= m["net"]]
    peak, gap, n = probe_checks(c.events, m["net"])
    return [check("probes for the return", len(returns), len(returns) == successes, f"== {successes}"),
            at_once_check(peak),
            spacing_check(gap, need=True),
            jitter_check(c.events, m["net"]),
            leak_check(c)]


# ---- 7. a balancer pinned by the host ---------------------------------------------

@scenario()
def pinned_start(stand, xray, work, name):
    """The host starts the balancer pinned to a1, and a1 dies. Traffic stays on it and the
    failure is visible to the host; nothing switches. After the host releases the balancer it
    works by all the usual rules: it leaves for b1, and returns to a1 once that is fixed."""
    hold, spacing, successes = 20, 5, 3
    strategy = {"startPinnedOutbound": "a1", "minHoldTime": f"{hold}s", "returnToCheaper": {"probeInterval": f"{spacing}s", "probesToReturn": successes}}
    # The paths have different server names, so the probe budget per name is no part of this: with
    # the same name for the path left and the path returned to, the failover probe and the check
    # after the network change use up the three probes a minute that the return needs.
    members = [("a1", "reality", 0), ("b1", "tcp_tls", 0), ("c1", "ws", 0)]
    cfg = bench.client_config(members, {"a1": 1, "b1": 2, "c1": 3}, strategy=strategy)
    m = Marks()
    seen = {}
    with bench.Client(xray, cfg, work, name) as c, bench.ConnSampler() as cs:
        tr = bench.Traffic(rate=2, big=0.5)
        tr.start()
        time.sleep(10)
        m["fault"] = time.time()
        bench.chaos(A0, "blackhole")
        time.sleep(40)
        seen["pinned"] = c.status()["balancers"][0]
        m["release"] = time.time()
        seen["released"] = c.api("release", "auto")
        wait_for(lambda: bench.switches(c.events, m["release"]), 30)
        time.sleep(8)
        m["fix"] = time.time()
        bench.chaos(A0, "pass")
        time.sleep(3)
        m["net"] = time.time()
        c.network("cafe")
        back = wait_for(lambda: switches_to(c.events, "a1", m["net"]), hold + successes * spacing + 40)
        time.sleep(3)
        tr.stop()
    fault, release = m["fault"], m["release"]
    window = (fault + 15, release)
    inside = [e for e in c.events if fault <= e["ts"] / 1000 < release]
    pinned_down = [e for e in inside if e["type"] == "pinned_down"]
    a1_down = [e for e in inside if e["type"] == "path_state" and e["fields"].get("outbound") == "a1"
               and e["fields"].get("state") == "down"]
    switched = [e for e in inside if e["type"] == "switch"]
    on_a1 = cs.between(A0, *window)
    others = cs.between([B0, address_port("ws", 0)], *window)
    others_probes = [e for e in c.events if e["type"] == "probe_start" and e["fields"]["outbound"] in ("b1", "c1")
                     and window[0] <= e["ts"] / 1000 <= window[1]]
    served = sum(1 for r in tr.records if window[0] <= r[0] <= window[1] and r[1])
    sent = sum(1 for r in tr.records if window[0] <= r[0] <= window[1])
    after = bench.switches(c.events, release)
    rec, _ = bench.recovery(tr.records, release)
    pinned = seen["pinned"]
    checks = [
        check("balancer pinned and on a1 while a1 is dead", f"pinned {pinned.get('pinned')!r}, active {pinned['active']!r}",
              pinned.get("pinned") == "a1" and pinned["active"] == "a1", "a1, a1"),
        check("a1 reported down to the host (path_state down)", len(a1_down), len(a1_down) >= 1, ">= 1"),
        check("pinned_down events before the release", len(pinned_down), len(pinned_down) >= 1, ">= 1"),
        check("switches before the release", len(switched), len(switched) == 0, "== 0"),
        check("connections to the dead pinned address, 25 s", on_a1, (on_a1 or 0) >= 20, ">= 20"),
        check("connections to the other addresses vs their probes", f"{others}/{len(others_probes)}",
              others == len(others_probes), "equal (0 expected)"),
        check("requests answered while pinned to the dead path", f"{served}/{sent}", served == 0 and sent > 0, "0"),
        check("release accepted", seen["released"], seen["released"] == {"ok": True}, "ok"),
        check("after release: first switch", after and f"{after[0]['fields']['to']} ({after[0]['fields']['reason']})",
              bool(after) and after[0]["fields"]["to"] == "b1" and after[0]["fields"]["reason"] == "failover",
              "b1 (failover)"),
        check("after release: recovery, s", rec and round(rec, 1), rec is not None and rec <= 10, "<= 10"),
    ]
    if back:
        took = back[0]["ts"] / 1000 - m["net"]
        checks += [check("return to a1 after the fix and a network change, s", round(took, 1),
                         took <= successes * spacing + 10, f"<= {successes} x {spacing} + 10"),
                   check("held on b1 before the return, s", round(back[0]["fields"]["held_ms"] / 1000, 1),
                         back[0]["fields"]["held_ms"] >= hold * 1000, f">= minHoldTime {hold}")]
    else:
        checks.append(check("return to a1 after the fix and a network change", None, False, "yes"))
    checks.append(leak_check(c))
    return checks


# The hybrid with Conquest: variants of one connection, the one Conquest verifies pinned from
# the first connection, and the timer of production, the verification timeout plus 10 s.
HYBRID_PIN_FOR = 30
# The probe timeout of the bench config: a failover after the release takes one probe.
PROBE_TIMEOUT = 6


def hybrid_config():
    return bench.client_config(THREE, THREE_COSTS,
                               strategy={"startPinnedOutbound": "a1", "startPinTimeout": f"{HYBRID_PIN_FOR}s"})


def first_event(events, typ, since=0, **fields):
    return next((e for e in events if e["type"] == typ and e["ts"] / 1000 >= since
                 and all(e["fields"].get(k) == v for k, v in fields.items())), None)


def held_on_dead_pin(c, cs, start, release):
    """a1 is dead and pinned from start to release: nothing switches, every user connection
    goes to a1, the host sees pinned_down, and the other addresses get nothing but probes."""
    inside = [e for e in c.events if start <= e["ts"] / 1000 < release]
    switched = [e for e in inside if e["type"] == "switch" and e["fields"].get("reason") != "start_pinned"]
    pinned_down = [e for e in inside if e["type"] == "pinned_down"]
    others = cs.between([A1, B0], start, release)
    others_probes = [e for e in inside if e["type"] == "probe_start" and e["fields"]["outbound"] in ("a2", "b1")]
    checks = [
        check("switches under the pin", [f"{e['fields']['to']} ({e['fields']['reason']})" for e in switched],
              not switched, "none"),
        check("pinned_down under the pin", len(pinned_down), len(pinned_down) >= 1, ">= 1"),
        check("connections to a2 and b1 under the pin vs their probes", f"{others}/{len(others_probes)}",
              others == len(others_probes), "equal (0 expected)"),
    ]
    routed = c.routed(start, release)
    if routed is not None:
        checks.append(check("user connections under the pin by outbound", routed,
                            set(routed) == {"a1"} and routed["a1"] >= 10, "a1 only, >= 10"))
    return checks


def failover_after_release(c, tr, release):
    """After the release of a dead pinned a1 the balancer moves to a2 after one round of failover
    probes, a2 first and b1 beside it when two probes run at once, and nothing is rechecked: the
    network did not change under the pin."""
    after = [e for e in c.events if e["ts"] / 1000 >= release]
    sw = next((e for e in after if e["type"] == "switch"), None)
    took = sw and round(sw["ts"] / 1000 - release, 2)
    before = [f"{e['fields']['outbound']} {e['fields']['reason']}" for e in after
              if e["type"] == "probe_start" and (sw is None or e["ts"] <= sw["ts"])]
    rechecks = [e for e in after if e["type"] == "probe_start" and e["fields"].get("reason") == "network_change"]
    rec, _ = bench.recovery(tr.records, release)
    return [
        check("first switch after the release, s", sw and f"{sw['fields']['to']} ({sw['fields']['reason']}) {took}",
              bool(sw) and sw["fields"]["to"] == "a2" and sw["fields"]["reason"] == "failover"
              and took <= PROBE_TIMEOUT, f"a2 (failover) <= {PROBE_TIMEOUT}"),
        check("probes before that switch", before,
              before[:1] == ["a2 failover"] and len(before) <= bench.CONCURRENT
              and all(p.endswith(" failover") for p in before),
              f"a2 failover first, <= {bench.CONCURRENT} failover"),
        check("rechecks without a network change under the pin", len(rechecks), not rechecks, "== 0"),
        check("recovery after the release, s", rec and round(rec, 1), rec is not None and rec <= 10, "<= 10"),
    ]


@scenario()
def hybrid_pin_release(stand, xray, work, name):
    """Hybrid with Conquest, the host releases the pin: a1 is dead while Conquest verifies it.
    Traffic stays on a1 and nothing switches; a release meant for another run of the core is
    ignored and the pin stands; the release of this run moves the balancer to a2 after one probe."""
    m = Marks()
    seen = {}
    with bench.Client(xray, hybrid_config(), work, name) as c, bench.ConnSampler() as cs:
        tr = bench.Traffic(rate=2, big=0.5)
        m["start"] = time.time()
        tr.start()
        time.sleep(2)
        bench.chaos(A0, "blackhole")
        wait_for(lambda: bench.events_of(c.events, "pinned_down", m["start"]), 15)
        run = c.status()["run"]
        other_run = run[:-1] + ("1" if run[-1] == "0" else "0")
        m["stale"] = time.time()
        seen["stale"] = c.api("release", "auto", other_run)
        time.sleep(1.5)
        seen["after_stale"] = c.status()["balancers"][0]
        m["release"] = time.time()
        seen["release"] = c.api("release", "auto", run)
        wait_for(lambda: bench.switches(c.events, m["release"]), 10)
        time.sleep(8)
        tr.stop()
    stale = first_event(c.events, "stale_request", m["stale"], request="release", run=other_run)
    early = [e for e in c.events if e["type"] == "release" and e["ts"] / 1000 < m["release"]]
    released = first_event(c.events, "release", m["release"])
    pinned = seen["after_stale"]
    t_pin = first_event(c.events, "switch", reason="start_pinned")
    return held_on_dead_pin(c, cs, m["start"], m["release"]) + [
        check("release for another run", f"{seen['stale']}, stale_request {bool(stale)}",
              seen["stale"] == {"ok": False} and bool(stale), "ok false, stale_request"),
        check("after it: pinned and on a1", f"pinned {pinned.get('pinned')!r}, active {pinned['active']!r}",
              pinned.get("pinned") == "a1" and pinned["active"] == "a1" and not early, "a1, a1, no release"),
        check("release of this run, s after the pin", released and t_pin
              and f"{released['fields'].get('by')} {round(released['ts'] / 1000 - t_pin['ts'] / 1000, 1)}",
              seen["release"] == {"ok": True} and bool(released) and released["fields"].get("by") == "host"
              and bool(t_pin) and released["ts"] - t_pin["ts"] < HYBRID_PIN_FOR * 1000, f"host, < {HYBRID_PIN_FOR}"),
    ] + failover_after_release(c, tr, m["release"]) + [leak_check(c)]


@scenario()
def hybrid_pin_timer(stand, xray, work, name):
    """Hybrid with Conquest, nobody releases the pin: a1 is dead and traffic stays on it until
    startPinTimeout after the first connection; then the balancer moves to a2 after one probe."""
    m = Marks()
    with bench.Client(xray, hybrid_config(), work, name) as c, bench.ConnSampler() as cs:
        tr = bench.Traffic(rate=2, big=0.5)
        m["start"] = time.time()
        tr.start()
        time.sleep(2)
        bench.chaos(A0, "blackhole")
        released = wait_for(lambda: bench.events_of(c.events, "release", m["start"]), HYBRID_PIN_FOR + 15)
        if released:
            m["release"] = released[0]["ts"] / 1000
            wait_for(lambda: bench.switches(c.events, m["release"]), 10)
        time.sleep(8)
        tr.stop()
    if "release" not in m:
        return [check("release by the timer", None, False, f"{HYBRID_PIN_FOR} s after the pin")]
    t_pin = first_event(c.events, "switch", reason="start_pinned")
    released = first_event(c.events, "release", m["start"])
    late = t_pin and m["release"] - t_pin["ts"] / 1000 - HYBRID_PIN_FOR
    return held_on_dead_pin(c, cs, m["start"], m["release"]) + [
        check("release by the timer, s after the pin", t_pin and f"{released['fields'].get('by')} "
              f"{round(m['release'] - t_pin['ts'] / 1000, 2)}",
              bool(t_pin) and released["fields"].get("by") == "timer" and -0.1 <= late <= 1.5,
              f"timer, {HYBRID_PIN_FOR} .. {HYBRID_PIN_FOR} + 1.5"),
    ] + failover_after_release(c, tr, m["release"]) + [leak_check(c)]


@scenario()
def hybrid_pin_stop(stand, xray, work, name):
    """The core stops under the pin after pinned_down, as when Conquest gives up on a dead a1.
    Under the start pin that failure is no decision, so the stop leaves no final report: the host
    reports its failed check itself. The same stop under a pin of the host sends one."""
    clients = {}
    pinned = None
    for label, cfg in (("start", hybrid_config()), ("host", bench.client_config(THREE, THREE_COSTS))):
        with bench.Client(xray, cfg, work, f"{name}-{label}") as c:
            tr = bench.Traffic(rate=2, big=0.5)
            start = time.time()
            tr.start()
            time.sleep(1)
            if label == "host":
                pinned = c.api("pin", "auto", "a1", c.status()["run"])
            bench.chaos(A0, "blackhole")
            wait_for(lambda: bench.events_of(c.events, "pinned_down", start), 15)
            tr.stop()
        bench.chaos(A0, "pass")
        clients[label] = c
    checks = [check("pin of the host", pinned, pinned == {"ok": True}, "ok")]
    for label, c in clients.items():
        downs = bench.events_of(c.events, "pinned_down")
        marks = sorted({e["fields"].get("is_start_pinned") is True for e in downs})
        sw = [f"{e['fields']['to']} ({e['fields']['reason']})" for e in bench.switches(c.events)]
        released = bench.events_of(c.events, "release")
        final = c.final_report
        checks += [
            check(f"pinned_down, start_pinned ({label})", f"{len(downs)} {marks}",
                  bool(downs) and marks == [label == "start"], f">= 1, {label == 'start'}"),
            check(f"switches and releases before the stop ({label})", sw + [len(released)],
                  len(sw) == 1 and sw[0].startswith("a1 (start") and not released, "the start only"),
        ]
        if label == "start":
            checks.append(check("final report (start)", final and f"{final['report_reason']} "
                                f"pinned_down_count {final['pinned_down_count']}", final is None, "none"))
        else:
            checks.append(check("final report (host)", final and f"{final['report_reason']} "
                                f"pinned_down_count {final['pinned_down_count']}",
                                bool(final) and final["report_reason"] == "stop" and final["pinned_down_count"] >= 1,
                                "stop, pinned_down_count >= 1"))
        checks.append(leak_check(c))
    return checks


@scenario()
def hybrid_pin_network(stand, xray, work, name):
    """The network changes under the pin while a1 works, and nothing is probed until the pin is
    released. Traffic pauses over the change under the start pin, so the release by the timer
    probes a1 once in the new network, as after a change without the pin. Under the pin of the
    host traffic shows a1 working in the new network, so the release by the host probes nothing.
    The balancer stays on a1."""
    m = Marks()
    seen = {}
    with bench.Client(xray, hybrid_config(), work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        m["start"] = time.time()
        tr.start()
        time.sleep(5)
        tr.paused.set()
        time.sleep(1)
        m["net1"] = time.time()
        c.network("cafe")
        released = wait_for(lambda: bench.events_of(c.events, "release", m["start"]), HYBRID_PIN_FOR + 15)
        if released:
            m["timer"] = released[0]["ts"] / 1000
        time.sleep(3)
        tr.paused.clear()
        time.sleep(2)
        run = c.status()["run"]
        m["pin"] = time.time()
        seen["pin"] = c.api("pin", "auto", "a1", run)
        time.sleep(2)
        m["net2"] = time.time()
        c.network("home")
        time.sleep(4)
        m["release"] = time.time()
        seen["release"] = c.api("release", "auto", run)
        time.sleep(6)
        tr.stop()
    if "timer" not in m:
        return [check("release by the timer", None, False, f"{HYBRID_PIN_FOR} s after the pin")]
    checks = []
    for label, net, release in (("timer", m["net1"], m["timer"]), ("host", m["net2"], m["release"])):
        held = [e for e in bench.probes(c.events, net) if e["ts"] / 1000 < release]
        checks.append(check(f"probes under the pin after the change ({label})", len(held), not held, "== 0"))
    after = bench.probes(c.events, m["timer"], m["pin"])
    first = after[0] if after else None
    took = first and round(first["ts"] / 1000 - m["timer"], 2)
    result = first and first_event(c.events, "probe_result", first["ts"] / 1000, outbound="a1",
                                   reason="network_change")
    seen_up = first_event(c.events, "path_state", m["net2"], outbound="a1", state="up", reason="traffic")
    up_in = seen_up and round(seen_up["ts"] / 1000 - m["net2"], 1)
    after_host = [f"{e['fields']['outbound']} {e['fields']['reason']}" for e in bench.probes(c.events, m["release"])]
    sw = [f"{e['fields']['to']} ({e['fields']['reason']})" for e in bench.switches(c.events)]
    return checks + [
        check("probes after the release by timer, s",
              [f"{e['fields']['outbound']} {e['fields']['reason']}" for e in after] + [took],
              len(after) == 1 and first["fields"]["outbound"] == "a1"
              and first["fields"]["reason"] == "network_change" and took <= 3,
              "one: a1 network_change <= 3 (a spacing)"),
        check("its result (timer)", result and result["fields"].get("is_ok"),
              bool(result) and result["fields"].get("is_ok") is True, "ok"),
        check("a1 up by traffic under the pin of the host, s after the change", up_in,
              bool(seen_up) and seen_up["ts"] / 1000 < m["release"], "before the release"),
        check("probes after the release by host", after_host, not after_host, "none"),
        check("pin and release of the host", f"{seen['pin']}, {seen['release']}",
              seen["pin"] == seen["release"] == {"ok": True}, "ok, ok"),
        check("switches", sw, sw == ["a1 (start_pinned)"], "a1 (start_pinned) only"),
        leak_check(c),
    ]


# ---- 8. probes the host asks for ------------------------------------------------

HOST_PROBES_PER_MINUTE = 4


def host_starts_peak(events, since=0, window=60.0):
    """The most host probe starts inside any 60 s."""
    ts = sorted(e["ts"] / 1000 for e in events if e["type"] == "probe_start"
                and e["fields"].get("reason") == "host" and e["ts"] / 1000 >= since)
    return max((sum(1 for u in ts[i:] if u < t + window - LIMIT_CLOCK_SLACK) for i, t in enumerate(ts)),
               default=0), len(ts)


@scenario()
def host_probe_failover(stand, xray, work, name):
    """a1 dies while nothing uses it, so no user connection can notice. The host asks for
    probes of the balancer: the request answers with its id at once, the command waits for
    the result, and the failed probe of a1 moves the balancer to a2 without user traffic."""
    cfg = bench.client_config(THREE, THREE_COSTS)
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        tr.start()
        time.sleep(8)
        tr.stop()
        # Past hostProbes.reuse (15 s): the last proof of a1 no longer answers for it.
        time.sleep(17)
        bench.chaos(A0, "blackhole")
        m["ask"] = time.time()
        resp = c.api("-t", "25", "probe", "auto", timeout=30)
        m["answer"] = time.time()
        status = c.status()
    request = resp.get("request") or {}
    got = {o["outbound"]: f"{o['result']}/{o.get('source', '')}" for o in request.get("outbounds", [])}
    sw = bench.switches(c.events, m["ask"])
    took = sw[0]["ts"] / 1000 - m["ask"] if sw else None
    after = [e for e in c.events if e["ts"] / 1000 >= m["ask"]]
    a1_down = [e for e in after if e["type"] == "path_state" and e["fields"].get("outbound") == "a1"
               and e["fields"].get("state") == "down"]
    suspects = [e for e in after if e["type"] == "path_state" and e["fields"].get("state") == "suspect"]
    host_failed = [e for e in after if e["type"] == "probe_result" and e["fields"].get("reason") == "host"
                   and e["fields"].get("outbound") == "a1" and e["fields"].get("is_ok") is False
                   and e["fields"].get("request") == resp.get("id")]
    ended = [e for e in after if e["type"] == "probe_request" and e["fields"].get("request") == resp.get("id")]
    kept = [r for r in status.get("probe_requests", []) if r["id"] == resp.get("id")]
    peak, gap, _ = probe_checks(c.events, m["ask"])
    return [
        check("ticket of the run, as in status", f"id {resp.get('id')}, run {resp.get('run') == status.get('run')}",
              resp.get("id") == 1 and resp.get("run") == status.get("run") and bool(kept), "id 1, same run, kept"),
        check("request answered within the wait", f"{request.get('end')} after {round(m['answer'] - m['ask'], 1)} s",
              request.get("is_done") is True and request.get("end") == "done", "done"),
        check("answers", got, got.get("a1") == "failed/probe" and got.get("a2", "").startswith("ok/")
              and got.get("b1", "").startswith("ok/"), "a1 failed/probe, a2 and b1 ok"),
        check("a1 ruled out by the host probe", [e["fields"].get("reason") for e in a1_down],
              len(a1_down) == 1 and a1_down[0]["fields"].get("reason") == "host_probe" and len(host_failed) == 1,
              "host_probe"),
        check("suspicions from traffic", len(suspects), not suspects, "== 0 (no traffic)"),
        check("switch after the request, s", sw and f"{sw[0]['fields']['to']} ({sw[0]['fields']['reason']}) "
              f"{round(took, 1)}", len(sw) == 1 and sw[0]["fields"]["to"] == "a2"
              and sw[0]["fields"]["reason"] == "failover" and took <= 10, "a2 (failover) <= 10"),
        check("probe_request event", ended and ended[0]["fields"].get("end"),
              len(ended) == 1 and ended[0]["fields"].get("failed") == 1, "one, done, failed 1"),
        at_once_check(peak),
        per_name_check(c.events, server_names(status), m["ask"]),
        leak_check(c),
    ]


@scenario()
def host_probe_loop(stand, xray, work, name):
    """The host asks for probes of the balancer over and over for 95 s while traffic flows;
    a1 dies midway. Host probes stay within their own minute budget, the failover is as
    quick as without the requests, and no server name is probed twice at once."""
    cfg = bench.client_config(THREE, THREE_COSTS)
    m = Marks()
    stop = threading.Event()
    calls = []

    def ask_in_a_loop():
        while not stop.is_set():
            try:
                r = c.api("-t", "1", "probe", "auto")
                calls.append((time.time(), r["id"], None))
            except Exception as e:
                calls.append((time.time(), None, repr(e)))
            time.sleep(0.1)

    asker = threading.Thread(target=ask_in_a_loop, daemon=True)
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(5, asker.start), (40, m.at("fault", lambda: bench.chaos(A0, "blackhole"))),
                          (100, stop.set)], 102)
        asker.join(5)
        status = c.status()
    tr.save(os.path.join(work, f"{name}-traffic.json"))
    rec, _ = bench.recovery(tr.records, m["fault"])
    sw = bench.switches(c.events, m["fault"])
    host_peak, host_n = host_starts_peak(c.events)
    errors = [e for _, _, e in calls if e]
    ids = {i for _, i, _ in calls if i}
    peak, _, _ = probe_checks(c.events)
    heap = [h for _, h, _ in c.mem]
    return [
        check("requests made", f"{len(calls)} calls, {len(ids)} requests, {len(errors)} errors",
              len(calls) >= 60 and not errors, ">= 60 calls, no errors"),
        check("host probe starts in any 60 s", f"{host_peak} ({host_n} in all)",
              host_peak <= HOST_PROBES_PER_MINUTE, f"<= {HOST_PROBES_PER_MINUTE}"),
        check("recovery, s", rec and round(rec, 1), rec is not None and rec <= 10, "<= 10 (failover_blackhole)"),
        check("switches after the fault", [f"{e['fields']['to']} ({e['fields']['reason']})" for e in sw],
              len(sw) == 1 and sw[0]["fields"]["to"] == "a2", "a2 once"),
        at_once_check(peak),
        per_name_check(c.events, server_names(status)),
        check("heap in use, MB", heap and f"{round(min(heap) / 2**20, 1)}..{round(max(heap) / 2**20, 1)}", True,
              "info"),
        leak_check(c),
    ]


@scenario(slow=True)
def soak(stand, xray, work, name):
    """Hours of traffic with a fault every five minutes: memory must not grow."""
    mins = minutes("BENCH_SOAK_MIN", 120)
    members = [("a1", "reality", 0), ("a2", "reality", 1), ("b1", "tcp_tls", 0), ("c1", "xhttp", 0)]
    cfg = bench.client_config(members, {"a1": 1, "a2": 1, "b1": 3, "c1": 10},
                              strategy={"minHoldTime": "1m", "returnToCheaper": {"interval": "3m", "probeInterval": "10s"}})
    if os.environ.get("BENCH_SOAK_PLAIN"):
        # The same faults without the observatory, to tell growth of the
        # mechanism from growth of Xray itself.
        cfg = bench.plain(cfg, "a1")
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
        heap0, gor0 = c.gc_memory("start")
        mheap0, mgor0 = c.mechanism_heap("soak-start"), c.mechanism_goroutines()
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
        heap1, gor1 = c.gc_memory("end")
        mheap1, mgor1 = c.mechanism_heap("soak-end"), c.mechanism_goroutines()
        tr.stop()
    ok = sum(1 for r in tr.records if r[1]) / max(1, len(tr.records))
    # The whole process grows from user connections that Xray keeps dialing
    # on frozen addresses; the same run without the observatory grows more.
    return [check(f"mechanism heap after {mins:g} min vs start, MB", round((mheap1 - mheap0) / 2**20, 2),
                  mheap1 - mheap0 <= 2**20, "<= 1"),
            check("mechanism goroutines after vs start", mgor1 - mgor0, mgor1 - mgor0 <= 2, "<= 2"),
            check(f"whole heap after {mins:g} min vs start, MB", round((heap1 - heap0) / 2**20, 2), True, "info"),
            check("whole goroutines after vs start", gor1 - gor0, True, "info"),
            check("events dropped", c.dropped, c.dropped == 0, "== 0"),
            check("requests ok, %", round(100 * ok, 1), True, "info"),
            leak_check(c)]


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


# ---- 5. fresh outbounds from a provider ---------------------------------------

BANNED = [("a1", "reality", 0), ("a2", "reality", 1)]
FRESH = [("x1", "reality", 2), ("x2", "reality", 3)]


def refresh(c):
    return c.api("refresh", "fresh")


def gaps(times):
    return [round(b - a, 2) for a, b in zip(times, times[1:])]


def answer_body(stand, body):
    with open(stand.provider_answer, "w") as f:
        json.dump(body, f)


@scenario()
def provider_subnet_ban(stand, xray, work, name):
    """Every address of the set is blocked; the provider brings new ones."""
    stand.answer(FRESH, {"x1": 1, "x2": 1})
    prov = bench.provider_config(["auto", "direct"], triggers={"shouldRefreshWhenExhausted": True})
    cfg = bench.client_config(BANNED, {"a1": 1, "a2": 1}, providers=[prov])
    banned = ",".join(str(address_port(k, i)) for _, k, i in BANNED)
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(10, m.at("ban", lambda: bench.chaos(banned, "blackhole")))], 60)
    ex = bench.events_of(c.events, "exhausted", m["ban"])
    t_ex = ex[0]["ts"] / 1000 if ex else None
    rec = bench.recovery(tr.records, t_ex)[0] if t_ex else None
    fetches = bench.events_of(c.events, "provider_fetch", m["ban"])
    applied = [e for e in bench.events_of(c.events, "provider_apply", m["ban"]) if e["fields"].get("is_changed")]
    fresh = [e for e in bench.switches(c.events, m["ban"]) if e["fields"]["to"].startswith("p-")]
    reqs = stand.provider_requests(m["ban"])
    first = fetches[0]["fields"] if fetches else {}
    ok_via = next((e["fields"]["via"] for e in fetches if e["fields"].get("is_ok")), None)
    return [
        check("exhausted after the ban, s", t_ex and round(t_ex - m["ban"], 1),
              t_ex is not None and t_ex - m["ban"] <= 20, "<= 20"),
        check("refresh trigger", first.get("trigger"), first.get("trigger") == "exhausted", "exhausted"),
        check("exhausted balancer skipped as via", first.get("error"),
              first.get("via") == "auto" and first.get("error") == "balancer exhausted", "balancer exhausted"),
        check("set fetched via", ok_via, ok_via == "direct", "direct"),
        check("outbounds added", applied and applied[0]["fields"].get("added"),
              bool(applied) and applied[0]["fields"].get("added") == len(FRESH), f"== {len(FRESH)}"),
        check("switched to a fresh outbound", fresh and fresh[0]["fields"]["to"], bool(fresh), "p-*"),
        check("connectivity back after exhausted, s", rec and round(rec, 1), rec is not None and rec <= 30, "<= 30"),
        check("provider requests", len(reqs), len(reqs) == 1, "== 1"),
        check("token in a header, not in the URL", reqs and f"{reqs[0][1]} {reqs[0][2]}",
              bool(reqs) and all(a == "Bearer bench-token" and p == "/set" for _, a, p in reqs), "Bearer, /set"),
    ]


@scenario()
def provider_post_state(stand, xray, work, name):
    """Every address of the set the provider gave is blocked: the request posts
    what broke, both outbounds down with their failures, and every via sends
    the same body. No destination of the user and no address goes out."""
    stand.answer(FRESH, {"x1": 1, "x2": 1})
    banned = [("p-a1", "reality", 0), ("p-a2", "reality", 1)]
    prov = bench.provider_config(["net", "direct"], triggers={"shouldRefreshWhenExhausted": True},
                                 limits={"exhaustedMinInterval": "5s", "backoff": {"initialDelay": "5s"}})
    cfg = bench.client_config(banned, {"p-a1": 1, "p-a2": 1}, providers=[prov])
    ports = ",".join(str(address_port(k, i)) for _, k, i in banned)
    m = Marks()

    def answer_after_garbage():
        # Both via get a broken answer, so both send their body; the retry
        # after the backoff gets the set.
        deadline = time.time() + 30
        while len(stand.provider_bodies()) < 2 and time.time() < deadline:
            time.sleep(0.1)
        bench.provider_mode("ok")

    bench.provider_mode("garbage")
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(10, m.at("ban", lambda: bench.chaos(ports, "blackhole"))),
                          (11, answer_after_garbage)], 60)
    bodies = stand.provider_bodies(m["ban"])
    first = bodies[0] if bodies else {}
    body = first.get("body") if isinstance(first.get("body"), dict) else {}
    outs = {o.get("tag"): o for o in body.get("outbounds", [])}
    bal = body.get("balancer", {})
    raw = json.dumps([b.get("body") for b in bodies])
    leaks = [s for s in bench.TARGETS + ["probe.test", bench.HOST, "dead.test"] if s in raw]
    pair = [(b["body"].get("run"), b["body"].get("refreshNumber")) for b in bodies[:2] if isinstance(b.get("body"), dict)]
    applied = [e for e in bench.events_of(c.events, "provider_apply", m["ban"]) if e["fields"].get("is_changed")]
    fresh = [e for e in bench.switches(c.events, m["ban"]) if e["fields"]["to"] in ("p-x1", "p-x2")]
    # The chosen one fails on traffic; the other is found down by its failover
    # probe, which the body tells by class.
    down = {t: (o.get("state"), sorted(o.get("failures", {})) or (o.get("probe") or {}).get("error"))
            for t, o in outs.items()}
    why = [o for o in outs.values() if o.get("state") == "down"
           and ("dial_timeout" in o.get("failures", {}) or (o.get("probe") or {}).get("isPassed") is False)]
    return [
        check("posted as JSON", first.get("type"), first.get("type") == "application/json" and bool(body),
              "application/json"),
        check("trigger", body.get("trigger"), body.get("trigger") in ("exhausted", "min_healthy"),
              "exhausted or min_healthy"),
        check("outbounds down with what failed", down,
              sorted(outs) == ["a1", "a2"] and len(why) == 2
              and any("dial_timeout" in o.get("failures", {}) for o in outs.values()),
              "a1, a2 down: dial_timeout or a failed probe"),
        check("balancer exhausted, not own", (bal.get("isExhausted"), bal.get("isOwn")),
              bal.get("isExhausted") is True and bal.get("isOwn") is False, "(True, False)"),
        check("no destination or address in the body", leaks, not leaks, "none"),
        check("body size, bytes", first.get("bytes"), 0 < (first.get("bytes") or 0) < 4096, "< 4096"),
        check("same run and refresh through both via", pair,
              len(pair) == 2 and pair[0] == pair[1] and pair[0][0] and pair[0][1], "equal"),
        check("set applied after the broken answers", len(applied), len(applied) == 1, "== 1"),
        check("switched to a fresh outbound", fresh and fresh[0]["fields"]["to"], bool(fresh), "p-x*"),
    ]


@scenario()
def provider_first_via_dead(stand, xray, work, name):
    """The first way to the provider is dead: the request goes the second way."""
    stand.answer(FRESH, {"x1": 1, "x2": 1})
    prov = bench.provider_config(["spare", "direct"], limits={"exhaustedMinInterval": "1s"})
    cfg = bench.client_config(BANNED[:1], {"a1": 1}, providers=[prov])
    cfg["outbounds"].insert(-1, bench.member("spare", "tcp_tls", 1, bench.PIN))
    bench.chaos(str(address_port("tcp_tls", 1)), "blackhole")
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=1, big=0)
        run_timeline(tr, [(3, m.at("refresh", lambda: refresh(c)))], 15)
    fetches = [e["fields"] for e in bench.events_of(c.events, "provider_fetch", m["refresh"])]
    applied = [e["fields"] for e in bench.events_of(c.events, "provider_apply", m["refresh"])]
    reqs = stand.provider_requests(m["refresh"])
    took = reqs[0][0] - m["refresh"] if reqs else None
    order = [(f.get("via"), f.get("is_ok")) for f in fetches]
    return [
        check("attempts (via, ok)", order, order == [("spare", False), ("direct", True)],
              "[spare failed, direct ok]"),
        check("dead via error", fetches and fetches[0].get("error"), bool(fetches) and fetches[0].get("error") == "timeout",
              "timeout"),
        check("trigger in every attempt", sorted({f.get("trigger") for f in fetches}),
              {f.get("trigger") for f in fetches} == {"host"}, "host"),
        check("set applied", applied and applied[-1].get("is_ok"), bool(applied) and applied[-1].get("is_ok") is True, "ok"),
        check("provider requests", len(reqs), len(reqs) == 1, "== 1"),
        check("answer after the first via timed out, s", took and round(took, 1),
              took is not None and 3 <= took <= 5, "timeout 3 .. 5"),
    ]


@scenario()
def provider_invalid_answer(stand, xray, work, name):
    """A broken answer and a set that does not build change nothing; failures back off."""
    stand.answer(FRESH, {"x1": 1, "x2": 1})
    limits = {"exhaustedMinInterval": "1s", "maxRefreshesPerHour": 0,
              "backoff": {"initialDelay": "2s", "multiplier": 2, "maxDelay": "8s", "jitterFraction": 0.3}}
    prov = bench.provider_config(["direct"], limits=limits)
    cfg = bench.client_config(BANNED[:1], {"a1": 1}, providers=[prov])
    m = Marks()

    def garbage():
        bench.provider_mode("garbage")
        refresh(c)

    def unbuildable():
        answer_body(stand, {"outbounds": [bench.path_members([("x1", "reality", 2)], {"x1": 1})[0],
                                          {"tag": "x3", "protocol": "vless", "settings": {"vnext": []},
                                           "path": {"balancer": "auto", "cost": 1, "probe": "default"}}]})
        bench.provider_mode("ok")

    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(2, m.at("good", lambda: refresh(c))), (6, m.at("garbage", garbage)),
                          (26, m.at("unbuildable", unbuildable))], 50)
        members = c.status()["providers"][0]["members"]
    applies = bench.events_of(c.events, "provider_apply", m["garbage"])
    fetches = bench.events_of(c.events, "provider_fetch", m["garbage"])
    broken = [e for e in fetches if "invalid answer" in (e["fields"].get("error") or "")]
    not_built = [e for e in applies if e["ts"] / 1000 >= m["unbuildable"] and e["fields"].get("is_ok") is False
                 and "build" in (e["fields"].get("error") or "")]
    changed = [e for e in applies if e["fields"].get("is_changed")]
    tries = [t for t, _, _ in stand.provider_requests(m["garbage"]) if t < m["unbuildable"]]
    g = gaps(tries)[:3]
    base = [2, 4, 8]
    within = len(g) == 3 and all(0.7 * b - 0.3 <= x <= 1.3 * b + 0.3 for x, b in zip(g, base))
    spread = [round(x / b, 2) for x, b in zip(g, base)]
    failed = sum(1 for r in tr.records if not r[1])
    return [
        check("broken answers seen as errors", len(broken), len(broken) >= 3, ">= 3"),
        check("set that does not build rejected", len(not_built), len(not_built) >= 1, ">= 1"),
        check("set changes after the first one", len(changed), not changed, "== 0"),
        check("members kept", members, members == len(FRESH), f"== {len(FRESH)}"),
        check("pauses after failures, s", g, within and g == sorted(g), "2, 4, 8 +-30% growing"),
        check("pauses vary (pause / backoff)", spread, any(abs(x - 1) > 0.05 for x in spread), "not all 1"),
        check("user requests failed", failed, failed == 0, "== 0"),
    ]


@scenario()
def provider_strict_answer(stand, xray, work, name):
    """An answer with a key protocol 1 does not know is refused whole: the set
    stays, and the second via is not asked, since it leads to the same backend."""
    stand.answer(FRESH, {"x1": 1, "x2": 1}, version="v1")
    prov = bench.provider_config(["direct", "net"], limits={"exhaustedMinInterval": "1s"})
    cfg = bench.client_config(BANNED[:1], {"a1": 1}, providers=[prov])
    m = Marks()

    def strict():
        answer_body(stand, {"protocol": 1, "version": "v2", "dns": {"servers": ["1.1.1.1"]},
                            "outbounds": bench.path_members(FRESH[:1], {"x1": 1})})
        refresh(c)

    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=1, big=0)
        run_timeline(tr, [(2, m.at("good", lambda: refresh(c))), (6, m.at("strict", strict))], 12)
        st = c.status()["providers"][0]
    applies = [e["fields"] for e in bench.events_of(c.events, "provider_apply", m["strict"])]
    fetches = [e["fields"] for e in bench.events_of(c.events, "provider_fetch", m["strict"])]
    reqs = stand.provider_requests(m["strict"])
    refused = applies[0] if applies else {}
    failed = sum(1 for r in tr.records if not r[1])
    return [
        check("answer with dns refused", refused.get("error"),
              refused.get("is_ok") is False and 'unknown key "dns"' in (refused.get("error") or ""), 'unknown key "dns"'),
        check("provider requests for it", len(reqs), len(reqs) == 1, "== 1"),
        check("attempts (via, ok)", [(f.get("via"), f.get("is_ok")) for f in fetches],
              [(f.get("via"), f.get("is_ok")) for f in fetches] == [("direct", True)], "[direct ok]"),
        check("set kept (version, members)", (st.get("version"), st.get("members")),
              st.get("version") == "v1" and st.get("members") == len(FRESH), f"(v1, {len(FRESH)})"),
        check("user requests failed", failed, failed == 0, "== 0"),
    ]


@scenario()
def provider_retry_after(stand, xray, work, name):
    """A busy provider answers 429 with Retry-After: the host request is asked
    again after that pause, spread upward, without a growing backoff and
    without the second via. An error body of the protocol ends the attempts too."""
    stand.answer(FRESH, {"x1": 1, "x2": 1})
    # exhaustedMinInterval would otherwise hold the retry of the host request
    # for 30 s, longer than Retry-After. Three busy requests fit before the
    # provider answers again: the third comes 12-15.6 s after the first.
    prov = bench.provider_config(["direct", "net"], limits={"exhaustedMinInterval": "1s"})
    cfg = bench.client_config(BANNED[:1], {"a1": 1}, providers=[prov])
    m = Marks()

    def busy():
        bench.provider_mode("status:429:6")
        refresh(c)

    def failing():
        bench.provider_mode("error:500:internal")
        refresh(c)

    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=1, big=0)
        run_timeline(tr, [(2, m.at("busy", busy)), (19.5, m.at("ok", lambda: bench.provider_mode("ok"))),
                          (30, m.at("failing", failing))], 34)
    reqs = [t for t, _, _ in stand.provider_requests(m["busy"])]
    busy_reqs = [t for t in reqs if t < m["ok"]]
    g = gaps(busy_reqs)[:2]
    fetches = [e["fields"] for e in bench.events_of(c.events, "provider_fetch", m["busy"])]
    busy_fetches = [f for f in fetches if f.get("status") == 429]
    skips = [e for e in bench.events_of(c.events, "provider_skip", m["busy"]) if e["fields"]["reason"] == "retry_after"]
    applied = [e["fields"] for e in bench.events_of(c.events, "provider_apply", m["ok"]) if e["fields"].get("is_changed")]
    failed = [f for f in fetches if f.get("code")]
    counted = sum(r.get("provider_retry_after", 0) for r in c.reports)
    return [
        # Retry-After 6 spread by up to 30% upward is 6 .. 7.8 s; the timer
        # of the loop and the request add up to a second on a busy machine.
        check("pauses after 429 with Retry-After 6, s", g,
              len(g) == 2 and all(5.9 <= x <= 8.8 for x in g), "6 .. 7.8 (+1 to deliver) twice, not growing"),
        check("attempts per busy refresh", [f.get("via") for f in busy_fetches],
              len(busy_fetches) >= 3 and len(busy_fetches) == len(busy_reqs)
              and all(f.get("via") == "direct" and f.get("retry_after_ms") == 6000 for f in busy_fetches),
              "direct only, retry_after_ms 6000"),
        check("failures counted while busy", [f.get("retry") for f in busy_fetches],
              all(f.get("retry") == 0 for f in busy_fetches), "retry 0"),
        check("retry_after skips", len(skips), len(skips) >= 1, ">= 1"),
        check("set applied once the provider answers", applied and applied[0].get("added"),
              bool(applied) and applied[0].get("added") == len(FRESH), f"== {len(FRESH)}"),
        check("error body ends the attempts", [(f.get("via"), f.get("code")) for f in failed],
              [(f.get("via"), f.get("code")) for f in failed] == [("direct", "internal")], "[direct internal]"),
        check("report provider_retry_after", counted, counted >= 1, ">= 1"),
    ]


@scenario()
def provider_refresh_keeps_download(stand, xray, work, name):
    """Refreshes while the chosen outbound works: a long download on it goes on,
    also after the outbound leaves the set, until its drain ends."""
    size, seconds, drain = 16 << 20, 24, 30
    prov = bench.provider_config(["auto", "direct"], limits={"exhaustedMinInterval": "1s"},
                                 apply={"drainTimeout": f"{drain}s"})
    cfg = bench.client_config([("p-x1", "reality", 1)], {"p-x1": 1}, providers=[prov])
    m = Marks()
    got = {}

    def long_download():
        got["start"] = time.time()
        try:
            got["bytes"] = bench.download("t1.test", f"/slow/{size}/{seconds}", timeout=seconds + 20)
        except OSError as e:
            got["error"] = repr(e)
        got["end"] = time.time()

    def first():
        stand.answer([("x1", "reality", 1), ("x2", "reality", 2)], {"x1": 1, "x2": 1})
        refresh(c)

    def second():
        stand.answer([("x2", "reality", 2)], {"x2": 1})
        refresh(c)

    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        dl = threading.Thread(target=long_download, daemon=True)
        run_timeline(tr, [(2, dl.start), (6, m.at("first", first)), (12, m.at("second", second))], 50)
        dl.join(timeout=30)
    applies = [e["fields"] for e in bench.events_of(c.events, "provider_apply", m["first"]) if e["fields"].get("is_ok")]
    one = applies[0] if applies else {}
    two = applies[1] if len(applies) > 1 else {}
    left = [e for e in bench.switches(c.events, m["second"]) if e["fields"]["from"] == "p-x1"]
    drained = [e for e in bench.events_of(c.events, "provider_drained", m["second"])
               if e["fields"]["outbound"] == "p-x1"]
    t_drained = drained[0]["ts"] / 1000 - m["second"] if drained else None
    failed = sum(1 for r in tr.records if not r[1])
    return [
        check("long download complete, MB", round(got.get("bytes", 0) / 2**20, 1), got.get("bytes") == size,
              f"== {size >> 20}"),
        check("download outlived the removal, s", got.get("end") and round(got["end"] - m["second"], 1),
              got.get("end", 0) > m["second"] + 5, "> 5"),
        check("first apply: kept, added", (one.get("unchanged"), one.get("added")),
              one.get("unchanged") == 1 and one.get("added") == 1, "(1, 1)"),
        check("second apply: removed", two.get("removed"), two.get("removed") == 1, "== 1"),
        check("balancer left the removed outbound", left and f"{left[0]['fields']['to']} {left[0]['fields']['reason']}",
              bool(left) and left[0]["fields"]["to"] == "p-x2" and left[0]["fields"]["reason"] == "retired",
              "p-x2 retired"),
        check("removed outbound closed after drain, s", t_drained and round(t_drained, 1),
              t_drained is not None and drain - 1 <= t_drained <= drain + 5, f"{drain} .. {drain + 5}"),
        check("user requests failed", failed, failed == 0, "== 0"),
    ]


@scenario()
def provider_limits(stand, xray, work, name):
    """A trigger that wants a refresh every second gets one per minInterval;
    a failing provider is asked again after growing, spread pauses."""
    stand.answer(FRESH, {"x1": 1, "x2": 1}, version="v1")
    prov = bench.provider_config(["direct"], triggers={"interval": "1s", "jitterFraction": 0},
                                 limits={"minInterval": "4s", "maxRefreshesPerHour": 0,
                                         "backoff": {"initialDelay": "5s", "multiplier": 2, "maxDelay": "1m", "jitterFraction": 0.3}})
    cfg = bench.client_config(BANNED[:1], {"a1": 1}, providers=[prov])
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0)
        start = run_timeline(tr, [(30, m.at("fail", lambda: bench.provider_mode("status:500")))], 85)
    reqs = [t for t, _, _ in stand.provider_requests(start)]
    ok_phase = [t for t in reqs if t < m["fail"]]
    fail_phase = [t for t in reqs if t >= m["fail"]]
    g_ok = gaps(ok_phase)
    g_fail = gaps(fail_phase)[:3]
    base = [5, 10, 20]
    within = len(g_fail) == 3 and all(max(4, 0.7 * b) - 0.3 <= x <= 1.3 * b + 0.3 for x, b in zip(g_fail, base))
    skips = [e for e in bench.events_of(c.events, "provider_skip", start) if e["fields"]["reason"] == "min_interval"]
    return [
        check("refreshes in 30 s with interval 1s", len(ok_phase), 6 <= len(ok_phase) <= 9, "30 / minInterval 4"),
        check("min pause between refreshes, s", g_ok and min(g_ok), bool(g_ok) and min(g_ok) >= 3.9, ">= 4"),
        check("min_interval skips reported", len(skips), 0 < len(skips) <= len(ok_phase) + 1, "one per wait"),
        check("pauses after failures, s", g_fail, within and g_fail == sorted(g_fail), "5, 10, 20 +-30% growing"),
    ]


def lease_ends(events, tag, since=0):
    return [e for e in bench.events_of(events, "provider_expired", since) if e["fields"]["outbound"] == tag]


@scenario()
def provider_lease_expiry(stand, xray, work, name):
    """The lease of the chosen outbound ends while the provider is down: its
    renewal at half the lease fails, the balancer moves to the outbound whose
    lease runs on, without a probe of the expired one or ruling it out, and a
    long download on it goes on."""
    size, seconds = 16 << 20, 40
    stand.answer([("x1", "reality", 1), ("x2", "reality", 2)], costs={"x1": 1, "x2": 2},
                 expires={"x1": "@+30s", "x2": "@+300s"}, server_time="@+0s")
    prov = bench.provider_config(["direct"], limits={"exhaustedMinInterval": "1s",
                                                     "backoff": {"initialDelay": "5s", "maxDelay": "5s"}})
    cfg = bench.client_config([("p-x1", "reality", 1)], {"p-x1": 1}, providers=[prov])
    m = Marks()
    got = {}

    def long_download():
        try:
            got["bytes"] = bench.download("t1.test", f"/slow/{size}/{seconds}", timeout=seconds + 20)
        except OSError as e:
            got["error"] = repr(e)
        got["end"] = time.time()

    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        dl = threading.Thread(target=long_download, daemon=True)
        run_timeline(tr, [(2, m.at("answer", lambda: refresh(c))), (4, dl.start),
                          (10, m.at("down", lambda: bench.provider_mode("status:500")))], 50)
        dl.join(timeout=30)
    applied = [e for e in bench.events_of(c.events, "provider_apply", m["answer"]) if e["fields"].get("is_ok")]
    t_apply = applied[0]["ts"] / 1000 if applied else None
    ended = lease_ends(c.events, "p-x1", m["answer"])
    t_end = ended[0]["ts"] / 1000 if ended else None
    left = [e for e in bench.switches(c.events, m["answer"]) if e["fields"]["from"] == "p-x1"]
    probed = [e for e in bench.probes(c.events, m["answer"]) if e["fields"]["outbound"] == "p-x1"]
    down = [e for e in bench.events_of(c.events, "path_state", m["answer"])
            if e["fields"]["outbound"] == "p-x1" and e["fields"]["state"] == "down"]
    asked = [e for e in bench.events_of(c.events, "provider_fetch", t_end or m["down"])
             if e["fields"].get("trigger") == "expiry"]
    failed = sum(1 for r in tr.records if not r[1])
    return [
        check("lease of p-x1 ended after the answer, s", t_end and t_apply and round(t_end - t_apply, 1),
              t_end is not None and t_apply is not None and 29 <= t_end - t_apply <= 33, "29 .. 33"),
        check("expired event (was_active, renewal)", ended and (ended[0]["fields"]["was_active"], ended[0]["fields"]["renewal"]),
              bool(ended) and ended[0]["fields"]["was_active"] is True and ended[0]["fields"]["renewal"] == "failed",
              "(True, failed)"),
        check("balancer left the expired outbound", left and f"{left[0]['fields']['to']} {left[0]['fields']['reason']}",
              bool(left) and left[0]["fields"]["to"] == "p-x2" and left[0]["fields"]["reason"] == "expired"
              and t_end is not None and left[0]["ts"] / 1000 - t_end <= 5, "p-x2 expired within 5 s"),
        check("probes of the expired outbound", len(probed), not probed, "== 0"),
        check("expired outbound ruled out", len(down), not down, "== 0"),
        check("refresh for the ended lease", len(asked), len(asked) >= 1, ">= 1 expiry"),
        check("long download complete, MB", round(got.get("bytes", 0) / 2**20, 1), got.get("bytes") == size,
              f"== {size >> 20}"),
        check("download outlived the lease, s", got.get("end") and t_end and round(got["end"] - t_end, 1),
              t_end is not None and got.get("end", 0) > t_end + 3, "> 3"),
        check("user requests failed", failed, failed == 0, "== 0"),
    ]


@scenario()
def provider_all_expired(stand, xray, work, name):
    """The only lease ends while the provider is down: traffic goes on through
    the expired outbound rather than to the fallback, and the provider is
    asked again with a growing backoff."""
    stand.answer([("x1", "reality", 1)], {"x1": 1}, expires={"x1": "@+10s"}, server_time="@+0s")
    prov = bench.provider_config(["direct"], limits={
        "exhaustedMinInterval": "1s", "maxRefreshesPerHour": 0,
        "backoff": {"initialDelay": "2s", "multiplier": 2, "maxDelay": "16s", "jitterFraction": 0.3}})
    cfg = bench.client_config([("p-x1", "reality", 1)], {"p-x1": 1}, providers=[prov])
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(2, m.at("answer", lambda: refresh(c))),
                          (3, m.at("down", lambda: bench.provider_mode("status:500")))], 45)
        st = c.status()
    ended = lease_ends(c.events, "p-x1", m["answer"])
    t_end = ended[0]["ts"] / 1000 if ended else None
    exhausted = bench.events_of(c.events, "exhausted", m["answer"])
    left = [e for e in bench.switches(c.events, m["answer"]) if e["fields"]["from"] == "p-x1"]
    reqs = [t for t, _, _ in stand.provider_requests(m["down"])]
    g = gaps(reqs)[:3]
    base = [2, 4, 8]
    within = len(g) == 3 and all(0.7 * b - 0.3 <= x <= 1.3 * b + 0.3 for x, b in zip(g, base))
    after = [r for r in tr.records if t_end and r[0] >= t_end + 1]
    failed = sum(1 for r in after if not r[1])
    active = (st.get("balancers") or [{}])[0].get("active")
    return [
        check("lease ended", len(ended), len(ended) == 1, "== 1"),
        check("exhausted", len(exhausted), not exhausted, "== 0"),
        check("still on the expired outbound", (active, len(left)), active == "p-x1" and not left, "(p-x1, 0)"),
        check("requests after the end", len(after), len(after) >= 40, ">= 40"),
        check("of them failed", failed, failed == 0, "== 0"),
        check("pauses between provider requests, s", g, within and g == sorted(g), "2, 4, 8 +-30% growing"),
    ]


@scenario()
def provider_clock_skew(stand, xray, work, name):
    """The provider runs a day ahead of the device: a lease of 40 seconds by
    its clock still ends 40 seconds after the answer."""
    stand.answer([("x1", "reality", 1)], {"x1": 1}, expires={"x1": "@+86440s"}, server_time="@+86400s")
    prov = bench.provider_config(["direct"], limits={"exhaustedMinInterval": "1s"})
    cfg = bench.client_config([("p-x1", "reality", 1)], {"p-x1": 1}, providers=[prov])
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=1, big=0)
        run_timeline(tr, [(2, m.at("answer", lambda: refresh(c))),
                          (4, lambda: bench.provider_mode("status:500"))], 50)
    applied = [e for e in bench.events_of(c.events, "provider_apply", m["answer"]) if e["fields"].get("is_ok")]
    first = applied[0] if applied else {"fields": {}, "ts": 0}
    skew = first["fields"].get("skew_ms")
    ended = lease_ends(c.events, "p-x1", m["answer"])
    took = ended[0]["ts"] / 1000 - first["ts"] / 1000 if ended and applied else None
    return [
        check("skew_ms of the answer", skew, skew is not None and abs(skew - 86_400_000) <= 5000,
              "86 400 000 +- 5000"),
        check("clock", first["fields"].get("clock"), first["fields"].get("clock") == "server", "server"),
        check("lease ended after the answer, s", took and round(took, 1), took is not None and 37 <= took <= 43,
              "40 +- 3"),
    ]


@scenario()
def provider_lease_renewal(stand, xray, work, name):
    """Leases of 40 seconds and no user traffic at all: the provider is asked
    to renew them at half their length, and they never end."""
    stand.answer(FRESH, {"x1": 1, "x2": 1}, version="v1", expires={"x1": "@+40s", "x2": "@+40s"}, server_time="@+0s")
    # The leases come with a host request; the default minInterval of five
    # minutes after it would hold the planned renewal until the urgent point.
    prov = bench.provider_config(["direct"], triggers={"expiry": {"renewAtFraction": 0.5}}, limits={"minInterval": "1s"})
    cfg = bench.client_config(BANNED[:1], {"a1": 1}, providers=[prov])
    m = Marks()
    with bench.Client(xray, cfg, work, name) as c:
        run_timeline(None, [(2, m.at("answer", lambda: refresh(c)))], 50)
        st = c.status()
    applies = [e for e in bench.events_of(c.events, "provider_apply", m["answer"]) if e["fields"].get("is_ok")]
    t_apply = applies[0]["ts"] / 1000 if applies else None
    renewals = [e for e in bench.events_of(c.events, "provider_fetch", m["answer"]) if e["fields"].get("trigger") == "expiry"]
    took = renewals[0]["ts"] / 1000 - t_apply if renewals and t_apply else None
    second = applies[1]["fields"] if len(applies) > 1 else {}
    ended = bench.events_of(c.events, "provider_expired", m["answer"])
    reqs = stand.provider_requests(m["answer"])
    counted = sum(r.get("provider_renewals", 0) for r in c.reports)
    return [
        check("core idle throughout", st.get("is_idle"), st.get("is_idle") is True, "True"),
        check("renewal after the answer, s", took and round(took, 1), took is not None and 17.5 <= took <= 22.5,
              "20 +- 2"),
        check("renewal applied (changed, renewed)", (second.get("is_changed"), second.get("renewed")),
              second.get("is_changed") is False and second.get("renewed") == 2, "(False, 2)"),
        check("leases ended", len(ended), not ended, "== 0"),
        check("provider requests", len(reqs), len(reqs) == 1 + len(renewals) and len(renewals) <= 3,
              "the answer and the renewals"),
        check("report provider_renewals", counted, counted >= 1, ">= 1"),
    ]


@scenario()
def provider_token_rotation(stand, xray, work, name):
    """The backend rotates the token in an answer and later refuses it: the
    provider stops asking with the dead token instead of every second, and
    the token the host hands over on stdin goes with the next request. No
    token shows in events, Status or reports."""
    stand.answer(FRESH, {"x1": 1, "x2": 1}, version="v1")
    prov = bench.provider_config(["direct"], triggers={"interval": "1s", "jitterFraction": 0},
                                 limits={"minInterval": "1s", "exhaustedMinInterval": "1s", "maxRefreshesPerHour": 0})
    cfg = bench.client_config(BANNED[:1], {"a1": 1}, providers=[prov])
    m = Marks()
    tokens = ["bench-token", "answer-t2", "host-t3"]

    def host_token():
        c.api("headers", "fresh", stdin='{"Authorization": "Bearer host-t3"}')
        refresh(c)

    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=1, big=0)
        run_timeline(tr, [(2, m.at("rotate", lambda: bench.provider_mode("rotate:answer-t2"))),
                          (6, m.at("require", lambda: bench.provider_mode("require:host-t3"))),
                          (30, m.at("host", host_token))], 36)
        st = c.status()
    reqs = stand.provider_requests(m["rotate"])
    rotated = [a for t, a, _ in reqs if m["rotate"] + 1.5 <= t < m["require"]]
    held = [t for t, _, _ in reqs if m["require"] <= t < m["host"]]
    after = [(t, a) for t, a, _ in reqs if t >= m["host"]]
    refused = [e["fields"] for e in bench.events_of(c.events, "provider_auth_failed", m["require"])]
    by = [e["fields"].get("by") for e in bench.events_of(c.events, "provider_headers", m["rotate"])]
    seen = json.dumps([c.events, c.reports, st])
    shown = [tok for tok in tokens if tok in seen]
    counted = sum(r.get("provider_auth_failed", 0) for r in c.reports)
    took = after[0][0] - m["host"] if after else None
    return [
        check("requests after the rotation carry the new token", rotated,
              len(rotated) >= 2 and all(a == "Bearer answer-t2" for a in rotated), "Bearer answer-t2"),
        check("refusals of the token", [(f.get("status"), f.get("code")) for f in refused],
              [(f.get("status"), f.get("code")) for f in refused] == [(401, "unauthorized")], "[(401, unauthorized)]"),
        check("requests while the token is refused", len(held), 1 <= len(held) <= 2, "1 .. 2"),
        check("quiet before the host token, s", held and round(m["host"] - held[-1], 1),
              bool(held) and m["host"] - held[-1] >= 20, ">= 20"),
        check("host token sent after, s", took and round(took, 2),
              took is not None and took <= 2 and after[0][1] == "Bearer host-t3", "Bearer host-t3 within 2"),
        check("provider_headers by", by, by == ["answer", "host"], "[answer, host]"),
        check("tokens in events, Status, reports", shown, not shown, "none"),
        check("report provider_auth_failed", counted, counted == 1, "== 1"),
    ]


def rfc3339(t):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


@scenario()
def provider_initial(stand, xray, work, name):
    """The core starts with the set a connect brought: the balancer is on an
    outbound of it before the provider is asked, the first request comes from
    a trigger rather than the start, and a quiet session reports nothing."""
    stand.answer(FRESH, {"x1": 1, "x2": 1}, version="v1")
    now = time.time()
    initial = {"protocol": 1, "version": "v1", "outbounds": bench.path_members(FRESH, {"x1": 1, "x2": 1}),
               "expiresAt": {"x1": rfc3339(now + 600), "x2": rfc3339(now + 600)}, "serverTime": rfc3339(now)}
    prov = bench.provider_config(["direct"], triggers={"interval": "10s", "jitterFraction": 0}, limits={"minInterval": "1s"})
    prov["initialAnswer"] = initial
    # The outbound of the config is dearer, so the start takes one of the set.
    cfg = bench.client_config(BANNED[:1], {"a1": 5}, providers=[prov])
    with bench.Client(xray, cfg, work, name) as c:
        start = time.time()
        tr = bench.Traffic(rate=1, big=0)
        run_timeline(tr, [], 15)
        st = c.status()["providers"][0]
    reqs = stand.provider_requests(start - 5)
    first_req = reqs[0][0] if reqs else None
    applies = [e["fields"] for e in c.events if e["type"] == "provider_apply"]
    first_switch = next((e for e in c.events if e["type"] == "switch"), None)
    fetches = [e["fields"] for e in c.events if e["type"] == "provider_fetch"]
    failed = sum(1 for r in tr.records if not r[1])
    return [
        check("initial set applied (trigger, added, clock)",
              applies and (applies[0].get("trigger"), applies[0].get("added"), applies[0].get("clock")),
              bool(applies) and applies[0].get("trigger") == "initial" and applies[0].get("added") == len(FRESH)
              and applies[0].get("clock") == "server", f"(initial, {len(FRESH)}, server)"),
        check("first switch", first_switch and f"{first_switch['fields']['to']} {first_switch['fields']['reason']}",
              first_switch is not None and first_switch["fields"]["to"].startswith("p-")
              and (first_req is None or first_switch["ts"] / 1000 < first_req), "p-* before any provider request"),
        check("first provider request after the start, s", first_req and round(first_req - start, 1),
              first_req is not None and first_req - start >= 8, ">= 8 (interval)"),
        check("trigger of the first request", fetches and fetches[0].get("trigger"),
              bool(fetches) and fetches[0].get("trigger") == "interval", "interval"),
        check("set kept (version, members)", (st.get("version"), st.get("members")),
              st.get("version") == "v1" and st.get("members") == len(FRESH), f"(v1, {len(FRESH)})"),
        check("final report of the quiet session", c.final_report, c.final_report is None and not c.reports, "null"),
        check("user requests failed", failed, failed == 0, "== 0"),
    ]


@scenario()
def provider_saved_set(stand, xray, work, name):
    """The host keeps the set after the backend rotated the token and starts
    the next core with it: that core runs on the set at once, and its first
    request, from a trigger, carries the rotated token."""
    stand.answer(FRESH, {"x1": 1, "x2": 1}, version="v1", expires={"x1": "@+600s", "x2": "@+600s"}, server_time="@+0s")
    prov = bench.provider_config(["direct"])
    cfg = bench.client_config(BANNED[:1], {"a1": 5}, providers=[prov])
    bench.provider_mode("rotate:answer-t2")
    m = Marks()
    kept = {}
    with bench.Client(xray, cfg, work, name + "-first") as c:
        run_timeline(None, [(1, m.at("answer", lambda: refresh(c))),
                            (3, lambda: kept.update(c.api("set", "fresh")))], 4)
    saved = kept.get("set") or {}
    # The backend accepts only the rotated token from now on.
    bench.provider_mode("require:answer-t2")
    prov2 = bench.provider_config(["direct"], triggers={"interval": "5s", "jitterFraction": 0}) | {"initialAnswer": saved}
    cfg2 = bench.client_config(BANNED[:1], {"a1": 5}, providers=[prov2])
    launched = time.time()
    with bench.Client(xray, cfg2, work, name + "-second") as c2:
        start = time.time()
        tr = bench.Traffic(rate=1, big=0)
        run_timeline(tr, [], 8)
    reqs = stand.provider_requests(launched)
    applies = [e["fields"] for e in c2.events if e["type"] == "provider_apply"]
    first_switch = next((e for e in c2.events if e["type"] == "switch"), None)
    refused = [e for e in c2.events if e["type"] == "provider_auth_failed"]
    failed = sum(1 for r in tr.records if not r[1])
    return [
        check("kept set (seq, version, token)",
              (kept.get("seq"), saved.get("version"), (saved.get("headers") or {}).get("Authorization")),
              kept.get("seq", 0) >= 1 and saved.get("version") == "v1"
              and (saved.get("headers") or {}).get("Authorization") == "Bearer answer-t2"
              and "receivedAt" in saved, "(>= 1, v1, Bearer answer-t2)"),
        check("second core starts on the kept set (trigger, clock, version)",
              applies and (applies[0].get("trigger"), applies[0].get("clock"), applies[0].get("version")),
              bool(applies) and applies[0].get("trigger") == "initial" and applies[0].get("clock") == "saved"
              and applies[0].get("version") == "v1", "(initial, saved, v1)"),
        check("first switch", first_switch and f"{first_switch['fields']['to']} {first_switch['fields']['reason']}",
              first_switch is not None and first_switch["fields"]["to"].startswith("p-"), "p-*"),
        check("first request of the second core, s", reqs and round(reqs[0][0] - start, 1),
              bool(reqs) and reqs[0][0] - start >= 3, ">= 3 (interval)"),
        check("token of the first request", reqs and reqs[0][1], bool(reqs) and reqs[0][1] == "Bearer answer-t2",
              "Bearer answer-t2"),
        check("refused tokens", len(refused), not refused, "== 0"),
        check("user requests failed", failed, failed == 0, "== 0"),
    ]


@scenario()
def provider_base_changed(stand, xray, work, name):
    """The backend serves another base config than the one the core started
    with: the host is told once per value, and again after an answer named
    the base of the core, while every set applies and every request tells
    the base of the core."""
    prov = bench.provider_config(["direct"], limits={"exhaustedMinInterval": "1s"}) | {"baseVersion": "b1"}
    cfg = bench.client_config(BANNED[:1], {"a1": 1}, providers=[prov])
    served = ["b2", "b2", "b1", "b2"]
    m = Marks()

    def serve(base):
        def go():
            stand.answer(FRESH, {"x1": 1, "x2": 1}, version="v1", base_version=base)
            refresh(c)
        return go

    with bench.Client(xray, cfg, work, name) as c:
        tr = bench.Traffic(rate=1, big=0)
        run_timeline(tr, [(2 + 3 * i, m.at(f"answer{i}", serve(b))) for i, b in enumerate(served)], 15)
        st = c.status()["providers"][0]
    told = [(e["fields"]["from"], e["fields"]["to"]) for e in bench.events_of(c.events, "provider_base_changed")]
    applies = [e["fields"] for e in bench.events_of(c.events, "provider_apply", m["answer0"])]
    bodies = [b["body"] for b in stand.provider_bodies(m["answer0"]) if isinstance(b.get("body"), dict)]
    counted = sum(r.get("provider_base_changed", 0) for r in c.reports)
    failed = sum(1 for r in tr.records if not r[1])
    return [
        check("base changes told (from, to)", told, told == [("b1", "b2"), ("b1", "b2")], "[(b1, b2)] twice"),
        check("answers applied (ok, base_version)", [(f.get("is_ok"), f.get("base_version")) for f in applies],
              [(f.get("is_ok"), f.get("base_version")) for f in applies] == [(True, b) for b in served],
              "every one, " + ", ".join(served)),
        check("base in the requests", [b.get("baseVersion") for b in bodies],
              len(bodies) == len(served) and all(b.get("baseVersion") == "b1" for b in bodies), "b1 each"),
        check("status (base, answer base, changed)",
              (st.get("base_version"), st.get("answer_base_version"), st.get("is_base_changed")),
              (st.get("base_version"), st.get("answer_base_version"), st.get("is_base_changed")) == ("b1", "b2", True),
              "(b1, b2, True)"),
        check("report provider_base_changed", counted, counted == 2, "== 2"),
        check("user requests failed", failed, failed == 0, "== 0"),
    ]


SLOTS = [("s1", "reality", 1), ("s2", "reality", 2), ("s3", "reality", 3)]


def slot_set(slots, costs, lease, version):
    """The answer of a connect with leases of lease seconds from now."""
    now = time.time()
    return {"protocol": 1, "version": version, "outbounds": bench.path_members(slots, costs),
            "expiresAt": {t: rfc3339(now + lease) for t, _, _ in slots}, "serverTime": rfc3339(now)}


@scenario()
def provider_slots_flow(stand, xray, work, name):
    """Slots of a backend from start to restart. A connect brings a primary
    slot and two reserves with leases. The primary is banned while the
    backend is busy: the core moves to a reserve without it, asks with what
    broke, waits as long as the backend asks, and takes the answer that
    replaces the dead slot, renews the others and rotates the token. The next
    core starts on the set the host kept: no early request, no refused token."""
    initial = slot_set(SLOTS, {"s1": 1, "s2": 2, "s3": 3}, 300, "v1")
    # minHealthyOutbounds 4: a1 of the config and the three slots; the ban leaves three.
    prov = bench.provider_config(["direct", "net"], triggers={"minHealthyOutbounds": 4},
                                 limits={"minInterval": "1s", "exhaustedMinInterval": "1s", "maxRefreshesPerHour": 0}) | {
        "initialAnswer": initial}
    cfg = bench.client_config(BANNED[:1], {"a1": 5}, providers=[prov])
    fresh = [("s4", "reality", 0), ("s2", "reality", 2), ("s3", "reality", 3)]
    m = Marks()
    kept = {}

    def ban():
        bench.chaos(str(address_port("reality", 1)), "blackhole")

    def answer_after_busy():
        # The backend is busy for the first request after the ban and serves
        # the new set by the time the core asks again.
        deadline = time.time() + 40
        while not stand.provider_requests(m["ban"]) and time.time() < deadline:
            time.sleep(0.1)
        time.sleep(1)
        stand.answer(fresh, costs={"s4": 1, "s2": 2, "s3": 3}, version="v2",
                     expires={t: "@+600s" for t, _, _ in fresh}, server_time="@+0s")
        m["ok"] = time.time()
        bench.provider_mode("rotate:slot-t2")

    bench.provider_mode("status:429:6")
    with bench.Client(xray, cfg, work, name + "-first") as c:
        tr = bench.Traffic(rate=2, big=0.5)
        busy = threading.Thread(target=answer_after_busy, daemon=True)
        run_timeline(tr, [(4, m.at("ban", ban)), (4.1, busy.start)], 40)
        busy.join(timeout=5)
        st = c.status()["providers"][0]
        kept.update(c.api("set", "fresh"))
    rec = bench.recovery(tr.records, m["ban"])[0]
    left = [e for e in bench.switches(c.events, m["ban"]) if e["fields"]["from"] == "p-s1"]
    t_left = left[0]["ts"] / 1000 if left else None
    applied = [e for e in bench.events_of(c.events, "provider_apply", m["ban"]) if e["fields"].get("is_ok")]
    t_applied = applied[0]["ts"] / 1000 if applied else None
    one = applied[0]["fields"] if applied else {}
    bodies = [b for b in stand.provider_bodies(m["ban"]) if isinstance(b.get("body"), dict)]
    first = bodies[0]["body"] if bodies else {}
    outs = {o.get("tag"): o for o in first.get("outbounds", [])}
    s1 = outs.get("s1", {})
    fetches = [e["fields"] for e in bench.events_of(c.events, "provider_fetch", m["ban"])]
    busy_fetches = [f for f in fetches if f.get("status") == 429]
    reqs = [t for t, _, _ in stand.provider_requests(m["ban"])]
    skips = [e for e in bench.events_of(c.events, "provider_skip", m["ban"]) if e["fields"]["reason"] == "retry_after"]
    ended = bench.events_of(c.events, "provider_expired")

    saved = kept.get("set") or {}
    bench.provider_mode("require:slot-t2")
    prov2 = bench.provider_config(["direct"], triggers={"interval": "5s", "jitterFraction": 0}) | {"initialAnswer": saved}
    cfg2 = bench.client_config(BANNED[:1], {"a1": 5}, providers=[prov2])
    launched = time.time()
    with bench.Client(xray, cfg2, work, name + "-second") as c2:
        start = time.time()
        tr2 = bench.Traffic(rate=1, big=0)
        run_timeline(tr2, [], 8)
    reqs2 = stand.provider_requests(launched)
    applies2 = [e["fields"] for e in c2.events if e["type"] == "provider_apply"]
    switch2 = next((e for e in c2.events if e["type"] == "switch"), None)
    refused2 = [e for e in c2.events if e["type"] == "provider_auth_failed"]
    failed2 = sum(1 for r in tr2.records if not r[1])
    return [
        check("primary left for a reserve (to, reason)", left and (left[0]["fields"]["to"], left[0]["fields"]["reason"]),
              bool(left) and left[0]["fields"]["to"] == "p-s2" and left[0]["fields"]["reason"] == "failover",
              "(p-s2, failover)"),
        check("failover before the backend answered, s", t_left and t_applied and round(t_applied - t_left, 1),
              t_left is not None and (t_applied is None or t_left < t_applied), "switch first"),
        check("connectivity back after the ban, s", rec and round(rec, 1), rec is not None and rec <= 30, "<= 30"),
        check("first request (trigger, s1 state, failures)",
              (first.get("trigger"), s1.get("state"), sorted(s1.get("failures", {})) or (s1.get("probe") or {})),
              first.get("trigger") == "min_healthy" and s1.get("state") in ("suspect", "down")
              and ("dial_timeout" in s1.get("failures", {}) or (s1.get("probe") or {}).get("isPassed") is False),
              "min_healthy, s1 failed"),
        check("leases in the request", sorted(t for t, o in outs.items() if (o.get("expiresInMs") or 0) > 0),
              sorted(t for t, o in outs.items() if (o.get("expiresInMs") or 0) > 0) == ["s1", "s2", "s3"],
              "s1, s2, s3"),
        check("busy answers (via, retry_after_ms)", [(f.get("via"), f.get("retry_after_ms")) for f in busy_fetches],
              len(busy_fetches) >= 1 and all(f.get("via") == "direct" and f.get("retry_after_ms") == 6000
                                             for f in busy_fetches), "direct only, 6000"),
        check("pause after the busy answer, s", gaps(reqs)[:1], len(reqs) >= 2 and 5.9 <= reqs[1] - reqs[0] <= 8.8,
              "6 .. 7.8 (+1 to deliver)"),
        check("retry_after skips", len(skips), len(skips) >= 1, ">= 1"),
        check("refresh (added, removed, unchanged, renewed)",
              (one.get("added"), one.get("removed"), one.get("unchanged"), one.get("renewed")),
              (one.get("added"), one.get("removed"), one.get("unchanged"), one.get("renewed")) == (1, 1, 2, 2),
              "(1, 1, 2, 2)"),
        check("token rotated (headers of the answer)", one.get("headers"), one.get("headers") == ["Authorization"],
              "[Authorization]"),
        check("set after it (version, members)", (st.get("version"), st.get("members")),
              (st.get("version"), st.get("members")) == ("v2", 3), "(v2, 3)"),
        check("leases ended", len(ended), not ended, "== 0"),
        check("next core starts on the kept set (trigger, clock, version)",
              applies2 and (applies2[0].get("trigger"), applies2[0].get("clock"), applies2[0].get("version")),
              bool(applies2) and applies2[0].get("trigger") == "initial" and applies2[0].get("clock") == "saved"
              and applies2[0].get("version") == "v2", "(initial, saved, v2)"),
        check("its first switch", switch2 and switch2["fields"]["to"],
              switch2 is not None and switch2["fields"]["to"] == "p-s4", "p-s4"),
        check("its first request, s", reqs2 and round(reqs2[0][0] - start, 1),
              bool(reqs2) and reqs2[0][0] - start >= 3, ">= 3 (interval)"),
        check("its token", reqs2 and reqs2[0][1], bool(reqs2) and reqs2[0][1] == "Bearer slot-t2", "Bearer slot-t2"),
        check("refused tokens", len(refused2), not refused2, "== 0"),
        check("its user requests failed", failed2, failed2 == 0, "== 0"),
    ]


@scenario()
def provider_slots_last_resort(stand, xray, work, name):
    """The lease of a reserve slot ends while the backend is down. The slot
    gets no probe and no traffic while anything else works; once the primary
    and the outbound of the config are banned, the balancer fails over to it
    as the last resort instead of going to the fallback."""
    slots = SLOTS[:2]
    initial = slot_set(slots, {"s1": 1, "s2": 2}, 300, "v1")
    initial["expiresAt"]["s2"] = rfc3339(time.time() + 12)
    prov = bench.provider_config(["direct"], limits={"minInterval": "1s", "exhaustedMinInterval": "1s",
                                                     "maxRefreshesPerHour": 0, "backoff": {"initialDelay": "2s", "maxDelay": "4s"}})
    prov["initialAnswer"] = initial
    cfg = bench.client_config(BANNED[:1], {"a1": 5}, providers=[prov])
    banned = ",".join(str(address_port(k, i)) for k, i in [("reality", 0), ("reality", 1)])
    m = Marks()
    bench.provider_mode("status:500")
    with bench.Client(xray, cfg, work, name) as c:
        start = time.time()
        tr = bench.Traffic(rate=2, big=0.5)
        run_timeline(tr, [(20, m.at("ban", lambda: bench.chaos(banned, "blackhole")))], 55)
        st = c.status()
    ended = [e for e in bench.events_of(c.events, "provider_expired") if e["fields"]["outbound"] == "p-s2"]
    t_end = ended[0]["ts"] / 1000 if ended else None
    early = [e for e in bench.probes(c.events, 0, m["ban"]) if e["fields"]["outbound"] == "p-s2"]
    moved = [e for e in bench.switches(c.events, m["ban"]) if e["fields"]["from"] == "p-s1"]
    t_moved = moved[0]["ts"] / 1000 if moved else None
    tried = [e["fields"]["outbound"] for e in bench.probes(c.events, m["ban"], t_moved)] if t_moved else []
    exhausted = bench.events_of(c.events, "exhausted", m["ban"])
    after = [r for r in tr.records if t_moved and r[0] >= t_moved + 2]
    failed = sum(1 for r in after if not r[1])
    active = (st.get("balancers") or [{}])[0].get("active")
    path = next((p for p in st.get("paths", []) if p.get("outbound") == "p-s2"), {})
    return [
        check("lease of the reserve ended, s", t_end and round(t_end - start, 1),
              t_end is not None and t_end < m["ban"] and ended[0]["fields"]["was_active"] is False
              and ended[0]["fields"]["renewal"] in ("failed", "pending"), "before the ban, not active"),
        check("probes of the expired reserve before the ban", len(early), not early, "== 0"),
        check("failover to the expired reserve (to, reason)", moved and (moved[0]["fields"]["to"], moved[0]["fields"]["reason"]),
              bool(moved) and moved[0]["fields"]["to"] == "p-s2" and moved[0]["fields"]["reason"] == "failover",
              "(p-s2, failover)"),
        check("probed before it", tried, bool(tried) and tried[-1] == "p-s2" and "a1" in tried, "a1 first, p-s2 last"),
        check("exhausted", len(exhausted), not exhausted, "== 0"),
        check("user requests on it", len(after), len(after) >= 20, ">= 20"),
        check("of them failed", failed, failed == 0, "== 0"),
        check("still on it, expired (active, expired)", (active, path.get("is_expired")),
              active == "p-s2" and path.get("is_expired") is True, "(p-s2, True)"),
    ]


# ---- AmneziaWG: sleep and wake ------------------------------------------------
#
# No scenario for low memory: SetMemoryLow has no API call or command, only the
# Go call a host makes.

AWG_SLEEP = 10  # sleep.afterLastUse: its minimum
AWG_MEMBERS = [("v1", "reality", 0), ("w1", "awg", 0)]
# Wakes of awg_wake_by_probe, and the delay of the datagrams of w1 in ms on those that lose nothing.
AWG_WAKES = int(os.environ.get("BENCH_AWG_WAKES", "6"))
AWG_DELAY = os.environ.get("BENCH_AWG_DELAY", "")


def awg_config():
    """v1 is cheaper; w1 is the AmneziaWG outbound, with a keepalive of 2-3 s. The balancer
    starts pinned to w1, so traffic goes through it first; once the host releases the pin, it
    returns to v1 after one probe a few seconds later."""
    return bench.client_config(
        AWG_MEMBERS, {"v1": 1, "w1": 3},
        health={"sleep": {"afterLastUse": f"{AWG_SLEEP}s"},
                # A probe the host asks for must reach w1, not be answered by its last success.
                "schedule": {"hostProbes": {"reuseWindow": "1s"}}},
        strategy={"startPinnedOutbound": "w1", "minHoldTime": "1s",
                  "returnToCheaper": {"interval": "3s", "hasRandomPhase": False, "probesToReturn": 1, "probeInterval": "1s"}})


def path_status(c, tag):
    return next((p for p in c.status()["paths"] if p["outbound"] == tag), {})


def w1_sleeps(c, since):
    return [e for e in bench.events_of(c.events, "outbound_sleep", since) if e["fields"].get("outbound") == "w1"]


def awg_to_sleep(c, tr, m):
    """Traffic through w1 under the start pin, then the release: the balancer returns to v1
    and w1 idles until it sleeps. Marks start, release, away (the switch off w1) and asleep;
    returns those two events, None for one that did not come."""
    m["start"] = time.time()
    tr.start()
    time.sleep(8)
    m["release"] = time.time()
    c.api("release", "auto")
    away = wait_for(lambda: [e for e in bench.switches(c.events, m["release"]) if e["fields"]["from"] == "w1"], 15)
    if away:
        m["away"] = away[0]["ts"] / 1000
    slept = wait_for(lambda: w1_sleeps(c, m["release"]), AWG_SLEEP + 20)
    if slept:
        m["asleep"] = slept[0]["ts"] / 1000
    return away and away[0], slept and slept[0]


def requests_ok(tr, since, until):
    rs = [r for r in tr.records if since <= r[0] < until]
    return sum(1 for r in rs if r[1]), len(rs)


def awg_away_checks(c, tr, m, away):
    """Traffic went through w1 under the pin, and the release moved the balancer to v1."""
    ok, n = requests_ok(tr, m["start"], m["release"])
    checks = [check("requests answered under the pin to w1", f"{ok}/{n}", n >= 10 and ok == n, "all, >= 10")]
    routed = c.routed(m["start"], m["release"])
    if routed is not None:
        checks.append(check("user connections under the pin by outbound", routed,
                            set(routed) == {"w1"} and routed["w1"] >= 10, "w1 only, >= 10"))
    took = away and round(away["ts"] / 1000 - m["release"], 1)
    checks.append(check("switch off w1 after the release, s",
                        away and f"{away['fields']['to']} ({away['fields']['reason']}) {took}",
                        bool(away) and away["fields"]["to"] == "v1" and away["fields"]["reason"] == "return"
                        and took <= 10, "v1 (return) <= 10"))
    return checks


@scenario()
def awg_sleep(stand, xray, work, name):
    """Traffic goes through the AmneziaWG outbound w1 under a start pin; the host releases it
    and the balancer returns to the cheaper v1. Unchosen and idle, w1 sleeps sleep.afterLastUse
    later: outbound_sleep, asleep in Status, and its keepalives stop at the UDP proxy."""
    m = Marks()
    with bench.Client(xray, awg_config(), work, name) as c, bench.ConnSampler() as cs:
        tr = bench.Traffic(rate=2, big=0.5, timeout=5)
        away, slept = awg_to_sleep(c, tr, m)
        status = path_status(c, "w1") if slept else {}
        # Room for three keepalives, had they gone on.
        time.sleep(10)
        m["end"] = time.time()
        tr.stop()
    tr.save(os.path.join(work, f"{name}-traffic.json"))
    checks = awg_away_checks(c, tr, m, away)
    f = slept["fields"] if slept else {}
    idle = f.get("idle_ms")
    checks.append(check("outbound_sleep of w1: reason, idle ms", slept and f"{f.get('reason')} {idle}",
                        bool(slept) and f.get("reason") == "idle"
                        and AWG_SLEEP * 1000 <= idle <= (AWG_SLEEP + 3) * 1000,
                        f"idle, {AWG_SLEEP * 1000} .. {(AWG_SLEEP + 3) * 1000}"))
    if away and slept:
        gap = round(m["asleep"] - m["away"], 1)
        checks.append(check("sleep after the switch off w1, s", gap, AWG_SLEEP <= gap <= AWG_SLEEP + 5,
                            f"{AWG_SLEEP} .. {AWG_SLEEP + 5}"))
        # Past the switch the connections through w1 are over: what the proxy sees is keepalives.
        kept = cs.packets(bench.AWG_ADDRESS, m["away"] + 2, m["asleep"] - 0.5)
        checks.append(check("keepalives of the idle w1 before it slept, datagrams up/down", kept,
                            kept[0] is not None and kept[0] >= 2, "up >= 2"))
        after = cs.packets(bench.AWG_ADDRESS, m["asleep"] + 1.5, m["end"])
        checks.append(check(f"datagrams at the proxy in {round(m['end'] - m['asleep'] - 1.5, 1)} s after the "
                            "sleep, up/down", after, after == (0, 0), "(0, 0)"))
        routed = c.routed(m["away"] + 3)
        if routed is not None:
            checks.append(check("user connections to w1 after the switch off it", routed.get("w1", 0),
                                routed.get("w1", 0) == 0, "== 0"))
    checks.append(check("Status of w1 after the sleep", f"asleep={status.get('is_asleep')}",
                        status.get("is_asleep") is True, "asleep=True"))
    ok, n = requests_ok(tr, m["start"], m["end"])
    checks.append(check("requests answered", f"{ok}/{n}", ok == n, "all"))
    checks.append(leak_check(c))
    return checks


@scenario()
def awg_wake(stand, xray, work, name):
    """w1 sleeps as in awg_sleep. A probe the host asks for wakes it: the probe passes through
    a new tunnel, and w1 sleeps again sleep.afterLastUse later. Then the host pins the balancer to the
    sleeping w1. The first thing through the new tunnel is one UDP datagram, which nothing
    retries: it must come back. User traffic then flows through w1."""
    m = Marks()
    seen = {}
    with bench.Client(xray, awg_config(), work, name) as c, bench.ConnSampler() as cs:
        tr = bench.Traffic(rate=2, big=0.5, timeout=5)
        away, slept = awg_to_sleep(c, tr, m)
        again = None
        if slept:
            m["probe"] = time.time()
            seen["probe"] = c.api("-t", "15", "probe", "auto", "w1", timeout=20)
            m["probed"] = time.time()
            seen["after_probe"] = path_status(c, "w1")
            again = wait_for(lambda: w1_sleeps(c, m["probed"]), AWG_SLEEP + 20)
        if again:
            m["asleep2"] = again[0]["ts"] / 1000
            seen["before_pin"] = path_status(c, "w1")
            # Requests wait, so that the datagram is the first thing through the tunnel.
            tr.paused.set()
            time.sleep(1)
            m["pin"] = time.time()
            seen["pin"] = c.api("pin", "auto", "w1")
            seen["udp"] = bench.udp_echo(bench.TUNNEL_TARGET, bench.UDP_ECHO_PORT, os.urandom(32))
            m["echoed"] = time.time()
            seen["after_pin"] = path_status(c, "w1")
            tr.paused.clear()
            m["resume"] = time.time()
            time.sleep(8)
        m["end"] = time.time()
        tr.stop()
    tr.save(os.path.join(work, f"{name}-traffic.json"))
    checks = awg_away_checks(c, tr, m, away)
    checks.append(check("w1 asleep before the probe", bool(slept), bool(slept), "True"))
    if not slept:
        return checks + [leak_check(c)]

    request = seen["probe"].get("request") or {}
    got = {o["outbound"]: f"{o['result']}/{o.get('source', '')}" for o in request.get("outbounds", [])}
    checks.append(check("probe of the host for w1", f"{got.get('w1')} after {round(m['probed'] - m['probe'], 1)} s",
                        request.get("is_done") is True and got.get("w1") == "ok/probe", "ok/probe"))
    sockets = cs.between(bench.AWG_ADDRESS, m["probe"] - 0.5, m["probed"] + 0.5)
    up, down = cs.packets(bench.AWG_ADDRESS, m["probe"] - 0.5, m["probed"] + 0.5)
    checks.append(check("the probe at the proxy: new sockets, datagrams up/down", (sockets, up, down),
                        sockets == 1 and up > 0 and down > 0, "1 new, both ways"))
    checks.append(check("Status of w1 after the probe", f"asleep={seen['after_probe'].get('is_asleep')}",
                        not seen["after_probe"].get("is_asleep"), "awake"))
    f = again[0]["fields"] if again else {}
    checks.append(check("outbound_sleep of w1 again after the probe: reason, idle ms",
                        again and f"{f.get('reason')} {f.get('idle_ms')}",
                        bool(again) and f.get("reason") == "idle" and f.get("idle_ms", 0) >= AWG_SLEEP * 1000,
                        f"idle, >= {AWG_SLEEP * 1000}"))
    if not again:
        return checks + [leak_check(c)]

    checks.append(check("pin to the sleeping w1", f"{seen['pin']}, asleep={seen['before_pin'].get('is_asleep')}",
                        seen["pin"] == {"ok": True} and seen["before_pin"].get("is_asleep") is True,
                        "ok, asleep=True"))
    early = [e for e in c.events if m["pin"] <= e["ts"] / 1000 <= m["echoed"] and e["type"] == "probe_start"
             and e["fields"].get("outbound") == "w1"]
    checks.append(check("probes of w1 before the datagram", len(early), not early, "== 0"))
    took = seen["udp"]
    checks.append(check("the one datagram through the woken w1 came back, s", took and round(took, 3),
                        took is not None, "back, no retry"))
    sockets = cs.between(bench.AWG_ADDRESS, m["pin"] - 0.5, m["echoed"] + 0.5)
    checks.append(check("new sockets at the proxy for the wake", sockets, sockets == 1, "== 1"))
    checks.append(check("Status of w1 after the datagram", f"asleep={seen['after_pin'].get('is_asleep')}",
                        not seen["after_pin"].get("is_asleep"), "awake"))
    ok, n = requests_ok(tr, m["resume"], m["end"])
    checks.append(check("requests answered after the pin", f"{ok}/{n}", n >= 10 and ok == n, "all, >= 10"))
    routed = c.routed(m["resume"])
    if routed is not None:
        checks.append(check("user connections after the pin by outbound", routed,
                            set(routed) == {"w1"} and routed["w1"] >= 10, "w1 only, >= 10"))
    checks.append(leak_check(c))
    return checks


def first_datagram(cs, port, t1, t2):
    """When the client first sent anything to the port between t1 and t2, by the sampler."""
    rows = [(t, s[port]) for t, s in cs.samples if t1 <= t <= t2 and port in s]
    return next((t for (t, s), (_, p) in zip(rows[1:], rows) if s[3] > p[3]), None)


@scenario()
def awg_wake_by_probe(stand, xray, work, name):
    """w1 sleeps as in awg_sleep, and a probe the host asks for wakes it, AWG_WAKES times in a
    row. Every other time the address of w1 loses the first datagram back to the new socket:
    the handshake response of the new device, which the device alone repeats only
    AWG_REKEY_TIMEOUT later, after progressTimeout. Its tunnel asks for the handshake again a
    second after the first, and every probe passes. A phone on Wi-Fi lost such a response,
    and the probe that woke w1 for a failover put it in quarantine."""
    m = Marks()
    wakes = []
    cfg = awg_config()
    # A host probe of w1 every sleep.afterLastUse: room in the budgets of its server name and of the host.
    cfg["healthObservatory"]["schedule"] |= {"probesPerServerNamePerMinute": 30, "probesPerMinute": 60,
                                             "hostProbes": {"reuseWindow": "1s", "probesPerMinute": 30}}
    with bench.Client(xray, cfg, work, name) as c, bench.ConnSampler(period=0.1) as cs:
        tr = bench.Traffic(rate=2, big=0.5, timeout=5)
        away, slept = awg_to_sleep(c, tr, m)
        tr.stop()
        since = m.get("asleep", 0)
        for i in range(AWG_WAKES if slept else 0):
            if not wait_for(lambda: w1_sleeps(c, since), AWG_SLEEP + 20):
                break
            lose = i % 2 == 0
            clean = f"delay:{AWG_DELAY}" if AWG_DELAY else "pass"
            bench.chaos(bench.AWG_ADDRESS, "lose_reply:1" if lose else clean)
            time.sleep(0.5)
            t0 = time.time()
            r = c.api("-t", "15", "probe", "auto", "w1", timeout=20)
            t1 = time.time()
            since = t0
            got = next((o for o in (r.get("request") or {}).get("outbounds", []) if o["outbound"] == "w1"), {})
            wakes.append({"lose": lose, "t0": t0, "t1": t1, "result": f"{got.get('result')}/{got.get('source')}",
                          "error": got.get("error", "")})
        bench.chaos(bench.AWG_ADDRESS, "pass")
        time.sleep(0.5)
        m["end"] = time.time()
    for w in wakes:
        w["sockets"] = cs.between(bench.AWG_ADDRESS, w["t0"] - 0.2, w["t1"] + 0.2)
        w["packets"] = cs.packets(bench.AWG_ADDRESS, w["t0"] - 0.2, w["t1"] + 0.2)
        w["first"] = first_datagram(cs, bench.AWG_ADDRESS, w["t0"] - 0.2, w["t1"] + 0.2)
    with open(os.path.join(work, f"{name}-wakes.json"), "w") as f:
        json.dump(wakes, f, indent=1)
    checks = [check("w1 asleep before the probes", bool(slept), bool(slept), "True")]
    hs = bench.AWG_HANDSHAKE_DATAGRAMS
    for lose, what in ((True, "first response lost"), (False, "nothing lost")):
        ws = [w for w in wakes if w["lose"] == lose]
        ok = [w for w in ws if w["result"] == "ok/probe"]
        want = (AWG_WAKES + lose) // 2
        took = max((round(w["t1"] - w["first"], 2) for w in ok if w["first"]), default=None)
        checks.append(check(f"wakes by a probe, {what}: passed, slowest s from the first datagram",
                            f"{len(ok)}/{len(ws)}, {took}", len(ws) == want and len(ok) == want,
                            f"{want}/{want}"))
        for w in ws:
            if w not in ok:
                checks.append(check(f"failed wake, {what}: result, error, s", f"{w['result']} {w['error']} "
                                    f"{round(w['t1'] - w['t0'], 2)}", False, "ok/probe"))
        socks = sorted({w["sockets"] for w in ws})
        checks.append(check(f"new sockets at the proxy for each wake, {what}", socks, socks == [1], "[1]"))
    # Without the response the device has no session to send anything else in: what goes up
    # beyond the first handshake starts with the second.
    lossy = [w for w in wakes if w["lose"] and w["packets"][0] is not None]
    twice = [w for w in lossy if w["packets"][0] >= 2 * hs]
    checks.append(check("wakes that lost the response and sent a second handshake", f"{len(twice)}/{len(lossy)}",
                        lossy and len(twice) == len(lossy), "all"))
    checks.append(leak_check(c))
    return checks


# ---- AmneziaWG: failover by traffic alone ---------------------------------------
#
# w1 and w2 are peers of the one AmneziaWG server, each behind a UDP address of its
# own, so that one fails alone, as one of two servers would. Each connection through a
# tunnel is a dial of its server for the observatory, which ends when the server answers
# anything; a server that answers nothing within dialTimeout is a dial that timed out.

AWG_PAIR = [("w1", "awg", 0), ("w2", "awg", 1)]
AWG_PAIR_COSTS = {"w1": 1, "w2": 3}
W1 = bench.AWG_ADDRESSES[0]


@scenario()
def awg_failover_blackhole(stand, xray, work, name):
    """The address of w1 drops every datagram both ways. The connections of users wait for
    the server in vain: dials that time out, strong evidence that rules w1 out without a
    probe. Left by the balancer, w1 sleeps at once, and its device stops sending to the
    dead address."""
    return failover(stand, xray, work, name, W1, "blackhole", 10, members=AWG_PAIR, costs=AWG_PAIR_COSTS,
                    expect_to="w2", tag="w1", kinds=("dial_timeout",), immediate=True, redials=True)


# passive.stallTimeout of the client: its default.
STALL = 8


class HeldConnection:
    """A keep-alive connection through the client that got its answer and then waits, quiet,
    as the push and DNS connections of a phone do. Marks when the client ends it."""

    def __init__(self, host):
        self.host = host
        self.sock = None
        self.answered = False
        self.ended = None

    def open(self):
        s = bench.open_stream(self.host, 10)
        if s is None:
            return
        s.sendall(f"GET /204 HTTP/1.1\r\nHost: {self.host}\r\n\r\n".encode())
        head, _ = bench.read_answer(s, time.time() + 10)
        self.answered = head.startswith(b"HTTP/1.1 204")
        s.settimeout(None)
        self.sock = s
        threading.Thread(target=self._wait, daemon=True).start()

    def _wait(self):
        try:
            self.sock.recv(1)
        except OSError:
            pass
        self.ended = time.time()

    def close(self):
        if self.sock is not None:
            self.sock.close()


@scenario()
def awg_failover_blackhole_held(stand, xray, work, name):
    """As awg_failover_blackhole, with a keep-alive connection through w1, opened before the
    fault, that got its answer and waits. It received something, so the decision keeps it,
    and it keeps the tunnel of w1 awake: its device repeats the handshake to the dead address.
    stallTimeout after a decision on strong evidence, the core ends it as silent, its client
    sees the end, and w1 sleeps with reason down: its device stops sending to the dead
    address."""
    held = HeldConnection(bench.TARGETS[3])

    def more(c, cs, sw, m, end):
        checks = [check("held connection answered through w1 before the fault", held.answered, held.answered,
                        "True")]
        if not sw:
            return checks
        t_sw = sw[0]["ts"] / 1000
        down = next((e for e in bench.events_of(c.events, "path_state", m["fault"])
                     if e["fields"].get("outbound") == "w1" and e["fields"].get("state") == "down"), None)
        silent = [e for e in bench.events_of(c.events, "pending_closed", m["fault"])
                  if e["fields"].get("outbound") == "w1" and e["fields"].get("is_silent") is True]
        at = silent and silent[0]["ts"] / 1000
        decided = silent and down and round(at - down["ts"] / 1000, 1)
        checks.append(check("silent close of w1: connections, s after the switch (after the decision)",
                            silent and f"{silent[0]['fields'].get('connections')}, {round(at - t_sw, 1)} ({decided})",
                            bool(silent) and silent[0]["fields"].get("connections", 0) >= 1 and bool(down)
                            and STALL - 0.5 <= decided <= STALL + 2, f">= 1, {STALL} .. {STALL + 2} after the decision"))
        ended = held.ended and silent and round(held.ended - at, 1)
        checks.append(check("held connection ended at the client after the silent close, s", ended,
                            bool(silent) and held.ended is not None and -0.5 <= ended <= 2, "0 .. 2"))
        slept = [e for e in bench.events_of(c.events, "outbound_sleep", t_sw) if e["fields"].get("outbound") == "w1"]
        asleep = slept and slept[0]["ts"] / 1000
        checks.append(check("w1 asleep after the switch: reason, s", slept and f"{slept[0]['fields'].get('reason')} "
                            f"{round(asleep - t_sw, 1)}", bool(slept) and slept[0]["fields"].get("reason") == "down"
                            and asleep - t_sw <= STALL + 5, f"down, <= {STALL + 5}"))
        if not (slept and silent):
            return checks
        checks.append(check("w1 held awake until the silent close, s from it to the sleep", round(asleep - at, 2),
                            -0.05 <= asleep - at <= 2, "0 .. 2"))
        up, _ = cs.packets(W1, t_sw, asleep)
        checks.append(check("datagrams to the dead address from the switch to the sleep", up, True, "info"))
        up, _ = cs.packets(W1, asleep + 1.5, end)
        woken = [e for e in bench.probes(c.events, asleep) if e["fields"].get("outbound") == "w1"]
        limit = bench.AWG_HANDSHAKE_DATAGRAMS * len(woken) * wake_handshakes_within(bench.PROBE_TIMEOUT)
        checks.append(check(f"datagrams to the dead address in {round(end - asleep - 1.5, 1)} s after the sleep",
                            up, up is not None and up <= limit, f"<= {limit}"))
        return checks

    try:
        return failover(stand, xray, work, name, W1, "blackhole", 10, members=AWG_PAIR, costs=AWG_PAIR_COSTS,
                        expect_to="w2", tag="w1", kinds=("dial_timeout",), immediate=True, redials=True,
                        duration=45, actions=[(4, held.open)], more=more)
    finally:
        held.close()


@scenario()
def awg_probe_down_held(stand, xray, work, name):
    """Traffic and a keep-alive connection go through the AmneziaWG outbound w1 under a start
    pin; the connection gets its answer and waits, quiet, and the host releases the pin, so the
    balancer returns to the cheaper v1 and the connection alone holds w1 awake past
    sleep.afterLastUse. The address of w1 then drops every datagram both ways, and a probe the
    host asks for finds w1 dead: a down on a failed probe alone, with no evidence from traffic
    and no switch. stallTimeout after the decision the core ends the connection as silent, its
    client sees the end, w1 sleeps with reason down, and its device stops sending to the dead
    address. On a phone, push connections opened through w1 before such a down kept its device
    handshaking with the dead address for 102 s."""
    m = Marks()
    seen = {}
    held = HeldConnection(bench.TARGETS[3])
    try:
        with bench.Client(xray, awg_config(), work, name) as c, bench.ConnSampler() as cs:
            tr = bench.Traffic(rate=2, big=0.5, timeout=5)
            m["start"] = time.time()
            tr.start()
            time.sleep(1)
            held.open()
            time.sleep(7)
            m["release"] = time.time()
            c.api("release", "auto")
            away = wait_for(lambda: [e for e in bench.switches(c.events, m["release"])
                                     if e["fields"]["from"] == "w1"], 15)
            m["away"] = away[0]["ts"] / 1000 if away else time.time()
            # Unchosen, w1 would sleep sleep.afterLastUse after the switch; the connection holds it.
            time.sleep(max(0.0, m["away"] + AWG_SLEEP + 3 - time.time()))
            seen["held"] = path_status(c, "w1")
            m["fault"] = time.time()
            bench.chaos(bench.AWG_ADDRESS, "blackhole")
            time.sleep(1)
            m["probe"] = time.time()
            seen["probe"] = c.api("-t", "15", "probe", "auto", "w1", timeout=20)
            m["probed"] = time.time()
            slept = wait_for(lambda: w1_sleeps(c, m["fault"]), STALL + 15)
            if slept:
                m["asleep"] = slept[0]["ts"] / 1000
            # Room for two handshakes of a device left awake.
            time.sleep(2 * bench.AWG_REKEY_TIMEOUT)
            m["end"] = time.time()
            tr.stop()
    finally:
        held.close()
    tr.save(os.path.join(work, f"{name}-traffic.json"))
    checks = awg_away_checks(c, tr, m, away and away[0])
    checks.append(check("held connection answered through w1 before the fault", held.answered, held.answered,
                        "True"))
    early = [e for e in w1_sleeps(c, m["release"]) if e["ts"] / 1000 < m["fault"]]
    checks.append(check(f"w1 held awake {round(m['fault'] - m['away'], 1)} s after the switch off it: sleeps, asleep",
                        f"{len(early)}, {bool(seen['held'].get('is_asleep'))}",
                        not early and seen["held"] and not seen["held"].get("is_asleep"), "0, False"))
    request = seen["probe"].get("request") or {}
    got = {o["outbound"]: f"{o['result']}/{o.get('source', '')}" for o in request.get("outbounds", [])}
    checks.append(check("probe of the host for w1", f"{got.get('w1')} after {round(m['probed'] - m['probe'], 1)} s",
                        got.get("w1") == "failed/probe", "failed/probe"))
    down = next((e for e in bench.events_of(c.events, "path_state", m["fault"])
                 if e["fields"].get("outbound") == "w1" and e["fields"].get("state") == "down"), None)
    suspects = [e for e in bench.events_of(c.events, "path_state", m["fault"])
                if e["fields"].get("outbound") == "w1" and e["fields"].get("state") == "suspect"]
    checks.append(check("w1 down: reason, suspicions from traffic", down and f"{down['fields'].get('reason')}, "
                        f"{len(suspects)}", bool(down) and down["fields"].get("reason") == "host_probe"
                        and not suspects, "host_probe, 0"))
    sw = bench.switches(c.events, m["fault"])
    checks.append(check("switches after the fault", len(sw), not sw, "== 0"))
    if not down:
        return checks + [leak_check(c)]
    t_down = down["ts"] / 1000
    silent = [e for e in bench.events_of(c.events, "pending_closed", m["fault"])
              if e["fields"].get("outbound") == "w1" and e["fields"].get("is_silent") is True]
    at = silent and silent[0]["ts"] / 1000
    decided = silent and round(at - t_down, 1)
    checks.append(check("silent close of w1: connections, reason, s after the decision",
                        silent and f"{silent[0]['fields'].get('connections')}, {silent[0]['fields'].get('reason')}, "
                        f"{decided}", bool(silent) and silent[0]["fields"].get("connections", 0) >= 1
                        and silent[0]["fields"].get("reason") == "host_probe" and STALL - 0.5 <= decided <= STALL + 2,
                        f">= 1, host_probe, {STALL} .. {STALL + 2}"))
    ended = held.ended and silent and round(held.ended - at, 1)
    checks.append(check("held connection ended at the client after the silent close, s", ended,
                        bool(silent) and held.ended is not None and -0.5 <= ended <= 2, "0 .. 2"))
    asleep = m.get("asleep")
    f = slept[0]["fields"] if slept else {}
    checks.append(check("outbound_sleep of w1: reason, s after the decision",
                        slept and f"{f.get('reason')} {round(asleep - t_down, 1)}",
                        bool(slept) and f.get("reason") == "down" and asleep - t_down <= STALL + 3,
                        f"down, <= {STALL + 3}"))
    if not (slept and silent):
        up, _ = cs.packets(bench.AWG_ADDRESS, t_down, m["end"])
        checks.append(check(f"datagrams to the dead address in {round(m['end'] - t_down, 1)} s after the decision",
                            up, True, "info"))
        return checks + [leak_check(c)]
    checks.append(check("w1 held awake until the silent close, s from it to the sleep", round(asleep - at, 2),
                        -0.05 <= asleep - at <= 2, "0 .. 2"))
    up, _ = cs.packets(bench.AWG_ADDRESS, t_down, asleep)
    checks.append(check("datagrams to the dead address from the decision to the sleep", up, True, "info"))
    up, _ = cs.packets(bench.AWG_ADDRESS, asleep + 1.5, m["end"])
    woken = [e for e in bench.probes(c.events, asleep) if e["fields"].get("outbound") == "w1"]
    limit = bench.AWG_HANDSHAKE_DATAGRAMS * len(woken) * wake_handshakes_within(bench.PROBE_TIMEOUT)
    checks.append(check(f"datagrams to the dead address in {round(m['end'] - asleep - 1.5, 1)} s after the sleep",
                        up, up is not None and up <= limit, f"<= {limit}"))
    ok, n = requests_ok(tr, m["start"], m["end"])
    checks.append(check("requests answered", f"{ok}/{n}", ok == n, "all"))
    checks.append(leak_check(c))
    return checks


@scenario()
def awg_probe_down_hanging(stand, xray, work, name):
    """As awg_probe_down_held, with a connection that waits inside the dead tunnel instead of one
    that got its answer. Traffic goes through the AmneziaWG outbound w1 under a start pin and then
    pauses, the address of w1 drops every datagram both ways, and a connection opened through w1
    to the address of the target, not a name, waits for the answer to its SYN, which never comes,
    while its client waits for its answer: inside the tunnel its dial waits for the handshake
    timeout of its policy, 60 s by default, where one to a name gives up after its lookups there,
    20 s. 13 s later, past the 11 s after which the tunnel asks the server for no more handshakes
    for it, the host releases the pin, the balancer returns to the cheaper v1, and a probe the host
    asks for finds w1 dead: a down on a failed probe alone. The core ends the connection at the
    decision as one that has received nothing, its client sees the end, w1 sleeps with reason down
    at once, and its device stops sending to the dead address. On a phone such a connection
    outlived the decision and the silent close until the handshake timeout of its policy, 60 s
    after it started, and kept the device handshaking with the dead address for 49 s after the
    down."""
    m = Marks()
    seen = {}
    # The check of the phone went to an address as well.
    hanging = HeldConnection(bench.HOST)
    try:
        with bench.Client(xray, awg_config(), work, name) as c, bench.ConnSampler() as cs:
            tr = bench.Traffic(rate=2, big=0.5, timeout=5)
            m["start"] = time.time()
            tr.start()
            time.sleep(7)
            # Users go quiet until the balancer leaves w1: their dials into the dead tunnel would
            # rule it out by traffic first.
            tr.paused.set()
            time.sleep(tr.timeout + 0.5)
            m["fault"] = time.time()
            bench.chaos(bench.AWG_ADDRESS, "blackhole")
            m["hanging"] = time.time()
            # Waits up to 10 s for an answer that never comes, and then for the end.
            hanging.open()
            time.sleep(max(0.0, m["hanging"] + 13 - time.time()))
            seen["waiting"] = hanging.ended is None
            m["release"] = time.time()
            c.api("release", "auto")
            away = wait_for(lambda: [e for e in bench.switches(c.events, m["release"])
                                     if e["fields"]["from"] == "w1"], 15)
            m["away"] = away[0]["ts"] / 1000 if away else time.time()
            tr.paused.clear()
            m["probe"] = time.time()
            seen["probe"] = c.api("-t", "15", "probe", "auto", "w1", timeout=20)
            m["probed"] = time.time()
            slept = wait_for(lambda: w1_sleeps(c, m["fault"]), STALL + 15)
            if slept:
                m["asleep"] = slept[0]["ts"] / 1000
            # Room for two handshakes of a device left awake.
            time.sleep(2 * bench.AWG_REKEY_TIMEOUT)
            m["end"] = time.time()
            tr.stop()
    finally:
        hanging.close()
    tr.save(os.path.join(work, f"{name}-traffic.json"))
    checks = awg_away_checks(c, tr, m, away and away[0])
    checks.append(check("connection opened after the fault: answered, waiting at the release",
                        f"{hanging.answered}, {seen['waiting']}", not hanging.answered and seen["waiting"],
                        "False, True"))
    request = seen["probe"].get("request") or {}
    got = {o["outbound"]: f"{o['result']}/{o.get('source', '')}" for o in request.get("outbounds", [])}
    checks.append(check("probe of the host for w1", f"{got.get('w1')} after {round(m['probed'] - m['probe'], 1)} s",
                        got.get("w1") == "failed/probe", "failed/probe"))
    down = next((e for e in bench.events_of(c.events, "path_state", m["fault"])
                 if e["fields"].get("outbound") == "w1" and e["fields"].get("state") == "down"), None)
    checks.append(check("w1 down: reason, s after the connection opened",
                        down and f"{down['fields'].get('reason')}, {round(down['ts'] / 1000 - m['hanging'], 1)}",
                        bool(down) and down["fields"].get("reason") == "host_probe"
                        and down["ts"] / 1000 - m["hanging"] > 12, "host_probe, > 12"))
    if not down:
        return checks + [leak_check(c)]
    t_down = down["ts"] / 1000
    closed = [e for e in bench.events_of(c.events, "pending_closed", m["fault"])
              if e["fields"].get("outbound") == "w1" and not e["fields"].get("is_silent")]
    at = closed and closed[0]["ts"] / 1000
    checks.append(check("pending close of w1 at the decision: connections, reason, s after it",
                        closed and f"{closed[0]['fields'].get('connections')}, {closed[0]['fields'].get('reason')}, "
                        f"{round(at - t_down, 1)}", bool(closed) and closed[0]["fields"].get("connections", 0) >= 1
                        and closed[0]["fields"].get("reason") == "host_probe" and at - t_down <= 0.5,
                        ">= 1, host_probe, <= 0.5"))
    ended = hanging.ended and round(hanging.ended - t_down, 1)
    checks.append(check("waiting connection ended at the client, s after the decision", ended,
                        hanging.ended is not None and -0.5 <= ended <= 2, "0 .. 2"))
    asleep = m.get("asleep")
    f = slept[0]["fields"] if slept else {}
    checks.append(check("outbound_sleep of w1: reason, s after the decision",
                        slept and f"{f.get('reason')} {round(asleep - t_down, 1)}",
                        bool(slept) and f.get("reason") == "down" and asleep - t_down <= 3, "down, <= 3"))
    if not slept:
        up, _ = cs.packets(bench.AWG_ADDRESS, t_down, m["end"])
        checks.append(check(f"datagrams to the dead address in {round(m['end'] - t_down, 1)} s after the decision",
                            up, True, "info"))
        return checks + [leak_check(c)]
    up, _ = cs.packets(bench.AWG_ADDRESS, t_down, asleep)
    checks.append(check("datagrams to the dead address from the decision to the sleep", up, True, "info"))
    up, _ = cs.packets(bench.AWG_ADDRESS, asleep + 1.5, m["end"])
    woken = [e for e in bench.probes(c.events, asleep) if e["fields"].get("outbound") == "w1"]
    limit = bench.AWG_HANDSHAKE_DATAGRAMS * len(woken) * wake_handshakes_within(bench.PROBE_TIMEOUT)
    checks.append(check(f"datagrams to the dead address in {round(m['end'] - asleep - 1.5, 1)} s after the sleep",
                        up, up is not None and up <= limit, f"<= {limit}"))
    ok, n = requests_ok(tr, m["start"], m["end"])
    checks.append(check("requests answered", f"{ok}/{n}", ok == n, "all"))
    checks.append(leak_check(c))
    return checks


@scenario()
def awg_short_glitch(stand, xray, work, name):
    """Datagrams of w1 lost both ways for 3 s. Connections that began meanwhile wait for the
    server, and the first answer after the glitch, to any of them, ends every wait well
    within dialTimeout."""
    return glitch(stand, xray, work, name, "blackhole", members=AWG_PAIR, costs=AWG_PAIR_COSTS, port=W1)


def cut_after(port, more, marks):
    """A fault for a UDP address that one socket of the client uses, as a censor that lets
    the first datagrams of a flow through and drops the rest: from the call on, the address
    forwards more datagrams each way, then none. drop_after counts from the first datagram
    of the socket, and the address counts those of earlier clients too, so its counters are
    taken before the client starts. Marks the cut in marks."""
    base = bench.chaos_stats()[port]

    def cut():
        marks["cut"] = time.time()
        now = bench.chaos_stats()[port]
        bench.chaos(port, f"drop_after:{max(now[3] - base[3], now[4] - base[4]) + more}")
    return cut


@scenario()
def awg_failover_freeze(stand, xray, work, name):
    """The address of w1 cuts its flow under steady traffic: 20 more datagrams each way, then
    none. Requests caught in the middle stall, weak evidence that waits for stallTimeout; the
    connections that begin after the cut wait for the server in vain, and their dials time
    out first: strong evidence, as for a blackhole."""
    m = Marks()
    tr = bench.Traffic(rate=2, big=0.5)
    checks = failover(stand, xray, work, name, W1, cut_after(W1, 20, m), 10, members=AWG_PAIR,
                      costs=AWG_PAIR_COSTS, traffic=tr, expect_to="w2", tag="w1", kinds=("dial_timeout",),
                      immediate=True, redials=True)
    # The cut comes some datagrams after the call: a request it catches got part of its answer.
    caught = [r for r in tr.records if r[0] >= m["cut"] - tr.timeout and not r[1] and r[3] > 0]
    checks.append(check("requests cut in the middle of their answer", len(caught), True, "info"))
    return checks


# sleep.closeIdleAfter of awg_unchosen_idle_close, and the wait from the end of the last
# connection to the sleep after an idle close: the shortest sleep.afterLastUse.
AWG_IDLE_CLOSE = 15
AWG_IDLE_SETTLE = 10


class QuietConnection:
    """A keep-alive connection through the client that asks for an empty answer every few
    seconds until hush, then stays open and quiet, as the push connections of a phone do once
    their application has nothing to say. Marks its last answer and when the client ends it."""

    def __init__(self, host, every=2.0):
        self.host = host
        self.every = every
        self.sock = None
        self.answers = 0
        self.last = None
        self.ended = None
        self._hush = threading.Event()

    def open(self):
        s = bench.open_stream(self.host, 10)
        if s is None:
            return
        self.sock = s
        threading.Thread(target=self._run, daemon=True).start()

    def hush(self):
        self._hush.set()

    def _run(self):
        s = self.sock
        try:
            while not self._hush.is_set():
                s.sendall(f"GET /204 HTTP/1.1\r\nHost: {self.host}\r\n\r\n".encode())
                head, _ = bench.read_answer(s, time.time() + 10)
                if not head.startswith(b"HTTP/1.1 204"):
                    break
                self.answers += 1
                self.last = time.time()
                self._hush.wait(self.every)
            s.settimeout(None)
            s.recv(1)
        except OSError:
            pass
        self.ended = time.time()

    def close(self):
        if self.sock is not None:
            self.sock.close()


def awg_idle_close_phase(xray, work, name, close_idle):
    """One client with sleep.closeIdleAfter close_idle seconds, 0 for off. Traffic and a quiet
    connection go through w1 under the start pin; the connection stops talking and the host
    releases the pin, so the balancer returns to v1 and only the connection holds w1. Returns
    the client, the sampler, the traffic, the connection and the marks."""
    cfg = awg_config()
    cfg["healthObservatory"]["sleep"]["closeIdleAfter"] = f"{close_idle}s"
    m = Marks()
    quiet = QuietConnection(bench.TARGETS[3])
    try:
        with bench.Client(xray, cfg, work, name) as c, bench.ConnSampler() as cs:
            tr = bench.Traffic(rate=2, big=0.5, timeout=5)
            m["start"] = time.time()
            tr.start()
            time.sleep(1)
            quiet.open()
            time.sleep(7)
            quiet.hush()
            time.sleep(0.5)
            m["release"] = time.time()
            c.api("release", "auto")
            away = wait_for(lambda: [e for e in bench.switches(c.events, m["release"])
                                     if e["fields"]["from"] == "w1"], 15)
            m["away"] = away[0]["ts"] / 1000 if away else time.time()
            if close_idle:
                slept = wait_for(lambda: w1_sleeps(c, m["release"]), AWG_SLEEP + close_idle + 20)
                if slept:
                    m["asleep"] = slept[0]["ts"] / 1000
                    m["status"] = path_status(c, "w1")
                # Room for three keepalives, had they gone on.
                time.sleep(10)
            else:
                # The window in which the close and the sleep would have come, and some.
                time.sleep(max(0.0, m["away"] + AWG_SLEEP + AWG_IDLE_CLOSE + 10 - time.time()))
                m["status"] = path_status(c, "w1")
                # The connection ends at the client: w1 then sleeps sleep.afterLastUse later.
                m["hung_up"] = time.time()
                quiet.close()
                slept = wait_for(lambda: w1_sleeps(c, m["hung_up"]), AWG_SLEEP + 10)
                if slept:
                    m["asleep"] = slept[0]["ts"] / 1000
            m["end"] = time.time()
            tr.stop()
    finally:
        quiet.close()
    tr.save(os.path.join(work, f"{name}-{close_idle}-traffic.json"))
    return c, cs, tr, quiet, m, away and away[0]


@scenario()
def awg_unchosen_idle_close(stand, xray, work, name):
    """Traffic and a keep-alive connection that answers every 2 s go through the AmneziaWG
    outbound w1 under a start pin; the connection then falls quiet and stays open, as the push
    connections of a phone do, and the host releases the pin, so the balancer returns to v1.
    Unchosen, w1 is held by that connection alone. With closeIdleAfter 15 s the core ends it,
    at the later of sleep.afterLastUse after the switch off w1 and 15 s after its last answer, with
    idle_closed; its client sees the end, w1 sleeps with reason idle 10 s later, once the end
    has crossed the tunnel, and its datagrams stop at the UDP proxy; the server sends at most
    the one passive keepalive that answers the end. With closeIdleAfter 0s nothing is ended and w1 stays awake through
    the same window, with its keepalives; it sleeps sleep.afterLastUse after the client hangs up."""
    c, cs, tr, quiet, m, away = awg_idle_close_phase(xray, work, f"{name}-on", AWG_IDLE_CLOSE)
    checks = [check(f"{k} (closeIdleAfter {AWG_IDLE_CLOSE}s)", v, ok, lim) for k, v, ok, lim in
              idle_close_on_checks(c, cs, tr, quiet, m, away)]
    c, cs, tr, quiet, m, away = awg_idle_close_phase(xray, work, f"{name}-off", 0)
    checks += [check(f"{k} (closeIdleAfter 0s)", v, ok, lim) for k, v, ok, lim in
               idle_close_off_checks(c, cs, tr, quiet, m, away)]
    return checks


def idle_close_common(c, tr, quiet, m, away):
    """What both phases share: the quiet connection talked through w1 under the pin, and the
    release moved the balancer to v1."""
    out = [("quiet connection: answers through w1 under the pin", quiet.answers, quiet.answers >= 3, ">= 3")]
    for x in awg_away_checks(c, tr, m, away):
        out.append((x["check"], x["value"], x["ok"], x["limit"]))
    return out


def idle_close_on_checks(c, cs, tr, quiet, m, away):
    out = idle_close_common(c, tr, quiet, m, away)
    closed = [e for e in bench.events_of(c.events, "idle_closed", m["release"]) if e["fields"].get("outbound") == "w1"]
    at = closed and closed[0]["ts"] / 1000
    # The core counts sleep.afterLastUse from the last use of w1, which the connections that were
    # still under way at the switch push back by up to the request timeout.
    due = away and quiet.last and max(m["away"] + AWG_SLEEP, quiet.last + AWG_IDLE_CLOSE)
    late = closed and due and round(at - due, 1)
    out.append(("idle_closed of w1: connections, s after max(switch + sleep.afterLastUse, last answer + closeIdleAfter)",
                closed and f"{closed[0]['fields'].get('connections')}, {late}",
                len(closed) == 1 and closed[0]["fields"].get("connections") == 1 and bool(due)
                and -0.5 <= late <= tr.timeout + 1, f"1, 0 .. {tr.timeout + 1}"))
    ended = quiet.ended and closed and round(quiet.ended - at, 1)
    out.append(("quiet connection ended at the client after idle_closed, s", ended,
                bool(closed) and quiet.ended is not None and -0.5 <= ended <= 2, "0 .. 2"))
    slept = w1_sleeps(c, m["release"])
    asleep = slept and slept[0]["ts"] / 1000
    f = slept[0]["fields"] if slept else {}
    out.append(("outbound_sleep of w1 after idle_closed: reason, s", slept and closed
                and f"{f.get('reason')} {round(asleep - at, 2)}",
                bool(slept) and bool(closed) and f.get("reason") == "idle"
                and AWG_IDLE_SETTLE - 0.5 <= asleep - at <= AWG_IDLE_SETTLE + 2,
                f"idle, {AWG_IDLE_SETTLE} .. {AWG_IDLE_SETTLE + 2}"))
    if away and slept and closed:
        kept = cs.packets(bench.AWG_ADDRESS, m["away"] + 2, at - 0.5)
        out.append(("keepalives of w1 held awake by the connection, datagrams up/down", kept,
                    kept[0] is not None and kept[0] >= 2, "up >= 2"))
        # A tunnel gone before the end crossed it, the server would repeat its end of the
        # connection; one passive keepalive answers the end 10 s after it, about at the sleep.
        after = cs.packets(bench.AWG_ADDRESS, asleep, m["end"])
        out.append((f"datagrams at the proxy in {round(m['end'] - asleep, 1)} s after the sleep, up/down",
                    after, after[0] == 0 and after[1] is not None, "up 0; residual server down is info"))
        out.append(("residual server datagrams after the client slept", after[1], True, "info"))
        sockets = cs.between(bench.AWG_ADDRESS, asleep, m["end"])
        out.append(("new sockets of w1 after the sleep", sockets, sockets == 0, "== 0"))
    status = m.get("status", {})
    out.append(("Status of w1 after the sleep", f"asleep={status.get('is_asleep')}", status.get("is_asleep") is True,
                "asleep=True"))
    ok, n = requests_ok(tr, m["start"], m["end"])
    out.append(("requests answered", f"{ok}/{n}", ok == n, "all"))
    lc = leak_check(c)
    out.append((lc["check"], lc["value"], lc["ok"], lc["limit"]))
    return out


def idle_close_off_checks(c, cs, tr, quiet, m, away):
    out = idle_close_common(c, tr, quiet, m, away)
    closed = bench.events_of(c.events, "idle_closed", m["start"])
    out.append(("idle_closed events", len(closed), not closed, "== 0"))
    early = [e for e in w1_sleeps(c, m["release"]) if e["ts"] / 1000 < m["hung_up"]]
    out.append((f"outbound_sleep of w1 in {round(m['hung_up'] - m['away'], 1)} s after the switch off it",
                len(early), not early, "== 0"))
    out.append(("quiet connection open until the client hung up", quiet.ended is None or quiet.ended >= m["hung_up"],
                quiet.ended is None or quiet.ended >= m["hung_up"], "True"))
    if away:
        kept = cs.packets(bench.AWG_ADDRESS, m["away"] + AWG_SLEEP + 2, m["hung_up"])
        out.append(("keepalives of w1 past sleep.afterLastUse, datagrams up/down", kept,
                    kept[0] is not None and kept[0] >= 2, "up >= 2"))
    status = m.get("status", {})
    out.append(("Status of w1 before the hang-up", f"asleep={bool(status.get('is_asleep'))}",
                bool(status) and not status.get("is_asleep"), "asleep=False"))
    late = [e for e in w1_sleeps(c, m["hung_up"])]
    gap = late and round(late[0]["ts"] / 1000 - m["hung_up"], 1)
    out.append(("outbound_sleep of w1 after the hang-up: reason, s", late and f"{late[0]['fields'].get('reason')} {gap}",
                bool(late) and late[0]["fields"].get("reason") == "idle" and AWG_SLEEP - 0.5 <= gap <= AWG_SLEEP + 3,
                f"idle, {AWG_SLEEP} .. {AWG_SLEEP + 3}"))
    ok, n = requests_ok(tr, m["start"], m["end"])
    out.append(("requests answered", f"{ok}/{n}", ok == n, "all"))
    lc = leak_check(c)
    out.append((lc["check"], lc["value"], lc["ok"], lc["limit"]))
    return out
