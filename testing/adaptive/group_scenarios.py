"""Before/after bench for 24 relay addresses and one independent CDN path.

The six synthetic subnets are port sets, not operating-system IP subnets.
Relay tags interleave their members: relay-01..relay-06 are the first address
of subnets A..F, relay-07..relay-12 the second, and so on. A relay address
uses TCP/TLS and the same SNI as every other relay address. CDN uses WebSocket
and another SNI. The proxy can block each address independently.

BENCH_GROUPS=0 records independent paths. BENCH_GROUPS=1 groups relay
addresses and checks the spec's outcomes.
"""

import json
import os
import time

import bench
import scenarios


GROUPS = os.environ.get("BENCH_GROUPS", "0") == "1"
RELAY_COUNT = 24
SUBNET_COUNT = 6
K = 3
RELAY = [f"relay-{i:02d}" for i in range(1, RELAY_COUNT + 1)]
CDN = "cdn-01"
MEMBERS = [(tag, "tcp_tls", i) for i, tag in enumerate(RELAY)] + [(CDN, "ws", 0)]
PATH_COSTS = {tag: 1 for tag in RELAY} | {CDN: 3}
ALL = []


def scenario(fn):
    ALL.append(scenarios.Scenario(fn.__name__, fn))
    return fn


def check(name, value, ok, limit):
    return scenarios.check(name, value, ok, limit)


def config():
    cfg = bench.client_config(
        MEMBERS, PATH_COSTS,
        health={"schedule": {"quarantine": {"initialDelay": "12s", "jitterFraction": 0.0}}},
        strategy={"confirmWithProbe": "never", "minHoldTime": "1s", "returnToCheaper": {
            "interval": "3s", "hasRandomPhase": False,
            "probeInterval": "3s", "probesToReturn": 1}},
    )
    if GROUPS:
        cfg["routing"]["balancers"][0]["strategy"]["settings"]["groups"] = [
            {"name": "relay", "cost": 1, "probe": "default", "failedInRowToLeave": K}]
        for outbound in cfg["outbounds"]:
            if outbound.get("tag") in RELAY:
                outbound["path"] = {"balancer": "auto", "group": "relay"}
    return cfg


def active(c):
    return c.status()["balancers"][0]["active"]


def index_of(tag):
    return RELAY.index(tag) if tag in RELAY else None


def next_tag(tag, distance=1):
    return RELAY[(index_of(tag) + distance) % RELAY_COUNT]


def subnet_of(tag):
    return index_of(tag) % SUBNET_COUNT


def subnet_tags(subnet):
    return [RELAY[i] for i in range(RELAY_COUNT) if i % SUBNET_COUNT == subnet]


def ports(tags):
    return [bench.address_port("tcp_tls", index_of(t)) for t in tags]


def set_ports(tags, mode):
    bench.chaos(",".join(map(str, ports(tags))), mode)


def event_time(e):
    return e["ts"] / 1000


def events_between(events, typ, start, end=None):
    return [e for e in events if e["type"] == typ and event_time(e) >= start
            and (end is None or event_time(e) <= end)]


def first_switch(c, since, timeout):
    found = scenarios.wait_for(lambda: bench.switches(c.events, since), timeout)
    return found[0] if found else None


def group_events(events, since, state=None):
    return [e for e in events_between(events, "group_state", since)
            if e["fields"].get("group") == "relay"
            and (state is None or e["fields"].get("state") == state)]


def group_quarantine_checks(events, selected, since, end, restored_at=None, back=None):
    """Check the down group's probe embargo and its one recovery candidate."""
    positions = {id(e): i for i, e in enumerate(events)}
    downs = group_events(events, since, "down")
    relay_starts = [e for e in events_between(events, "probe_start", since, end)
                    if e["fields"].get("outbound") in RELAY]
    end_ms = round(end * 1000)
    early = []
    for down in downs:
        due_ms = down["ts"] + down["fields"].get("quarantine_ms", 0)
        early += [e for e in relay_starts if positions[id(e)] > positions[id(down)]
                  and e["ts"] < min(due_ms, end_ms)]
    checks = [check("no relay probes during group quarantine",
                    [(e["fields"].get("outbound"), e["fields"].get("reason"), e["ts"]) for e in early],
                    bool(downs) and not early, "none from group down until quarantine due")]
    if restored_at is None:
        return checks
    ups = group_events(events, restored_at, "up")
    down = downs[0] if downs else None
    up = ups[0] if ups else None
    down_pos = positions[id(down)] if down else None
    up_pos = positions[id(up)] if up else None
    due_ms = down["ts"] + down["fields"].get("quarantine_ms", 0) if down else None
    rechecks = [e for e in relay_starts if due_ms is not None and up_pos is not None
                and due_ms <= e["ts"] and positions[id(e)] < up_pos]
    all_rechecks = [e for e in relay_starts if down_pos is not None and positions[id(e)] > down_pos
                    and e["fields"].get("reason") == "group_recheck"]
    expected = next_tag(selected, K)
    checks.append(check("one group recheck after quarantine, next around ring",
                        [(e["fields"].get("outbound"), e["fields"].get("reason")) for e in rechecks],
                        bool(up) and bool(down) and up_pos > down_pos and up["ts"] >= due_ms
                        and len(rechecks) == 1
                        and len(all_rechecks) == 1 and rechecks[0] is all_rechecks[0]
                        and rechecks[0]["fields"].get("reason") == "group_recheck"
                        and rechecks[0]["fields"].get("outbound") == expected,
                        f"one group_recheck on {expected} after due and before up"))
    recheck = rechecks[0] if len(rechecks) == 1 else None
    results = [e for e in events_between(events, "probe_result", since, end)
               if recheck and up_pos is not None
               and positions[id(recheck)] < positions[id(e)] < up_pos
               and e["fields"].get("outbound") == expected
               and e["fields"].get("reason") == "group_recheck"
               and e["fields"].get("is_ok") is True]
    checks.append(check("group recheck succeeded before up", len(results),
                        len(results) == 1, "one successful group_recheck result"))
    if back is not None:
        back_pos = positions.get(id(back))
        returns = [e for e in events_between(events, "probe_result", since, end)
                   if up_pos is not None and back_pos is not None
                   and up_pos < positions[id(e)] < back_pos
                   and e["fields"].get("outbound") in RELAY
                   and e["fields"].get("reason") == "return"
                   and e["fields"].get("is_ok") is True]
        checks.append(check("normal return probe succeeds after group up", len(returns),
                            len(returns) == 1, "one return success for probesToReturn=1"))
    return checks


def probe_windows(events, names):
    """Peak scheduled probes per SNI from start/result events.

    The proxy exposes aggregate connection counters, without timestamps per
    connection or TLS parsing. Its counters corroborate that dials occurred,
    but cannot prove that TLS handshakes or user dials did not overlap. This
    event check covers only probes scheduled by the observatory.
    """
    timeline = []
    for e in events:
        if e["type"] in ("probe_start", "probe_result"):
            tag = e["fields"].get("outbound")
            if tag in names:
                timeline.append((e["ts"], 0 if e["type"] == "probe_result" else 1,
                                 names[tag], e["type"]))
    running = {}
    peak = {}
    for _, _, name, typ in sorted(timeline):
        running[name] = max(0, running.get(name, 0) + (1 if typ == "probe_start" else -1))
        peak[name] = max(peak.get(name, 0), running[name])
    return peak


def scheduled_server_names(status):
    names = {tag: bench.SERVER_NAMES["tcp_tls"] for tag in RELAY}
    names[CDN] = bench.SERVER_NAMES["ws"]
    for path in status["paths"]:
        tag, actual = path["outbound"], path.get("server_name")
        if tag in names and actual:
            names[tag] = actual
    return names


def parse_server_handshakes(lines):
    """Pair native server TLS begin/end records by ID, including failed calls.

    A begin without an end stays open in overlap calculations. Its SNI is
    unknown because Go learns the name from the ClientHello during handshake.
    """
    by_id = {}
    problems = []
    for number, line in enumerate(lines, 1):
        if '"bench_tls_handshake"' not in line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            problems.append(f"line {number}: malformed trace JSON")
            continue
        if record.get("bench_tls_handshake") is not True:
            problems.append(f"line {number}: invalid trace marker")
            continue
        ident, stamp, phase = record.get("id"), record.get("ts_ns"), record.get("phase")
        if (not isinstance(ident, int) or isinstance(ident, bool) or ident <= 0
                or not isinstance(stamp, int) or isinstance(stamp, bool) or stamp <= 0
                or phase not in ("begin", "end")
                or not isinstance(record.get("serverName"), str)
                or not isinstance(record.get("remote"), str)
                or not isinstance(record.get("error"), str)):
            problems.append(f"line {number}: invalid trace fields")
            continue
        if phase == "begin":
            if ident in by_id:
                problems.append(f"line {number}: duplicate begin for id {ident}")
                continue
            by_id[ident] = {"id": ident, "begin_ns": stamp, "end_ns": None,
                            "server_name": record["serverName"], "remote": record["remote"],
                            "error": ""}
            continue
        interval = by_id.get(ident)
        if interval is None:
            problems.append(f"line {number}: end without begin for id {ident}")
        elif interval["end_ns"] is not None:
            problems.append(f"line {number}: duplicate end for id {ident}")
        elif stamp < interval["begin_ns"]:
            problems.append(f"line {number}: end before begin for id {ident}")
        elif interval["server_name"] and record["serverName"] != interval["server_name"]:
            problems.append(f"line {number}: SNI changed for id {ident}")
        elif interval["remote"] != record["remote"]:
            problems.append(f"line {number}: remote changed for id {ident}")
        else:
            interval["end_ns"] = stamp
            interval["server_name"] = record["serverName"]
            interval["error"] = record["error"]
    return list(by_id.values()), problems


def handshake_window(intervals, start, end):
    """Count overlapping actual server calls; an unknown SNI may be any name."""
    begin, finish = round(start * 1e9), round(end * 1e9)
    relevant = [h for h in intervals if h["begin_ns"] < finish
                and (h["end_ns"] is None or h["end_ns"] > begin)]
    names = {h["server_name"] for h in relevant if h["server_name"]}
    if not names and relevant:
        names = {"<unknown SNI>"}
    peaks = {}
    for name in sorted(names):
        edges = []
        for h in relevant:
            if h["server_name"] not in ("", name):
                continue
            left = max(begin, h["begin_ns"])
            right = min(finish, h["end_ns"] if h["end_ns"] is not None else finish)
            if left < right:
                edges.extend(((left, 1), (right, -1)))
        running = peak = 0
        for _, delta in sorted(edges, key=lambda e: (e[0], e[1])):
            running += delta
            peak = max(peak, running)
        peaks[name] = peak
    return {"count": len(relevant), "failed": sum(bool(h["error"]) for h in relevant),
            "unfinished": sum(h["end_ns"] is None for h in relevant),
            "open_at_window_end": sum(h["end_ns"] is None or h["end_ns"] > finish for h in relevant),
            "unknown_sni": sum(not h["server_name"] for h in relevant), "peak_by_sni": peaks}


def actual_handshake_checks(work, windows):
    if not bench.SERVER_BIN:
        return [check("native server TLS trace", "BENCH_SERVER_BIN unset", not GROUPS,
                      "BEFORE info; AFTER requires an instrumented server binary")]
    path = os.path.join(work, "server.log")
    if not os.path.exists(path):
        return [check("native server TLS trace", path, False, "server.log exists")]
    with open(path, errors="replace") as f:
        intervals, problems = parse_server_handshakes(f)
    first = min(start for _, start, _ in windows)
    last = max(end for _, _, end in windows)
    seen = handshake_window(intervals, first, last)["count"]
    checks = [check("native server TLS trace parsed", f"{seen} handshakes; {problems[:3]}",
                    (not problems and seen > 0) if GROUPS else True,
                    ">= 1 handshake, no malformed or unmatched records in AFTER")]
    for label, start, end in windows:
        if end <= start:
            continue
        summary = handshake_window(intervals, start, end)
        peak = max(summary["peak_by_sni"].values(), default=0)
        checks.append(check(f"actual server TLS overlap: {label}", summary,
                            (peak <= 1 and not problems) if GROUPS else True,
                            "<= 1 per SNI including failed/open calls; unknown SNI counts against every name"
                            if GROUPS else "info in BEFORE"))
    return checks


def measured_checks(c, cs, marks, selected, banned, switch, rec):
    fault = marks["fault"]
    until = event_time(switch) if switch else marks["end"]
    starts = [e for e in events_between(c.events, "probe_start", fault, until)
              if e["fields"].get("outbound") in RELAY]
    failed = [e for e in events_between(c.events, "probe_result", fault, until)
              if e["fields"].get("outbound") in RELAY and e["fields"].get("is_ok") is False]
    candidate_ports = ports({e["fields"]["outbound"] for e in starts if e["fields"]["outbound"] != selected})
    proxy_conns = cs.between(candidate_ports, fault, until) if candidate_ports else 0
    failed_others = [e for e in failed if e["fields"]["outbound"] != selected]
    failed_candidates = {e["fields"]["outbound"] for e in failed_others}
    failed_dials = cs.between(ports(failed_candidates), fault, marks["end"]) if failed_candidates else 0
    names = scheduled_server_names(marks["status"])
    peak = probe_windows(c.events, names)
    relay_sni = names.get(selected, bench.SERVER_NAMES["tcp_tls"])
    cdn_sni = names.get(CDN, bench.SERVER_NAMES["ws"])
    return [
        check("selected relay from Status", selected, selected in RELAY, "one of relay-01..relay-24"),
        check("blocked tags and synthetic subnets",
              {"tags": sorted(banned), "subnets": sorted({subnet_of(t) for t in banned})}, True, "info"),
        check("time to first switch, s", round(until - fault, 2) if switch else None, True,
              "info in BEFORE; compared with spec in AFTER"),
        check("traffic recovery, s", round(rec, 2) if rec is not None else None, True,
              "info in BEFORE; compared with spec in AFTER"),
        check("relay probe starts / failed results before switch", f"{len(starts)} / {len(failed)}", True, "info"),
        check("proxy connections to probed relay candidates", proxy_conns, True,
              "info: aggregate counters cannot timestamp individual TLS handshakes"),
        check("failed relay probe dials visible at proxy", f"{failed_dials}/{len(failed_others)}",
              failed_dials is not None and failed_dials >= len(failed_others),
              "at least one connection per failed probe; counters cannot prove non-overlap"),
        check("relay SNI concurrent probe windows", peak.get(relay_sni, 0),
              c.dropped == 0 and peak.get(relay_sni, 0) <= 1,
              "<= 1 scheduled probe, no dropped events; user handshakes are outside this check"),
        check("relay and CDN have different TLS names", f"{relay_sni} / {cdn_sni}",
              relay_sni != cdn_sni, "different"),
        check("event buffer drops", c.dropped, c.dropped == 0, "== 0"),
        scenarios.leak_check(c),
    ], starts, failed


def exercise(stand, xray, work, name, fault_kind):
    cfg = config()
    marks = {"trace_start": time.time()}
    before_checks = []
    selected = None
    banned = []
    switch = None
    back = None
    with bench.Client(xray, cfg, work, name) as c, bench.ConnSampler() as cs:
        traffic = bench.Traffic(rate=2, big=0.5)
        traffic.start()
        try:
            time.sleep(6)
            status = c.status()
            selected = status["balancers"][0]["active"]
            marks["status"] = status
            if selected not in RELAY:
                return [check("initial active relay", selected, False, "relay-01..relay-24")]
            subnet = subnet_of(selected)
            if fault_kind == "subnet":
                untouched = subnet_tags((subnet + 3) % SUBNET_COUNT)
                t_unrelated = time.time()
                set_ports(untouched, "blackhole")
                time.sleep(8)
                unchanged = active(c) == selected and not bench.switches(c.events, t_unrelated)
                before_checks.append(check("ban in another subnet leaves selected relay active",
                                           active(c), unchanged, selected))
                set_ports(untouched, "pass")
            if fault_kind == "one":
                banned = [selected]
            elif fault_kind == "subnet":
                banned = subnet_tags(subnet)
            elif fault_kind == "sni":
                banned = list(RELAY)
            elif fault_kind == "half":
                subnets = {(subnet + d) % SUBNET_COUNT for d in (0, 2, 4)}
                banned = [t for t in RELAY if subnet_of(t) in subnets]
            elif fault_kind == "contiguous":
                subnets = {(subnet + d) % SUBNET_COUNT for d in (0, 1, 2)}
                banned = [t for t in RELAY if subnet_of(t) in subnets]
            marks["fault"] = time.time()
            set_ports(banned, "blackhole")
            switch = first_switch(c, marks["fault"], 900 if not GROUPS and fault_kind == "sni" else 100)
            time.sleep(9)
            if fault_kind == "contiguous" and GROUPS:
                marks["restore"] = time.time()
                set_ports(banned, "pass")
                back_events = scenarios.wait_for(
                    lambda: [e for e in bench.switches(c.events, marks["restore"])
                             if e["fields"].get("to") in RELAY], 150)
                back = back_events[0] if back_events else None
                time.sleep(5)
            marks["end"] = time.time()
        finally:
            traffic.stop()
    marks["trace_end"] = time.time()
    traffic.save(os.path.join(work, f"{name}-traffic.json"))
    rec, _ = bench.recovery(traffic.records, marks["fault"])
    checks, starts, failed = measured_checks(c, cs, marks, selected, banned, switch, rec)
    checks = before_checks + checks
    windows = [("whole run", marks["trace_start"], marks["trace_end"]),
               ("before fault", marks["trace_start"], marks["fault"])]
    if switch:
        switched_at = event_time(switch)
        windows.append(("fault to switch", marks["fault"], switched_at))
        if fault_kind == "contiguous" and GROUPS:
            windows.append(("switch to repair", switched_at, marks["restore"]))
            if back:
                windows.append(("repair to return", marks["restore"], event_time(back)))
                windows.append(("after return", event_time(back), marks["trace_end"]))
            else:
                windows.append(("after repair", marks["restore"], marks["trace_end"]))
        else:
            windows.append(("after switch", switched_at, marks["trace_end"]))
    checks += actual_handshake_checks(work, windows)
    got = switch["fields"].get("to") if switch else None
    checks.append(check("first switch destination", got, bool(switch) if not GROUPS else
                        got == (CDN if fault_kind in ("sni", "contiguous") else next_tag(selected)),
                        "observed in BEFORE" if not GROUPS else
                        (CDN if fault_kind in ("sni", "contiguous") else next_tag(selected))))
    if not GROUPS:
        checks.append(check("group events in BEFORE", len(group_events(c.events, marks["fault"])), True,
                            "info: the config contains no group"))
        return checks
    limit = {"one": 12, "subnet": 12, "sni": 30, "half": 20, "contiguous": 30}[fault_kind]
    checks.append(check("first switch within spec window, s", round(event_time(switch) - marks["fault"], 2)
                        if switch else None, bool(switch) and event_time(switch) - marks["fault"] <= limit,
                        f"<= {limit}"))
    checks.append(check("traffic recovery within spec window, s", round(rec, 2) if rec is not None else None,
                        rec is not None and rec <= limit, f"<= {limit}"))
    downs = group_events(c.events, marks["fault"], "down")
    if fault_kind in ("sni", "contiguous"):
        pre_switch = event_time(switch) if switch else marks["end"]
        failed_before = [e for e in failed if event_time(e) <= pre_switch]
        checks.append(check("failed relay probes before group departure", len(failed_before),
                            len(failed_before) == K - 1, f"exactly {K - 1}"))
        checks.append(check("failed candidates are next around the ring",
                            [e["fields"].get("outbound") for e in failed_before],
                            [e["fields"].get("outbound") for e in failed_before] ==
                            [next_tag(selected, 1), next_tag(selected, 2)],
                            f"{next_tag(selected, 1)}, {next_tag(selected, 2)}"))
        checks.append(check("group down and quarantined", [e["fields"] for e in downs],
                            len(downs) >= 1 and downs[0]["fields"].get("failed") == K
                            and downs[0]["fields"].get("quarantine_ms", 0) > 0,
                            f"failed={K}, quarantine_ms>0"))
        remaining = [e["fields"].get("outbound") for e in starts
                     if event_time(e) <= pre_switch
                     and e["fields"].get("outbound") not in
                     (next_tag(selected, 1), next_tag(selected, 2))]
        checks.append(check("unvisited relay members not probed before CDN", remaining,
                            not remaining, "empty"))
        cdn_success = [e for e in events_between(c.events, "probe_result", marks["fault"], pre_switch)
                       if e["fields"].get("outbound") == CDN and e["fields"].get("is_ok") is True]
        checks.append(check("CDN passed a probe before selection", len(cdn_success),
                            len(cdn_success) == 1, "== 1"))
        checks += group_quarantine_checks(c.events, selected, marks["fault"], marks["trace_end"],
                                          marks.get("restore"), back)
    else:
        checks.append(check("successful relay stays in the group", len(downs),
                            not downs and got in RELAY, "no group down; relay selected"))
        successful = [e for e in events_between(c.events, "probe_result", marks["fault"],
                                                 event_time(switch) if switch else marks["end"])
                      if e["fields"].get("outbound") == next_tag(selected)
                      and e["fields"].get("is_ok") is True]
        checks.append(check("next relay passed one probe", len(successful), len(successful) == 1, "== 1"))
        if fault_kind == "half":
            checks.append(check("relay probes with half the subnets banned", len(starts),
                                1 <= len(starts) <= 2, "1..2"))
        else:
            checks.append(check("relay probes for one failed address or subnet", len(starts),
                                len(starts) == 1, "== 1"))
    if fault_kind == "contiguous":
        ups = group_events(c.events, marks["restore"], "up")
        checks.append(check("group returns after quarantine and repair", len(ups), bool(ups), ">= 1 up event"))
        checks.append(check("returns from CDN to a relay", back and back["fields"].get("to"),
                            bool(back) and back["fields"].get("to") in RELAY, "relay-01..relay-24"))
        returned = [r for r in traffic.records if back and r[0] >= event_time(back) + 1]
        checks.append(check("requests answered after return", f"{sum(r[1] for r in returned)}/{len(returned)}",
                            len(returned) >= 3 and all(r[1] for r in returned),
                            "at least 3, all answered"))
    return checks


@scenario
def group_one_ip(stand, xray, work, name):
    return exercise(stand, xray, work, name, "one")


@scenario
def group_subnet_ban(stand, xray, work, name):
    return exercise(stand, xray, work, name, "subnet")


@scenario
def group_sni_ban(stand, xray, work, name):
    return exercise(stand, xray, work, name, "sni")


@scenario
def group_half_ban(stand, xray, work, name):
    return exercise(stand, xray, work, name, "half")


@scenario
def group_contiguous_ban(stand, xray, work, name):
    return exercise(stand, xray, work, name, "contiguous")


@scenario
def group_spread(stand, xray, work, name):
    trace_start = time.time()
    histogram = {tag: 0 for tag in RELAY}
    runs = set()
    failed = []
    for i in range(50):
        with bench.Client(xray, config(), work, f"{name}-{i:02d}") as c:
            bench.download("t1.test", "/bytes/32768", timeout=10)
            status = scenarios.wait_for(
                lambda: s if (s := c.status())["balancers"][0]["active"] in RELAY else None, 5, 0.1)
            if not status:
                status = c.status()
            selected = status["balancers"][0]["active"]
            run = status.get("run")
            if selected in histogram:
                histogram[selected] += 1
            else:
                failed.append((i, selected))
            if run:
                runs.add(run)
    with open(os.path.join(work, f"{name}-histogram.json"), "w") as f:
        json.dump(histogram, f, indent=1)
    trace_end = time.time()
    occupied = sum(v > 0 for v in histogram.values())
    maximum = max(histogram.values())
    checks = [check("50 distinct production run IDs", len(runs), len(runs) == 50, "== 50"),
              check("initial selections on relay", f"{sum(histogram.values())}/50; other={failed}",
                    sum(histogram.values()) == 50, "50/50")]
    checks.append(check("position histogram relay-01..relay-24",
                        [histogram[t] for t in RELAY], True, "numeric histogram, in tag order"))
    checks += actual_handshake_checks(work, [("50 independent clients", trace_start, trace_end)])
    if GROUPS:
        # For 50 independent uniform choices in 24 cells, the union bound for
        # a cell with >=10 choices is <0.00084. Fewer than 16 occupied cells
        # has exact multinomial probability <0.00006. Neither asks for an
        # impossible 50/24 equal split.
        checks.append(check("largest relay occupancy", maximum, maximum <= 9,
                            "<= 9; P(any >=10 under uniformity) < 0.00084"))
        checks.append(check("relay positions reached", occupied, occupied >= 16,
                            ">= 16; P(<16 under uniformity) < 0.00006"))
    else:
        checks.append(check("baseline occupancy", f"{occupied} positions, max {maximum}", True,
                            "info; group hashing absent"))
    return checks


@scenario
def group_declared_order(stand, xray, work, name):
    if not GROUPS:
        return [check("declared order applies in grouped run", "baseline", True, "info")]
    checks = []
    for label, order in (("ten-first", ["relay-10", "relay-1", "relay-2"]),
                         ("two-first", ["relay-2", "relay-10", "relay-1"])):
        stand.reset()
        members = [(tag, "tcp_tls", i) for i, tag in enumerate(order)]
        cfg = bench.client_config(members, {tag: 1 for tag in order}, strategy={
            "startOutbound": order[0], "confirmWithProbe": "never", "minHoldTime": "0s",
            "groups": [{"name": "relay", "cost": 1, "probe": "default", "failedInRowToLeave": 3}]})
        for item in cfg["outbounds"]:
            if item.get("tag") in order:
                item["path"] = {"balancer": "auto", "group": "relay"}
        with bench.Client(xray, cfg, work, f"{name}-{label}") as client:
            traffic = bench.Traffic(rate=2, big=0)
            traffic.start()
            time.sleep(3)
            initial = client.status()["balancers"][0]["active"]
            mark = time.time()
            bench.chaos(str(bench.address_port("tcp_tls", 0)), "blackhole")
            switch = first_switch(client, mark, 20)
            traffic.stop()
        expected = order[1]
        actual = switch["fields"].get("to") if switch else None
        checks.extend([
            check(f"{label}: starts at declared first", initial, initial == order[0], order[0]),
            check(f"{label}: failover follows declared next", actual, actual == expected, expected),
        ])
    return checks
