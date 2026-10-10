"""Provider feedback and return intervals for expensive paths."""
import time

import bench
from scenarios import check, first_event, leak_check, run_timeline, scenario


def current(client):
    return next(b["active"] for b in client.status()["balancers"] if b["balancer"] == "auto")


@scenario()
def cost_above_immediate(stand, xray, work, name):
    members = [("p-cheap1", "reality", 0), ("p-cheap2", "reality", 1),
               ("p-expensive", "tcp_tls", 0)]
    costs = {"p-cheap1": 1, "p-cheap2": 1, "p-expensive": 50}
    prov = bench.provider_config(
        ["net"], triggers={"whenCostAbove": {"cost": 10, "after": "0s"}},
        limits={"minInterval": "1h", "exhaustedMinInterval": "2s", "maxRefreshesPerHour": 0})
    cfg = bench.client_config(
        members, costs, providers=[prov],
        health={"passive": {"failuresToSuspect": 1, "minDistinctTargets": 1},
                "schedule": {"quarantine": {"initialDelay": "2m"}}},
        strategy={"confirmWithProbe": "never", "minHoldTime": "0s",
                  "returnToCheaper": {"interval": "3s", "hasRandomPhase": False,
                                      "probesToReturn": 1, "probeInterval": "0s"}})
    before = [("cheap1", "reality", 0), ("cheap2", "reality", 1), ("expensive", "tcp_tls", 0)]
    stand.answer(before, costs={"cheap1": 1, "cheap2": 1, "expensive": 50}, version="before")
    marks = {}

    def prime():
        c.api("refresh", "fresh")

    def ban():
        marks["ban"] = time.time()
        stand.answer([("cheap3", "reality", 2), ("expensive", "tcp_tls", 0)],
                     costs={"cheap3": 1, "expensive": 50}, version="fresh-cheap")
        bench.chaos(",".join(str(bench.address_port("reality", i)) for i in (0, 1)), "blackhole")

    with bench.Client(xray, cfg, work, name) as c:
        traffic = bench.Traffic(rate=2, big=0.5)
        run_timeline(traffic, [(2, prime), (5, ban)], 45)
    expensive = first_event(c.events, "switch", marks["ban"], to="p-expensive")
    fetch = first_event(c.events, "provider_fetch", marks["ban"], trigger="cost_above", is_ok=True)
    returned = first_event(c.events, "switch", marks["ban"], to="p-cheap3")
    delay = (fetch["ts"] - expensive["ts"]) / 1000 if expensive and fetch else None
    requests = stand.provider_requests()
    gaps = [b[0] - a[0] for a, b in zip(requests, requests[1:])]
    return [
        check("moved onto the expensive path", expensive and expensive["fields"]["to"], bool(expensive),
              "p-expensive"),
        check("cost feedback after the switch, s", delay and round(delay, 2),
              delay is not None and 0 <= delay <= 3, "0..3, despite minInterval 1h"),
        check("refresh starts respect the short limit, s", [round(g, 2) for g in gaps],
              bool(gaps) and all(g >= 1.95 for g in gaps), ">= exhaustedMinInterval 2"),
        check("fresh cheap address took traffic", returned and returned["fields"]["to"],
              bool(returned) and bool(fetch) and returned["ts"] >= fetch["ts"], "p-cheap3 after feedback"),
        leak_check(c),
    ]


@scenario()
def expensive_revoke(stand, xray, work, name):
    checks = []
    members = [("p-cheap", "reality", 2), ("p-expensive", "tcp_tls", 0)]
    for selected in (True, False):
        stand.reset()
        initial = [("cheap", "reality", 2), ("expensive", "tcp_tls", 0)]
        stand.answer(initial, costs={"cheap": 1, "expensive": 50}, version="both", next_refresh="3s")
        prov = bench.provider_config(
            ["net"], limits={"minInterval": "0s", "exhaustedMinInterval": "0s",
                               "maxRefreshesPerHour": 0}, apply={"drainTimeout": "4s"})
        cfg = bench.client_config(members, {"p-cheap": 1, "p-expensive": 50}, providers=[prov],
                                  strategy={"startOutbound": "p-expensive" if selected else "p-cheap"})
        marks = {}

        def first_answer():
            c.api("refresh", "fresh")

        def revoke():
            marks["revoke"] = time.time()
            stand.answer([("cheap", "reality", 2)], costs={"cheap": 1}, version="revoked",
                         next_refresh="2h")

        with bench.Client(xray, cfg, work, f"{name}-{'selected' if selected else 'unused'}") as c:
            run_timeline(bench.Traffic(rate=2, big=0.5), [(2, first_answer), (3.5, revoke)], 16)
            active = current(c)
        applied = first_event(c.events, "provider_apply", marks["revoke"], version="revoked", is_ok=True)
        drained = first_event(c.events, "provider_drained", marks["revoke"], outbound="p-expensive")
        switched = first_event(c.events, "switch", marks["revoke"], **{"from": "p-expensive"})
        elapsed = drained["ts"] / 1000 - marks["revoke"] if drained else None
        label = "selected" if selected else "unused"
        checks += [
            check(f"{label}: revocation applied on nextRefreshIn", bool(applied), bool(applied), "True"),
            check(f"{label}: removed path drained, s", elapsed and round(elapsed, 2),
                  elapsed is not None and elapsed <= 7, "<= nextRefreshIn 3 + drainTimeout 4"),
            check(f"{label}: chosen cheap after revocation", active, active == "p-cheap", "p-cheap"),
            check(f"{label}: switch only for a client on the revoked path", bool(switched),
                  bool(switched) == selected, str(selected)),
            leak_check(c),
        ]
        if not selected:
            probes = [e for e in c.events if e["type"] == "probe_start"
                      and e["fields"].get("outbound") == "p-expensive"]
            checks.append(check("unused: no probe of the expensive path", len(probes), not probes, "0"))
    return checks
