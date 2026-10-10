"""Return scheduling follows the cost entry of the chosen outbound."""
import bench
from scenarios import check, first_event, leak_check, run_timeline, scenario


@scenario()
def return_interval_by_cost(stand, xray, work, name):
    checks = []
    waits = {}
    for selected, cost, expected in [("expensive", 50, 5), ("cdn", 3, 30)]:
        stand.reset()
        members = [("cheap", "reality", 2), (selected, "tcp_tls", 0)]
        cfg = bench.client_config(
            members, {"cheap": 1, selected: cost},
            strategy={"startOutbound": selected, "minHoldTime": "0s",
                      "returnToCheaper": {"interval": "30s", "hasRandomPhase": False,
                                          "probesToReturn": 1, "probeInterval": "0s"}})
        if selected == "expensive":
            chosen = next(outbound for outbound in cfg["outbounds"] if outbound.get("tag") == selected)
            chosen["path"]["returnInterval"] = "5s"
        with bench.Client(xray, cfg, work, f"{name}-{selected}") as c:
            run_timeline(bench.Traffic(rate=2, big=0.5), [], 37)
        started = first_event(c.events, "switch", to=selected, reason="start_with")
        probe = first_event(c.events, "probe_start", outbound="cheap", reason="return")
        returned = first_event(c.events, "switch", to="cheap", reason="return")
        wait = (probe["ts"] - started["ts"]) / 1000 if probe and started else None
        waits[selected] = wait
        checks += [
            check(f"{selected}: initial choice", started and started["fields"]["to"], bool(started), selected),
            check(f"{selected}: first return probe, s", wait and round(wait, 2),
                  wait is not None and expected - 0.1 <= wait <= expected + 2,
                  f"{expected}..{expected + 2}"),
            check(f"{selected}: returned after a successful probe", bool(returned),
                  bool(returned) and bool(probe) and returned["ts"] >= probe["ts"], "True"),
            leak_check(c),
        ]
    faster = waits["expensive"] is not None and waits["cdn"] is not None and waits["expensive"] < waits["cdn"] / 3
    checks.append(check("expensive entry returns sooner than the default", waits, faster, "5s versus 30s"))
    return checks
