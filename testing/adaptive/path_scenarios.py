"""Acceptance checks for explicit outbound paths and provider path validation."""
import copy
import json
import os
import subprocess
import time

import bench
import scenarios
from scenarios import check, first_event, run_timeline, scenario


def outbound(cfg, tag):
    return next(item for item in cfg["outbounds"] if item.get("tag") == tag)


def group_position_tags(order, position):
    if not isinstance(position, int) or not 0 <= position < len(order):
        return None, None
    return f"p-{order[position]}", f"p-{order[(position + 1) % len(order)]}"


def rejected_apply(events, version, start, end):
    return next((event for event in events
                 if start <= event["ts"] / 1000 < end
                 and event["fields"].get("version") == version
                 and event["fields"].get("is_ok") is False), None)


def pathless_config():
    cfg = bench.client_config([("chosen", "reality", 0), ("forgotten", "tcp_tls", 0)],
                              {"chosen": 2, "forgotten": 1})
    outbound(cfg, "forgotten").pop("path")
    return cfg


@scenario()
def pathless_outbound(stand, xray, work, name):
    cfg = pathless_config()
    with bench.Client(xray, cfg, work, name) as client:
        traffic = bench.Traffic(rate=2, big=0)
        run_timeline(traffic, [], 8)
        status = client.status()
    active = status["balancers"][0]["active"]
    observed = [entry["outbound"] for entry in status["paths"]]
    probes = [entry for entry in client.events if entry["type"] == "probe_start"
              and entry["fields"].get("outbound") == "forgotten"]
    return [check("pathless live outbound is not selected", active, active == "chosen", "chosen"),
            check("pathless outbound is not observed", observed,
                  "chosen" in observed and "forgotten" not in observed, "chosen only"),
            check("pathless outbound is not probed", len(probes), not probes, "0")]


def invalid_configs():
    base = bench.client_config([("a1", "reality", 0)], {"a1": 1})

    def changed(edit):
        cfg = copy.deepcopy(base)
        edit(cfg)
        return cfg

    def group(cfg):
        cfg["routing"]["balancers"][0]["strategy"]["settings"]["groups"] = [
            {"name": "relay", "cost": 1}]
        outbound(cfg, "a1")["path"] = {"balancer": "auto", "group": "relay"}

    def unknown_group(cfg):
        outbound(cfg, "a1")["path"] = {"balancer": "auto", "group": "absent"}

    def stock_balancer(cfg):
        balancer = cfg["routing"]["balancers"][0]
        balancer["strategy"] = {"type": "random"}
        balancer["selector"] = ["a1"]

    cases = [
        ("unknown balancer", changed(lambda c: outbound(c, "a1")["path"].update(balancer="absent")), "a1"),
        ("non leastCost balancer", changed(stock_balancer), "a1"),
        ("missing cost", changed(lambda c: outbound(c, "a1")["path"].pop("cost")), "a1"),
        ("unknown group", changed(unknown_group), "a1"),
        ("unknown probe", changed(lambda c: outbound(c, "a1")["path"].update(probe="absent")), "a1"),
        ("leastCost selector", changed(lambda c: c["routing"]["balancers"][0].update(selector=["a1"])), "selector"),
        ("duplicate group", changed(lambda c: c["routing"]["balancers"][0]["strategy"]["settings"].update(
            groups=[{"name": "relay", "cost": 1}, {"name": "relay", "cost": 2}])), "relay"),
        ("empty group", changed(lambda c: c["routing"]["balancers"][0]["strategy"]["settings"].update(
            groups=[{"name": "relay", "cost": 1}])), "relay"),
        ("no participants", changed(lambda c: outbound(c, "a1").pop("path")), "auto"),
    ]
    grouped = changed(group)
    outbound(grouped, "a1")["path"]["cost"] = 2
    cases.append(("group property override", grouped, "a1"))
    for field, value in (("isLazy", True), ("returnInterval", "5s"), ("probe", "default")):
        grouped = changed(group)
        outbound(grouped, "a1")["path"][field] = value
        cases.append((f"group {field} override", grouped, "a1"))
    no_group_cost = changed(group)
    no_group_cost["routing"]["balancers"][0]["strategy"]["settings"]["groups"][0].pop("cost")
    cases.append(("group missing cost", no_group_cost, "relay"))
    unknown_group_probe = changed(group)
    unknown_group_probe["routing"]["balancers"][0]["strategy"]["settings"]["groups"][0]["probe"] = "absent"
    cases.append(("unknown group probe", unknown_group_probe, "absent"))
    return cases


@scenario()
def invalid_path_config(stand, xray, work, name):
    checks = []
    for label, cfg, expected in invalid_configs():
        path = os.path.join(work, f"{name}-{label.replace(' ', '-')}.json")
        with open(path, "w") as stream:
            json.dump(cfg, stream)
        result = subprocess.run([xray, "run", "-test", "-c", path], capture_output=True, text=True, timeout=15)
        output = result.stdout + result.stderr
        checks.append(check(label, output[-300:], result.returncode != 0 and expected in output,
                            f"nonzero exit and error containing {expected!r}"))
    return checks


@scenario()
def provider_path_rejection(stand, xray, work, name):
    fresh = [("x1", "reality", 2), ("x2", "reality", 3)]
    stand.answer(fresh, {"x1": 1, "x2": 1}, version="valid")
    provider = bench.provider_config(["direct"], limits={
        "minInterval": "1s", "exhaustedMinInterval": "1s", "maxRefreshesPerHour": 3600,
        "backoff": {"initialDelay": "1s", "maxDelay": "1s", "jitterFraction": 0}})
    cfg = bench.client_config([], {}, providers=[provider], strategy={"groups": []})
    marks = {}

    def fetch(label):
        marks[label] = time.time()
        client.api("refresh", "fresh")

    def missing_path():
        body = {"version": "missing", "outbounds": bench.path_members(fresh, {"x1": 1, "x2": 1})}
        body["outbounds"][0].pop("path")
        with open(stand.provider_answer, "w") as stream:
            json.dump(body, stream)
        fetch("missing")

    def group_override():
        scenarios.wait_for(lambda: first_event(client.events, "provider_apply",
                                               marks["missing"], version="missing", is_ok=False), 10)
        body = {"version": "override", "groups": [{"name": "relay", "cost": 1, "probe": "default"}],
                "outbounds": bench.path_members(fresh, {"x1": 1, "x2": 1})}
        for item in body["outbounds"]:
            item["path"] = {"balancer": "auto", "group": "relay"}
        body["outbounds"][0]["path"]["cost"] = 1
        with open(stand.provider_answer, "w") as stream:
            json.dump(body, stream)
        fetch("override")

    with bench.Client(xray, cfg, work, name) as client:
        traffic = bench.Traffic(rate=2, big=0)
        run_timeline(traffic, [(2, lambda: fetch("valid")), (6, missing_path),
                               (7, group_override), (50, lambda: fetch("last_failure"))], 55)
        status = client.status()
        kept = client.api("set", "fresh").get("set") or {}
    applies = [event for event in client.events if event["type"] == "provider_apply"]
    valid = first_event(client.events, "provider_apply", marks["valid"], version="valid", is_ok=True)
    bad_missing = rejected_apply(applies, "missing", marks["missing"], marks["override"])
    bad_override = rejected_apply(applies, "override", marks["override"], marks["last_failure"])
    rejected_fetch = next((event for event in reversed(client.events)
                           if bad_missing and event["seq"] < bad_missing["seq"]
                           and event["type"] == "provider_fetch"), None)
    rejected_refresh = rejected_fetch["fields"].get("refresh", 0) if rejected_fetch else 0
    bodies = [entry.get("body", {}) for entry in stand.provider_bodies(marks["missing"])
              if entry.get("body", {}).get("refreshNumber", 0) > rejected_refresh]
    next_body = bodies[0] if bodies else {}
    members = status["providers"][0]["members"]
    active = status["balancers"][0]["active"]
    served = [record for record in traffic.records if record[0] >= marks["missing"] + 1]
    kept_tags = [item.get("tag") for item in kept.get("outbounds", [])]
    return [check("valid set applied", bool(valid), bool(valid), "True"),
            check("missing provider path rejected", bad_missing and bad_missing["fields"].get("error"),
                  bool(bad_missing) and "selection" in (bad_missing["fields"].get("error") or "").lower(), "path error"),
            check("group property override rejected", bad_override and bad_override["fields"].get("error"),
                  bool(bad_override) and (not bad_missing or bad_override["seq"] != bad_missing["seq"])
                  and "x1" in (bad_override["fields"].get("error") or ""), "separate outbound error"),
            check("old set remains live", (members, active, kept.get("version"), kept_tags),
                  members == 2 and active in {"p-x1", "p-x2"} and kept.get("version") == "valid"
                  and kept_tags == ["x1", "x2"], "valid version, two raw tags, active old member"),
            check("old set keeps answering traffic", f"{sum(record[1] for record in served)}/{len(served)}",
                  bool(served) and all(record[1] for record in served), "all requests after first rejection"),
            check("next request reports last failure", next_body.get("lastFailure"),
                  next_body.get("lastFailure", {}).get("code") == "invalid_set"
                  and next_body.get("lastFailure", {}).get("status") == 200
                  and next_body.get("lastFailure", {}).get("agoMs", -1) >= 0, "status=200, code=invalid_set, agoMs>=0"),
            check("invalid sets never applied", [event["fields"].get("version") for event in applies
                                                 if event["fields"].get("is_ok")],
                  all(event["fields"].get("version") == "valid" for event in applies
                      if event["fields"].get("is_ok")), "valid only")]


@scenario()
def provider_group_names(stand, xray, work, name):
    members = [("relay-10", "reality", 1), ("relay-2", "reality", 2), ("relay-1", "reality", 3)]
    order = [tag for tag, _, _ in members]
    groups = [{"name": "relay", "cost": 1, "probe": "default"}]
    stand.answer(members, {tag: 1 for tag in order}, version="grouped", groups=groups)
    with open(stand.provider_answer) as stream:
        body = json.load(stream)
    for item in body["outbounds"]:
        item.pop("selection", None)
        item["path"] = {"balancer": "auto", "group": "relay"}
    with open(stand.provider_answer, "w") as stream:
        json.dump(body, stream)
    provider = bench.provider_config(["direct"])
    cfg = bench.client_config([], {}, providers=[provider], strategy={"groups": []})
    with bench.Client(xray, cfg, work, name) as client:
        client.api("refresh", "fresh")
        traffic = bench.Traffic(rate=2, big=0)
        traffic.start()
        try:
            status = scenarios.wait_for(
                lambda: state if (state := client.status())["balancers"][0]["active"].startswith("p-relay-") else None,
                10, 0.1)
            if status:
                chosen = status["balancers"][0]["active"]
            else:
                chosen = None
            if chosen:
                time.sleep(2)
                mark = time.time()
                chosen_port = next(bench.address_port(kind, index) for tag, kind, index in members if f"p-{tag}" == chosen)
                bench.chaos(str(chosen_port), "blackhole")
                switched = scenarios.wait_for(
                    lambda: [event for event in bench.switches(client.events, mark)
                             if event["fields"].get("from") == chosen], 20, 0.1)
            else:
                switched = None
        finally:
            traffic.stop()
    applied = first_event(client.events, "provider_apply", version="grouped", is_ok=True)
    named_groups = status.get("groups", []) if status else []
    names = [group.get("group") for group in named_groups]
    position = next((group.get("client_position") for group in named_groups
                     if group.get("group") == "p-relay"), None)
    expected_initial, expected_next = group_position_tags(order, position)
    actual_next = switched[0]["fields"].get("to") if switched else None
    return [check("raw group name in provider answer", body["groups"][0]["name"],
                  body["groups"][0]["name"] == "relay", "relay"),
            check("provider response declared order", [item["tag"] for item in body["outbounds"]],
                  [item["tag"] for item in body["outbounds"]] == order, str(order)),
            check("provider group applied", bool(applied), bool(applied), "True"),
            check("group name prefixed in status", names, "p-relay" in names, "p-relay"),
            check("initial member follows raw response position", (position, chosen),
                  expected_initial is not None and chosen == expected_initial, expected_initial),
            check("provider failover follows raw response order", actual_next,
                  expected_next is not None and actual_next == expected_next, expected_next)]
