"""Negative controls for mux, path ownership, report windows and AWG cold starts."""

import contextlib
import hashlib
import os
import re
import socket
import time

import bench
from udp_matrix import HELPER_PORTS, Ledger, SocksUDP, UDPOracle, reconcile_flows, save_json
from udp_matrix_topology import PathSwitchHop, destination_host, matrix_config


def _echo(channel, host, port=HELPER_PORTS["echo"]):
    payload = os.urandom(12)
    channel.send(host, port, payload)
    return channel.receive() == payload


def _source_access(path, flows):
    if not os.path.exists(path):
        return {"path": path, "by_source": {}, "error": "access log missing"}
    with open(path, errors="replace") as stream:
        lines = [line.rstrip("\n") for line in stream]
    by_source = {}
    for flow in flows:
        matching = [line for line in lines if f"{flow.source} " in line]
        by_source[flow.source] = {"count": len(matching), "lines": matching}
    return {"path": path, "by_source": by_source}


def _routed_starts(access, flows, tag):
    identities = {(flow.source, flow.first_destination) for flow in flows if flow.tag == tag}
    starts = []
    pattern = re.compile(r"(?:^| )from udp:(\S+) accepted udp:(\S+) \[socks -> ([^\]]+)\]$")
    for source, evidence in access.get("by_source", {}).items():
        for line in evidence["lines"]:
            match = pattern.search(line)
            if match and match[3] == tag and (match[1], match[2]) in identities and match[1] == source:
                starts.append({"source": match[1], "destination": match[2], "tag": tag, "raw": line})
    return starts


def mux_two_flows(stand, xray, work):
    config, tag = matrix_config("reality")
    outbound = next(item for item in config["outbounds"] if item.get("tag") == tag)
    outbound["mux"] = {"enabled": True, "concurrency": 8, "xudpConcurrency": 8,
                       "xudpProxyUDP443": "allow"}
    ledger = Ledger(os.path.join(work, "mux-ledger.json"))
    port = bench.address_port("reality", 0)
    with bench.Client(xray, config, work, "mux-two-flows") as client:
        before = bench.chaos_stats()[port][0]
        with contextlib.ExitStack() as stack:
            first = stack.enter_context(SocksUDP(ledger, tag, "mux-first"))
            second = stack.enter_context(SocksUDP(ledger, tag, "mux-second"))
            first_ok = _echo(first, "127.0.0.1")
            second_ok = _echo(second, "127.0.0.1")
            workers_during = bench.chaos_stats()[port][0] - before
        after = bench.chaos_stats()[port][0] - before
    comparison = reconcile_flows(ledger.flows, client.final_report or {}, tag)
    result = {"first_ok": first_ok, "second_ok": second_ok,
              "physical_connections_during": workers_during,
              "physical_connections_after": after, "udp": comparison,
              "same_physical_worker_proved": workers_during == 1 and first_ok and second_ok,
              "ok": workers_during == 1 and first_ok and second_ok and comparison["ok"]}
    save_json(os.path.join(work, "mux-two-flows.json"), result)
    return result


def reconnect_flow(stand, xray, work):
    config, tag = matrix_config("reality")
    outbound = next(item for item in config["outbounds"] if item.get("tag") == tag)
    outbound["mux"] = {"enabled": True, "concurrency": 8, "xudpConcurrency": 8,
                       "xudpProxyUDP443": "allow"}
    access_path = os.path.join(work, "mux-reconnect-client-access.log")
    config["log"]["access"] = access_path
    ledger = Ledger(os.path.join(work, "mux-reconnect-ledger.json"))
    port = bench.address_port("reality", 0)
    with bench.Client(xray, config, work, "mux-reconnect") as client:
        before = bench.chaos_stats()[port][0]
        with SocksUDP(ledger, tag, "mux-reconnect") as channel:
            first_ok = _echo(channel, "127.0.0.1")
            fault = {"arm_started_ns": time.monotonic_ns(), "echo_ok": None, "error": None}
            try:
                bench.chaos(str(port), "reset_after:1")
                fault["armed_ns"] = time.monotonic_ns()
                fault["send_started_ns"] = time.monotonic_ns()
                try:
                    fault["echo_ok"] = _echo(channel, "127.0.0.1")
                except (OSError, socket.timeout) as error:
                    fault["error"] = repr(error)
                finally:
                    fault["attempt_ended_ns"] = time.monotonic_ns()
            finally:
                fault["restore_started_ns"] = time.monotonic_ns()
                bench.chaos(str(port), "pass")
                fault["restored_ns"] = time.monotonic_ns()
            recovered = False
            for _ in range(5):
                try:
                    if _echo(channel, "127.0.0.1"):
                        recovered = True
                        break
                except (OSError, socket.timeout):
                    pass
                time.sleep(1)
        workers = bench.chaos_stats()[port][0] - before
    access = _source_access(access_path, ledger.flows)
    starts = _routed_starts(access, ledger.flows, tag)
    comparison = reconcile_flows(ledger.flows, client.final_report or {}, tag, routed_starts=starts)
    result = {"first_ok": first_ok, "recovered": recovered, "physical_connections": workers,
              "fault": fault, "source_access": access, "routed_starts": starts,
              "association_count": len(ledger.flows), "routed_start_count": len(starts),
              "udp": comparison, "ok": first_ok and recovered and len(ledger.flows) == 1 and
              workers >= 2 and len(starts) >= 2 and
              comparison["actual"].get("udp_flows_unanswered") == 0 and comparison["ok"]}
    save_json(os.path.join(work, "mux-reconnect.json"), result)
    return result


def path_switch_old_flow(stand, xray, work):
    config = bench.client_config([("old", "reality", 0), ("new", "reality", 1)],
                                 {"old": 1, "new": 2}, health={"report": {"quietShare": 1}},
                                 loglevel="info")
    ledger = Ledger(os.path.join(work, "path-switch-ledger.json"))
    config["log"]["access"] = os.path.join(work, "path-switch-client-access.log")
    for tag in ("old", "new"):
        outbound = next(item for item in config["outbounds"] if item.get("tag") == tag)
        outbound["settings"]["vnext"][0].update(address="127.0.0.1", port=HELPER_PORTS[f"switch_{tag}"])
    tokens = {name: os.urandom(24) for name in ("old_before", "old_after", "new_after")}
    targets = {}
    with contextlib.ExitStack() as stack:
        for tag in ("old", "new"):
            targets[tag] = stack.enter_context(UDPOracle(HELPER_PORTS[f"switch_{tag}_echo"], "echo",
                                                        os.path.join(work, f"switch-{tag}-target.json")))
        stack.enter_context(PathSwitchHop(xray, work))
        with bench.Client(xray, config, work, "path-switch-udp") as client:
            with SocksUDP(ledger, "old", "old-active") as old:
                old.send("127.0.0.1", HELPER_PORTS["echo"], tokens["old_before"])
                old_before = old.receive() == tokens["old_before"]
                client.api("pin", "auto", "new")
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline and client.status()["balancers"][0]["active"] != "new":
                    time.sleep(0.1)
                switched = client.status()["balancers"][0]["active"] == "new"
                with SocksUDP(ledger, "new", "new-after-switch") as new:
                    new.send("127.0.0.1", HELPER_PORTS["echo"], tokens["new_after"])
                    new_ok = new.receive() == tokens["new_after"]
                active_before_old_after = client.status()["balancers"][0]["active"]
                old_after_sent_ns = time.monotonic_ns()
                old.send("127.0.0.1", HELPER_PORTS["echo"], tokens["old_after"])
                old_after = old.receive() == tokens["old_after"]
                active_after_old_after = client.status()["balancers"][0]["active"]
    proof = {}
    for name, token in tokens.items():
        proof[name] = {tag: [receipt for receipt in target.receipts
                             if receipt["payload_sha256"] == hashlib.sha256(token).hexdigest()]
                       for tag, target in targets.items()}
    old_after_route = (len(proof["old_after"]["old"]) == 1 and not proof["old_after"]["new"] and
                       proof["old_after"]["old"][0]["when_ns"] >= old_after_sent_ns)
    report = client.final_report or {}
    old_count = report.get("paths", {}).get("old", {}).get("udp_flows")
    new_count = report.get("paths", {}).get("new", {}).get("udp_flows")
    result = {"old_before": old_before, "old_after": old_after, "new_ok": new_ok,
              "switched": switched, "old_udp_flows": old_count, "new_udp_flows": new_count,
              "tokens_hex": {name: token.hex() for name, token in tokens.items()},
              "old_after_sent_ns": old_after_sent_ns, "independent_route_receipts": proof,
              "active_before_old_after": active_before_old_after, "active_after_old_after": active_after_old_after,
              "old_after_old_outbound_proved": old_after_route,
              "ok": all((old_before, old_after, new_ok, switched, old_after_route)) and
              active_before_old_after == active_after_old_after == "new" and
              len(proof["old_before"]["old"]) == len(proof["new_after"]["new"]) == 1 and
              not proof["old_before"]["new"] and not proof["new_after"]["old"] and
              old_count == new_count == 1}
    save_json(os.path.join(work, "path-switch-udp.json"), result)
    return result


def cross_report_window(stand, xray, work):
    tag, reserve = "matrix-reality", "matrix-reality-reserve"
    config = bench.client_config([(tag, "reality", 0), (reserve, "reality", 1)],
                                 {tag: 1, reserve: 2},
                                 health={"report": {"quietShare": 1, "interval": "1s"}},
                                 loglevel="info")
    access_path = os.path.join(work, "cross-window-client-access.log")
    config["log"]["access"] = access_path
    ledger = Ledger(os.path.join(work, "cross-window-ledger.json"))
    with bench.Client(xray, config, work, "cross-window-udp") as client:
        with SocksUDP(ledger, tag, "cross-window-open") as channel:
            opened = _echo(channel, "127.0.0.1")
            initial_status = client.status()
            client.drain()
            event_start = len(client.events)
            pin = client.api("pin", "auto", reserve)
            first_report, pin_events, switched = None, [], False
            selected_status = initial_status
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                selected_status = client.status()
                switched = any(item.get("balancer") == "auto" and item.get("active") == reserve
                               for item in selected_status.get("balancers", []))
                client.drain()
                pin_events = [event for event in client.events[event_start:]
                              if event.get("type") == "switch" and
                              event.get("fields", {}).get("reason") == "pin" and
                              event["fields"].get("from") == tag and event["fields"].get("to") == reserve]
                if pin.get("ok") is True and switched and pin_events:
                    report = client.take_report()
                    if report and report.get("report_reason") == "interval":
                        first_report = report
                        break
                time.sleep(0.1)
            snapshot = client.api("report")
            still_open = _echo(channel, "127.0.0.1")
    final = client.final_report
    first_count = (first_report or {}).get("paths", {}).get(tag, {}).get("udp_flows")
    final_count = (final or {}).get("paths", {}).get(tag, {}).get("udp_flows")
    started_on_old = any(item.get("balancer") == "auto" and item.get("active") == tag
                         for item in initial_status.get("balancers", []))
    result = {"opened": opened, "still_open": still_open, "first_report": first_report,
              "snapshot": snapshot, "final_report": final, "first_flows": first_count,
              "final_flows": final_count, "pin": pin, "pin_switch_events": pin_events,
              "initial_status": initial_status, "selected_status": selected_status,
              "source_access": _source_access(access_path, ledger.flows),
              "ok": opened and still_open and started_on_old and pin.get("ok") is True and
              switched and bool(pin_events) and first_report is not None and
              first_report.get("report_index") == 1 and first_count == 0 and final_count == 1 and
              (final or {}).get("report_reason") == "stop" and (final or {}).get("report_index") == 2 and
              (snapshot or {}).get("report_reason") == "snapshot" and
              (snapshot or {}).get("report_index") == 2}
    save_json(os.path.join(work, "cross-window-udp.json"), result)
    return result


def awg_cold_case(stand, xray, work, answered):
    config, tag = matrix_config("awg")
    outbound = next(item for item in config["outbounds"] if item.get("tag") == tag)
    outbound["path"]["isLazy"] = True
    name = "awg-cold-answer" if answered else "awg-cold-unanswered"
    ledger = Ledger(os.path.join(work, f"{name}-ledger.json"))
    before = bench.chaos_stats()[bench.AWG_ADDRESS][3]
    with bench.Client(xray, config, work, name) as client:
        prior_to_user = bench.chaos_stats()[bench.AWG_ADDRESS][3] - before
        with SocksUDP(ledger, tag, name, controlled_no_reply=not answered) as channel:
            target_port = HELPER_PORTS["echo"] if answered else HELPER_PORTS["blackhole"]
            payload = os.urandom(12)
            channel.send(destination_host("awg"), target_port, payload)
            try:
                response = channel.receive(timeout=5)
                got_answer = response == payload
            except socket.timeout:
                got_answer = False
        outer_packets = bench.chaos_stats()[bench.AWG_ADDRESS][3] - before
    comparison = reconcile_flows(ledger.flows, client.final_report or {}, tag)
    result = {"name": name, "prior_to_user_outer_packets": prior_to_user,
              "outer_packets_after_user": outer_packets, "answered": got_answer,
              "udp": comparison, "cold_proved": prior_to_user == 0,
              "ok": prior_to_user == 0 and outer_packets > 0 and got_answer == answered and comparison["ok"]}
    save_json(os.path.join(work, f"{name}.json"), result)
    return result
