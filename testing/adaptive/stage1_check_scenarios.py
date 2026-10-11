"""Сценарии A-D: настоящий трафик, события и неизменные окна acceptance."""
import threading
import time

import bench
import stage1_helpers as h

A0 = bench.address_port("reality", 0)


def comparison(stand, xray, work, name, mode, fault):
    stand.reset()
    marks = []
    traffic = h.OfferedTraffic(250 if fault == "burst" else 10)
    with h.Client(xray, h.config(mode), work, name) as client:
        h.prime(client)
        before = bench.chaos_stats()
        with h.Memory(client) as memory:
            traffic.start()
            try:
                time.sleep(2)
                began = time.time()
                if fault == "burst":
                    time.sleep(10)
                else:
                    ports = A0 if fault != "network" else ",".join(map(str, bench.all_address_ports()))
                    with h.Fault(ports, "blackhole" if fault in ("blackhole", "network") else "refuse") as injection:
                        time.sleep({"blackhole": 20, "refuse": 2, "network": 3}[fault])
                        injection.repair()
                        time.sleep(12)
                        marks = injection.marks
            finally:
                traffic.stop()
        ends = time.time()
        evs = h.events(client, since=began)
        downs = [e for e in evs if e["type"] == "path_state" and e["fields"].get("state") == "down"]
        switches = [e for e in evs if e["type"] == "switch" and e["fields"].get("reason") not in bench.START_REASONS]
        target_switch = next((e for e in switches if e["fields"].get("to") == "b1"), None)
        samples = [r for r in traffic.records if began <= r[0] < ends]
        metric = {"mode": mode, "scene": fault, "started_at": began,
                  "switch_delay_s": target_switch["ts"] / 1000 - began if target_switch else None,
                  "false_down": len(downs) if fault != "blackhole" else 0,
                  "downs": len(downs), "rescue": sum(e["type"] == "path_state" and
                    e["fields"].get("reason") == "rescued" for e in evs),
                  "switches": len(switches), "user_requests": len(samples),
                  "user_errors": sum(not r[1] for r in samples),
                  "peak_heap_bytes": max((s["heap_bytes"] for s in memory.samples), default=None),
                  "peak_rss_bytes": max((s["rss_bytes"] for s in memory.samples), default=None),
                  "memory_scope": "локальный Xray, iPhone измеряется отдельно",
                  "actual_tcp_offers": traffic.offers, "faults": marks,
                  "chaos_before": before, "chaos_after": bench.chaos_stats()}
        h.METRICS[name] = metric
        checks, report = h.report_checks(client)
        checks += [h.check("Сэмплы heap/RSS", len(memory.samples), len(memory.samples) >= 5 and
                           not memory.errors, memory.errors or ">=5, без ошибок"),
                   h.check("Пользовательский трафик", len(samples), len(samples) > 50, ">50 запросов"),
                   h.check("Прямой обход не использован", client.outbound_bytes("direct"),
                           client.outbound_bytes("direct") == (0, 0), "0/0")]
        if fault == "blackhole":
            checks += [h.check("Переключение с чёрной дыры", metric["switch_delay_s"],
                target_switch is not None and 0 < metric["switch_delay_s"] < 20, "0..20 с")]
        if fault == "network":
            checks += [h.check("NetworkChanged не вызывался", sum(e["type"] == "network_change" for e in evs),
                not any(e["type"] == "network_change" for e in evs), "0")]
        if fault == "burst":
            offers = [at for at in traffic.offers if began <= at < began + 10]
            checks += [h.check("Реально предложено TCP за 10 с", len(offers), 2475 <= len(offers) <= 2525,
                              "2500 +/-1%"),
                       h.check("Здоровая пачка не даёт down", len(downs), not downs, "0")]
        h.save(work, name + "-measurements", {"metrics": metric, "memory": memory.samples,
               "memory_errors": memory.errors, "events": evs, "report": report})
    traffic.save(work + "/" + name + "-traffic.json")
    return checks


def register_a(mode, fault):
    def run(stand, xray, work, name):
        return comparison(stand, xray, work, name, mode, fault)
    run.__name__ = "stage1_a_%s_%s" % (mode, fault)
    h.scenario(run)


for _mode in ("auto", "always"):
    for _fault in ("blackhole", "refuse", "network", "burst"):
        register_a(_mode, _fault)


def transient(client, ledger):
    traffic = h.OfferedTraffic(15).start()
    try:
        with h.Fault(A0, "refuse") as fault:
            began_mono = time.monotonic()
            down = h.wait_for(lambda: next((e for e in h.events(client, "path_state", "a1", since=fault.marks[0]["at"])
                if e["fields"].get("state") == "down"), None), 1.8)
            h.wait_until(began_mono + 2)
            fault.repair()
            rescued = h.wait_for(lambda: h.events(client, "path_state", "a1", "rescued", down["ts"] / 1000), 15)[0]
            ledger.extend(fault.marks)
        return down, rescued, traffic
    except Exception:
        traffic.stop()
        raise


def return_checks(client, down, rescued, timeout=210):
    switched = h.wait_for(lambda: next((e for e in h.events(client, "switch", since=rescued["ts"] / 1000)
                         if e["fields"].get("to") == "a1"), None), timeout)
    evs = h.events(client, until=switched["ts"] / 1000)
    away = next((e for e in evs if e["type"] == "switch" and e["fields"].get("to") == "b1"
                 and e["ts"] >= down["ts"]), None)
    passed = [e for e in evs if e["type"] == "probe_result" and e["fields"].get("outbound") == "a1"
              and e["fields"].get("reason") == "return" and e["fields"].get("is_ok") is True
              and e["ts"] >= rescued["ts"]]
    hold = (switched["ts"] - away["ts"]) / 1000 if away else None
    gaps = [(b["ts"] - a["ts"]) / 1000 for a, b in zip(passed, passed[1:])]
    return [h.check("Rescue выполнен аудитом", rescued["fields"].get("by"),
                    rescued["fields"].get("by") == "audit", "audit"),
            h.check("Нет возврата до minHoldTime", hold, hold is not None and hold >= 120, ">=120 с"),
            h.check("Возврат после трёх UP-проб", len(passed), len(passed) == 3, "3"),
            h.check("Интервал возвратных проб", gaps, len(gaps) == 2 and min(gaps) >= 19.9, ">=20 с")]


@h.scenario
def stage1_b_transient_rescue(stand, xray, work, name):
    ledger = []
    with h.Client(xray, h.config(slow_reserve=True), work, name) as client:
        h.prime(client)
        down, rescued, traffic = transient(client, ledger)
        try:
            rescued_path = h.path(client)
            checks = [h.check("Карантин снят", rescued_path.get("quarantine_ms", 0),
                             rescued_path.get("state") == "up" and rescued_path.get("quarantine_level", 0) == 0
                             and rescued_path.get("quarantine_ms", 0) == 0, "UP, level=0, ms=0")]
            checks += return_checks(client, down, rescued)
            more, report = h.report_checks(client)
            checks += more
            h.save(work, name + "-evidence", {"faults": ledger, "rescued_status": rescued_path,
                    "events": client.events, "report": report})
        finally:
            traffic.stop()
    return checks


@h.scenario
def stage1_b_blackhole_no_double_quarantine(stand, xray, work, name):
    with h.Client(xray, h.config(), work, name) as client:
        h.prime(client)
        traffic = h.OfferedTraffic().start()
        try:
            with h.Fault(A0, "blackhole") as fault:
                down = h.wait_for(lambda: next((e for e in h.events(client, "path_state", "a1")
                                   if e["fields"].get("state") == "down"), None))
                first = h.path(client)
                result = h.wait_for(lambda: h.events(client, "probe_result", "a1", "audit"), 20)[0]
                last = h.path(client)
                checks, report = h.report_checks(client)
                checks += [h.check("Аудит провалился", result["fields"].get("is_ok"),
                                   result["fields"].get("is_ok") is False, "false"),
                           h.check("Один down", len(h.events(client, "path_state", "a1", since=down["ts"] / 1000)),
                            len([e for e in h.events(client, "path_state", "a1") if e["fields"].get("state") == "down"]) == 1, "1"),
                           h.check("Карантин не вырос от аудита", last.get("quarantine_level"),
                            first.get("quarantine_level") == last.get("quarantine_level") == 1 and
                            last.get("quarantine_ms", 0) <= first.get("quarantine_ms", 0), "level=1, срок не продлён")]
                h.save(work, name + "-evidence", {"faults": fault.marks, "first": first,
                       "last": last, "result": result, "report": report})
        finally:
            traffic.stop()
    return checks


@h.scenario
def stage1_b_two_flaps(stand, xray, work, name):
    ledger = []
    with h.Client(xray, h.config(slow_reserve=True), work, name) as client:
        h.prime(client)
        down, rescued, traffic = transient(client, ledger)
        try:
            rescued_level = h.path(client).get("quarantine_level", 0)
            checks = return_checks(client, down, rescued)
            with h.Fault(A0, "refuse") as fault:
                t0 = time.monotonic()
                second = h.wait_for(lambda: next((e for e in h.events(client, "path_state", "a1", since=fault.marks[0]["at"])
                    if e["fields"].get("state") == "down"), None), 1.8)
                h.wait_until(t0 + 2)
                fault.repair()
                time.sleep(8)
                second_path = h.path(client)
                ledger.extend(fault.marks)
            checks += [h.check("Оба мерцания в penaltyWindow", (second["ts"] - down["ts"]) / 1000,
                 0 < second["ts"] - down["ts"] < 300000, "<300 с"),
                h.check("Второй down без аудита", len(h.events(client, "probe_start", "a1", "audit", second["ts"] / 1000)),
                 not h.events(client, "probe_start", "a1", "audit", second["ts"] / 1000), "0"),
                h.check("Карантин вырос от спасённого уровня", second_path.get("quarantine_level"),
                 second_path.get("state") == "down" and second_path.get("quarantine_level", 0) > rescued_level,
                 "> level после rescue"),
                h.check("Второй down не спасён", len(h.events(client, "path_state", "a1", "rescued", second["ts"] / 1000)),
                 not h.events(client, "path_state", "a1", "rescued", second["ts"] / 1000), "0")]
            more, report = h.report_checks(client)
            checks += more
            checks.append(h.check("Throttle аудита посчитан", report.get("audit_skipped_count"),
                                  h.strict_count(report.get("audit_skipped_count"), 1), "1"))
            h.save(work, name + "-evidence", {"faults": ledger, "second_status": second_path, "report": report})
        finally:
            traffic.stop()
    return checks


def queued_audit(stand, xray, work, name, change_network):
    cfg = h.config(schedule={"probesPerMinute": 1, "maxConcurrentProbes": 1})
    with h.Client(xray, cfg, work, name) as client:
        h.prime(client)
        traffic = h.OfferedTraffic().start()
        try:
            with h.Fault(A0, "refuse") as fault:
                down = h.wait_for(lambda: next((e for e in h.events(client, "path_state", "a1")
                                if e["fields"].get("state") == "down"), None))
                h.wait_for(lambda: h.events(client, "probe_result", "b1", "failover"))
                queued = client.status()
                changed_at = time.time() if change_network else None
                if change_network:
                    client.network("stage1-new-generation")
                wait_start = time.monotonic()
                h.wait_until(wait_start + 33)
                checks, report = h.report_checks(client)
                audits = h.events(client, "probe_start", "a1", "audit", down["ts"] / 1000)
                checks += [h.check("Аудит действительно ожидал бюджет", queued.get("probes_queued"),
                                  queued.get("probes_queued", 0) >= 1, ">=1"),
                           h.check("Аудит не запущен", len(audits), not audits, "0"),
                           h.check("Пропуск аудита посчитан", report.get("audit_skipped_count"),
                                   h.strict_count(report.get("audit_skipped_count"), 1), "1")]
                if change_network:
                    checks.append(h.check("Поколение сети изменилось", client.status().get("generation"),
                        client.status().get("generation", 0) > queued.get("generation", 0), "> прежнего"))
                else:
                    checks.append(h.check("Полное окно ожидания", time.monotonic() - wait_start,
                                          time.monotonic() - wait_start >= 33, ">33 с, auditWindow=30 с"))
                h.save(work, name + "-evidence", {"down": down, "queued": queued, "network_at": changed_at,
                       "faults": fault.marks, "report": report, "events": client.events})
        finally:
            traffic.stop()
    return checks


@h.scenario
def stage1_b_queue_expire(stand, xray, work, name):
    return queued_audit(stand, xray, work, name, False)


@h.scenario
def stage1_b_network_cancels(stand, xray, work, name):
    return queued_audit(stand, xray, work, name, True)


def c_config(before=False):
    cfg = h.config(health={"passive": {"dialTimeout": "3s"}})
    if not before:
        cfg["healthObservatory"]["passive"]["loneFailureAfter"] = "2m"
    else:
        cfg["healthObservatory"]["passive"].pop("loneFailureAfter", None)
        cfg["routing"]["balancers"][0]["strategy"]["settings"].pop("shouldSkipDownAudit", None)
    return cfg


@h.scenario
def stage1_c_dead_periodic_before_after(stand, xray, work, name):
    if not h.BASELINE:
        raise RuntimeError("Для C обязателен отдельный baseline binary 8b3d839f")
    checks = []
    for before, binary in ((True, h.BASELINE), (False, xray)):
        stand.reset()
        label = name + ("-before" if before else "-after")
        with (bench.Client if before else h.Client)(binary, c_config(before), work, label) as client:
            h.prime(client)
            time.sleep(122)
            ledger = []
            with h.Fault(A0, "refuse") as fault:
                started_mono, started = time.monotonic(), time.time()
                duration = 600 if before else 100
                due = started_mono
                while time.monotonic() < started_mono + duration:
                    h.wait_until(min(due, started_mono + duration))
                    if time.monotonic() >= started_mono + duration:
                        break
                    ledger.append(h.request(timeout=5))
                    due += 90
                h.wait_until(started_mono + duration)
                elapsed = time.monotonic() - started_mono
                evs = h.events(client, since=started)
                switches = [e for e in evs if e["type"] == "switch"]
                downs = [e for e in evs if e["type"] == "path_state" and e["fields"].get("state") == "down"]
                gaps = [b["at"] - a["at"] for a, b in zip(ledger, ledger[1:])]
                if before:
                    checks += [h.check("BEFORE: полные 10 мин", elapsed, elapsed >= 600, ">=600 с"),
                               h.check("BEFORE: отказ каждые 90 с", gaps,
                                       len(ledger) == 7 and all(89.5 <= g <= 90.5 for g in gaps)
                                       and all(not r["ok"] for r in ledger), "7 отказов, 90 +/-0.5 с"),
                               h.check("BEFORE: нет решений", (len(downs), len(switches)),
                                       not downs and not switches and h.active(client) == "a1", "0/0, a1")]
                    report = client.api("report")
                else:
                    results = h.events(client, "probe_result", "a1", "lone", started)
                    checks += [h.check("AFTER: down от первой одинокой пробы", (len(downs), len(results)),
                        len(downs) == 1 and len(results) == 1 and results[0]["fields"].get("is_ok") is False
                        and ledger and not ledger[0]["ok"] and downs[0]["ts"] / 1000 < started + 90,
                        "1 failed lone, 1 down до второго отказа"),
                        h.check("AFTER: failover", h.active(client), h.active(client) == "b1", "b1")]
                    more, report = h.report_checks(client)
                    checks += more
                h.save(work, label + "-evidence", {"window_s": elapsed, "requests": ledger,
                       "faults": fault.marks, "events": evs, "report": report})
    return checks


@h.scenario
def stage1_c_live_periodic(stand, xray, work, name):
    cfg = c_config()
    cfg["healthObservatory"]["schedule"]["maxConcurrentProbes"] = 1
    cfg["healthObservatory"]["probes"].append({"name": "guard", "destination":
        "http://probe.test:%s/hold/8" % bench.TARGET_PORT, "minBytes": 8,
        "timeout": "12s", "progressTimeout": "4s"})
    next(o for o in cfg["outbounds"] if o.get("tag") == "b1")["path"]["probe"] = "guard"
    with h.Client(xray, cfg, work, name) as client:
        h.prime(client)
        time.sleep(122)
        began_mono, began = time.monotonic(), time.time()
        requests, marks = [], []
        for index in range(7):
            h.wait_until(began_mono + 90 * index)
            guard_started = time.time()
            guard_errors = []
            def guard():
                try:
                    client.api("-t", "12", "probe", "auto", "b1", timeout=15)
                except Exception as exc:
                    guard_errors.append(repr(exc))
            guard_thread = threading.Thread(target=guard, daemon=True)
            guard_thread.start()
            h.wait_for(lambda: h.events(client, "probe_start", "b1", "host", guard_started), 5)
            h.wait_until(began_mono + 3 + 90 * index)
            with h.Fault(A0, "refuse") as fault:
                requests.append(h.request(timeout=4))
                fault.repair()
                marks.extend(fault.marks)
            guard_thread.join(15)
            if guard_thread.is_alive() or guard_errors:
                raise RuntimeError("Защитная проба b1 не завершилась: %r" % guard_errors)
            if index == 0:
                h.wait_for(lambda: h.events(client, "probe_result", "a1", "lone", began), 12)
        starts = h.events(client, "probe_start", "a1", "lone", began)
        results = h.events(client, "probe_result", "a1", "lone", began)
        downs = [e for e in h.events(client, "path_state", "a1", since=began) if e["fields"].get("state") == "down"]
        gaps = [(b["ts"] - a["ts"]) / 1000 for a, b in zip(starts, starts[1:])]
        request_gaps = [b["at"] - a["at"] for a, b in zip(requests, requests[1:])]
        checks, report = h.report_checks(client)
        checks += [h.check("LIVE: реальные интервалы отказов", request_gaps,
                           len(request_gaps) == 6 and all(89.5 <= gap <= 90.5 for gap in request_gaps),
                           "90 +/-0.5 с"),
                   h.check("Реальные редкие отказы", [r["ok"] for r in requests],
                           len(requests) == 7 and all(not r["ok"] for r in requests), "7 отказов"),
                   h.check("Одинокие ложные тревоги подтверждены UP", len(results),
                           len(results) >= 2 and all(e["fields"].get("is_ok") is True for e in results), ">=2 passed"),
                   h.check("Следующая lone не раньше 5 мин", gaps, len(gaps) >= 1 and min(gaps) >= 300,
                           ">=300 с"),
                   h.check("Нет down или переключений", (len(downs), h.active(client)),
                           not downs and h.active(client) == "a1" and not h.events(client, "switch", since=began), "0, a1")]
        h.save(work, name + "-evidence", {"requests": requests, "faults": marks,
               "events": client.events, "report": report})
    return checks


@h.scenario
def stage1_c_fresh_no_probe(stand, xray, work, name):
    with h.Client(xray, c_config(), work, name) as client:
        prime = h.prime(client)
        with h.Fault(A0, "refuse") as fault:
            failed = h.request(timeout=4)
            fault.repair()
        time.sleep(5)
        starts = h.events(client, "probe_start", "a1", "lone", prime["at"])
        checks, report = h.report_checks(client)
        checks += [h.check("Отказ после свежего успеха", failed["ok"], not failed["ok"], "false"),
                   h.check("Свежий успех подавил lone", len(starts), not starts, "0")]
        h.save(work, name + "-evidence", {"prime": prime, "failure": failed, "report": report})
    return checks


def wake(stand, xray, work, name, kind):
    cfg = h.config(health={"wake": {"freshness": "60s", "minInterval": "5m"}})
    with h.Client(xray, cfg, work, name) as client:
        prime = h.prime(client)
        if kind != "fresh":
            time.sleep(62)
        began = time.time()
        calls, user = [], None
        ports = A0 if kind == "dead" else ""
        context = h.Fault(ports, "refuse") if ports else None
        if context:
            context.__enter__()
        try:
            call_epoch = time.monotonic()
            for index in range(10 if kind == "rate" else 1):
                h.wait_until(call_epoch + index * 6)
                at = time.time()
                answer = client.api("present")
                calls.append({"at": at, "response": answer, "returned_at": time.time()})
            if kind == "rate":
                h.wait_until(call_epoch + 60)
            elif kind != "fresh":
                h.wait_for(lambda: h.events(client, "probe_result", "a1", "wake", began), 15)
            else:
                time.sleep(5)
            if kind == "dead":
                h.wait_for(lambda: h.active(client) == "b1", 15)
                user = h.request()
            results = h.events(client, "probe_result", "a1", "wake", began)
            starts = h.events(client, "probe_start", "a1", "wake", began)
            checks, report = h.report_checks(client, wake=True)
            checks += [h.check("UserPresent сразу возвращает пустой ответ", calls,
                all(c["response"] == {} and c["returned_at"] - c["at"] < 2 for c in calls), "{}, <2 с")]
            if kind == "fresh":
                checks += [h.check("Fresh: нет проб", len(starts), not starts, "0"),
                           h.check("Fresh: пропуск посчитан", report.get("wake_skipped_count"),
                                   h.strict_count(report.get("wake_skipped_count"), 1), "1")]
            else:
                checks += [h.check("Ровно одна wake-проба", len(starts), len(starts) == len(results) == 1, "1 start/result"),
                    h.check("Результат wake", results[0]["fields"] if results else None,
                        len(results) == 1 and results[0]["fields"].get("is_ok") is (kind != "dead"),
                        "failed" if kind == "dead" else "passed")]
                if kind != "dead":
                    checks.append(h.check("Живой путь остался выбран", h.active(client), h.active(client) == "a1", "a1"))
            if kind == "rate":
                checks.append(h.check("9 вызовов подавлены лимитом", report.get("wake_skipped_count"),
                                      h.strict_count(report.get("wake_skipped_count"), 9), "9"))
            if kind == "dead":
                switches = [e for e in h.events(client, "switch", since=began) if e["fields"].get("to") == "b1"]
                checks.append(h.check("Failover ДО первого пользовательского запроса", user,
                    len(switches) == 1 and user["ok"] and switches[0]["ts"] / 1000 < user["at"], "switch < request"))
            h.save(work, name + "-evidence", {"prime": prime, "calls": calls, "first_user": user,
                   "events": client.events, "report": report, "status": client.status()})
        finally:
            if context:
                context.__exit__()
    return checks


def register_d(kind):
    def run(stand, xray, work, name):
        return wake(stand, xray, work, name, kind)
    run.__name__ = "stage1_d_" + kind
    h.scenario(run)


for _kind in ("fresh", "alive", "dead", "rate"):
    register_d(_kind)
