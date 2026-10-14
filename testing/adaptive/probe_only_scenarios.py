"""Functional probe-only checks. Importing the module opens no sockets."""
import copy
import datetime
import json
import os
import subprocess
import threading
import time

import bench
import scenarios
from scenarios import check, scenario

MEMBERS = [("a", "reality", 0), ("b", "tcp_tls", 0), ("c", "ws", 0), ("d", "grpc", 0)]
DRAIN_SECONDS = 30
HOST_REUSE_SECONDS = 15


def fixture(name, real_tags, capacity, promote=True, delay=0, count=3):
    outbounds = bench.path_members(MEMBERS[:count], {tag: i + 1 for i, (tag, _, _) in enumerate(MEMBERS[:count])})
    for outbound in outbounds:
        bench.set_probe_only(outbound, outbound["tag"] not in real_tags)
    return {"protocol": 1, "version": name, "outbounds": outbounds,
            "_probeOnlyBench": {"generation": name, "capacity": capacity, "promote": promote,
                               "responseDelay": delay, "drainSeconds": DRAIN_SECONDS,
                               "probeUUID": bench.PROBE_UUID, "userUUID": bench.UUID}}


def client_config(document, threshold):
    real = [o["tag"] for o in document["outbounds"] if not o["path"]["isProbeOnly"]]
    prov = bench.provider_config(["direct"], triggers={"minHealthyOutbounds": threshold},
        limits={"minInterval": "1s", "exhaustedMinInterval": "1s", "maxRefreshesPerHour": 100},
        apply={"drainTimeout": f"{DRAIN_SECONDS}s"})
    prov["timeout"] = "8s"
    cfg = bench.client_config([], {}, providers=[prov], loglevel="info", strategy={
        "startOutbound": "p-" + real[0], "confirmWithProbe": "never", "shouldSkipDownAudit": True,
        "minHoldTime": "1m", "returnToCheaper": {"interval": "30m", "hasRandomPhase": False}}, health={
        "passive": {"failuresToSuspect": 1, "loneFailureAfter": "0s", "dialTimeout": "2s"},
        "report": {"interval": "0s", "quietShare": 1},
        "events": {"maxEvents": 4096, "maxBytes": 262144},
        "schedule": {"idleAfter": "1h", "maxConcurrentProbes": 1, "minProbeInterval": "1s",
                     "probeJitterFraction": 0.000001, "probesPerMinute": 120,
                     "probesPerServerNamePerMinute": 60,
                     "quarantine": {"initialDelay": "10m", "maxDelay": "10m", "jitterFraction": 0},
                     "hostProbes": {"reuseWindow": f"{HOST_REUSE_SECONDS}s"},
                     "probeOnly": {"keepVerified": 1, "recheckInterval": "15m"}},
        "probes": [{"name": "default", "steps": [
            {"name": "transport", "destinations": [f"http://probe.test:{bench.TARGET_PORT}/bytes/32768"],
             "minBytes": 32768, "timeout": "4s", "progressTimeout": "2s"},
            {"name": "transport-second", "destinations": [f"http://probe.test:{bench.TARGET_PORT}/bytes/8192"],
             "minBytes": 8192, "timeout": "4s", "progressTimeout": "2s"},
            {"name": "egress", "isOptional": True, "destinations": [f"http://dead.test:{bench.TARGET_PORT}/204"],
             "timeout": "2s", "progressTimeout": "1s"}]}]})
    for outbound in document["outbounds"]:
        item = copy.deepcopy(outbound)
        item["tag"] = "p-" + item["tag"]
        cfg["outbounds"].insert(-1, item)
    return cfg


def write_answer(stand, document):
    temporary = stand.provider_answer + ".tmp"
    with open(temporary, "w") as stream:
        json.dump(document, stream)
    os.replace(temporary, stand.provider_answer)


def active(status):
    return next((b.get("active") for b in status.get("balancers", []) if b.get("balancer") == "auto"), None)


def credential_access(path, since):
    entries = []
    with open(path, errors="replace") as stream:
        for line in stream:
            if " email: " not in line or " accepted " not in line:
                continue
            at = datetime.datetime.strptime(line[:26], "%Y/%m/%d %H:%M:%S.%f").timestamp()
            if at < since:
                continue
            destination = line.split(" accepted ", 1)[1].split(" ", 1)[0]
            entries.append({"at": at, "destination": destination,
                            "email": line.rsplit(" email: ", 1)[1].strip(), "raw": line.strip()})
    return entries


def inventory_check(body, tags):
    outbounds = body.get("outbounds", [])
    return ({o.get("tag") for o in outbounds} == set(tags)
            and all(type(o.get("isProbeOnly")) is bool for o in outbounds))


def candidate_passes(events):
    return [e for e in events if e["type"] == "probe_result"
            and e["fields"].get("reason") == "candidate" and e["fields"].get("is_ok") is True]


def probe_credential_leaks(access):
    permitted = f"tcp:probe.test:{bench.TARGET_PORT}"
    return [entry for entry in access if entry["email"] == bench.PROBE_EMAIL
            and entry["destination"] != permitted]


def exercise(stand, xray, work, name, real_tags, capacity, threshold, promote=True,
             delay=0, all_dead=False, demote=False, no_traffic=False):
    count = 4 if all_dead else 3
    document = fixture(name, real_tags, capacity, promote, delay, count)
    if name == "probe_only_one_slot":
        document["_probeOnlyBench"]["promoteOnExhausted"] = True
    write_answer(stand, document)
    cfg = client_config(document, threshold)
    if name == "probe_only_one_slot":
        cfg["healthObservatory"]["providers"][0]["triggers"].update(
            minHealthyOutbounds=0, shouldRefreshWhenExhausted=True)
    began = time.time()
    marks, statuses, errors, downloaded = {}, {}, [], {}
    checks = []
    duration = 42 if demote else 25
    client = None
    traffic = None
    worker = None
    real_now = []
    try:
        if all_dead:
            bench.chaos(",".join(str(bench.address_port(kind, index)) for tag, kind, index in MEMBERS[:count]
                                 if tag not in real_tags), "refuse")
        with bench.Client(xray, cfg, work, name) as client:
            traffic = bench.Traffic(rate=3, big=1, timeout=3, tls=False).start()
            epoch = time.monotonic()

            def until(seconds):
                remaining = epoch + seconds - time.monotonic()
                if remaining > 0:
                    time.sleep(remaining)

            def fault():
                marks["fault"] = time.time()
                bench.chaos(bench.address_port("reality", 0), "refuse")

            def download():
                downloaded["start"] = time.time()
                try:
                    downloaded["bytes"] = bench.download(bench.TARGETS[0], "/slow/8388608/18", 25, tls=False)
                except Exception as exc:
                    downloaded["error"] = repr(exc)
                downloaded["end"] = time.time()

            try:
                if demote:
                    until(2)
                    worker = threading.Thread(target=download, daemon=True)
                    worker.start()
                until(3)
                client.drain()
                statuses["before"] = client.status()
                if len(real_tags) >= threshold:
                    early = [e for e in client.events if e["type"] == "probe_start"
                             and e["fields"].get("reason") == "candidate"]
                    expected = count - len(real_tags) if all_dead else 1
                    checks.append(check("v2 поддерживает keepVerified независимо от minHealthy", len(early),
                                        len(early) == expected, f"{expected} maintenance probes"))
                if demote:
                    marks["demotion"] = time.time()
                    changed = fixture(name + "-demote", {"b"}, capacity, promote=False)
                    write_answer(stand, changed)
                    client.api("refresh", "fresh")
                elif real_tags == {"a", "b"} or no_traffic or name == "probe_only_one_slot":
                    fault()
                until(16)
                statuses["after"] = client.status()
                if promote:
                    real_now = [p["outbound"] for p in statuses["after"].get("paths", [])
                                if p.get("is_probe_only") is False and p["outbound"] not in {"p-" + t for t in real_tags}]
                    if real_now and name != "probe_only_one_slot":
                        traffic.paused.set()
                        time.sleep(HOST_REUSE_SECONDS + 1)
                        marks["full_probe"] = client.api("-t", "6", "probe", "auto", real_now[0], timeout=8)
                        traffic.paused.clear()
                if no_traffic:
                    marks["network"] = client.network("probe-only-unusable")
                    try:
                        client.api("pin", "auto", "p-c")
                    except RuntimeError as exc:
                        marks["pin"] = {"error": str(exc)}
                until(duration)
                if name == "probe_only_one_slot" and real_now:
                    traffic.stop()
                    marks["full_probe_traffic_stop_end"] = time.time()
                    marks["full_probe_reuse_window_seconds"] = HOST_REUSE_SECONDS
                    time.sleep(HOST_REUSE_SECONDS + 1)
                    marks["full_probe"] = client.api("-t", "6", "probe", "auto", real_now[0], timeout=8)
                statuses["final"] = client.status()
            except Exception as exc:
                errors.append(repr(exc))
            finally:
                traffic.stop()
                if worker is not None:
                    worker.join(2)
                    if worker.is_alive():
                        errors.append("long download did not finish")
                client.drain()
    finally:
        bodies = stand.provider_bodies(began)
        access = credential_access(stand.server_access_log, began)
        response_path = stand.provider_log + ".responses"
        responses = []
        if os.path.exists(response_path):
            with open(response_path) as stream:
                responses = [row for row in map(json.loads, stream) if row["at"] >= began]
        raw = {"began": began, "marks": marks, "statuses": statuses, "errors": errors,
               "provider_bodies": bodies, "provider_responses": responses, "credential_access": access,
               "events": client.events if client else [], "download": downloaded,
               "traffic": sorted(traffic.records) if traffic else [], "duration_seconds": duration,
               "elapsed_seconds": round(time.time() - began, 3)}
        with open(os.path.join(work, name + "-raw.json"), "w") as stream:
            json.dump(raw, stream, indent=1)
    events = client.events
    leaks = probe_credential_leaks(access)
    checks += [check("Сценарий завершился без исключений", errors, not errors, "[]"),
               check("Пробные данные доступа не получили пользовательских соединений", leaks, not leaks, "0"),
               check("Настоящие данные доступа получили пользовательский трафик", len(access),
                     any(a["email"] == bench.USER_EMAIL and any(t in a["destination"] for t in bench.TARGETS)
                         for a in access), ">0 user-UUID accepted")]
    min_bodies = [b["body"] for b in bodies if isinstance(b.get("body"), dict)
                  and b["body"].get("trigger") in ("min_healthy", "exhausted")]
    inventory_bodies = ([b["body"] for b in bodies if isinstance(b.get("body"), dict)
                         and b["body"].get("trigger") == "host"] if demote else min_bodies)
    tags = [tag for tag, _, _ in MEMBERS[:count]]
    fetches = [e["fields"] for e in events if e["type"] == "provider_fetch" and e["fields"].get("is_ok") is True]
    checks.append(check("Запрос провайдера прошёл напрямую", fetches,
                        bool(fetches) and all(f.get("via") == "direct" for f in fetches), "direct"))
    checks += [check("POST содержит весь набор с boolean isProbeOnly", len(inventory_bodies),
                     bool(inventory_bodies) and all(inventory_check(b, tags) for b in inventory_bodies), tags),
               check("Провайдер удержал заданное число настоящих слотов", [(r["realSlots"], r["capacity"]) for r in responses],
                     bool(responses) and all(r["realSlots"] <= capacity for r in responses), f"<= {capacity}")]
    passes = candidate_passes(events)
    checks.append(check("Проверки использовали отдельные пробные данные доступа", len(access),
                        not passes or any(a["email"] == bench.PROBE_EMAIL for a in access),
                        "probe UUID observed for candidate pass"))
    checks.append(check("Probe-only выполнил все mandatory и пропустил optional", [e["fields"] for e in passes],
        all([step.get("name") for step in e["fields"].get("steps", [])] == ["transport", "transport-second"]
            and all(step.get("is_passed") is True for step in e["fields"]["steps"])
            and e["fields"].get("state") == "up" for e in passes)
        and (bool(passes) if not all_dead else True), "2 mandatory passes, state up"))
    if promote:
        promotions = [r for r in responses if r.get("promoted")]
        proof = [b for b in min_bodies if any(o.get("isProbeOnly") is True and o.get("probe", {}).get("isPassed") is True
                                            for o in b.get("outbounds", []))]
        full = [e for e in events if e["type"] == "probe_result" and e["fields"].get("reason") == "host"
                and any(step.get("name") == "egress" for step in e["fields"].get("steps", []))]
        same_tag = bool(promotions) and any(e["fields"].get("outbound") == "p-" + promotions[0]["promoted"] for e in passes)
        if name == "probe_only_one_slot":
            first = min_bodies[0] if min_bodies else {}
            checked_pending = bool(promotions) and any(
                e["fields"].get("outbound") == "p-" + promotions[0]["promoted"] and
                e["ts"] / 1000 <= promotions[0]["at"] for e in passes)
            checks += [check("При исчерпании POST не ждёт нового кандидата", first,
                             first.get("balancer", {}).get("isExhausted") is True, "exhaustion bypass"),
                       check("Кандидат подтверждён до ответа с новым слотом", checked_pending,
                             checked_pending and same_tag, "candidate passed before delayed response")]
        else:
            checks.append(check("Промоушен под тем же тегом после подтверждённой пробы в POST",
                                [r.get("promoted") for r in promotions], same_tag and bool(proof),
                                "same tag, isPassed=true in POST"))
        checks += [check("Настоящий режим выполняет optional и получает degraded", [e["fields"] for e in full],
                         bool(full) and full[-1]["fields"].get("state") == "degraded", "egress attempted, degraded")]
        if capacity == 2:
            pending = [(row["ts"], r["at"]) for row in bodies for r in responses if r.get("promoted") and row["ts"] <= r["at"]]
            checks.append(check("Второй настоящий путь работал до ответа провайдера", pending,
                bool(pending) and any(ok and pending[0][0] <= at <= pending[0][1] for at, ok, *_ in traffic.records),
                "successful user request while POST pending"))
        else:
            recovered, failures = bench.recovery(traffic.records, marks.get("fault", began), streak=3)
            checks.append(check("Единственный слот восстановился; измерен перерыв", {"seconds": recovered, "failed": failures},
                                recovered is not None, "raw outage measurement, no performance threshold"))
        checks.append(check("Старый слот сохранён на drain после промоушена", [r["heldForDrain"] for r in responses],
                            any(r["heldForDrain"] for r in responses), "old slot lease >= drain"))
    elif name == "probe_only_capacity_one":
        provider = statuses.get("final", {}).get("providers", [{}])[0]
        checks += [check("При capacity=1 и пороге 2 ровно один min_healthy POST", len(min_bodies), len(min_bodies) == 1, "1"),
                   check("Проверенный адрес остаётся наготове", provider.get("probe_only_verified"),
                         provider.get("probe_only_verified") == 1, "1")]
    elif all_dead:
        last = min_bodies[-1] if min_bodies else {}
        probes = [o.get("probe", {}) for o in last.get("outbounds", []) if o.get("isProbeOnly") is True]
        starts = [e for e in events if e["type"] == "probe_start" and e["fields"].get("reason") == "candidate"]
        checks += [check("POST после проверки всех мёртвых кандидатов", probes,
                         len(probes) == 2 and all(p.get("isPassed") is False for p in probes), "2 failed probes"),
                   check("Мёртвые кандидаты не проверяются бесконечно", len(starts), len(starts) == 2, "2")]
    elif no_traffic:
        checks += [check("Исчерпанный балансировщик не выбрал probe-only", active(statuses.get("final", {})),
                         active(statuses.get("final", {})) == "", "empty"),
                   check("Pin кандидата отклонён", marks.get("pin"), "pin was not accepted" in marks.get("pin", {}).get("error", ""), "false")]
    if demote:
        demoted_at = marks.get("demotion", began)
        after = [e for e in events if e["ts"] / 1000 >= demoted_at and e["type"] in {"switch", "exhausted"}
                 and e["fields"].get("from") == "p-a"]
        recovered = [e for e in bench.switches(events, demoted_at) if e["fields"].get("to") == "p-b"]
        final = statuses.get("final", {})
        modes = {p["outbound"]: p.get("is_probe_only") for p in final.get("paths", [])}
        candidates = next((b.get("candidates") for b in final.get("balancers", [])
                           if b.get("balancer") == "auto"), None)
        drained = [e for e in events if e["type"] == "provider_drained" and e["fields"].get("outbound") == "p-a"]
        checks += [check("Выбранный путь после демоушена покинут",
                         {"departure": after, "recovery": recovered, "modes": modes, "candidates": candidates},
                         bool(after) and bool(recovered) and recovered[0]["ts"] >= after[0]["ts"]
                         and active(final) == "p-b" and modes.get("p-a") is True and modes.get("p-b") is False
                         and isinstance(candidates, list) and "p-a" not in candidates, "p-b, p-a probe-only excluded"),
                   check("Долгая загрузка завершилась после демоушена", downloaded,
                         downloaded.get("bytes") == 8388608 and downloaded.get("end", 0) > marks.get("demotion", began), "8 MiB complete"),
                   check("Старая версия не закрылась до конца существующей загрузки", drained,
                         bool(drained) and drained[0]["ts"] / 1000 >= downloaded.get("end", float("inf")) - .5,
                         "download complete before old handler closes")]
    return checks


@scenario()
def probe_only_two_slots(stand, xray, work, name):
    return exercise(stand, xray, work, name, {"a", "b"}, 2, 2, delay=2)


@scenario()
def probe_only_one_slot(stand, xray, work, name):
    return exercise(stand, xray, work, name, {"a"}, 1, 1, delay=5)


@scenario()
def probe_only_capacity_one(stand, xray, work, name):
    return exercise(stand, xray, work, name, {"a"}, 1, 2, promote=False)


@scenario()
def probe_only_demote(stand, xray, work, name):
    return exercise(stand, xray, work, name, {"a", "b"}, 2, 2, promote=False, demote=True)


@scenario()
def probe_only_all_dead(stand, xray, work, name):
    return exercise(stand, xray, work, name, {"a", "b"}, 2, 2, promote=False, all_dead=True)


@scenario()
def probe_only_no_traffic(stand, xray, work, name):
    return exercise(stand, xray, work, name, {"a"}, 1, 1, promote=False, no_traffic=True)


@scenario()
def probe_only_start_and_pin(stand, xray, work, name):
    cfg = bench.api_config(client_config(fixture(name, {"a", "b"}, 2, promote=False), 2))
    checks, outputs = [], []
    for field in ("startup.outbound", "startup.pin"):
        invalid = copy.deepcopy(cfg)
        settings = invalid["routing"]["balancers"][0]["strategy"]["settings"]
        startup = settings.setdefault("startup", {})
        startup.pop("outbound", None)
        if field == "startup.outbound":
            startup["outbound"] = "p-c"
        else:
            startup["pin"] = {"outbound": "p-c", "timeout": "0s"}
        path = os.path.join(work, name + "-" + field + ".json")
        with open(path, "w") as stream:
            json.dump(invalid, stream)
        result = subprocess.run([xray, "run", "-test", "-c", path], capture_output=True, text=True, timeout=10)
        output = result.stdout + result.stderr
        outputs.append({"field": field, "returncode": result.returncode, "output": output})
        checks.append(check("Ошибка загрузки " + field, output[-300:],
                            result.returncode != 0 and "startup" in output and
                            "p-c" in output and "probe-only" in output, "probe-only startup rejected"))
    with bench.Client(xray, cfg, work, name) as client:
        # Negative commands return the v2 Result envelope. Client.api unwraps
        # failures by raising, so inspect the real CLI envelope here instead.
        result = subprocess.run([xray, "api", "health", f"--server=127.0.0.1:{bench.API_PORT}",
                                 "pin", "auto", "p-c"], capture_output=True, text=True, timeout=10, check=True)
        response = json.loads(result.stdout)
        status = client.status()
    checks.append(check("Pin probe-only отклонён", response,
                        response.get("success") is False and
                        response.get("error", {}).get("code") == "invalid_argument" and
                        active(status) != "p-c", "invalid_argument; selection unchanged"))
    with open(os.path.join(work, name + "-raw.json"), "w") as stream:
        json.dump({"config_errors": outputs, "pin": response, "status": status, "events": client.events}, stream, indent=1)
    return checks
