"""Общие измерения стенда этапа 1. Импорт не открывает сокеты и не запускает процессы."""
import collections
import json
import os
import subprocess
import threading
import time

import bench

ALL = []
BASELINE = None
METRICS = {}
CURRENT = None


def check(name, value, ok, limit):
    return {"check": name, "value": value, "ok": bool(ok), "limit": limit}


def scenario(fn):
    ALL.append(fn)
    return fn


def save(work, name, data):
    with open(os.path.join(work, name + ".json"), "w") as stream:
        json.dump(data, stream, indent=2)


def events(client, typ=None, tag=None, reason=None, since=0, until=float("inf")):
    client.drain()
    return [e for e in client.events if since <= e["ts"] / 1000 <= until
            and (typ is None or e["type"] == typ)
            and (tag is None or e["fields"].get("outbound") == tag)
            and (reason is None or e["fields"].get("reason") == reason)]


def path(client, tag="a1"):
    return next((p for p in client.status().get("paths", []) if p["outbound"] == tag), {})


def active(client):
    return next((b.get("active") for b in client.status().get("balancers", [])
                 if b.get("balancer") == "auto"), None)


def wait_for(predicate, timeout=20):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise RuntimeError("Условие не выполнено за %.1f с" % timeout)


def wait_until(deadline):
    while time.monotonic() < deadline:
        time.sleep(min(0.2, max(0, deadline - time.monotonic())))


def request(host=None, timeout=8, path="/bytes/32768"):
    host = host or bench.TARGETS[0]
    began = time.time()
    stream = None
    ok, size, error = False, 0, None
    try:
        stream = bench.open_stream(host, timeout, tls=False)
        if stream is None:
            error = "socks_refused"
        else:
            stream.sendall(("GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n" % (path, host)).encode())
            head, size = bench.read_answer(stream, began + timeout)
            ok = head.startswith(b"HTTP/1.1 200") and size == 32768
            if not ok:
                error = repr(head[:80])
    except OSError as exc:
        error = repr(exc)
    finally:
        if stream:
            stream.close()
    finished = time.time()
    if ok and CURRENT is not None:
        CURRENT.proofs.append({"at": finished, "outbound": active(CURRENT), "kind": "traffic"})
    return {"at": began, "finished_at": finished, "ok": ok, "bytes": size, "error": error}


def prime(client):
    sample = request()
    if not sample["ok"] or active(client) != "a1":
        raise RuntimeError("Первый реальный запрос не прошёл через a1: %r" % sample)
    return sample


def config(mode="auto", schedule=None, strategy=None, health=None, slow_reserve=False):
    h = {"passive": {"loneFailureAfter": "0s", "dialTimeout": "3s"},
         "report": {"interval": "0s", "quietShare": 1},
         "events": {"maxEvents": 4096, "maxBytes": 262144},
         "schedule": {"idleAfter": "1h", "minProbeInterval": "1s",
                      "probeJitterFraction": 1e-6, "quarantine": {
                          "initialDelay": "10m", "maxDelay": "30m", "multiplier": 2,
                          "jitterFraction": 0}}}
    h["schedule"].update(schedule or {})
    h.update(health or {})
    s = {"confirmWithProbe": mode, "shouldSkipDownAudit": False, "minHoldTime": "2m",
         "returnToCheaper": {"interval": "30m", "hasRandomPhase": False,
                            "probesToReturn": 3, "probeInterval": "20s", "penaltyWindow": "5m"}}
    s.update(strategy or {})
    cfg = bench.client_config([("a1", "reality", 0), ("b1", "tcp_tls", 0)],
                             {"a1": 1, "b1": 3}, health=h, strategy=s, loglevel="info")
    if slow_reserve:
        cfg["healthObservatory"]["probes"].append({"name": "reserve", "destination":
            "http://probe.test:%s/hold/4" % bench.TARGET_PORT, "minBytes": 4,
            "timeout": "8s", "progressTimeout": "4s"})
        next(o for o in cfg["outbounds"] if o.get("tag") == "b1")["path"]["probe"] = "reserve"
    return cfg


class Fault:
    def __init__(self, ports, mode):
        self.ports = str(ports)
        self.mode = mode
        self.marks = []

    def __enter__(self):
        bench.chaos(self.ports, self.mode)
        self.marks.append({"at": time.time(), "mode": self.mode, "ports": self.ports})
        if CURRENT is not None:
            CURRENT.faults.append(dict(self.marks[-1]))
        return self

    def repair(self):
        bench.chaos(self.ports, "pass")
        self.marks.append({"at": time.time(), "mode": "pass", "ports": self.ports})
        if CURRENT is not None:
            CURRENT.faults.append(dict(self.marks[-1]))

    def __exit__(self, *exc):
        self.repair()


class OfferedTraffic(bench.Traffic):
    """Абсолютное расписание фиксирует фактически предложенную частоту TCP."""
    def __init__(self, rate=10):
        super().__init__(rate=rate, big=1, timeout=6, tls=False)
        self.offers = []
        self.workers = []

    def _run(self):
        epoch = time.monotonic()
        index = 0
        while not self._stop.is_set():
            due = epoch + index / self.rate
            if self._stop.wait(max(0, due - time.monotonic())):
                break
            self.offers.append(time.time())
            worker = threading.Thread(target=self._one,
                args=(self.targets[index % len(self.targets)], "/bytes/32768", 32768), daemon=True)
            self.workers.append(worker)
            worker.start()
            index += 1

    def stop(self):
        self._stop.set()
        self.thread.join(10)
        deadline = time.monotonic() + self.timeout + 2
        for worker in self.workers:
            worker.join(max(0, deadline - time.monotonic()))
        if any(w.is_alive() for w in self.workers):
            raise RuntimeError("Незавершённые пользовательские запросы")


class Memory:
    def __init__(self, client):
        self.client = client
        self.samples, self.errors = [], []
        self.stop_event = threading.Event()

    def __enter__(self):
        if bench.ADB_SERIAL:
            raise RuntimeError("RSS стенда рассчитан на локальный процесс; iPhone нужен отдельно")
        def sample():
            while not self.stop_event.is_set():
                try:
                    heap, _ = self.client.memory()
                    pid = self.client.process.proc.pid
                    result = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)],
                                            capture_output=True, text=True, check=True, timeout=3)
                    self.samples.append({"at": time.time(), "heap_bytes": heap,
                                         "rss_bytes": int(result.stdout.strip()) * 1024})
                except Exception as exc:
                    self.errors.append(repr(exc))
                self.stop_event.wait(0.5)
        self.thread = threading.Thread(target=sample, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop_event.set()
        self.thread.join(5)



GLOBAL_FIELDS = ("audit_count", "audit_passed_count", "audit_skipped_count", "lone_probe_count",
                 "lone_probe_failed_count", "wake_probe_count", "wake_probe_failed_count",
                 "wake_skipped_count", "path_degraded_count")
PATH_FIELDS = ("audits", "audits_passed", "lone_probes", "lone_probes_failed", "downs_strong",
               "downs_weak", "degraded_ms", "udp_flows", "udp_flows_unanswered", "udp_bytes_up",
               "udp_bytes_down", "udp_writes", "udp_reads", "udp_first_reply_p50_ms")


def strict_count(value, expected):
    return type(value) is int and value >= 0 and value == expected


def strength_decisions(history, faults):
    suspects, suspect_at, results, decisions, last_down, states = {}, {}, {}, [], {}, {}
    selected, network_at = {}, float("-inf")
    for event in history:
        fields, typ = event["fields"], event["type"]
        tag = fields.get("outbound")
        if typ == "switch":
            selected[fields.get("balancer", "")] = fields.get("to")
        elif typ == "exhausted":
            selected[fields.get("balancer", "")] = ""
        if typ == "network_change" and fields.get("is_ignored") is not True:
            states.clear()
            last_down.clear()
            suspects.clear()
            suspect_at.clear()
            results.clear()
            network_at = event["ts"] / 1000
        if typ == "probe_result" and fields.get("is_stale") is not True:
            results[tag] = event
        if typ != "path_state":
            continue
        if fields.get("state") == "suspect":
            if type(fields.get("is_strong")) is not bool:
                raise ValueError("У подозрения нет наблюдаемой силы улик")
            suspects[tag] = fields["is_strong"]
            suspect_at[tag] = event["ts"] / 1000
        if fields.get("state") != "down":
            states[tag] = fields.get("state")
            continue
        down_key = (fields.get("quarantine_level"), fields.get("is_provisional", False))
        if states.get(tag) == "down" and last_down.get(tag) == down_key:
            continue
        states[tag], last_down[tag] = "down", down_key
        strong = None
        if fields.get("reason") == "traffic":
            inactive = bool(selected) and all(value is not None for value in selected.values()) and tag not in selected.values()
            strong = True if inactive else suspects.get(tag)
            expected_port = bench.address_port("reality" if tag.endswith("a1") else "tcp_tls", 0)
            actual = [f for f in faults if max(network_at, suspect_at.get(tag, float("-inf"))) <= f["at"] <= event["ts"] / 1000
                      and str(expected_port) in f["ports"].split(",") and f["mode"] in ("refuse", "blackhole")]
            if actual:
                strong = True
            if strong is None:
                raise ValueError("Down по трафику без события suspect или фактического отказа пути")
        elif fields.get("reason") == "probe":
            result = results.get(tag)
            if result is None or result["fields"].get("is_ok") is not False:
                raise ValueError("Down от пробы без наблюдаемого failed результата")
            reason = result["fields"].get("reason")
            if reason == "lone":
                strong = True
            elif reason == "confirm":
                if tag not in suspects:
                    raise ValueError("Провал confirm без наблюдаемой силы улик")
                strong = suspects[tag]
        if strong is not None:
            decisions.append({"seq": event["seq"], "outbound": tag, "strong": strong})
    return decisions


def audit_skips(history, context, end_at):
    settings = context["strategy"]
    if settings.get("confirmWithProbe", "auto") != "auto":
        return []
    strength = {d["seq"]: d["strong"] for d in strength_decisions(history, context["faults"])}
    penalty = context["penalty_seconds"]
    selected = None
    starts = [e for e in history if e["type"] == "probe_start" and e["fields"].get("reason") == "audit"]
    skipped = []
    for event in history:
        fields, at = event["fields"], event["ts"] / 1000
        if event["type"] == "switch":
            selected = fields.get("to")
        if event["type"] != "path_state" or fields.get("state") != "down" or fields.get("reason") != "traffic":
            continue
        tag = fields.get("outbound")
        if tag != selected or not strength.get(event["seq"]):
            continue
        previous = [e for e in starts if e["fields"].get("outbound") == tag and e["seq"] < event["seq"]]
        if settings.get("shouldSkipDownAudit") is True or (previous and at - previous[-1]["ts"] / 1000 < penalty):
            skipped.append(at)
            continue
        next_down = next((e for e in history if e["seq"] > event["seq"] and e["type"] == "path_state"
                          and e["fields"].get("outbound") == tag and e["fields"].get("state") == "down"), None)
        following = next((e for e in starts if e["fields"].get("outbound") == tag and e["seq"] > event["seq"]
                          and (next_down is None or e["seq"] < next_down["seq"])), None)
        network = next((e for e in history if e["seq"] > event["seq"] and e["type"] == "network_change"), None)
        cancel = network["ts"] / 1000 if network else float("inf")
        if following and following["ts"] / 1000 <= min(at + 30, cancel):
            continue
        expires = min(at + 30.001, cancel)
        if expires <= end_at:
            skipped.append(expires)
    return skipped


def degraded_range(tag, history, start_bounds, end_bounds):
    intervals, since = [], None
    for event in history:
        if event["type"] == "network_change" and event["fields"].get("is_ignored") is not True:
            if since is not None:
                intervals.append((since, event["ts"] / 1000))
                since = None
            continue
        if event["type"] != "path_state" or event["fields"].get("outbound") != tag:
            continue
        state = event["fields"].get("state")
        at = event["ts"] / 1000
        if state == "degraded" and since is None:
            since = at
        elif state != "degraded" and since is not None:
            intervals.append((since, at))
            since = None
    if since is not None:
        intervals.append((since, float("inf")))
    lower = sum(max(0, min(end_bounds[0], end - .001) - max(start_bounds[1], began + .001))
                for began, end in intervals)
    upper = sum(max(0, min(end_bounds[1], end + .001) - max(start_bounds[0], began - .001))
                for began, end in intervals)
    if upper == 0:
        return 0, 0
    return max(0, int(lower * 1000) - 2), int(upper * 1000) + 2


def window_checks(report, history, window, context, start_bounds, end_bounds, required_paths):
    if not isinstance(report, dict):
        return [check("Отчёт существует", report, False, "JSON object")]
    def count(typ, **fields):
        return sum(e["type"] == typ and all(e["fields"].get(k) == v for k, v in fields.items()) for e in window)
    skips = audit_skips(history, context, end_bounds[1])
    audit_skipped = sum(start_bounds[0] <= at <= end_bounds[1] for at in skips)
    wake_skipped = sum(a.get("skip") is True and start_bounds[0] <= a["at"] <= end_bounds[1]
                       for a in context["present_actions"])
    wanted = {"audit_count": count("probe_start", reason="audit"),
              "audit_passed_count": count("path_state", reason="rescued", by="audit"),
              "audit_skipped_count": audit_skipped,
              "lone_probe_count": count("probe_start", reason="lone"),
              "lone_probe_failed_count": count("probe_result", reason="lone", is_ok=False),
              "wake_probe_count": count("probe_start", reason="wake"),
              "wake_probe_failed_count": count("probe_result", reason="wake", is_ok=False),
              "wake_skipped_count": wake_skipped,
              "path_degraded_count": count("path_state", state="degraded")}
    checks = [check("Счётчик " + key, report.get(key), strict_count(report.get(key), value), value)
              for key, value in wanted.items()]
    checks.append(check("is_quiet_sample: boolean", report.get("is_quiet_sample"),
                        type(report.get("is_quiet_sample")) is bool, "bool"))
    checks.append(check("Число событий своего окна", report.get("events"),
                        strict_count(report.get("events"), len(window)), len(window)))
    for key in ("report_index", "window_ms", "session_ms"):
        checks.append(check(key + ": тип", report.get(key), type(report.get(key)) is int and report[key] >= 0, "int >=0"))
    rows = report.get("paths")
    checks.append(check("Все пути присутствуют в отчёте", sorted(rows) if isinstance(rows, dict) else rows,
                        isinstance(rows, dict) and bool(required_paths) and set(required_paths) <= set(rows), sorted(required_paths)))
    if not isinstance(rows, dict):
        return checks
    decisions = strength_decisions(history, context["faults"])
    seqs = {e["seq"] for e in window}
    for tag, path_report in rows.items():
        for key in PATH_FIELDS:
            checks.append(check(tag + "." + key + ": тип", path_report.get(key),
                                type(path_report.get(key)) is int and path_report[key] >= 0, "int >=0"))
        expected = {"audits": count("probe_start", outbound=tag, reason="audit"),
                    "audits_passed": count("path_state", outbound=tag, reason="rescued", by="audit"),
                    "lone_probes": count("probe_start", outbound=tag, reason="lone"),
                    "lone_probes_failed": count("probe_result", outbound=tag, reason="lone", is_ok=False),
                    "downs_strong": sum(d["seq"] in seqs and d["outbound"] == tag and d["strong"] is True for d in decisions),
                    "downs_weak": sum(d["seq"] in seqs and d["outbound"] == tag and d["strong"] is False for d in decisions)}
        if context["tcp_only"]:
            checks.append(check(tag + ".udp_ports: TCP-сцена без UDP", path_report.get("udp_ports", {}),
                                path_report.get("udp_ports", {}) == {}, "потоков UDP не предлагалось"))
            expected.update({key: 0 for key in PATH_FIELDS if key.startswith("udp_")})
        else:
            checks.append(check("Измерения UDP заданы", False, False, "этот стенд предлагает только TCP"))
        for key, value in expected.items():
            checks.append(check(tag + "." + key, path_report.get(key), strict_count(path_report.get(key), value), value))
        totals = collections.OrderedDict()
        for event in window:
            if event["type"] == "probe_result" and event["fields"].get("outbound") == tag:
                for step in event["fields"].get("steps", []):
                    row = totals.setdefault(step["name"], {"name": step["name"], "passed": 0, "failed": 0})
                    row["passed" if step["is_passed"] else "failed"] += 1
        wanted_steps = list(totals.values())
        got_steps = path_report.get("probe_steps")
        valid_steps = isinstance(got_steps, list) and got_steps == wanted_steps and all(
            isinstance(row, dict) and set(row) == {"name", "passed", "failed"}
            and type(row.get("passed")) is int and type(row.get("failed")) is int for row in got_steps)
        checks.append(check(tag + ".probe_steps: окно, числа и порядок", got_steps, valid_steps, wanted_steps))
        lower, upper = degraded_range(tag, history, start_bounds, end_bounds)
        measured = path_report.get("degraded_ms")
        checks.append(check(tag + ".degraded_ms: своё окно", measured,
            type(measured) is int and lower <= measured <= upper, [lower, upper]))
    return checks


class Client(bench.Client):
    """Один потребитель окон; журнал действий и границы API нужны для независимой сверки."""
    def __init__(self, xray, cfg, work, name, **kwargs):
        super().__init__(xray, cfg, work, name, **kwargs)
        self.cfg = cfg
        self.proofs, self.faults, self.present_actions = [], [], []
        self.boundaries, self.poll_errors = {}, []
        self.drain_lock = threading.RLock()
        self.start_bounds = None
        self.new_report_checks = []

    def __enter__(self):
        global CURRENT
        began = time.time()
        value = super().__enter__()
        self.start_bounds = (began, time.time())
        CURRENT = self
        return value

    def __exit__(self, *exc):
        global CURRENT
        try:
            return super().__exit__(*exc)
        finally:
            CURRENT = None

    def drain(self):
        with self.drain_lock:
            return super().drain()

    def _poll(self):
        while not self._stop.wait(1):
            try:
                self.drain()
            except Exception as exc:
                self.poll_errors.append(repr(exc))

    def context(self):
        settings = self.cfg["routing"]["balancers"][0]["strategy"]["settings"]
        penalty = settings.get("returnToCheaper", {}).get("penaltyWindow", "5m")
        multiplier = 60 if penalty.endswith("m") else 1
        protocols = [o.get("protocol") for o in self.cfg["outbounds"] if "path" in o]
        for provider in self.cfg["healthObservatory"].get("providers", []):
            protocols += [o.get("protocol") for o in provider.get("initialAnswer", {}).get("outbounds", [])]
        required_paths = {o["tag"] for o in self.cfg["outbounds"] if "path" in o}
        for provider in self.cfg["healthObservatory"].get("providers", []):
            required_paths.update(provider["apply"]["tagPrefix"] + o["tag"] for o in
                provider.get("initialAnswer", {}).get("outbounds", []))
        return {"required_paths": required_paths, "strategy": settings, "penalty_seconds": float(penalty[:-1]) * multiplier,
                "faults": self.faults, "present_actions": self.present_actions,
                "tcp_only": bool(protocols) and all(p == "vless" for p in protocols)}

    def api(self, *args, **kwargs):
        if args == ("present",):
            self.drain()
            status = self.status()
            now = time.time()
            freshness = self.cfg["healthObservatory"].get("wake", {}).get("freshness", "60s")
            interval = self.cfg["healthObservatory"].get("wake", {}).get("minInterval", "5m")
            seconds = lambda value: float(value[:-1]) * (60 if value.endswith("m") else 1)
            for balancer in status["balancers"]:
                tag = balancer.get("active")
                if balancer.get("pinned") or balancer.get("is_exhausted") or not status.get("is_network_up"):
                    continue
                proofs = [p["at"] for p in self.proofs if p["outbound"] == tag]
                proofs += [e["ts"] / 1000 for e in self.events if e["type"] == "probe_result"
                           and e["fields"].get("outbound") == tag and e["fields"].get("is_ok") is True
                           and e["fields"].get("is_stale") is not True]
                starts = [e["ts"] / 1000 for e in self.events if e["type"] == "probe_start"
                          and e["fields"].get("outbound") == tag and e["fields"].get("reason") == "wake"]
                fresh = bool(proofs) and now - max(proofs) <= seconds(freshness)
                limited = bool(starts) and now - starts[-1] < seconds(interval)
                self.present_actions.append({"at": now, "outbound": tag, "skip": fresh or limited,
                    "proofs": proofs, "wake_starts": starts, "status": status, "fresh": fresh, "limited": limited})
        began = time.time()
        result = super().api(*args, **kwargs)
        ended = time.time()
        if args and args[0] == "report" and isinstance(result, dict):
            if len(args) == 1:
                self.snapshot_bounds = (began, ended)
            elif args[1] in ("take", "final"):
                self.boundaries[result["report_index"]] = (began, ended)
        return result

    def _check_reports(self):
        old = super()._check_reports()
        last, start = 0, self.start_bounds
        for report in self.reports:
            end = self.boundaries[report["report_index"]]
            stop = last + report["events"]
            history = [e for e in self.events if e["seq"] <= stop]
            window = [e for e in history if last < e["seq"] <= stop]
            rows = window_checks(report, history, window, self.context(), start, end, self.context()["required_paths"])
            rows.append(check("Непрерывные события окна", [e["seq"] for e in window],
                              [e["seq"] for e in window] == list(range(last + 1, stop + 1)), "без потерь"))
            rows.append(check("Потери событий", self.dropped, strict_count(self.dropped, 0), 0))
            self.new_report_checks += [{**r, "check": "Окно %s: %s" % (report["report_index"], r["check"])} for r in rows]
            last, start = stop, end
        bench.REPORT_CHECKS.extend(self.new_report_checks)
        return old


def report_checks(client, wake=False, steps=False):
    client.drain()
    report = client.api("report")
    client.drain()
    count = report.get("events") if isinstance(report, dict) else None
    if type(count) is not int:
        return [check("Отчёт содержит число событий", report, False, "int")], report
    last = sum(r["events"] for r in client.reports)
    stop = last + count
    history = [e for e in client.events if e["seq"] <= stop]
    window = [e for e in history if last < e["seq"] <= stop]
    start = client.boundaries[client.reports[-1]["report_index"]] if client.reports else client.start_bounds
    required_paths = {p["outbound"] for p in client.status().get("paths", [])}
    rows = window_checks(report, history, window, client.context(), start, client.snapshot_bounds, required_paths)
    rows.append(check("Непрерывные события snapshot", [e["seq"] for e in window],
                      [e["seq"] for e in window] == list(range(last + 1, stop + 1)), "без потерь"))
    rows.append(check("Потери событий", client.dropped, strict_count(client.dropped, 0), 0))
    return rows, report
