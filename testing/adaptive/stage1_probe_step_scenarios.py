"""Probe-steps §4: две настоящие ноды, недоступный DNS и сырые результаты проб."""
import copy
import json
import os
import socket
import struct
import subprocess
import threading
import time
import uuid
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import bench
import stage1_helpers as h

A, B = "p-a1", "p-b1"
TRANSPORT_IP, EGRESS_IP = "10.12.0.1", "198.18.0.1"


class NodeDNS:
    """Неотвечающий UDP-резолвер сохраняет реальные DNS-пакеты; repair включает ответы."""
    def __init__(self, broken=False):
        self.broken = broken
        self.packets = []
        self.stop = threading.Event()

    def __enter__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("127.0.0.1", 0))
        self.socket.settimeout(0.2)
        self.port = self.socket.getsockname()[1]
        def receive():
            while not self.stop.is_set():
                try:
                    packet, peer = self.socket.recvfrom(65535)
                except socket.timeout:
                    continue
                except OSError:
                    break
                cursor, parts = 12, []
                while cursor < len(packet) and packet[cursor]:
                    length = packet[cursor]
                    cursor += 1
                    parts.append(packet[cursor:cursor + length].decode("ascii"))
                    cursor += length
                end = cursor + 5
                if end > len(packet):
                    continue
                qtype = struct.unpack("!H", packet[cursor + 1:cursor + 3])[0]
                row = {"at": time.time(), "name": ".".join(parts), "type": qtype,
                       "wire_hex": packet.hex(), "dropped": self.broken, "answers": 0}
                if not self.broken:
                    answer = qtype == 1
                    header = packet[:2] + struct.pack("!HHHHH", 0x8180, 1, int(answer), 0, 0)
                    reply = header + packet[12:end]
                    if answer:
                        reply += b"\xc0\x0c" + struct.pack("!HHIH", 1, 1, 0, 4) + socket.inet_aton("127.0.0.1")
                    self.socket.sendto(reply, peer)
                    row["answers"] = int(answer)
                    row["reply_hex"] = reply.hex()
                self.packets.append(row)
        self.thread = threading.Thread(target=receive, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        self.thread.join(1)
        self.socket.close()


class NodeHTTP:
    def __init__(self):
        self.requests = []
        self.held_started = threading.Event()
        self.transport_broken = False
        self.egress_blocked = False

    def __enter__(self):
        fixture = self
        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            def log_message(self, *args):
                pass
            def do_GET(self):
                if self.path == "/held":
                    row = {"at": time.time(), "host": self.headers.get("Host"), "path": self.path,
                           "status": None, "body_bytes": 0, "closed_at": None, "own_timeout_s": 3}
                    fixture.requests.append(row)
                    fixture.held_started.set()
                    self.connection.settimeout(.1)
                    deadline = time.monotonic() + 8
                    while time.monotonic() < deadline:
                        try:
                            if not self.connection.recv(1):
                                row["closed_at"] = time.time()
                                break
                        except socket.timeout:
                            continue
                        except OSError:
                            row["closed_at"] = time.time()
                            break
                    row["finished_at"] = time.time()
                    self.close_connection = True
                    return
                if self.path == "/winner-32k":
                    fixture.held_started.wait(1)
                    time.sleep(.3)
                if self.path == "/closed" or (self.path == "/check-32k" and fixture.transport_broken) or (self.path == "/window-egress" and fixture.egress_blocked):
                    fixture.requests.append({"at": time.time(), "host": self.headers.get("Host"),
                                             "path": self.path, "status": None, "body_bytes": 0})
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.close_connection = True
                    return
                body = b"x" * 32768 if self.path in ("/check-32k", "/winner-32k") else b"egress-alive"
                row = {"at": time.time(), "host": self.headers.get("Host"),
                       "path": self.path, "status": 200, "body_bytes": len(body)}
                fixture.requests.append(row)
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                row["response_started_at"] = time.time()
                try:
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                row["finished_at"] = time.time()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_port
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


def available_port():
    with socket.socket() as temporary:
        temporary.bind(("127.0.0.1", 0))
        return temporary.getsockname()[1]


class Nodes:
    def __init__(self, xray, work, name, broken_dns=False, blocked=False, no_transport=False):
        self.xray, self.work, self.name = xray, work, name
        self.broken_dns, self.blocked, self.no_transport = broken_dns, blocked, no_transport
        self.processes, self.logs, self.configs = [], [], []
        self.domain = "uncached-" + uuid.uuid4().hex + ".stage1.test"

    def __enter__(self):
        self.stack = ExitStack()
        try:
            self.http = self.stack.enter_context(NodeHTTP())
            self.dns = [self.stack.enter_context(NodeDNS(self.broken_dns)),
                        self.stack.enter_context(NodeDNS(False))]
            self.ports = [available_port(), available_port()]
            for index, kind in enumerate(("reality", "tcp_tls")):
                cfg = copy.deepcopy(bench.server_config())
                inbound = next(i for i in cfg["inbounds"] if i["tag"] == kind)
                inbound["port"] = self.ports[index]
                cfg["inbounds"] = [inbound]
                cfg["dns"] = {"servers": [{"address": "127.0.0.1", "port": self.dns[index].port}],
                              "queryStrategy": "UseIPv4", "disableCache": True,
                              "hosts": {name: "127.0.0.1" for name in bench.TARGETS}}
                cfg["outbounds"] = [
                    {"tag": "exit", "protocol": "freedom", "settings": {
                        "domainStrategy": "UseIPv4", "finalRules": [{"action": "allow"}]}},
                    {"tag": "local-ip", "protocol": "freedom", "settings": {
                        "redirect": "127.0.0.1:%s" % self.http.port,
                        "finalRules": [{"action": "allow"}]}},
                    {"tag": "deny", "protocol": "blackhole"}]
                rules = []
                if self.blocked:
                    rules += [{"domain": ["full:" + self.domain], "outboundTag": "deny"},
                              {"ip": [EGRESS_IP], "outboundTag": "deny"}]
                if self.no_transport and index == 0:
                    rules.append({"ip": [TRANSPORT_IP], "outboundTag": "deny"})
                rules.append({"ip": [TRANSPORT_IP, EGRESS_IP], "outboundTag": "local-ip"})
                cfg["routing"] = {"rules": rules}
                self.configs.append(cfg)
                filename = os.path.join(self.work, self.name + "-node%s.json" % index)
                with open(filename, "w") as stream:
                    json.dump(cfg, stream, indent=2)
                log = open(filename + ".log", "w")
                self.logs.append(log)
                process = subprocess.Popen([bench.SERVER_BIN or self.xray, "run", "-c", filename],
                                           stdout=log, stderr=subprocess.STDOUT)
                self.processes.append(process)
                bench.wait_port(self.ports[index])
            return self
        except Exception:
            self.__exit__()
            raise

    def __exit__(self, *exc):
        for process in self.processes:
            process.terminate()
            try:
                process.wait(5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(5)
        for log in self.logs:
            log.close()
        self.stack.close()
        if hasattr(self, "dns"):
            h.save(self.work, self.name + "-fixture", {"exact_transport_destination": TRANSPORT_IP,
                "mapping": "server-side freedom redirect на локальный HTTP, без изменений host network",
                "domain": self.domain, "dns_packets": [d.packets for d in self.dns],
                "http_requests": self.http.requests, "node_configs": self.configs})

    def profile(self, reserve=False, legacy=False):
        transport = "http://%s:%s/check-32k" % (TRANSPORT_IP, self.http.port)
        egress = "http://%s:%s/egress" % (self.domain, self.http.port)
        if legacy:
            return {"name": "default", "destination": transport, "minBytes": 32768,
                    "timeout": "6s", "progressTimeout": "3s"}
        options = {"timeout": "6s", "progressTimeout": "3s"}
        steps = [] if reserve else [{"name": "transport", "destinations": [transport],
                                    "minBytes": 32768, **options}]
        steps.append({"name": "egress", "isOptional": not reserve, "destinations": [egress], **options})
        if not reserve:
            steps.append({"name": "egress_ip", "isOptional": True, "destinations": [
                "http://%s:%s/egress_ip" % (EGRESS_IP, self.http.port)], **options})
        return {"name": "reserve" if reserve else "full", "steps": steps}

    def client_config(self, stand, reserve=False, legacy=False):
        profile = self.profile(reserve, legacy)
        cfg = h.config(health={"probes": [profile], "passive": {"dialTimeout": "3s",
            "firstByteTimeout": "3s", "loneFailureAfter": "0s"}}, strategy={"networkChangeAction": "recheck",
            "minHoldTime": "5s", "returnToCheaper": {"interval": "10s", "hasRandomPhase": False,
                "probesToReturn": 3, "probeInterval": "2s", "penaltyWindow": "5m"}})
        outbounds = bench.path_members([("a1", "reality", 0), ("b1", "tcp_tls", 0)], {"a1": 1, "b1": 3})
        for outbound, port in zip(outbounds, self.ports):
            outbound["settings"]["vnext"][0]["port"] = port
            outbound["path"]["probe"] = profile["name"]
        answer = {"protocol": 1, "version": self.name, "outbounds": outbounds}
        provider = bench.provider_config(["direct"], limits={"minInterval": "1s"})
        provider["initialAnswer"] = answer
        cfg["healthObservatory"]["providers"] = [provider]
        cfg["outbounds"] = [o for o in cfg["outbounds"] if o.get("tag") not in ("a1", "b1")]
        with open(stand.provider_answer, "w") as stream:
            json.dump(answer, stream)
        return cfg


def prime(client):
    sample = h.request()
    if not sample["ok"] or h.active(client) != A:
        raise RuntimeError("Нода провайдера не обслужила реальный запрос: %r / %r" % (sample, client.status()))
    return sample


def state(client, tag=A):
    return h.path(client, tag).get("state")


def probe(client, tag=A):
    start = time.time()
    response = client.api("-t", "35", "probe", "auto", tag, timeout=40)
    result = h.wait_for(lambda: h.events(client, "probe_result", tag, since=start), 5)[-1]
    return result, response


def canonical(step, camel=False):
    pairs = (("isPassed", "is_passed"), ("connectMs", "connect_ms")) if camel else ()
    return {dict(pairs).get(k, k): v for k, v in step.items()}


def format_checks(stand, client, result, profile):
    fields = result["fields"]
    tag = fields["outbound"]
    raw = fields.get("steps")
    status = h.path(client, tag).get("probe", {})
    asked = time.time()
    client.api("refresh", "fresh")
    def body_with_probe():
        for entry in reversed(stand.provider_bodies(asked - 0.1)):
            for item in (entry.get("body") or {}).get("outbounds", []):
                if item.get("tag") == tag.removeprefix("p-") and item.get("probe"):
                    return item
        return None
    outbound = h.wait_for(body_with_probe, 10)
    external = outbound["probe"]
    checks = [h.check("Событие: сырые шаги профиля", fields,
               fields.get("profile") == profile["name"] and isinstance(raw, list) and bool(raw),
               "profile + state + непустой steps"),
              h.check("Status совпадает с событием", status,
               status.get("profile") == fields.get("profile") and status.get("state") == fields.get("state")
               and status.get("steps") == raw and isinstance(status.get("ago_ms"), int), "точные сырые steps"),
              h.check("Provider совпадает с событием", external,
               external.get("profile") == fields.get("profile") and external.get("state") == fields.get("state")
               and [canonical(s, True) for s in external.get("steps", [])] == raw
               and external.get("isPassed") is (fields.get("state") != "down")
               and isinstance(external.get("agoMs"), int), "точные сырые steps + совместимые поля")]
    configured = profile.get("steps", [{"name": profile["name"], "destinations": [profile.get("destination")]}])
    names = [s["name"] for s in raw or []]
    checks.append(h.check("Порядок и имена запущенных шагов", names,
                         names == [s["name"] for s in configured][:len(names)], "порядок профиля"))
    for got, declared in zip(raw or [], configured):
        required = {"name", "is_passed", "connect_ms", "ms", "bytes"}
        wanted = required | ({"destination"} if got.get("is_passed") else {"error"})
        valid = set(got) == wanted and type(got.get("is_passed")) is bool and all(
            type(got.get(key)) is int and got[key] >= 0 for key in ("connect_ms", "ms", "bytes"))
        if got.get("is_passed"):
            valid = valid and type(got.get("destination")) is int and 0 <= got["destination"] < len(declared["destinations"])
        else:
            valid = valid and got.get("error") in {
                "dial_failed", "dial_timeout", "server_unverified", "closed", "timeout", "no_progress", "short_body"}
        checks.append(h.check("Точный формат шага " + got["name"], got, valid, sorted(wanted)))
    return checks, {"event": result, "status": status, "provider_outbound": outbound}


def trigger(client):
    began = time.time()
    client.network("stage1-" + uuid.uuid4().hex)
    return began


@h.scenario
def stage1_steps_broken_node_dns(stand, xray, work, name):
    if not h.BASELINE:
        raise RuntimeError("Нужен отдельный BEFORE binary 8b3d839f")
    checks = []
    for before, binary in ((True, h.BASELINE), (False, xray)):
        stand.reset()
        label = name + ("-before" if before else "-after")
        with Nodes(xray, work, label, broken_dns=True) as nodes:
            cfg = nodes.client_config(stand, legacy=before)
            cfg["routing"]["balancers"][0]["strategy"]["settings"]["confirmWithProbe"] = "always"
            if before:
                cfg["healthObservatory"].pop("report", None)
                cfg["routing"]["balancers"][0]["strategy"]["settings"].pop("shouldSkipDownAudit", None)
            with (bench.Client if before else h.Client)(binary, cfg, work, label) as client:
                prime(client)
                began = time.time()
                user_failures = []
                workers = [threading.Thread(target=lambda host="cold-%s.%s" % (i, nodes.domain):
                    user_failures.append(h.request(host=host, timeout=12)), daemon=True) for i in range(3)]
                for worker in workers:
                    worker.start()
                for worker in workers:
                    worker.join(15)
                if any(worker.is_alive() for worker in workers):
                    raise RuntimeError("DNS-запросы пользователя не завершились")
                checks.append(h.check(("BEFORE" if before else "AFTER") + ": настоящие DNS-отказы пользователя",
                    user_failures, len(user_failures) == 3 and all(not r["ok"] for r in user_failures), "3 failed requests"))
                result = h.wait_for(lambda: h.events(client, "probe_result", A, "confirm", began), 45)[-1]
                if before:
                    time.sleep(2)
                    checks.append(h.check("BEFORE: DNS не виден транспортной пробе", (state(client), h.active(client)),
                                         state(client) == "up" and h.active(client) == A, "UP, дешёвая нода выбрана"))
                    surface = {"event": result, "status": client.status(), "user_failures": user_failures}
                else:
                    h.wait_for(lambda: h.active(client) == B, 35)
                    more, surface = format_checks(stand, client, result, nodes.profile())
                    checks += more
                    surface["user_failures"] = user_failures
                    raw = result["fields"].get("steps", [])
                    checks += [h.check("DNS: transport/egress/egress_ip", [s.get("is_passed") for s in raw],
                        [s.get("name") for s in raw] == ["transport", "egress", "egress_ip"] and
                        [s.get("is_passed") for s in raw] == [True, False, True], "true/false/true"),
                        h.check("DNS: DEGRADED без карантина", h.path(client, A), state(client) == "degraded" and
                            h.path(client, A).get("quarantine_ms", 0) == 0 and
                            h.path(client, A).get("reason") == "egress", "degraded, egress, 0"),
                        h.check("Переход на другую UP-ноду", (h.active(client), state(client, B)),
                            h.active(client) == B and state(client, B) == "up", "b1, up"),
                        h.check("Неотвечающий DNS получил некешированное имя", nodes.dns[0].packets,
                            any(p["name"] == nodes.domain and p["dropped"] for p in nodes.dns[0].packets)
                            and not any(p["answers"] for p in nodes.dns[0].packets), "реальный DNS-запрос, 0 ответов"),
                        h.check("Transport отвечает на точный Host 10.12.0.1", nodes.http.requests,
                            any(r["host"] == "%s:%s" % (TRANSPORT_IP, nodes.http.port) and
                                r["path"] == "/check-32k" and r["body_bytes"] == 32768 for r in nodes.http.requests),
                            "10.12.0.1, 32768 байт")]
                    more, report = h.report_checks(client, steps=True)
                    checks += more
                    surface["report"] = report
                    checks.append(h.check("Время DEGRADED посчитано", report["paths"][A].get("degraded_ms"),
                        report["paths"][A].get("degraded_ms", 0) > 0, ">0"))
                h.save(work, label + "-surfaces", surface)
    return checks


@h.scenario
def stage1_steps_all_egress_blocked(stand, xray, work, name):
    with Nodes(xray, work, name, blocked=True) as nodes:
        with h.Client(xray, nodes.client_config(stand), work, name) as client:
            prime(client)
            began = trigger(client)
            h.wait_for(lambda: state(client) == state(client, B) == "degraded", 50)
            time.sleep(5)
            bytes_before = {tag: client.outbound_bytes(tag) for tag in (A, "direct", "block")}
            request_begin = time.time()
            actual_user = h.request()
            time.sleep(.2)
            routed = client.routed(request_begin, time.time())
            bytes_after = {tag: client.outbound_bytes(tag) for tag in (A, "direct", "block")}
            results = h.events(client, "probe_result", A, since=began)
            checks, surface = format_checks(stand, client, results[-1], nodes.profile())
            more, report = h.report_checks(client, steps=True)
            checks += more
            switches = h.events(client, "switch", since=began)
            downs = [e for e in h.events(client, "path_state", since=began) if e["fields"].get("state") == "down"]
            checks += [h.check("Пользовательский запрос после DEGRADED прошёл через A",
                    {"request": actual_user, "routed": routed}, actual_user["ok"] and routed == {A: 1}, "success, только A"),
                h.check("Пользовательские байты только на A, direct/block не изменились",
                    {"before": bytes_before, "after": bytes_after},
                    all(bytes_after[tag] == bytes_before[tag] == (0, 0) for tag in ("direct", "block"))
                    and bytes_after[A][0] > bytes_before[A][0] and bytes_after[A][1] > bytes_before[A][1],
                    "A up/down выросли, direct/block=0"),
                h.check("Все ноды DEGRADED", [state(client), state(client, B)],
                state(client) == state(client, B) == "degraded", "degraded/degraded"),
                h.check("Нет переключений или block", (len(switches), h.active(client)),
                    not switches and not downs and h.active(client) == A and not report.get("is_balancer_exhausted"),
                    "0 switches/down, дешёвая нода, no block"),
                h.check("Блокировка egress задана на выходе обеих нод", nodes.configs,
                    all(any(r.get("outboundTag") == "deny" and "domain" in r for r in c["routing"]["rules"])
                        and any(r.get("outboundTag") == "deny" and r.get("ip") == [EGRESS_IP]
                        for r in c["routing"]["rules"]) for c in nodes.configs), "domain + IP -> blackhole"),
                h.check("Обязательные шаги прошли у обеих нод", client.events,
                    all(any(e["type"] == "probe_result" and e["fields"].get("outbound") == tag and
                        [s.get("is_passed") for s in e["fields"].get("steps", [])] == [True, False, False]
                        for e in client.events) for tag in (A, B)), "transport=true, оба egress=false")]
            surface.update(report=report, user_request=actual_user, routed=routed,
                           user_bytes_before=bytes_before, user_bytes_after=bytes_after)
            h.save(work, name + "-surfaces", surface)
    return checks


@h.scenario
def stage1_steps_reserve_without_endpoint(stand, xray, work, name):
    with Nodes(xray, work, name, no_transport=True) as nodes:
        with h.Client(xray, nodes.client_config(stand, reserve=True), work, name) as client:
            prime(client)
            result, response = probe(client)
            checks, surface = format_checks(stand, client, result, nodes.profile(reserve=True))
            raw = result["fields"].get("steps", [])
            checks += [h.check("Reserve UP по внешнему хосту", result["fields"].get("state"),
                result["fields"].get("state") == "up" and len(raw) == 1 and raw[0]["name"] == "egress"
                and raw[0]["is_passed"], "up, egress passed"),
                h.check("Endpoint 10.12.0.1 отсутствует на ноде", nodes.configs[0]["routing"],
                    any(r.get("ip") == [TRANSPORT_IP] and r.get("outboundTag") == "deny"
                        for r in nodes.configs[0]["routing"]["rules"]), "10.12.0.1 -> blackhole"),
                h.check("Transport endpoint не запрашивался", nodes.http.requests,
                    not any(r["path"] == "/check-32k" for r in nodes.http.requests), "0")]
            more, report = h.report_checks(client, steps=True)
            checks += more
            h.save(work, name + "-surfaces", {**surface, "report": report, "response": response})
    return checks


@h.scenario
def stage1_steps_repaired_cheap_return(stand, xray, work, name):
    with Nodes(xray, work, name, broken_dns=True) as nodes:
        with h.Client(xray, nodes.client_config(stand), work, name) as client:
            prime(client)
            began = trigger(client)
            h.wait_for(lambda: h.active(client) == B and state(client) == "degraded", 45)
            initial_result = h.events(client, "probe_result", A, since=began)[-1]
            checks, surface = format_checks(stand, client, initial_result, nodes.profile())
            degraded_began = time.time()
            h.wait_for(lambda: h.events(client, "probe_result", A, "return", degraded_began), 60)
            no_return_switches = [e for e in h.events(client, "switch", since=degraded_began)
                                  if e["fields"].get("to") == A]
            checks.append(h.check("DEGRADED не возвращается поверх UP", (h.active(client), len(no_return_switches)),
                                  h.active(client) == B and not no_return_switches, "b1, 0 return switches"))
            repaired_at = time.time()
            nodes.dns[0].broken = False
            switched = h.wait_for(lambda: next((e for e in h.events(client, "switch", since=repaired_at)
                                 if e["fields"].get("to") == A), None), 120)
            raw_results = h.events(client, "probe_result", A, "return", repaired_at, switched["ts"] / 1000)
            checks += [h.check("После ремонта ровно три UP-пробы перед возвратом", raw_results,
                len(raw_results) == 3 and all(e["fields"].get("state") == "up" for e in raw_results), "3 UP"),
                h.check("Ремонт DNS подтверждён настоящим ответом", nodes.dns[0].packets,
                    any(p["at"] >= repaired_at and p["name"] == nodes.domain and p["answers"] == 1
                        for p in nodes.dns[0].packets), "DNS A answer после repair"),
                h.check("Дешёвая нода выбрана UP", (h.active(client), state(client)),
                    h.active(client) == A and state(client) == "up", "a1 UP")]
            more, report = h.report_checks(client, steps=True)
            checks += more
            h.save(work, name + "-surfaces", {**surface, "repaired_at": repaired_at, "report": report,
                   "return_results": raw_results, "switch": switched})
    return checks


@h.scenario
def stage1_steps_required_failure_short_circuit(stand, xray, work, name):
    with Nodes(xray, work, name, no_transport=True) as nodes:
        with h.Client(xray, nodes.client_config(stand), work, name) as client:
            prime(client)
            result, response = probe(client)
            checks, surface = format_checks(stand, client, result, nodes.profile())
            raw = result["fields"].get("steps", [])
            checks.append(h.check("После обязательного провала шаги не запускались", raw,
                result["fields"].get("state") == "down" and len(raw) == 1
                and raw[0].get("name") == "transport" and raw[0].get("is_passed") is False,
                "down, только failed transport"))
            checks.append(h.check("На DNS ноды A не было запросов egress", nodes.dns[0].packets,
                not any(p["name"] == nodes.domain for p in nodes.dns[0].packets), "0"))
            more, report = h.report_checks(client, steps=True)
            checks += more
            h.save(work, name + "-surfaces", {**surface, "report": report, "response": response})
    return checks


@h.scenario
def stage1_steps_closed_first_destination(stand, xray, work, name):
    with Nodes(xray, work, name) as nodes:
        cfg = nodes.client_config(stand)
        profile = cfg["healthObservatory"]["probes"][0]
        original = profile["steps"][0]["destinations"][0]
        profile["steps"][0]["destinations"] = [original.replace("/check-32k", "/closed"), original]
        with h.Client(xray, cfg, work, name) as client:
            prime(client)
            result, response = probe(client)
            checks, surface = format_checks(stand, client, result, profile)
            raw = result["fields"].get("steps", [])
            checks.append(h.check("Второй адрес победил после настоящего закрытия первого", raw,
                bool(raw) and raw[0].get("is_passed") is True and raw[0].get("destination") == 1
                and any(r["path"] == "/closed" and r.get("status") is None for r in nodes.http.requests)
                and any(r["path"] == "/check-32k" for r in nodes.http.requests), "destination=1, closed + 32768 bytes"))
            more, report = h.report_checks(client, steps=True)
            checks += more
            h.save(work, name + "-surfaces", {**surface, "report": report, "response": response})
    return checks


@h.scenario
def stage1_steps_parallel_destinations(stand, xray, work, name):
    with Nodes(xray, work, name) as nodes:
        cfg = nodes.client_config(stand)
        profile = cfg["healthObservatory"]["probes"][0]
        original = profile["steps"][0]["destinations"][0]
        profile["steps"][0]["destinations"] = [original.replace("/check-32k", "/held"),
                                                original.replace("/check-32k", "/winner-32k")]
        with h.Client(xray, cfg, work, name) as client:
            prime(client)
            result, response = probe(client)
            h.wait_for(lambda: any(r["path"] == "/held" and r.get("closed_at") for r in nodes.http.requests), 2)
            held = next(r for r in nodes.http.requests if r["path"] == "/held")
            winner = next(r for r in nodes.http.requests if r["path"] == "/winner-32k")
            checks, surface = format_checks(stand, client, result, profile)
            raw = result["fields"].get("steps", [])
            checks += [h.check("Второй адрес победил при работающем первом", raw,
                bool(raw) and raw[0].get("is_passed") is True and raw[0].get("destination") == 1,
                "destination=1"),
                h.check("HTTP-запросы пересекались во времени", {"held": held, "winner": winner},
                    held["at"] < winner["finished_at"] and winner["at"] < held["closed_at"], "фактическое перекрытие"),
                h.check("Проигравшее соединение закрыто по победе, до своего таймаута", held,
                    winner["response_started_at"] <= held["closed_at"] and held["closed_at"] - winner["response_started_at"] < 1
                    and held["closed_at"] - held["at"] < held["own_timeout_s"] - .5,
                    "закрыто <1с от победы, раньше progressTimeout=3с")]
            more, report = h.report_checks(client, steps=True)
            checks += more
            h.save(work, name + "-surfaces", {**surface, "report": report, "response": response,
                                              "overlap": {"held": held, "winner": winner}})
    return checks


def window_lone_pass(client):
    began = time.time()
    errors = []
    def guard():
        try:
            client.api("-t", "12", "probe", "auto", "b1", timeout=15)
        except Exception as exc:
            errors.append(repr(exc))
    worker = threading.Thread(target=guard, daemon=True)
    worker.start()
    h.wait_for(lambda: h.events(client, "probe_start", "b1", "host", began), 5)
    with h.Fault(bench.address_port("reality", 0), "refuse") as fault:
        actual = h.request(timeout=4)
        fault.repair()
    worker.join(15)
    if worker.is_alive() or errors or actual["ok"]:
        raise RuntimeError("Одинокий отказ не воспроизведён: %r / %r" % (actual, errors))
    result = h.wait_for(lambda: h.events(client, "probe_result", "a1", "lone", began), 15)[-1]
    if result["fields"].get("is_ok") is not True:
        raise RuntimeError("Проба после одиночного отказа не прошла")
    return {"request": actual, "probe": result, "fault": fault.marks}


@h.scenario
def stage1_report_three_real_windows(stand, xray, work, name):
    from stage1_check_scenarios import transient
    ledger, actions, checks = [], [], []
    with NodeHTTP() as target:
        cfg = h.config(slow_reserve=True, schedule={"maxConcurrentProbes": 1,
            "probesPerServerNamePerMinute": 60, "probesPerMinute": 120}, health={
            "probes": [{"name": "default", "steps": [
                {"name": "transport", "destinations": ["http://127.0.0.1:%s/check-32k" % target.port],
                 "minBytes": 32768, "timeout": "6s", "progressTimeout": "3s"},
                {"name": "egress", "isOptional": True, "destinations": ["http://127.0.0.1:%s/window-egress" % target.port],
                 "timeout": "6s", "progressTimeout": "3s"}]}],
            "report": {"interval": "1s", "quietShare": 1},
            "passive": {"loneFailureAfter": "30s", "dialTimeout": "3s", "firstByteTimeout": "3s", "minDistinctTargets": 2},
            "wake": {"freshness": "2s", "minInterval": "2s"}}, strategy={
            "networkChangeAction": "recheck", "minHoldTime": "5s",
            "returnToCheaper": {"interval": "10s", "hasRandomPhase": False,
                                "probesToReturn": 3, "probeInterval": "1s", "penaltyWindow": "5m"}})
        with h.Client(xray, cfg, work, name) as client:
            h.prime(client)
            down, rescued, traffic = transient(client, ledger)
            try:
                h.wait_for(lambda: h.active(client) == "a1", 40)
            finally:
                traffic.stop()
            time.sleep(32)
            actions.append(window_lone_pass(client))
            time.sleep(2.2)
            wake_at = time.time()
            client.api("present")
            h.wait_for(lambda: h.events(client, "probe_result", "a1", "wake", wake_at), 15)
            client.api("present")
            target.egress_blocked = True
            changed = time.time()
            client.network("window-one-degraded")
            h.wait_for(lambda: h.path(client).get("state") == "degraded" and h.active(client) == "b1", 20)
            time.sleep(2)
            first = client.take_report()
            if not first:
                raise RuntimeError("Первое interval-окно не закрыто после настоящего switch")
            checks += [h.check("Окно 1 содержит audit/lone/wake/degraded", first,
                all(type(first.get(k)) is int and first[k] >= 1 for k in
                    ("audit_count", "audit_passed_count", "lone_probe_count", "wake_probe_count", "wake_skipped_count", "path_degraded_count")),
                "все реально выполненные действия посчитаны"),
                h.check("Первое окно закрыто TakeReport(false)", first.get("report_reason"),
                        first.get("report_reason") == "interval", "interval")]
            target.egress_blocked = False
            client.network("window-two-repaired-egress")
            h.wait_for(lambda: h.active(client) == "a1", 60)
            h.prime(client)
            target.transport_broken = True
            weak_at = time.time()
            failed_requests = []
            workers = [threading.Thread(target=lambda host=host: failed_requests.append(h.request(host, 8, path="/hang")), daemon=True)
                       for host in bench.TARGETS[:3]]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join(10)
            if any(worker.is_alive() for worker in workers) or len(failed_requests) != 3 or any(r["ok"] for r in failed_requests):
                raise RuntimeError("Три настоящих слабых отказа назначения не получены")
            h.wait_for(lambda: h.active(client) == "b1" and h.path(client).get("state") == "down", 25)
            weak_results = h.events(client, "probe_result", "a1", "confirm", weak_at)
            suspects = [e for e in h.events(client, "path_state", "a1", since=weak_at)
                        if e["fields"].get("state") == "suspect"]
            checks.append(h.check("Окно 2 получило weak down от живой ноды", {"suspects": suspects, "results": weak_results},
                bool(suspects) and suspects[-1]["fields"].get("is_strong") is False
                and bool(weak_results) and weak_results[-1]["fields"].get("is_ok") is False, "weak suspect -> failed confirm -> down"))
            time.sleep(2.2)
            wake_at = time.time()
            client.api("present")
            h.wait_for(lambda: h.events(client, "probe_result", "b1", "wake", wake_at), 15)
            client.api("present")
            second = client.take_report()
            if not second:
                raise RuntimeError("Второе interval-окно не закрыто после настоящего failover")
            checks += [h.check("Окно 2 не унаследовало audit/lone", second,
                h.strict_count(second.get("audit_count"), 0) and h.strict_count(second.get("audit_passed_count"), 0)
                and h.strict_count(second.get("lone_probe_count"), 0) and h.strict_count(second.get("path_degraded_count"), 0)
                and h.strict_count(second["paths"]["a1"].get("downs_weak"), 1), "0 новых audit/lone/degraded, 1 actual weak down"),
                h.check("Второе окно закрыто TakeReport(false)", second.get("report_reason"),
                        second.get("report_reason") == "interval", "interval")]
            actions.append({"weak_user_requests": failed_requests, "weak_results": weak_results, "suspects": suspects})
            target.transport_broken = False
            client.network("window-final-live")
            h.wait_for(lambda: h.active(client) == "a1", 60)
            time.sleep(2.2)
            wake_at = time.time()
            client.api("present")
            h.wait_for(lambda: h.events(client, "probe_result", "a1", "wake", wake_at), 15)
            client.api("present")
            more, snapshot = h.report_checks(client, wake=True, steps=True)
            checks += more
        reports = client.reports
        checks.append(h.check("Два interval и отдельное final-окно", [r.get("report_reason") for r in reports],
            [r.get("report_reason") for r in reports] == ["interval", "interval", "stop"]
            and [r.get("report_index") for r in reports] == [1, 2, 3], "interval/interval/stop, 1/2/3"))
        h.save(work, name + "-windows", {"reports": reports, "boundaries": client.boundaries,
            "actions": actions, "faults": ledger, "present_actions": client.present_actions,
            "events": client.events, "new_window_checks": client.new_report_checks,
            "http_requests": target.requests, "last_snapshot": snapshot, "degraded_trigger_at": changed})
    return checks
