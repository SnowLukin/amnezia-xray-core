"""UDP observation scenarios. Importing this module starts no listeners or processes."""
import contextlib
import ipaddress
import json
import os
import struct
import threading
import time

import bench
import probe_only_scenarios as credentials
import stage1_helpers as h
import udp_matrix as matrix
import udp_matrix_topology as topology
from scenarios import check, scenario

COOKIE = 0x2112A442
HEADER = struct.Struct("!HHI12s")
MODES = ("all", "zero", "alternate")
PATHS = ("reality", "relay", "ws_local_proxy", "xhttp_local_proxy", "awg")


def parse_binding_request(packet):
    if len(packet) < HEADER.size:
        return None
    kind, size, cookie, transaction = HEADER.unpack_from(packet)
    if kind != 0x0001 or cookie != COOKIE or size + HEADER.size != len(packet) or size % 4:
        return None
    offset = HEADER.size
    while offset < len(packet):
        if offset + 4 > len(packet):
            return None
        length = struct.unpack_from("!H", packet, offset + 2)[0]
        offset += 4 + ((length + 3) & ~3)
        if offset > len(packet):
            return None
    return transaction


def binding_success(transaction, sender=("127.0.0.1", 12345)):
    if len(transaction) != 12:
        raise ValueError("STUN transaction ID must contain 12 bytes")
    address = int(ipaddress.IPv4Address(sender[0])) ^ COOKIE
    mapped = struct.pack("!BBHI", 0, 1, sender[1] ^ (COOKIE >> 16), address)
    attribute = struct.pack("!HH", 0x0020, len(mapped)) + mapped
    return HEADER.pack(0x0101, len(attribute), COOKIE, transaction) + attribute


def valid_success(packet, transactions):
    if len(packet) < HEADER.size:
        return False
    kind, size, cookie, transaction = HEADER.unpack_from(packet)
    if kind != 0x0101 or cookie != COOKIE or size % 4 or size + HEADER.size != len(packet):
        return False
    if transaction not in transactions:
        return False
    offset = HEADER.size
    while offset < len(packet):
        if offset + 4 > len(packet):
            return False
        length = struct.unpack_from("!H", packet, offset + 2)[0]
        offset += 4 + ((length + 3) & ~3)
        if offset > len(packet):
            return False
    return True


def fixture_reply(mode, ordinal, packet, sender=("127.0.0.1", 12345)):
    if mode not in MODES or ordinal < 1:
        raise ValueError("invalid STUN mode or ordinal")
    transaction = parse_binding_request(packet)
    if transaction is None:
        if len(packet) >= 2 and packet[0] & 0xc0 == 0:
            return None
        return packet if mode == "all" else None
    if mode == "zero" or (mode == "alternate" and ordinal % 2):
        return None
    return binding_success(transaction, sender)


class STUNTargets:
    def __init__(self, work):
        self.work = work
        self.ports = tuple(bench.UDP_STUN_PORTS)
        self.records = []
        self.listeners = []
        self.threads = []
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.epoch = 0
        self.modes = dict.fromkeys(self.ports, "all")
        self.ordinals = dict.fromkeys(self.ports, 0)
        self.errors = []

    def __enter__(self):
        import socket
        self.socket = socket
        try:
            for port in self.ports:
                listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.listeners.append(listener)
                listener.bind(("127.0.0.1", port))
                listener.settimeout(0.2)
                worker = threading.Thread(target=self._serve, args=(listener, port), daemon=True)
                self.threads.append(worker)
                worker.start()
        except BaseException:
            self.close()
            raise
        return self

    def reset(self):
        with self.lock:
            self.epoch += 1
            self.modes = dict.fromkeys(self.ports, "all")
            self.ordinals = dict.fromkeys(self.ports, 0)
        return self.epoch

    def set_mode(self, port, mode):
        if port in (0, 1):
            port = self.ports[port]
        if mode not in MODES or port not in self.ports:
            raise ValueError("unknown STUN endpoint or mode")
        with self.lock:
            self.modes[port] = mode
            self.ordinals[port] = 0

    def snapshot(self, epoch=None):
        with self.lock:
            return [dict(row) for row in self.records if epoch is None or row["epoch"] == epoch]

    def _serve(self, listener, port):
        while not self.stop.is_set():
            try:
                packet, sender = listener.recvfrom(65535)
            except self.socket.timeout:
                continue
            except OSError as error:
                if not self.stop.is_set():
                    self.errors.append(repr(error))
                return
            received = time.time()
            transaction = parse_binding_request(packet)
            with self.lock:
                if transaction is not None:
                    self.ordinals[port] += 1
                ordinal = self.ordinals[port]
                mode, epoch = self.modes[port], self.epoch
            response = fixture_reply(mode, max(ordinal, 1), packet, sender)
            answered = False
            if response is not None:
                try:
                    listener.sendto(response, sender)
                    answered = True
                except OSError as error:
                    if not self.stop.is_set():
                        self.errors.append(repr(error))
            with self.lock:
                self.records.append({"timestamp": received, "port": port, "ordinal": ordinal,
                    "txid": transaction.hex() if transaction is not None else None,
                    "answered": answered, "epoch": epoch, "mode": mode,
                    "kind": "stun" if transaction is not None else "echo",
                    "request_hex": packet.hex(), "response_hex": response.hex() if answered else None})

    def flush(self):
        matrix.save_json(os.path.join(self.work, "udp-stun-records.json"),
                         {"ports": self.ports, "records": self.snapshot(), "errors": self.errors})

    def close(self):
        self.stop.set()
        for listener in self.listeners:
            listener.close()
        for worker in self.threads:
            worker.join(2)
        self.flush()
        if any(worker.is_alive() for worker in self.threads):
            raise RuntimeError("STUN listener did not stop")

    def __exit__(self, *_):
        self.close()


def observation_config(path="reality", *, destinations=None, blackhole=False, fast=False):
    cfg, tag = topology.matrix_config(path)
    host = bench.TUNNEL_TARGET if path == "awg" else bench.UDP_STUN_DOMAINS[0]
    default = [f"{host}:19302", f"{bench.TUNNEL_TARGET if path == 'awg' else bench.UDP_STUN_DOMAINS[1]}:3478"]
    health = cfg["healthObservatory"]
    health["udpProbe"] = {"destinations": default if destinations is None else destinations,
                          "requests": 5, "interval": "10ms" if fast else "200ms",
                          "timeout": "50ms" if fast else "2s", "perHour": 6}
    health["passive"] = {"udpReplyTimeout": "5s", "loneFailureAfter": "0s"}
    health["report"] = {"interval": "0s", "quietShare": 1}
    health["schedule"] = {"idleAfter": "1h", "maxConcurrentProbes": 1,
        "minProbeInterval": "0s" if fast else "100ms", "probeJitterFraction": 0.000001,
        "probesPerMinute": 120, "probesPerServerNamePerMinute": 120,
        "hostProbes": {"probesPerMinute": 60, "reuseWindow": "0s", "timeout": "500ms" if fast else "15s"}}
    cfg["dns"]["hosts"].update(dict.fromkeys(bench.UDP_STUN_DOMAINS, "198.18.0.2"))
    if blackhole:
        next(o for o in cfg["outbounds"] if o.get("tag") == tag)["settings"]["vnext"][0]["users"][0]["id"] = bench.UDP_DROP_UUID
    return cfg, tag


def tcp64(client, host=None):
    host = host or bench.TARGETS[0]
    stream = bench.open_stream(host, 8, tls=False)
    if stream is None:
        raise RuntimeError("TCP user request refused")
    try:
        stream.sendall(f"GET /bytes/65536 HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n".encode())
        header, size = bench.read_answer(stream, time.time() + 8)
    finally:
        stream.close()
    if not header.startswith(b"HTTP/1.1 200") or size != 65536:
        raise RuntimeError(f"TCP user response incomplete: {header[:80]!r}, {size}")
    return {"at": time.time(), "bytes": size, "active": h.active(client)}


def host_udp(client, tag):
    raw = client.api("-t", "15", "probe", "--udp", "auto", tag, timeout=18)
    request = raw.get("request") or {}
    item = next((item for item in request.get("outbounds", []) if item.get("outbound") == tag), {})
    return {"raw": raw, "item": item, "udp": item.get("udp")}


def invariants(client, tag, before_active=None):
    evs = h.events(client)
    switches = [e for e in evs if e["type"] == "switch" and e["fields"].get("reason") not in bench.START_REASONS]
    path = h.path(client, tag)
    active = h.active(client)
    return [check("UDP не меняет выбранный путь", active, active == (before_active or tag), before_active or tag),
            check("Путь остаётся up", path.get("state"), path.get("state") == "up", "up"),
            check("Нет переключений после старта", len(switches), not switches, 0),
            check("Прямой обход не использован", client.outbound_bytes("direct"),
                  client.outbound_bytes("direct") == (0, 0), "0/0")]


def save_scene(work, name, client, targets, **data):
    targets.flush()
    h.save(work, name, {"status": client.status(), "events": h.events(client),
        "report": client.api("report"), "stun_records": targets.snapshot(targets.epoch), **data})


@scenario()
def udp_probe_matrix(stand, xray, work, name):
    checks, rows = [], []
    with contextlib.ExitStack() as stack:
        stack.enter_context(topology.RelayHop(xray, work))
        proxies = {kind: stack.enter_context(topology.TLSProxy(kind, work)) for kind in ("ws", "xhttp")}
        for path in PATHS:
            epoch = stand.udp_targets.reset()
            cfg, tag = observation_config(path)
            ledger = matrix.Ledger(os.path.join(work, f"{name}-{path}-flows.json"))
            with bench.Client(xray, cfg, work, f"{name}-{path}") as client:
                traffic = tcp64(client, bench.TUNNEL_TARGET if path == "awg" else None)
                with matrix.SocksUDP(ledger, tag, "live-user") as channel:
                    channel.send(bench.TUNNEL_TARGET if path == "awg" else bench.UDP_STUN_DOMAINS[0], 19302, b"live-user")
                    echo = channel.receive()
                result = host_udp(client, tag)
                udp = result["udp"] or {}
                checks += [check(f"{path}: пять ответов", udp, udp.get("state") == "ok" and
                                  udp.get("sent") == udp.get("answered") == 5, "ok 5/5"),
                    check(f"{path}: RTT и джиттер измерены", udp,
                          type(udp.get("rtt_ms")) is int and type(udp.get("jitter_ms")) is int and
                          udp["rtt_ms"] >= 0 and udp["jitter_ms"] >= 0, ">=0 ms"),
                    check(f"{path}: пользовательский UDP ответил", echo.hex(), echo == b"live-user", "echo")]
                checks += invariants(client, tag)
                rows.append({"path": path, "notes": "local TLS reverse proxy, CDN approximation" if path in
                    ("ws_local_proxy", "xhttp_local_proxy") else "two real Xray VLESS hops" if path == "relay" else path,
                    "tcp_user": traffic, "result": result, "stun_records": stand.udp_targets.snapshot(epoch),
                    "events": h.events(client), "report": client.api("report")})
        h.save(work, name, {"rows": rows, "proxy_records": {k: p.records for k, p in proxies.items()}})
    return checks


@scenario()
def udp_probe_blackhole(stand, xray, work, name):
    stand.udp_targets.reset()
    cfg, tag = observation_config(blackhole=True)
    ledger = matrix.Ledger(os.path.join(work, name + "-flows.json"))
    with bench.Client(xray, cfg, work, name) as client:
        traffic = tcp64(client)
        began = time.time()
        with contextlib.ExitStack() as stack:
            flows = [stack.enter_context(matrix.SocksUDP(ledger, tag, f"unanswered-{i}")) for i in range(3)]
            for i, flow in enumerate(flows):
                for sequence in range(3):
                    flow.send(bench.UDP_STUN_DOMAINS[i % 2], (19302, 3478)[i % 2], b"blackhole-user", sequence)
                    time.sleep(0.03)
            suspect = h.wait_for(lambda: h.events(client, "udp_suspect", tag, since=began), 9)[0]
            result = h.wait_for(lambda: h.events(client, "udp_probe", tag, "udp_suspect", began), 9)[0]
        fields = suspect["fields"]
        checks = [check("Улика охватывает три потока и два назначения", fields,
                         fields.get("flows", 0) >= 3 and fields.get("targets", 0) >= 2, ">=3 flows, >=2 targets"),
                  check("Улика не раньше udpReplyTimeout", suspect["ts"] / 1000 - began,
                        suspect["ts"] / 1000 - began >= 5, ">=5 s"),
                  check("Настоящий UDP blackhole подтверждён", result["fields"],
                        result["fields"].get("state") == "dead", "dead"),
                  check("Ответчик не получал заблокированные пакеты", len(stand.udp_targets.snapshot(stand.udp_targets.epoch)),
                        not stand.udp_targets.snapshot(stand.udp_targets.epoch), 0)]
        checks += invariants(client, tag)
        save_scene(work, name, client, stand.udp_targets, tcp_user=traffic,
                   fault="server rule UDP_DROP_EMAIL + network udp -> blackhole", suspect=suspect, result=result)
    return checks


def endpoint_scene(stand, xray, work, name, first_mode):
    epoch = stand.udp_targets.reset()
    stand.udp_targets.set_mode(0, first_mode)
    cfg, tag = observation_config()
    with bench.Client(xray, cfg, work, name) as client:
        traffic = tcp64(client)
        result = host_udp(client, tag)
        udp = result["udp"] or {}
        records = [r for r in stand.udp_targets.snapshot(epoch) if r["kind"] == "stun"]
        first = [r for r in records if r["port"] == bench.UDP_STUN_PORTS[0]]
        second = [r for r in records if r["port"] == bench.UDP_STUN_PORTS[1]]
        lossy = first_mode == "alternate"
        checks = [check("Итог серии", udp, udp.get("state") == ("lossy" if lossy else "ok") and
            udp.get("answered") == (2 if lossy else 5) and udp.get("sent") == 5, "lossy 2/5" if lossy else "ok 5/5"),
            check("Фактически отправлены пять независимых запросов", len(first), len(first) == 5 and
                  len({r["txid"] for r in first}) == 5, 5),
            check("Второе назначение только при полном молчании", len(second), len(second) == (0 if lossy else 5), 0 if lossy else 5)]
        if not lossy:
            report = client.api("report")
            checks.append(check("Счётчик второго назначения", report.get("udp_probe_second_destination_count"),
                                report.get("udp_probe_second_destination_count") == 1, 1))
        checks += invariants(client, tag)
        save_scene(work, name, client, stand.udp_targets, result=result, tcp_user=traffic)
    return checks


@scenario()
def udp_probe_lossy(stand, xray, work, name):
    return endpoint_scene(stand, xray, work, name, "alternate")


@scenario()
def udp_probe_second_destination(stand, xray, work, name):
    return endpoint_scene(stand, xray, work, name, "zero")


@scenario()
def udp_probe_noise(stand, xray, work, name):
    stand.udp_targets.reset()
    stand.udp_targets.set_mode(0, "zero")
    cfg, tag = observation_config()
    ledger = matrix.Ledger(os.path.join(work, name + "-flows.json"))
    with bench.Client(xray, cfg, work, name) as client:
        tcp64(client)
        began = time.time()
        with matrix.SocksUDP(ledger, tag, "dead-ICE") as dead, matrix.SocksUDP(ledger, tag, "live-UDP") as live:
            for i in range(3):
                dead.send(bench.UDP_STUN_DOMAINS[0], 19302, b"dead-ICE", i)
            live.send(bench.UDP_STUN_DOMAINS[1], 3478, b"live-UDP")
            echo = live.receive()
            h.wait_until(time.monotonic() + 5.5)
        suspects = h.events(client, "udp_suspect", tag, since=began)
        checks = [check("Живой поток ответил", echo.hex(), echo == b"live-UDP", "echo"),
                  check("Один тихий ICE-поток не создаёт улику", len(suspects), not suspects, 0)]
        checks += invariants(client, tag)
        save_scene(work, name, client, stand.udp_targets)
    return checks


@scenario()
def udp_probe_sleep(stand, xray, work, name):
    stand.udp_targets.reset()
    cfg, _ = observation_config("awg")
    awg = next(o for o in cfg["outbounds"] if o.get("tag") == "matrix-awg")
    awg["tag"] = "w1"
    cfg["outbounds"].insert(-1, bench.path_members([("a1", "reality", 0)], {"a1": 2})[0])
    cfg["routing"]["balancers"][0]["strategy"]["settings"]["startOutbound"] = "w1"
    cfg["healthObservatory"]["sleep"] = {"afterLastUse": "10s"}
    with bench.Client(xray, cfg, work, name) as client:
        tcp64(client, bench.TUNNEL_TARGET)
        client.api("pin", "auto", "a1")
        tcp64(client)
        asleep = h.wait_for(lambda: h.path(client, "w1") if h.path(client, "w1").get("is_asleep") else None, 20)
        began = time.time()
        result = host_udp(client, "w1")
        time.sleep(0.3)
        wakes = h.events(client, "outbound_wake", "w1", since=began)
        checks = [check("AWG спал до запроса", asleep.get("is_asleep"), asleep.get("is_asleep") is True, True),
            check("Хост получил asleep skip", result["item"], result["item"].get("result") == "skipped" and
                  result["item"].get("source") == "asleep", "skipped/asleep"),
            check("UDP-проба не разбудила AWG", wakes, not wakes and h.path(client, "w1").get("is_asleep") is True, "0 wakes"),
            check("Спящий AWG не отправил STUN", len(stand.udp_targets.snapshot(stand.udp_targets.epoch)),
                  not stand.udp_targets.snapshot(stand.udp_targets.epoch), 0)]
        save_scene(work, name, client, stand.udp_targets, result=result)
    return checks


@scenario()
def udp_probe_budget(stand, xray, work, name):
    epoch = stand.udp_targets.reset()
    cfg, tag = observation_config(fast=True)
    with bench.Client(xray, cfg, work, name) as client:
        tcp64(client)
        results = [host_udp(client, tag) for _ in range(7)]
        evs = h.events(client)
        probes = [e for e in evs if e["type"] == "udp_probe"]
        skips = [e for e in evs if e["type"] == "udp_skip" and e["fields"].get("reason") == "per_hour"]
        records = [r for r in stand.udp_targets.snapshot(epoch) if r["kind"] == "stun"]
        checks = [check("Шесть фактических UDP-проб в rolling hour", len(probes), len(probes) == 6, 6),
            check("Седьмой запрос удержан per_hour", len(skips), bool(skips) and not results[-1]["udp"], ">=1 skip, без результата пробы"),
            check("На STUN ушли ровно шесть серий", len(records), len(records) == 30, 30)]
        checks += invariants(client, tag)
        save_scene(work, name, client, stand.udp_targets, results=results,
                   notes="functional budget: interval10ms, timeout50ms, host timeout500ms; rolling hour unchanged")
    return checks


@scenario()
def udp_probe_idle(stand, xray, work, name):
    epoch = stand.udp_targets.reset()
    cfg, tag = observation_config()
    cfg["healthObservatory"]["schedule"]["idleAfter"] = "1s"
    with bench.Client(xray, cfg, work, name) as client:
        tcp64(client)
        h.wait_until(time.monotonic() + 1.4)
        result = host_udp(client, tag)
        checks = [check("Хостовая UDP-проба в простое пропущена", result["item"],
                        result["item"].get("result") == "skipped" and result["item"].get("source") == "idle", "skipped/idle"),
                  check("Простой не создаёт STUN-пакеты", len(stand.udp_targets.snapshot(epoch)),
                        not stand.udp_targets.snapshot(epoch), 0)]
        save_scene(work, name, client, stand.udp_targets, result=result)
    return checks


@scenario()
def udp_probe_credentials(stand, xray, work, name):
    epoch = stand.udp_targets.reset()
    cfg, tag = observation_config()
    candidate = bench.path_members([("candidate", "tcp_tls", 0)], {"candidate": 2})[0]
    bench.set_probe_only(candidate, True)
    cfg["outbounds"].insert(-1, candidate)
    cfg["healthObservatory"]["schedule"]["probeOnly"] = {"keepVerified": 1, "recheckInterval": "15m"}
    with bench.Client(xray, cfg, work, name) as client:
        began = time.time()
        tcp64(client)
        error = None
        try:
            host_udp(client, "candidate")
        except RuntimeError as exc:
            error = str(exc)
        access = credentials.credential_access(stand.server_access_log, began)
        udp_access = [entry for entry in access if entry["destination"].startswith("udp:")]
        probe_leaks = credentials.probe_credential_leaks(access)
        checks = [check("probe-only UDP отвергнут до отправки", error, error is not None and
                        ("probe-only" in error or "not a candidate: candidate" in error), "request error probe-only/not a candidate"),
            check("UDP не использовал пробные данные доступа", udp_access,
                  not any(entry["email"] == bench.PROBE_EMAIL for entry in udp_access) and not probe_leaks, "0 credential leaks"),
            check("Отклонённый запрос не отправил STUN", len(stand.udp_targets.snapshot(epoch)),
                  not stand.udp_targets.snapshot(epoch), 0),
            check("probe-only отсутствует среди пользовательских кандидатов", client.status().get("balancers", []),
                  not any("candidate" in b.get("candidates", []) for b in client.status().get("balancers", [])), True)]
        save_scene(work, name, client, stand.udp_targets, error=error, access=access)
    return checks


@scenario()
def udp_probe_public(stand, xray, work, name):
    stand.udp_targets.reset()
    cfg, tag = observation_config(destinations=["stun.cloudflare.com:3478", "stun.l.google.com:19302"])
    with bench.Client(xray, cfg, work, name) as client:
        tcp64(client)
        result = host_udp(client, tag)
        udp = result["udp"] or {}
        checks = [check("Фактический результат публичного STUN записан", udp,
                        udp.get("state") in ("ok", "lossy", "dead"), "ok/lossy/dead, external dead is observable")]
        checks += invariants(client, tag)
        save_scene(work, name, client, stand.udp_targets, result=result,
                   notes="one actual host request through REALITY; blocked external STUN stays dead, no synthetic replies")
    return checks


@scenario()
def udp_probe_priority(stand, xray, work, name):
    epoch = stand.udp_targets.reset()
    cfg, tag = observation_config()
    cfg["outbounds"].insert(-1, bench.path_members([("b1", "tcp_tls", 0)], {"b1": 2})[0])
    guard = bench.path_members([("guard", "ws", 0)], {"guard": 3})[0]
    guard["path"]["probe"] = "guard"
    cfg["outbounds"].insert(-1, guard)
    cfg["healthObservatory"]["passive"]["failuresToSuspect"] = 1
    cfg["healthObservatory"]["probes"].append({"name": "guard",
        "destination": f"http://probe.test:{bench.TARGET_PORT}/hold/3", "minBytes": 8,
        "timeout": "8s", "progressTimeout": "4s"})
    output, errors = {}, []
    with bench.Client(xray, cfg, work, name) as client:
        tcp64(client)
        def call(key, udp=False):
            try:
                output[key] = host_udp(client, "b1") if udp else client.api(
                    "-t", "15", "probe", "auto", "guard", timeout=18)
            except Exception as exc:
                errors.append(repr(exc))
        guard_worker = threading.Thread(target=call, args=("guard",), daemon=True)
        udp_worker = threading.Thread(target=call, args=("udp", True), daemon=True)
        guard_worker.start()
        try:
            h.wait_for(lambda: h.events(client, "probe_start", "guard", "host"), 5)
            udp_worker.start()
            with h.Fault(bench.address_port("reality", 0), "refuse"):
                traffic = h.OfferedTraffic(10).start()
                try:
                    h.wait_for(lambda: h.active(client) == "b1", 10)
                    h.wait_for(lambda: h.events(client, "udp_probe", "b1"), 10)
                finally:
                    traffic.stop()
        finally:
            guard_worker.join(18)
            if udp_worker.ident is not None:
                udp_worker.join(18)
        evs = h.events(client)
        tcp_results = [e for e in evs if e["type"] == "probe_result" and
                       e["fields"].get("reason") == "failover" and e["fields"].get("outbound") == "b1"]
        udp_results = [e for e in evs if e["type"] == "udp_probe" and e["fields"].get("outbound") == "b1"]
        receipts = [row["timestamp"] for row in stand.udp_targets.snapshot(epoch) if row["kind"] == "stun"]
        checks = [check("TCP failover завершился раньше UDP", [tcp_results, udp_results],
                        bool(tcp_results and udp_results) and tcp_results[0]["ts"] <= udp_results[0]["ts"], "TCP result <= UDP result"),
                  check("Первый настоящий UDP пакет после TCP failover", receipts[:1],
                        bool(receipts and tcp_results) and min(receipts) >= tcp_results[-1]["ts"] / 1000, "first STUN receipt >= TCP result"),
                  check("Все хостовые вызовы завершились", errors,
                        not errors and not guard_worker.is_alive() and not udp_worker.is_alive(), "0 errors/threads"),
                  check("Failover выбрал резерв", h.active(client), h.active(client) == "b1", "b1"),
                  check("Прямой обход не использован", client.outbound_bytes("direct"),
                        client.outbound_bytes("direct") == (0, 0), "0/0")]
        save_scene(work, name, client, stand.udp_targets, output=output, errors=errors,
                   notes="real TCP guard holds the worker while UDP queues; refused selected path queues real failover")
    return checks


@scenario()
def udp_probe_switch(stand, xray, work, name):
    stand.udp_targets.reset()
    cfg, tag = observation_config()
    with bench.Client(xray, cfg, work, name) as client:
        began = time.time()
        traffic = tcp64(client)
        result = h.wait_for(lambda: h.events(client, "udp_probe", tag, "udp_switch", began), 30)[0]
        time.sleep(3)
        probes = h.events(client, "udp_probe", tag, "udp_switch", began)
        checks = [check("Проба после пользовательского трафика и 20 секунд", result["ts"] / 1000 - began,
                        result["ts"] / 1000 - began >= 20, ">=20 s, default delay unchanged"),
                  check("Одна udp_switch проба на путь и сеть", len(probes), len(probes) == 1, 1),
                  check("Автоматическая проба получила пять ответов", result["fields"],
                        result["fields"].get("state") == "ok" and result["fields"].get("answered") == 5, "ok 5/5")]
        checks += invariants(client, tag)
        save_scene(work, name, client, stand.udp_targets, tcp_user=traffic, result=result)
    return checks
