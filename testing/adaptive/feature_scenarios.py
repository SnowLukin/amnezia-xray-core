"""Real traffic feature stimuli for the v2 live matrix.

The runner provides its native-v2 config factory and API wrapper. No private
controller thresholds, sweep periods, jitter or quarantine constants change.
Every traffic socket and polling loop has a deadline. Feature-off controls use
the same real fault; test instrumentation is declared in stimulus_features.
"""
import contextlib
import datetime
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import threading
import time

import bench


def _path(client, tag="a1"):
    return next((p for p in client.status()["paths"] if p["outbound"] == tag), {})


def _active(client):
    return next(b["active"] for b in client.status()["balancers"] if b["balancer"] == "auto")


def _count(client, kind, tag="a1"):
    return _path(client, tag).get("failures_by_kind", {}).get(kind, 0)


def _poll(predicate, seconds):
    deadline = time.monotonic() + seconds
    value = None
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.05)
    return value


def _hold(seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        time.sleep(min(.1, max(0, deadline - time.monotonic())))


def _open(host="t1.test", timeout=3, port=None):
    if port is None:
        stream = bench.open_stream(host, timeout, tls=False)
    else:
        # Use the same real SOCKS inbound for a locally controlled target port.
        # These public address/framing helpers start no workload on import.
        import udp_matrix
        stream = socket.create_connection(("127.0.0.1", bench.SOCKS_PORT), timeout)
        stream.settimeout(timeout)
        try:
            stream.sendall(b"\x05\x01\x00")
            if udp_matrix.recv_exact(stream, 2) != b"\x05\x00":
                raise AssertionError("SOCKS authentication refused")
            stream.sendall(b"\x05\x01\x00" + udp_matrix.socks_address(host, port))
            response = udp_matrix.recv_exact(stream, 4)
            if response[:3] != b"\x05\x00\x00":
                raise AssertionError("SOCKS target refused")
            udp_matrix.read_socks_address(stream, response[3])
        except BaseException:
            stream.close()
            raise
    if stream is None:
        raise AssertionError("SOCKS stream refused")
    stream.settimeout(.2)
    return stream


def _send(stream, host, path, keep=False, padding=False):
    extra = "X-Matrix-Padding: " + "x" * 256 + "\r\n" if padding else ""
    stream.sendall((f"GET {path} HTTP/1.1\r\nHost: {host}\r\n" + extra +
                    f"Connection: {'keep-alive' if keep else 'close'}\r\n\r\n").encode())


def _read(stream, seconds, *, quiet=False):
    deadline = time.monotonic() + seconds
    data = bytearray()
    length = None
    while time.monotonic() < deadline:
        stream.settimeout(min(.2, max(.001, deadline - time.monotonic())))
        try:
            chunk = stream.recv(65536)
        except socket.timeout:
            if quiet and data:
                break
            continue
        except OSError:
            break
        if not chunk:
            break
        data.extend(chunk)
        if b"\r\n\r\n" in data:
            head, body = bytes(data).split(b"\r\n\r\n", 1)
            if length is None:
                length = next((int(line.split(b":", 1)[1]) for line in head.split(b"\r\n")
                               if line.lower().startswith(b"content-length:")), 0)
            if len(body) >= length:
                break
    head, separator, body = bytes(data).partition(b"\r\n\r\n")
    return head if separator else b"", len(body) if separator else 0


def _request(host="t1.test", path="/bytes/65536", seconds=3):
    stream = None
    try:
        stream = _open(host, seconds)
        _send(stream, host, path)
        head, size = _read(stream, seconds)
        return {"is_ok": head.startswith(b"HTTP/1.1 200") and size >= 65536,
                "head": head[:80].decode(errors="replace"), "bytes": size}
    except (OSError, AssertionError) as exc:
        return {"is_ok": False, "error": str(exc)}
    finally:
        if stream is not None:
            stream.close()


def _logged(stand, host, path, since):
    rows = []
    for line in Path(stand.target_log).read_text().splitlines():
        at, target, request_path = line.split(" ", 2)
        if float(at) >= since and target == host and request_path == path:
            rows.append({"at": float(at), "host": target, "path": request_path})
    return rows


def _udp_drop_access(stand, since):
    """Independent Xray server proof that user associations reached its drop rule."""
    rows = []
    for line in Path(stand.server_access_log).read_text(errors="replace").splitlines():
        if (" accepted udp:" not in line or
                not any(route in line for route in (" -> dead]", " >> dead]")) or " email: " not in line):
            continue
        if line.rsplit(" email: ", 1)[1].strip() != bench.UDP_DROP_EMAIL:
            continue
        at = datetime.datetime.strptime(line[:26], "%Y/%m/%d %H:%M:%S.%f").timestamp()
        if at >= since - .1:
            rows.append(line)
    return rows


def _probe_access(stand, since):
    """Actual test-server accepts using its restricted probe credential."""
    rows = []
    for line in Path(stand.server_access_log).read_text(errors="replace").splitlines():
        if " accepted tcp:" not in line or " email: " not in line:
            continue
        if line.rsplit(" email: ", 1)[1].strip() != bench.PROBE_EMAIL:
            continue
        at = datetime.datetime.strptime(line[:26], "%Y/%m/%d %H:%M:%S.%f").timestamp()
        if at >= since - .1:
            rows.append(line)
    return rows


class _PartialTarget:
    """Send 16 KiB plaintext of an advertised 64 KiB body, then stay silent."""
    def __enter__(self):
        fixture = self
        self.hits = []
        self.release = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_):
                pass

            def do_GET(self):
                fixture.hits.append({"at": time.time(), "path": self.path})
                self.send_response(200)
                self.send_header("Content-Length", "65536")
                self.end_headers()
                try:
                    self.wfile.write(b"x" * 16384)
                    self.wfile.flush()
                    # A bounded guard; normal cleanup releases the handler first.
                    fixture.release.wait(20)
                except OSError:
                    pass
                self.close_connection = True

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.worker = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.worker.start()
        self.port = self.server.server_port
        return self

    def __exit__(self, *_):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.worker.join(2)


def _traffic_prime(check):
    result = _request()
    check("real user traffic primes cheapest path", result["is_ok"], result)
    if not result["is_ok"]:
        raise AssertionError("could not prime path with actual TCP response")


def _replace_transport(cfg, tag, kind, index=0):
    old = next(o for o in cfg["outbounds"] if o.get("tag") == tag)
    new = bench.member(tag, kind, index, bench.PIN)
    new["selection"] = dict(old["selection"])
    cfg["outbounds"][cfg["outbounds"].index(old)] = new


def _resource_outbound(cfg):
    w1 = bench.awg_member("w1", 0)
    reference = next(o["selection"] for o in cfg["outbounds"] if o.get("tag") == "a1")
    w1["selection"] = dict(reference, cost=3)
    cfg["outbounds"].insert(-1, w1)
    cfg["routing"]["rules"].insert(0, {"type": "field", "ip": [bench.TUNNEL_TARGET], "outboundTag": "w1"})


def run_scenarios(xray, stand, out, config_fn, raw_api_fn, select_case=None):
    """Return independent real-fault/resource cases; never invoke legacy config input."""
    out = Path(out)
    results = []

    def run(name, requested, body, prepare=None, dependencies=(), stimulus_features=(), notes=()):
        primary = next(iter(requested)) if len(requested) == 1 else None
        for prefix, feature in {'live-dial-failures-': 'dial_failures', 'live-dial-timeout-': 'dial_timeout',
                                'live-first-reply-': 'first_reply', 'live-early-close-': 'early_close',
                                'live-stalled-requests-': 'stalled_requests', 'live-partial-stalls-': 'partial_stalls',
                                'live-lone-failure-probe-': 'lone_failure_probe', 'live-udp-replies-': 'udp_replies',
                                'live-tunnel-sleep-': 'tunnel_sleep', 'live-close-unchosen-idle-': 'close_unchosen_idle'}.items():
            if name.startswith(prefix):
                primary = feature
                break
        if select_case is not None and not select_case(name, requested, primary):
            return
        stand.reset()
        wanted = set(requested) | set(dependencies) | set(stimulus_features)
        cfg, effective = config_fn(wanted)
        checks = []
        row = {"case": name, "kind": "behavioral_scenario", "requested": sorted(requested), "dependencies": sorted(dependencies),
               "stimulus_features": sorted(stimulus_features), "enabled": sorted(effective),
               "checks": checks}
        if notes:
            row["notes"] = list(notes)

        def check(label, condition, detail=None):
            checks.append({"check": label, "ok": bool(condition), "detail": detail})

        try:
            if prepare:
                prepare(cfg)
            with bench.Client(xray, cfg, str(out), name) as client:
                status = client.status()
                actual = {key for key, value in status["features"].items()
                          if value and not key.startswith(("balancers.", "providers."))}
                check("only declared observer features are effective", actual == set(effective), sorted(actual))
                body(client, check)
                row["final_status"] = client.status()
            check("direct fallback carries no user bytes", client.direct_bytes == (0, 0), client.direct_bytes)
        except Exception as exc:
            check("real scenario completed", False, repr(exc))
        finally:
            row["config_sha256"] = hashlib.sha256(json.dumps(cfg, sort_keys=True).encode()).hexdigest()
            row["behavior_verified"] = sorted(requested) if checks and all(c["ok"] for c in checks) else []
            results.append(row)
            (out / (name + "-checks.json")).write_text(json.dumps(row, indent=2))
            print(f"{name}: {sum(c['ok'] for c in checks)}/{len(checks)}", flush=True)

    # Three independent transport dials are strong evidence. The disabled
    # control sees the same failed requests but keeps its selected path.
    for enabled in (False, True):
        def dial_body(client, check, enabled=enabled):
            _traffic_prime(check)
            port = bench.address_port("tcp_tls", 0)
            bench.chaos(port, "refuse")
            failed = [_request(host, seconds=2) for host in bench.TARGETS[:3]]
            check("three real user dials fail at refused address", all(not r["is_ok"] for r in failed), failed)
            if enabled:
                _poll(lambda: _active(client) == "b1", 10)
            else:
                _hold(1)
            count = _count(client, "dial_failed")
            check("dial_failed evidence gate", count >= 3 if enabled else count == 0, count)
            check("strong evidence selects working reserve" if enabled else "disabled detector keeps prior choice",
                  _active(client) == ("b1" if enabled else "a1"), _active(client))
            if enabled:
                check("reserve carries real successful user request", (r := _request())["is_ok"], r)
        run("live-dial-failures-" + ("on" if enabled else "off"),
            {"dial_failures"} if enabled else set(), dial_body)

    for enabled in (False, True):
        def prepare(cfg):
            cfg["healthObservatory"]["evidence"].setdefault("dialTimeout", {})["timeout"] = "2s"

        def timeout_body(client, check, enabled=enabled):
            _traffic_prime(check)
            bench.chaos(bench.address_port("tcp_tls", 0), "blackhole")
            with contextlib.ExitStack() as stack:
                for host in bench.TARGETS[:3]:
                    stream = stack.enter_context(contextlib.closing(_open(host)))
                    _send(stream, host, "/bytes/65536")
                if enabled:
                    _poll(lambda: _active(client) == "b1", 12)
                else:
                    _hold(3.5)
                count = _count(client, "dial_timeout")
                check("dial_timeout evidence gate while user sockets remain open",
                      count >= 3 if enabled else count == 0, count)
                check("timeout failover" if enabled else "disabled timeout retains choice",
                      _active(client) == ("b1" if enabled else "a1"), _active(client))
            if enabled:
                check("reserve serves real traffic after timed-out transport", (r := _request())["is_ok"], r)
        run("live-dial-timeout-" + ("on" if enabled else "off"),
            {"dial_timeout"} if enabled else set(), timeout_body, prepare)

    for enabled in (False, True):
        def prepare(cfg):
            cfg["healthObservatory"]["evidence"].setdefault("firstReply", {})["timeout"] = "2s"

        def first_body(client, check, enabled=enabled):
            _traffic_prime(check)
            began = time.time()
            with contextlib.closing(_open()) as stream:
                _send(stream, "t1.test", "/hang")
                hits = _poll(lambda: _logged(stand, "t1.test", "/hang", began), 3)
                check("request reaches target after a successful transport dial", bool(hits), hits)
                _hold(3)
                count = _count(client, "no_response")
                check("first reply evidence gate", count == (1 if enabled else 0), count)
                check("one broken target does not switch the tunnel", _active(client) == "a1", _active(client))
        run("live-first-reply-" + ("on" if enabled else "off"),
            {"first_reply"} if enabled else set(), first_body, prepare)

    # XHTTP establishes its transport lazily: an unavailable server ends the
    # routed stream before a first response rather than failing the initial dial.
    for enabled in (False, True):
        def prepare(cfg):
            _replace_transport(cfg, "a1", "xhttp")
            bench.chaos(bench.address_port("xhttp", 0), "refuse")

        def close_body(client, check, enabled=enabled):
            result = _request(seconds=3)
            check("real lazy XHTTP request closes without response", not result["is_ok"], result)
            if enabled:
                _poll(lambda: _count(client, "closed") > 0, 3)
            else:
                _hold(.5)
            count = _count(client, "closed")
            check("early-close evidence gate", count == (1 if enabled else 0), count)
            bench.chaos(bench.address_port("xhttp", 0), "pass")
            check("same XHTTP fixture serves traffic when restored", (r := _request(seconds=5))["is_ok"], r)
        run("live-early-close-" + ("on" if enabled else "off"),
            {"early_close"} if enabled else set(), close_body, prepare)

    for enabled in (False, True):
        def prepare(cfg):
            cfg["healthObservatory"]["evidence"].setdefault("stalledRequests", {})["timeout"] = "2s"

        def stall_body(client, check, enabled=enabled):
            _traffic_prime(check)
            with contextlib.closing(_open()) as stream:
                _send(stream, "t1.test", "/bytes/1024", keep=True)
                head, size = _read(stream, 3)
                check("existing connection has a first successful response", head.startswith(b"HTTP/1.1 200") and size == 1024,
                      {"head": head[:80].decode(errors="replace"), "bytes": size})
                began = time.time()
                _send(stream, "t1.test", "/hang", keep=True, padding=True)
                hits = _poll(lambda: _logged(stand, "t1.test", "/hang", began), 3)
                check("new substantial request reaches target on that connection", bool(hits), hits)
                _hold(3)
                count = _count(client, "stall")
                check("stalled request evidence gate", count == (1 if enabled else 0), count)
                check("one stalled target keeps working tunnel selected", _active(client) == "a1", _active(client))
        run("live-stalled-requests-" + ("on" if enabled else "off"),
            {"stalled_requests"} if enabled else set(), stall_body, prepare)

    for enabled in (False, True):
        def partial_body(client, check, enabled=enabled):
            _traffic_prime(check)
            with _PartialTarget() as target, contextlib.closing(_open(port=target.port)) as stream:
                _send(stream, "t1.test", "/partial")
                head, size = _read(stream, 2)
                check("real response stops between partial and success thresholds",
                      head.startswith(b"HTTP/1.1 200") and size == 16384 and bool(target.hits),
                      {"body_bytes": size, "advertised_bytes": 65536, "partial_min": 8192,
                       "success": 24576, "target_hits": target.hits})
                # Partial stalls use the unchanged internal 8 s stall timeout.
                _hold(9.5)
                count = _count(client, "partial_stall")
                check("independent partial stall evidence gate", count == (1 if enabled else 0), count)
                check("one partial response is insufficient to switch", _active(client) == "a1", _active(client))
        run("live-partial-stalls-" + ("on" if enabled else "off"),
            {"partial_stalls"} if enabled else set(), partial_body,
            notes=("The 8 KiB/24 KiB thresholds and 8 s timeout are existing constants, never lowered.",))

    for enabled in (False, True):
        def prepare(cfg):
            cfg["healthObservatory"]["evidence"].setdefault("loneFailureProbe", {})["after"] = "2s"

        def lone_body(client, check, enabled=enabled):
            _traffic_prime(check)
            _hold(2.5)
            bench.chaos(bench.address_port("tcp_tls", 0), "refuse")
            failed = _request(seconds=2)
            check("exactly one real user request fails on previously proven path", not failed["is_ok"], failed)
            if enabled:
                _poll(lambda: (_path(client).get("probe") or {}).get("reason") == "lone", 12)
            else:
                _hold(3)
            path = _path(client)
            check("ordinary source records one independent failure",
                  path.get("failures_by_kind", {}).get("dial_failed") == 1, path.get("failures_by_kind"))
            probe = path.get("probe") or {}
            check("lone probe gate", probe.get("reason") == "lone" if enabled else not probe, probe)
            if enabled:
                _poll(lambda: _active(client) == "b1", 10)
                check("failed lone probe can replace the path before three user failures",
                      path.get("state") == "down" and _active(client) == "b1", {"path": path, "active": _active(client)})
                check("reserve works after lone decision", (r := _request())["is_ok"], r)
            else:
                check("one path failure alone leaves selection unchanged", _active(client) == "a1", _active(client))
        run("live-lone-failure-probe-" + ("on" if enabled else "off"),
            {"lone_failure_probe"} if enabled else set(), lone_body, prepare,
            dependencies={"dial_failures"},
            notes=("dial_failures is the documented path-level source; the control retains it.",))

    # The detector itself only writes a private suspicion timestamp when both
    # active UDP probing and event collection are disabled. Events are explicit
    # readout instrumentation, and no probes.udp flag is silently added.
    for enabled in (False, True):
        def prepare(cfg):
            cfg["healthObservatory"]["evidence"].setdefault("udpReplies", {})["timeout"] = "2s"
            next(o for o in cfg["outbounds"] if o.get("tag") == "a1")["settings"]["vnext"][0]["users"][0]["id"] = bench.UDP_DROP_UUID

        def udp_body(client, check, enabled=enabled):
            import udp_matrix
            _traffic_prime(check)
            before_tcp_state, before_active = _path(client).get("state"), _active(client)
            began = time.time()
            ledger = udp_matrix.Ledger(str(out / ("udp-evidence-" + ("on" if enabled else "off") + "-flows.json")))
            with contextlib.ExitStack() as stack:
                flows = [stack.enter_context(udp_matrix.SocksUDP(ledger, "a1", f"unanswered-{i}", timeout=2)) for i in range(3)]
                for index, flow in enumerate(flows):
                    for sequence in range(3):
                        flow.send(bench.TUNNEL_TARGET, (19302, 3478)[index % 2], b"matrix-evidence", sequence)
                _hold(3)
                check("real SOCKS driver sent three frames in each of three associations",
                      len(ledger.flows) == 3 and all(len(flow.sent) == 3 for flow in ledger.flows),
                      [flow.as_json() for flow in ledger.flows])
                accepted = _udp_drop_access(stand, began)
                check("independent server saw user UDP routed to the blackhole rule", len(accepted) >= 3, accepted)
                client.drain()
                events = [e for e in client.events if e["type"] == "udp_suspect" and e["fields"].get("outbound") == "a1"
                          and e["ts"] / 1000 >= began - .5]
                check("UDP replies evidence gate", bool(events) if enabled else not events, events)
                if enabled:
                    check("UDP suspicion spans three associations and two destinations",
                          events and events[0]["fields"].get("flows", 0) >= 3 and events[0]["fields"].get("targets", 0) >= 2,
                          events[0]["fields"] if events else None)
                    check("UDP suspicion waits for configured reply timeout",
                          events and events[0]["ts"] / 1000 - began >= 1.8, events[0]["ts"] / 1000 - began if events else None)
                check("UDP evidence leaves TCP health and choice intact",
                      _active(client) == before_active == "a1" and _path(client).get("state") == before_tcp_state,
                      {"before": {"active": before_active, "state": before_tcp_state},
                       "after": {"active": _active(client), "state": _path(client).get("state")}})
                check("no active UDP probe was added", _path(client).get("udp", {}).get("state") == "unknown", _path(client).get("udp"))
                check("controlled responder saw no server-dropped user UDP",
                      stand.udp_targets is not None and not stand.udp_targets.snapshot(stand.udp_targets.epoch),
                      stand.udp_targets.snapshot(stand.udp_targets.epoch) if stand.udp_targets is not None else None)
        run("live-udp-replies-" + ("on" if enabled else "off"),
            {"udp_replies"} if enabled else set(), udp_body, prepare, stimulus_features={"events"},
            notes=("telemetry.events is declared oracle instrumentation; standalone Status cannot expose the suspicion.",))

    for enabled in (False, True):
        def prepare(cfg):
            _resource_outbound(cfg)
            cfg["healthObservatory"].setdefault("resources", {}).setdefault("tunnelSleep", {})["afterLastUse"] = "10s"

        def sleep_body(client, check, enabled=enabled):
            _traffic_prime(check)
            actual = _request(bench.TUNNEL_TARGET, seconds=8)
            check("unchosen real AWG tunnel serves user data", actual["is_ok"], actual)
            check("cheapest selector remains a1 after directly routed AWG use", _active(client) == "a1", _active(client))
            if enabled:
                _poll(lambda: _path(client, "w1").get("is_asleep"), 22)
            else:
                _hold(13)
            status = _path(client, "w1")
            check("tunnel sleep gate after real idle time", status.get("is_asleep", False) is enabled, status)
            before = bench.chaos_stats()[bench.AWG_ADDRESS][3]
            _hold(7)
            after = bench.chaos_stats()[bench.AWG_ADDRESS][3]
            check("AWG outbound stops keepalive datagrams" if enabled else "disabled sleep keeps AWG alive",
                  after - before == 0 if enabled else after - before >= 1, {"up_datagrams": after - before, "interval_s": 7})
            if enabled:
                check("direct user traffic wakes sleeping AWG", (r := _request(bench.TUNNEL_TARGET, seconds=8))["is_ok"], r)
                check("AWG is awake again after the real request", not _path(client, "w1").get("is_asleep", False), _path(client, "w1"))
        run("live-tunnel-sleep-" + ("on" if enabled else "off"),
            {"tunnel_sleep"} if enabled else set(), sleep_body, prepare)

    for enabled in (False, True):
        def prepare(cfg):
            _resource_outbound(cfg)
            cfg["healthObservatory"].setdefault("resources", {}).setdefault("closeUnchosenIdle", {})["after"] = "10s"

        def idle_body(client, check, enabled=enabled):
            _traffic_prime(check)
            with contextlib.closing(_open(bench.TUNNEL_TARGET, timeout=8)) as stream:
                _send(stream, bench.TUNNEL_TARGET, "/bytes/1024", keep=True)
                head, size = _read(stream, 8)
                check("real unchosen AWG connection answers and remains open",
                      head.startswith(b"HTTP/1.1 200") and size == 1024,
                      {"head": head[:80].decode(errors="replace"), "bytes": size})
                began = time.monotonic()
                ended = False
                while time.monotonic() - began < 14:
                    stream.settimeout(.2)
                    try:
                        if not stream.recv(1):
                            ended = True
                            break
                    except socket.timeout:
                        continue
                    except OSError:
                        ended = True
                        break
                elapsed = time.monotonic() - began
                check("independent idle-close gate at real 10s minimum", ended is enabled,
                      {"ended": ended, "idle_seconds": round(elapsed, 3)})
                if enabled:
                    check("idle closure does not precede configured timeout", elapsed >= 9.5, round(elapsed, 3))
                check("idle closure does not enable or put AWG to sleep", not _path(client, "w1").get("is_asleep", False), _path(client, "w1"))
                check("healthy selector is unchanged by resource closure", _active(client) == "a1", _active(client))
            check("new real AWG request works after resource observation", (r := _request(bench.TUNNEL_TARGET, seconds=8))["is_ok"], r)
        run("live-close-unchosen-idle-" + ("on" if enabled else "off"),
            {"close_unchosen_idle"} if enabled else set(), idle_body, prepare,
            notes=("tunnelSleep, telemetry and passive detectors remain disabled; AWG is directly routed and unchosen.",))

    def prepare_candidate(cfg):
        real = next(o for o in cfg["outbounds"] if o.get("tag") == "a1")
        candidate = bench.member("candidate", "tcp_tls", 2, bench.PIN)
        candidate["settings"]["vnext"][0]["users"][0]["id"] = bench.PROBE_UUID
        candidate["selection"] = dict(real["selection"], cost=1, isProbeOnly=True)
        real["selection"]["cost"] = 2
        next(o for o in cfg["outbounds"] if o.get("tag") == "b1")["selection"]["cost"] = 3
        cfg["outbounds"].insert(-1, candidate)
        cfg["routing"]["rules"].insert(0, {"type": "field", "domain": ["full:t5.test"], "outboundTag": "candidate"})

    def candidate_body(client, check):
        began = time.time()
        _traffic_prime(check)
        status = client.status()
        balancer = next(b for b in status["balancers"] if b["balancer"] == "auto")
        check("cheaper base probe-only address is excluded from selection",
              balancer["active"] == "a1" and "candidate" not in balancer["candidates"], balancer)
        # Standalone maintenance is now contractual. Wait for an actual result,
        # so its automatic connection cannot be attributed to the later user
        # dispatch. Merely reading zero queued/running before scheduling is not
        # evidence that the candidate job has completed.
        _poll(lambda: (_path(client, "candidate").get("probe") or {}).get("reason") == "candidate" and
              (_path(client, "candidate").get("probe") or {}).get("state") == "up", 12)
        path = _path(client, "candidate")
        probe = path.get("probe") or {}
        completed = probe.get("reason") == "candidate" and probe.get("state") == "up"
        accepted = _probe_access(stand, began)
        check("automatic base-only candidate probe completes with restricted server credential",
              completed and bool(accepted) and
              all(f" accepted tcp:probe.test:{bench.TARGET_PORT} " in line for line in accepted),
              {"probe": probe, "server_accepts": accepted})
        if not completed:
            return
        settled = client.status()
        check("completed candidate job is absent from queues before forbidden dispatch measurement",
              settled["probes_queued"] == 0 and settled["probes_running"] == 0,
              {"queued": settled["probes_queued"], "running": settled["probes_running"]})
        server_before = len(_probe_access(stand, began))
        before = bench.chaos_stats()[bench.address_port("tcp_tls", 2)][0]
        rejected = _request("t5.test", seconds=2)
        after = bench.chaos_stats()[bench.address_port("tcp_tls", 2)][0]
        check("directly routed user traffic cannot dispatch probe-only credentials",
              not rejected["is_ok"] and after == before, {"request": rejected, "new_transport_connections": after - before})
        server_after = len(_probe_access(stand, began))
        check("independent server ledger is unchanged by forbidden user dispatch",
              server_after == server_before and not _logged(stand, "t5.test", "/bytes/65536", began),
              {"server_accepts_before": server_before, "server_accepts_after": server_after})
        check("normal user traffic remains usable after forbidden dispatch", (r := _request())["is_ok"], r)

    run("live-candidates-base-exclusion", {"candidates"}, candidate_body, prepare_candidate,
        notes=("Provider-free user exclusion scenario. Automatic standalone keepVerified/recheck has separate live/core evidence.",
               "A disabled+isProbeOnly fixture is a validation error, so no equivalent runtime off case exists.",
               "Status is read after completed automatic probing; early untouched inventory snapshots are not assumed."))

    if select_case is None or select_case("live-server-unverified-unavailable", {"server_unverified"}, "server_unverified"):
        results.append({"case": "live-server-unverified-unavailable", "kind": "behavioral_scenario", "requested": ["server_unverified"],
                    "dependencies": [], "stimulus_features": [], "checks": [], "coverage": "uncovered",
                    "notes": ["Current macOS Stand impostor has an untrusted local certificate and produces dial_failed, not exact server_unverified.",
                              "Needs a system-trusted REALITY decoy fixture; no trust-store mutation or public-network request is attempted."]})
    return results
