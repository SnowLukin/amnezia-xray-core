"""Black-box checks of the probe's connection and request time budgets.

Run the same scenarios against the binary at 8b3d839f with
BENCH_PHASE_EXPECT=before, then against the changed binary with
BENCH_PHASE_EXPECT=after. The HTTP fixture and network timings are identical.
"""
import json
import os
import statistics
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import bench
from scenarios import check, scenario


DIAL_TIMEOUT = 6.0
PROGRESS_TIMEOUT = 4.0
SLOW_CONNECT = 3.0
FIRST_BYTE_AFTER_SLOW_CONNECT = 1.7
BODY_CHUNKS = 6
BODY_INTERVAL = 2.0
BODY_BYTES = BODY_CHUNKS * 16384
MUX_LATE_HEADERS = 7.0
NETWORK_DURATION = 20 * 60
NETWORK_PROBE_INTERVAL = 25
NETWORK_SEED = 20261011


def expected(before, after):
    version = os.environ.get("BENCH_PHASE_EXPECT", "after")
    if version not in ("before", "after"):
        raise ValueError("BENCH_PHASE_EXPECT must be before or after")
    return before if version == "before" else after


class PhaseTarget:
    def __init__(self):
        self.hits = []
        self.lock = threading.Lock()
        target = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                with target.lock:
                    target.hits.append({"path": self.path, "received": time.time()})
                if self.path == "/slow-connect":
                    time.sleep(FIRST_BYTE_AFTER_SLOW_CONNECT)
                    self.answer(BODY_BYTES)
                elif self.path == "/late-first-byte":
                    time.sleep(PROGRESS_TIMEOUT + 1.5)
                    self.answer(BODY_BYTES)
                elif self.path == "/late-mux-headers":
                    time.sleep(MUX_LATE_HEADERS)
                    with target.lock:
                        target.hits[-1]["headers_at"] = time.time()
                    self.answer(BODY_BYTES)
                elif self.path == "/body-progress":
                    self.send_response(200)
                    self.send_header("Content-Length", str(BODY_BYTES))
                    self.end_headers()
                    try:
                        for index in range(BODY_CHUNKS):
                            if index:
                                time.sleep(BODY_INTERVAL)
                            self.wfile.write(b"x" * 16384)
                            self.wfile.flush()
                            with target.lock:
                                target.hits[-1].setdefault("chunks", []).append(time.time())
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                else:
                    self.send_error(404)

            def answer(self, size):
                self.send_response(200)
                self.send_header("Content-Length", str(size))
                self.end_headers()
                try:
                    self.wfile.write(b"x" * size)
                except (BrokenPipeError, ConnectionResetError):
                    pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True

    def __enter__(self):
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    @property
    def port(self):
        return self.server.server_port

    def save(self, work, name):
        with self.lock, open(os.path.join(work, f"{name}-fixture.json"), "w") as stream:
            json.dump(self.hits, stream, indent=1)


def config(port, path, mux=False, timeout="14s"):
    kind = "tcp_tls" if mux else "reality"
    warm_kind = "reality" if mux else "tcp_tls"
    cfg = bench.client_config([("warm", warm_kind, 1), ("a1", kind, 0)], {"warm": 1, "a1": 2}, health={
        "passive": {"dialTimeout": "6s"},
        "probes": [{"name": "default", "destination": f"http://probe.test:{port}{path}",
                    "expectedStatuses": [200], "minBytes": BODY_BYTES,
                    "timeout": timeout, "progressTimeout": "4s"}],
    })
    if mux:
        next(item for item in cfg["outbounds"] if item.get("tag") == "a1")["mux"] = {
            "enabled": True, "concurrency": 8}
    return cfg


def probe_result(client, started, tag):
    client.drain()
    starts = [event for event in client.events if event["type"] == "probe_start"
              and event["fields"].get("outbound") == tag and event["ts"] / 1000 >= started - 0.5]
    results = [event for event in client.events if event["type"] == "probe_result"
               and event["fields"].get("outbound") == tag and event["ts"] / 1000 >= started - 0.5]
    start = starts[0]["ts"] / 1000 if starts else None
    result = results[0] if results else None
    duration = result["ts"] / 1000 - start if start is not None and result else None
    return result, start, duration


def prime(client):
    """The host API accepts a probe only after the balancer has seen traffic."""
    stream = bench.open_stream(bench.TARGETS[0], 8, tls=False)
    if stream is None:
        raise RuntimeError("warm path refused the initial connection")
    try:
        stream.sendall(f"GET /204 HTTP/1.1\r\nHost: {bench.TARGETS[0]}\r\nConnection: close\r\n\r\n".encode())
        head, _ = bench.read_answer(stream, time.time() + 8)
        if not head.startswith(b"HTTP/1.1 204"):
            raise RuntimeError(f"warm path returned {head[:80]!r}")
    finally:
        stream.close()


def invoke(client, wait=20, tag="a1", prime_path=True):
    if prime_path:
        prime(client)
    started = time.time()
    response = client.api("-t", str(wait), "probe", "auto", tag, timeout=wait + 5)
    result, event_start, duration = probe_result(client, started, tag)
    request = response.get("result") or {}
    outbound = next((item for item in request.get("outbounds", []) if item.get("outbound") == tag), {})
    return result, event_start, duration, outbound, client.status()


def result_checks(result, outbound, duration, status, wanted_ok, wanted_error, tag="a1", actual_class=None):
    fields = result["fields"] if result else {}
    got_ok = fields.get("is_ok")
    path = next((item for item in status.get("paths", []) if item.get("outbound") == tag), {})
    raw_error = path.get("last_error") or fields.get("error") or ""
    if wanted_error is None:
        error_ok = not raw_error
    elif wanted_error == "no_progress":
        error_ok = raw_error == "no progress"
    elif wanted_error == "dial_timeout":
        error_ok = raw_error == "dial timeout"
    else:
        error_ok = "refused" in raw_error.lower()
    checks = [check("probe result and host request", (got_ok, outbound.get("result")),
                  got_ok is wanted_ok and outbound.get("result") == ("ok" if wanted_ok else "failed"),
                  (wanted_ok, "ok" if wanted_ok else "failed")),
            check("observable probe error", raw_error, error_ok, wanted_error or "empty"),
            check("probe duration, s", round(duration, 2) if duration is not None else None,
                  duration is not None, "measured")]
    if tag == "p-a1":
        checks.append(check("provider probe error class", actual_class, actual_class == wanted_error, wanted_error))
    return checks


def dead_config(stand, target_port):
    cfg = config(target_port, "/slow-connect")
    cfg["outbounds"] = [item for item in cfg["outbounds"] if item.get("tag") != "a1"]
    initial = {"protocol": 1, "version": "phase", "outbounds":
               bench.path_members([("a1", "reality", 0)], {"a1": 2})}
    provider = bench.provider_config(["direct"])
    provider["initialAnswer"] = initial
    cfg["healthObservatory"]["providers"] = [provider]
    with open(stand.provider_answer, "w") as stream:
        json.dump(initial, stream)
    return cfg


def provider_probe_class(stand, client):
    started = time.time()
    client.api("refresh", "fresh")
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        for entry in stand.provider_bodies(started - 0.1):
            body = entry.get("body") or {}
            outbound = next((item for item in body.get("outbounds", []) if item.get("tag") == "a1"), {})
            if outbound.get("probe"):
                return outbound["probe"].get("error")
        time.sleep(0.1)
    return None


@scenario()
def phase_slow_connect(stand, xray, work, name):
    """A live dial takes 3 s and its first response arrives 1.7 s later."""
    port = bench.address_port("reality", 0)
    with PhaseTarget() as target:
        cfg = config(target.port, "/slow-connect", timeout="8s")
        bench.chaos(str(port), "pause")
        with bench.Client(xray, cfg, work, name) as client:
            before = bench.chaos_stats()[port][0]
            answer = {}

            def ask():
                try:
                    answer["value"] = invoke(client)
                except Exception as exc:
                    answer["error"] = repr(exc)

            thread = threading.Thread(target=ask, daemon=True)
            thread.start()
            accepted = None
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline and accepted is None:
                if bench.chaos_stats()[port][0] > before:
                    accepted = time.time()
                else:
                    time.sleep(0.02)
            if accepted is not None:
                time.sleep(SLOW_CONNECT)
            bench.chaos(str(port), "pass")
            thread.join(25)
        target.save(work, name)
    result, event_start, duration, outbound, status = answer.get("value", (None, None, None, {}, {}))
    hit = target.hits[0] if target.hits else None
    wanted_ok, wanted_error = expected((False, "no_progress"), (True, None))
    checks = [check("probe invoked", answer.get("error") or thread.is_alive(),
                    "error" not in answer and not thread.is_alive(), "completed"),
              check("live server connection accepted", accepted, accepted is not None, "accepted"),
              check("request reached live target after dial", hit and round(hit["received"] - accepted, 2)
                    if accepted is not None and hit else None,
                    accepted is not None and hit is not None and hit["received"] - accepted >= SLOW_CONNECT - 0.15,
                    ">= 2.85 s after TCP accept")]
    checks += result_checks(result, outbound, duration, status, wanted_ok, wanted_error)
    checks.append(check("response exceeds old 4 s progress budget",
                        hit and event_start and round(hit["received"] + FIRST_BYTE_AFTER_SLOW_CONNECT - event_start, 2),
                        hit is not None and event_start is not None
                        and hit["received"] + FIRST_BYTE_AFTER_SLOW_CONNECT - event_start > PROGRESS_TIMEOUT + 0.3,
                        "> 4.3 s"))
    return checks


@scenario()
def phase_slow_first_byte(stand, xray, work, name):
    """A completed dial is followed by 5.5 s without a response byte."""
    with PhaseTarget() as target:
        with bench.Client(xray, config(target.port, "/late-first-byte"), work, name) as client:
            result, event_start, duration, outbound, status = invoke(client)
        target.save(work, name)
    hit = target.hits[0] if target.hits else None
    checks = result_checks(result, outbound, duration, status, False, "no_progress")
    checks.append(check("target received request after live dial", hit and round(hit["received"] - event_start, 2)
                        if hit and event_start else None,
                        hit is not None and event_start is not None and 0 <= hit["received"] - event_start < 2,
                        "0..2 s"))
    return checks


def body_progress(stand, xray, work, name, mux):
    with PhaseTarget() as target:
        with bench.Client(xray, config(target.port, "/body-progress", mux=mux), work, name) as client:
            result, event_start, duration, outbound, status = invoke(client)
        target.save(work, name)
    hit = target.hits[0] if target.hits else None
    chunks = hit.get("chunks", []) if hit else []
    checks = result_checks(result, outbound, duration, status, True, None)
    checks += [check("body chunks sent", len(chunks), len(chunks) == BODY_CHUNKS, str(BODY_CHUNKS)),
               check("body spans beyond dial timeout, s", round(chunks[-1] - chunks[0], 2) if len(chunks) > 1 else None,
                     len(chunks) == BODY_CHUNKS and chunks[-1] - chunks[0] > DIAL_TIMEOUT,
                     f"> {DIAL_TIMEOUT} s"),
               check("probe ran beyond dial timeout, s", round(duration, 2) if duration is not None else None,
                     duration is not None and duration > DIAL_TIMEOUT, f"> {DIAL_TIMEOUT} s")]
    return checks


@scenario()
def phase_body_progress(stand, xray, work, name):
    return body_progress(stand, xray, work, name, mux=False)


@scenario()
def phase_body_progress_mux(stand, xray, work, name):
    return body_progress(stand, xray, work, name, mux=True)


@scenario()
def phase_mux_late_headers(stand, xray, work, name):
    """A probe must finish after 7 s of silence while a user mux stream stays open."""
    port = bench.address_port("tcp_tls", 0)
    with PhaseTarget() as target:
        cfg = bench.client_config([("a1", "tcp_tls", 0)], {"a1": 1}, health={
            "passive": {"dialTimeout": "6s"},
            "probes": [{"name": "default", "destination": f"http://probe.test:{target.port}/late-mux-headers",
                        "expectedStatuses": [200], "minBytes": BODY_BYTES,
                        "timeout": "20s", "progressTimeout": "10s"}],
        })
        next(item for item in cfg["outbounds"] if item.get("tag") == "a1")["mux"] = {
            "enabled": True, "concurrency": 8}
        with bench.Client(xray, cfg, work, name) as client:
            before_warm = bench.chaos_stats()[port][0]
            stream = bench.open_stream(bench.TARGETS[0], 8, tls=False)
            if stream is None:
                raise RuntimeError("mux warm connection was refused")
            try:
                request = (f"GET /hold/12 HTTP/1.1\r\nHost: {bench.TARGETS[0]}\r\n"
                           "Connection: close\r\n\r\n")
                stream.sendall(request.encode())
                warm_head = stream.recv(4096)
                before_probe = bench.chaos_stats()[port][0]
                result, event_start, duration, outbound, status = invoke(client, wait=25, prime_path=False)
                after_probe = bench.chaos_stats()[port][0]
            finally:
                stream.close()
        target.save(work, name)
    hit = next((item for item in target.hits if item["path"] == "/late-mux-headers"), None)
    delta = after_probe - before_probe
    want_delta = expected(0, 1)
    checks = result_checks(result, outbound, duration, status, True, None)
    checks += [check("user mux stream received headers", warm_head[:40].decode("ascii", errors="replace"),
                     warm_head.startswith(b"HTTP/1.1 200"), "HTTP 200"),
               check("warm mux worker connections", before_probe - before_warm,
                     before_probe - before_warm == 1, "== 1"),
               check("new proxy connections during probe", delta, delta == want_delta,
                     f"== {want_delta} ({os.environ.get('BENCH_PHASE_EXPECT', 'after')})"),
               check("target first headers after request, s",
                     round(hit["headers_at"] - hit["received"], 2) if hit and "headers_at" in hit else None,
                     hit is not None and "headers_at" in hit
                     and MUX_LATE_HEADERS <= hit["headers_at"] - hit["received"] < MUX_LATE_HEADERS + 0.7,
                     "7.0..7.7"),
               check("probe survived beyond dial timeout, s", round(duration, 2) if duration is not None else None,
                     duration is not None and DIAL_TIMEOUT < duration < 20, "6..20"),
               check("probe bytes observed", result and result["fields"].get("bytes"), True, "info")]
    return checks


@scenario()
def phase_dead_connect_timeout(stand, xray, work, name):
    port = bench.address_port("reality", 0)
    with PhaseTarget() as target:
        bench.chaos(str(port), "blackhole")
        with bench.Client(xray, dead_config(stand, target.port), work, name) as client:
            result, _, duration, outbound, status = invoke(client, tag="p-a1")
            actual_class = provider_probe_class(stand, client)
        target.save(work, name)
    wanted = expected("no_progress", "dial_timeout")
    checks = result_checks(result, outbound, duration, status, False, wanted,
                           tag="p-a1", actual_class=actual_class)
    checks += [check("dead connection never reached target", len(target.hits), not target.hits, "0"),
               check("dead dial duration, s", round(duration, 2) if duration is not None else None,
                     duration is not None and (3.7 <= duration <= 4.8 if wanted == "no_progress"
                                               else 5.7 <= duration <= 6.8),
                     "3.7..4.8 before; 5.7..6.8 after")]
    return checks


@scenario()
def phase_dead_connect_refused(stand, xray, work, name):
    port = bench.address_port("reality", 0)
    with PhaseTarget() as target:
        bench.chaos(str(port), "refuse")
        with bench.Client(xray, dead_config(stand, target.port), work, name) as client:
            result, _, duration, outbound, status = invoke(client, tag="p-a1")
            actual_class = provider_probe_class(stand, client)
        target.save(work, name)
    wanted = expected("refused", "dial_failed")
    checks = result_checks(result, outbound, duration, status, False, wanted,
                           tag="p-a1", actual_class=actual_class)
    checks.append(check("refused dial never reached target", len(target.hits), not target.hits, "0"))
    return checks


def net_status(port):
    response = urllib.request.urlopen(f"http://127.0.0.1:{bench.CHAOS_CONTROL}/set", timeout=3).read().decode()
    line = next(line for line in response.splitlines() if line.startswith(f"{port} "))
    return {key: int(value) for field in line.split()[2:] if "=" in field
            for key, value in [field.split("=", 1)]}


def network_availability_buckets(samples, records, started, duration):
    if duration % 60:
        raise ValueError("network duration must contain whole minutes")
    buckets = []
    for minute in range(duration // 60):
        start = started + minute * 60
        end = start + 60
        echoes = [sample for sample in samples if sample["at"] >= started
                  and start <= sample["finished_at"] < end]
        user = [record for record in records if start <= record[0] < end]
        buckets.append({"minute": minute + 1, "start": start, "end": end,
                        "echo_attempts": len(echoes),
                        "echo_successes": sum(sample["rtt"] is not None for sample in echoes),
                        "echo_failures": sum(sample["rtt"] is None for sample in echoes),
                        "echo_errors": sum(bool(sample.get("error")) for sample in echoes),
                        "user_requests": len(user), "user_successes": sum(record[1] for record in user),
                        "user_failures": sum(not record[1] for record in user)})
    return buckets


@scenario(slow=True)
def phase_network_800ms_2pct(stand, xray, work, name):
    """Full 20-minute AWG run with real datagram delay and seeded datagram loss."""
    port = bench.AWG_ADDRESS
    cfg = bench.client_config([("w1", "awg", 0)], {"w1": 1}, health={
        "passive": {"dialTimeout": "6s"},
        "probes": [{"name": "default", "destination": f"http://probe.test:{bench.TARGET_PORT}/bytes/32768",
                    "expectedStatuses": [200], "minBytes": 32768,
                    "timeout": "8s", "progressTimeout": "4s"}],
        "schedule": {"probesPerServerNamePerMinute": 6, "hostProbes": {"reuseWindow": "0s"}},
    })
    bench.chaos(str(port), f"net:400:2:{NETWORK_SEED}")
    samples = []
    requests = []
    stop = threading.Event()

    def echo_loop():
        while not stop.is_set():
            at = time.time()
            try:
                rtt = bench.udp_echo(bench.TUNNEL_TARGET, bench.UDP_ECHO_PORT, os.urandom(24), timeout=6)
                error = None
            except Exception as exc:
                rtt = None
                error = repr(exc)
            samples.append({"at": at, "finished_at": time.time(), "rtt": rtt, "error": error})
            stop.wait(2)

    with bench.Client(xray, cfg, work, name) as client:
        traffic = bench.Traffic(rate=1, big=0.5, timeout=15).start()
        time.sleep(3)
        echoer = threading.Thread(target=echo_loop, daemon=True)
        echoer.start()
        began = time.time()
        began_mono = time.monotonic()
        window_end = began + NETWORK_DURATION
        next_probe = began
        while time.monotonic() - began_mono < NETWORK_DURATION:
            if time.time() >= next_probe:
                at = time.time()
                try:
                    answer = client.api("-t", "20", "probe", "auto", "w1", timeout=25)
                    request = answer.get("result") or {}
                    outbound = next((item for item in request.get("outbounds", [])
                                     if item.get("outbound") == "w1"), {})
                    requests.append({"at": at, "result": outbound.get("result"), "source": outbound.get("source")})
                except Exception as exc:
                    requests.append({"at": at, "error": repr(exc)})
                next_probe += NETWORK_PROBE_INTERVAL
            else:
                time.sleep(min(0.2, next_probe - time.time()))
        finished = time.time()
        duration = time.monotonic() - began_mono
        echoer_alive = echoer.is_alive()
        stats = net_status(port)
        stop.set()
        echoer.join(8)
        traffic.stop()
    traffic.save(os.path.join(work, f"{name}-traffic.json"))
    results = [event for event in client.events if event["type"] == "probe_result"
               and event["fields"].get("outbound") == "w1" and began <= event["ts"] / 1000 < window_end]
    downs = [event for event in client.events if event["type"] == "path_state"
             and event["fields"].get("outbound") == "w1" and event["fields"].get("state") == "down"
             and began <= event["ts"] / 1000 < window_end]
    window_samples = [sample for sample in samples if sample["at"] >= began
                      and sample["finished_at"] < window_end]
    window_records = [record for record in traffic.records if began <= record[0] < window_end]
    buckets = network_availability_buckets(samples, traffic.records, began, NETWORK_DURATION)
    successful = [sample["rtt"] for sample in window_samples if sample["rtt"] is not None]
    median_rtt = statistics.median(successful) if successful else None
    echo_errors = [sample for sample in samples if sample["error"]]
    drops = {direction: stats.get(f"net_dropped_{direction}", 0) for direction in ("up", "down")}
    offered = {direction: stats.get(f"net_offered_{direction}", 0) for direction in ("up", "down")}
    drop_rate = {direction: drops[direction] / offered[direction] if offered[direction] else None
                 for direction in ("up", "down")}
    summary = {"configured": {"one_way_delay_ms": 400, "datagram_loss_percent": 2,
                              "seed": NETWORK_SEED, "duration_seconds": NETWORK_DURATION},
               "measured": {"started_at": began, "window_end": window_end, "finished_at": finished,
                            "duration_seconds": duration, "echo_thread_alive_at_end": echoer_alive,
                            "udp_echo_samples": len(window_samples), "udp_echo_success": len(successful),
                            "udp_echo_errors": len(echo_errors),
                            "udp_echo_median_rtt_s": median_rtt, "offered": offered,
                            "dropped": drops, "drop_rate": drop_rate,
                            "probe_results": len(results),
                            "probe_failures": sum(event["fields"].get("is_ok") is False for event in results),
                            "path_down_events": len(downs),
                            "user_requests": len(window_records),
                            "user_successes": sum(record[1] for record in window_records)},
               "availability_by_minute": buckets, "chaos_status": stats,
               "host_requests": requests, "udp_echoes": samples}
    with open(os.path.join(work, f"{name}-network.json"), "w") as stream:
        json.dump(summary, stream, indent=1)
    return [check("configured one-way delay, ms", stats.get("net_delay_ms"),
                  stats.get("net_delay_ms") == 400, "400"),
            check("configured whole-datagram loss, %", stats.get("net_loss_percent"),
                  stats.get("net_loss_percent") == 2 and stats.get("net_seed") == NETWORK_SEED,
                  f"2, seed {NETWORK_SEED}"),
            check("observed echo median RTT, s", round(median_rtt, 3) if median_rtt else None,
                  median_rtt is not None and 0.75 <= median_rtt <= 1.5, "0.75..1.5"),
            check("actual network window duration, s", round(duration, 2),
                  NETWORK_DURATION <= duration <= NETWORK_DURATION + 30, "1200..1230"),
            check("echo RTT samples", len(successful), len(successful) >= 200, ">= 200"),
            check("successful echo in every minute, including final minute",
                  [bucket["echo_successes"] for bucket in buckets],
                  len(buckets) == 20 and all(bucket["echo_successes"] >= 1 for bucket in buckets),
                  "20/20 minutes have >= 1 success"),
            check("echo sampling thread and errors", (echoer_alive, len(echo_errors)),
                  echoer_alive and not echo_errors, "thread alive, 0 errors"),
            check("offered datagrams each way", offered,
                  min(offered.values()) >= 500, ">= 500 each way"),
            check("observed injected drop rate by direction", drop_rate,
                  all(rate is not None and 0.01 <= rate <= 0.03 for rate in drop_rate.values()),
                  "1%..3% each way, target 2%"),
            check("net counter conservation", {direction: (offered[direction],
                  stats.get(f"net_forwarded_{direction}"), drops[direction],
                  stats.get(f"net_pending_{direction}"), stats.get(f"net_cancelled_{direction}"))
                  for direction in ("up", "down")},
                  all(offered[direction] == sum(stats.get(f"net_{field}_{direction}", 0)
                      for field in ("forwarded", "dropped", "pending", "cancelled"))
                      for direction in ("up", "down")), "offered = forwarded + dropped + pending + cancelled"),
            check("probe results", len(results), len(results) >= 30, ">= 30"),
            check("probe failures for BEFORE/AFTER comparison", summary["measured"]["probe_failures"], True,
                  "compare rates over probe_results"),
            check("path down for BEFORE/AFTER comparison", len(downs), True,
                  "compare counts over 20 minutes"),
            check("user success for context", f"{summary['measured']['user_successes']}/{len(window_records)}",
                  True, "compare across runs")]
