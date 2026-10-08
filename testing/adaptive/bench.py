"""Bench for the health observatory and the leastCost balancer.

One machine, no root: Xray servers for five transports, a TCP proxy that
breaks server addresses on command, a local HTTP target instead of the
internet, and an Xray client built from this branch. See README.md.
"""
import json
import os
import socket
import struct
import subprocess
import tempfile
import threading
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
CERT_DIR = os.path.join(HERE, "certs")

UUID = "5f88c4df-2a2d-4b68-9b27-5d21339abb60"
REALITY_PRIVATE = "ODyRd0nJyxk9us_UzqG_jJM4NLTSPPLZfkpcCzuCvUk"
REALITY_PUBLIC = "SFQq-6EUyhY12O_zOjI9pVdY1b7y3aVaevZJg8JDaEw"

# BENCH_PORT_OFFSET moves every port, so two benches can run side by side.
OFFSET = int(os.environ.get("BENCH_PORT_OFFSET", "0"))
TARGET_PORT = 18999 + OFFSET
CHAOS_CONTROL = 19000 + OFFSET
PROVIDER_PORT = 19002 + OFFSET
SOCKS_PORT = 10808 + OFFSET
API_PORT = 10085 + OFFSET
METRICS_PORT = 11111 + OFFSET
TRANSPORTS = {"tcp_tls": 1, "reality": 2, "ws": 3, "xhttp": 4, "grpc": 5}
ADDRESSES = 4
TARGETS = [f"t{i}.test" for i in range(1, 7)]


def server_port(kind):
    return 20000 + OFFSET + TRANSPORTS[kind]


def address_port(kind, index):
    """The chaos port that stands in for address number index of a transport."""
    return 21000 + OFFSET + index * 10 + TRANSPORTS[kind]


def all_address_ports():
    return [address_port(k, i) for k in TRANSPORTS for i in range(ADDRESSES)] + [CONNECTIVITY_PORT]


# The connectivity check of the client goes through this chaos port to the
# target, so a scenario can take "the internet" away without touching servers.
CONNECTIVITY_PORT = 21099 + OFFSET

SERVER_NAMES = {"tcp_tls": "std.example.com", "reality": "example.com", "ws": "ws.example.com",
                "xhttp": "x.example.com", "grpc": "g.example.com"}


def ensure_cert(xray):
    os.makedirs(CERT_DIR, exist_ok=True)
    crt = os.path.join(CERT_DIR, "example.crt")
    if not os.path.exists(crt):
        subprocess.run([xray, "tls", "cert", "--domain=example.com", "--file=" + os.path.join(CERT_DIR, "example")],
                       check=True, stdout=subprocess.DEVNULL)
    out = subprocess.run(["openssl", "x509", "-in", crt, "-noout", "-fingerprint", "-sha256"],
                         check=True, capture_output=True, text=True).stdout
    return out.strip().split("=", 1)[1]


def server_config():
    tls = {"certificates": [{"certificateFile": os.path.join(CERT_DIR, "example.crt"),
                             "keyFile": os.path.join(CERT_DIR, "example.key")}]}

    def inbound(kind, stream, flow=None):
        client = {"id": UUID}
        if flow:
            client["flow"] = flow
        return {"tag": kind, "listen": "127.0.0.1", "port": server_port(kind), "protocol": "vless",
                "settings": {"clients": [client], "decryption": "none"}, "streamSettings": stream}

    hosts = {t: "127.0.0.1" for t in TARGETS + ["probe.test", "dead.test"]}
    return {
        "log": {"loglevel": "warning"},
        "dns": {"hosts": hosts},
        "inbounds": [
            inbound("tcp_tls", {"network": "tcp", "security": "tls", "tlsSettings": tls}),
            inbound("reality", {"network": "tcp", "security": "reality", "realitySettings": {
                "target": f"127.0.0.1:{server_port('tcp_tls')}", "serverNames": ["example.com"],
                "privateKey": REALITY_PRIVATE, "shortIds": [""]}}, "xtls-rprx-vision"),
            inbound("ws", {"network": "ws", "security": "tls", "tlsSettings": tls, "wsSettings": {"path": "/ws"}}),
            inbound("xhttp", {"network": "xhttp", "security": "tls", "tlsSettings": tls,
                              "xhttpSettings": {"path": "/x", "mode": "auto"}}),
            inbound("grpc", {"network": "grpc", "security": "tls", "tlsSettings": tls,
                             "grpcSettings": {"serviceName": "svc"}}),
        ],
        "outbounds": [
            {"protocol": "freedom", "settings": {"domainStrategy": "UseIPv4", "finalRules": [{"action": "allow"}]}},
            {"tag": "dead", "protocol": "blackhole"},
        ],
        "routing": {"rules": [{"domain": ["full:dead.test"], "outboundTag": "dead"}]},
    }


def member(tag, kind, index=0, pin=None):
    """A client outbound: transport kind through address number index.

    Transports use different server names, as different variants do in
    production; the probe budget per server name depends on it.
    """
    tls = {"serverName": SERVER_NAMES[kind], "pinnedPeerCertSha256": pin}
    streams = {
        "tcp_tls": {"network": "tcp", "security": "tls", "tlsSettings": tls},
        "reality": {"network": "tcp", "security": "reality", "realitySettings": {
            "serverName": "example.com", "password": REALITY_PUBLIC, "shortId": "", "fingerprint": "chrome"}},
        "ws": {"network": "ws", "security": "tls", "tlsSettings": tls, "wsSettings": {"path": "/ws"}},
        "xhttp": {"network": "xhttp", "security": "tls", "tlsSettings": dict(tls, alpn=["h2"]),
                  "xhttpSettings": {"path": "/x", "mode": "auto"}},
        "grpc": {"network": "grpc", "security": "tls", "tlsSettings": dict(tls, alpn=["h2"]),
                 "grpcSettings": {"serviceName": "svc"}},
    }
    user = {"id": UUID, "encryption": "none"}
    if kind == "reality":
        user["flow"] = "xtls-rprx-vision"
    return {"tag": tag, "protocol": "vless",
            "settings": {"vnext": [{"address": "127.0.0.1", "port": address_port(kind, index), "users": [user]}]},
            "streamSettings": streams[kind]}


def plain(cfg, outbound):
    """Turns a client config into one without the mechanism: all traffic to one outbound."""
    del cfg["healthObservatory"]
    cfg["api"]["services"] = ["RoutingService", "StatsService"]
    cfg["routing"] = {"rules": [{"inboundTag": ["socks"], "outboundTag": outbound}]}
    return cfg


def provider_config(via, triggers=None, limits=None, apply=None):
    """A provider served by provider.py. Every trigger is off unless the
    scenario turns it on, so a scenario sees only the trigger it checks."""
    return {
        "tag": "fresh", "url": f"http://127.0.0.1:{PROVIDER_PORT}/set",
        "headers": {"Authorization": "Bearer bench-token"}, "via": via, "timeout": "3s", "allowPlainHttp": True,
        "triggers": {"interval": "0s", "minHealthy": 0, "whenExhausted": False, "maxAge": "0s",
                     "onNetworkChange": "never"} | (triggers or {}),
        "limits": limits or {},
        "apply": {"balancer": "auto", "tagPrefix": "p-", "drain": "1m"} | (apply or {}),
    }


def client_config(members, costs, health=None, strategy=None, burst=None, loglevel="warning", providers=None):
    """members: list of (tag, kind, address index)."""
    outbounds = [member(t, k, i, PIN) for t, k, i in members]
    outbounds += [{"tag": "direct", "protocol": "freedom", "settings": {"finalRules": [{"action": "allow"}]}},
                  {"tag": "block", "protocol": "blackhole"}]
    selectors = [t for t, _, _ in members]
    cfg = {
        "log": {"loglevel": loglevel},
        "api": {"tag": "api", "listen": f"127.0.0.1:{API_PORT}",
                "services": ["HealthService", "RoutingService", "StatsService", "ObservatoryService"]},
        "metrics": {"tag": "metrics", "listen": f"127.0.0.1:{METRICS_PORT}"},
        "stats": {},
        "policy": {"system": {"statsOutboundUplink": True, "statsOutboundDownlink": True}},
        "inbounds": [{"tag": "socks", "listen": "127.0.0.1", "port": SOCKS_PORT, "protocol": "socks",
                      "settings": {"udp": True}}],
        "outbounds": outbounds,
        "routing": {"rules": [{"inboundTag": ["socks"], "balancerTag": "auto"}], "balancers": []},
    }
    if burst is not None:
        cfg["api"]["services"] = ["RoutingService", "StatsService", "ObservatoryService"]
        cfg["routing"]["balancers"].append({"tag": "auto", "selector": selectors, "strategy": {"type": "random"},
                                            "fallbackTag": burst["fallback"]})
        cfg["burstObservatory"] = {"subjectSelector": burst["subjects"], "pingConfig": burst["ping"]}
        return cfg
    settings = {"costs": [{"match": t, "value": c} for t, c in costs.items()]}
    settings.update(strategy or {})
    subjects = list(selectors)
    if providers:
        selectors = selectors + [p["apply"]["tagPrefix"] for p in providers]
    cfg["routing"]["balancers"].append({"tag": "auto", "selector": selectors,
                                        "strategy": {"type": "leastCost", "settings": settings},
                                        "fallbackTag": "block"})
    h = {
        "subjectSelector": subjects,
        "probes": [{"destination": f"http://probe.test:{TARGET_PORT}/bytes/32768", "minBytes": 32768,
                    "expectStatus": [200], "timeout": "6s", "progressTimeout": "3s"}],
        "connectivity": {"outbound": "direct", "destination": f"http://127.0.0.1:{CONNECTIVITY_PORT}/204",
                         "timeout": "2s"},
    }
    for k, v in (health or {}).items():
        if isinstance(v, dict) and isinstance(h.get(k), dict):
            h[k] = h[k] | v
        else:
            h[k] = v
    if providers:
        h["providers"] = providers
    cfg["healthObservatory"] = h
    return cfg


PIN = None


class Stand:
    """Starts the target, the servers and the chaos proxy; stops them on exit."""

    def __init__(self, xray, workdir):
        global PIN
        self.xray = xray
        self.workdir = workdir
        self.procs = []
        PIN = ensure_cert(xray)
        self.target_log = os.path.join(workdir, "target.log")
        self.provider_answer = os.path.join(workdir, "provider-answer.json")
        self.provider_log = os.path.join(workdir, "provider.log")

    def __enter__(self):
        busy = [p for p in [TARGET_PORT, CHAOS_CONTROL, PROVIDER_PORT, SOCKS_PORT, API_PORT, METRICS_PORT]
                + [server_port(k) for k in TRANSPORTS] + all_address_ports() if port_open(p)]
        if busy:
            raise RuntimeError(f"ports already in use, another bench running? {busy}")
        open(self.target_log, "w").close()
        self.procs.append(subprocess.Popen(
            ["python3", os.path.join(HERE, "target.py"), str(TARGET_PORT), self.target_log],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        path = os.path.join(self.workdir, "server.json")
        with open(path, "w") as f:
            json.dump(server_config(), f)
        self.procs.append(subprocess.Popen([self.xray, "run", "-c", path],
                                           stdout=open(os.path.join(self.workdir, "server.log"), "w"),
                                           stderr=subprocess.STDOUT))
        specs = [f"{address_port(k, i)}:{server_port(k)}" for k in TRANSPORTS for i in range(ADDRESSES)]
        specs.append(f"{CONNECTIVITY_PORT}:{TARGET_PORT}")
        self.procs.append(subprocess.Popen(["python3", os.path.join(HERE, "chaos.py"), str(CHAOS_CONTROL)] + specs,
                                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        self.answer([("n1", "reality", 2)])
        open(self.provider_log, "w").close()
        self.procs.append(subprocess.Popen(
            ["python3", os.path.join(HERE, "provider.py"), str(PROVIDER_PORT), self.provider_answer,
             self.provider_log], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        wait_port(TARGET_PORT)
        wait_port(CHAOS_CONTROL)
        wait_port(PROVIDER_PORT)
        for k in TRANSPORTS:
            wait_port(server_port(k))
        return self

    def __exit__(self, *exc):
        for p in self.procs:
            p.terminate()
        for p in self.procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()

    def reset(self):
        chaos(",".join(map(str, all_address_ports())), "pass")
        open(self.target_log, "w").close()
        open(self.provider_log, "w").close()
        provider_mode("ok")

    def answer(self, members, costs=None, version=None, next_refresh=None):
        """Sets what the provider serves: members as (tag, kind, address index)."""
        body = {"outbounds": [member(t, k, i, PIN) for t, k, i in members]}
        if costs:
            body["costs"] = costs
        if version:
            body["version"] = version
        if next_refresh:
            body["nextRefreshIn"] = next_refresh
        with open(self.provider_answer, "w") as f:
            json.dump(body, f)

    def provider_requests(self, since=0):
        """(time, authorization, path) of every request the provider got."""
        out = []
        with open(self.provider_log) as f:
            for line in f:
                ts, auth, path = line.strip().split(" ", 2)
                if float(ts) >= since:
                    out.append((float(ts), auth.replace("_", " "), path))
        return out

    def probe_hits(self, since):
        hits = []
        with open(self.target_log) as f:
            for line in f:
                ts, host, path = line.split(" ", 2)
                if host == "probe.test" and float(ts) >= since:
                    hits.append(float(ts))
        return hits


def port_open(port):
    try:
        socket.create_connection(("127.0.0.1", port), timeout=0.3).close()
        return True
    except OSError:
        return False


def wait_port(port, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"port {port} did not open")


def provider_mode(mode):
    return urllib.request.urlopen(f"http://127.0.0.1:{PROVIDER_PORT}/control?mode={mode}", timeout=3).read()


def chaos(ports, mode):
    return urllib.request.urlopen(f"http://127.0.0.1:{CHAOS_CONTROL}/set?port={ports}&mode={mode}", timeout=3).read()


def chaos_conns():
    out = urllib.request.urlopen(f"http://127.0.0.1:{CHAOS_CONTROL}/set", timeout=3).read().decode()
    res = {}
    for line in out.strip().splitlines():
        p, _, c = line.split(" ")
        res[int(p)] = int(c.split("=")[1])
    return res


class Client:
    """The Xray client under test with its API, metrics and event log."""

    def __init__(self, xray, config, workdir, name, profile=False):
        # profile records every allocation, so small heaps of the mechanism
        # show up in heap profiles (the default records one per 512 KB).
        self.env = dict(os.environ, GODEBUG="memprofilerate=1") if profile else None
        self.xray = xray
        self.path = os.path.join(workdir, f"client-{name}.json")
        with open(self.path, "w") as f:
            json.dump(config, f, indent=1)
        self.log_path = os.path.join(workdir, f"client-{name}.log")
        self.events = []
        self.dropped = 0
        self.mem = []
        self._stop = threading.Event()
        self.has_health = "healthObservatory" in config

    def __enter__(self):
        self.proc = subprocess.Popen([self.xray, "run", "-c", self.path], stdout=open(self.log_path, "w"),
                                     stderr=subprocess.STDOUT, env=self.env)
        wait_port(SOCKS_PORT)
        wait_port(API_PORT)
        self.thread = threading.Thread(target=self._poll, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self.thread.join(timeout=5)
        if self.has_health:
            self.drain()
            with open(self.log_path.replace(".log", "-events.json"), "w") as f:
                json.dump({"dropped": self.dropped, "events": self.events}, f, indent=1)
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        wait_closed(SOCKS_PORT)

    def api(self, *args):
        out = subprocess.run([self.xray, "api", "health", f"--server=127.0.0.1:{API_PORT}", *args],
                             capture_output=True, text=True, timeout=10)
        if out.returncode != 0:
            raise RuntimeError(f"api health {args}: {out.stderr.strip()}")
        return json.loads(out.stdout)

    def network(self, network_id=""):
        return self.api("network", network_id) if network_id else self.api("network")

    def drain(self):
        r = self.api("events")
        self.events.extend(r["events"])
        self.dropped += r["dropped"]

    def status(self):
        return self.api("status")

    def memory(self):
        vars_ = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{METRICS_PORT}/debug/vars", timeout=3).read())
        g = urllib.request.urlopen(f"http://127.0.0.1:{METRICS_PORT}/debug/pprof/goroutine?debug=1",
                                   timeout=3).read().decode()
        goroutines = int(g.split("total ")[1].split("\n")[0])
        return vars_["memstats"]["HeapInuse"], goroutines

    def gc_memory(self):
        urllib.request.urlopen(f"http://127.0.0.1:{METRICS_PORT}/debug/pprof/heap?gc=1", timeout=5).read()
        return self.memory()

    def mechanism_heap(self, label):
        """Live heap held by allocations made through the health observatory,
        probe connections included, measured from a heap profile after GC."""
        path = self.log_path.replace(".log", f"-heap-{label}.pb.gz")
        data = urllib.request.urlopen(f"http://127.0.0.1:{METRICS_PORT}/debug/pprof/heap?gc=1", timeout=10).read()
        with open(path, "wb") as f:
            f.write(data)
        out = subprocess.run(["go", "tool", "pprof", "-top", "-sample_index=inuse_space",
                              "-focus=observatory/health", path], capture_output=True, text=True).stdout
        for line in out.splitlines():
            if line.startswith("Showing nodes accounting for"):
                return parse_size(line.split("for ", 1)[1].split(",")[0])
        return 0

    def _poll(self):
        while not self._stop.wait(1.0):
            try:
                heap, gor = self.memory()
                self.mem.append((time.time(), heap, gor))
                if self.has_health:
                    self.drain()
            except Exception:
                pass


def parse_size(s):
    s = s.strip()
    units = {"B": 1, "kB": 1 << 10, "KB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30}
    for u in sorted(units, key=len, reverse=True):
        if s.endswith(u):
            return float(s[:-len(u)]) * units[u]
    return float(s)


def wait_closed(port, timeout=5):
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            time.sleep(0.1)
        except OSError:
            return


class Traffic:
    """User traffic: a new connection per request, rotating over targets.

    big is the share of requests that download 64 KB instead of an empty
    answer. plan, when given, is a list of (host, path, body bytes) cycled
    instead. timeout is how long a request waits, like an app would.
    """

    def __init__(self, rate=2.0, big=0.5, targets=None, timeout=10.0, plan=None):
        self.rate = rate
        self.big = big
        self.targets = targets or TARGETS[:3]
        self.timeout = timeout
        self.plan = plan
        self.records = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self.paused = threading.Event()

    def start(self):
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        return self

    def stop(self):
        self._stop.set()
        self.thread.join(timeout=10)
        time.sleep(self.timeout + 0.5)

    def _run(self):
        i = 0
        while not self._stop.is_set():
            if self.rate > 0 and not self.paused.is_set():
                if self.plan:
                    host, path, want = self.plan[i % len(self.plan)]
                else:
                    big = self.big >= 1 or (self.big > 0 and i % max(1, round(1 / self.big)) == 0)
                    host = self.targets[i % len(self.targets)]
                    path, want = ("/bytes/65536", 65536) if big else ("/204", 0)
                threading.Thread(target=self._one, args=(host, path, want), daemon=True).start()
                i += 1
                self._stop.wait(1.0 / self.rate)
            else:
                self._stop.wait(0.2)

    def _one(self, host, path, want):
        t0 = time.time()
        ok = False
        size = 0
        try:
            s = socket.create_connection(("127.0.0.1", SOCKS_PORT), timeout=self.timeout)
            s.settimeout(self.timeout)
            s.sendall(b"\x05\x01\x00")
            s.recv(2)
            hb = host.encode()
            s.sendall(b"\x05\x01\x00\x03" + bytes([len(hb)]) + hb + struct.pack(">H", TARGET_PORT))
            rep = s.recv(10)
            if len(rep) >= 2 and rep[1] == 0:
                s.sendall(f"GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n".encode())
                data = b""
                deadline = t0 + self.timeout
                while time.time() < deadline:
                    chunk = s.recv(65536)
                    if not chunk:
                        break
                    data += chunk
                ok = data.startswith(b"HTTP/1.1 20") and len(data.split(b"\r\n\r\n", 1)[-1]) >= want
                size = len(data)
            s.close()
        except OSError:
            pass
        with self._lock:
            self.records.append((t0, ok, time.time() - t0, size, host))


class ConnSampler:
    """Samples chaos connection counters, so a check can count connections
    to an address within a time window."""

    def __init__(self, period=0.25):
        self.period = period
        self.samples = []
        self._stop = threading.Event()

    def __enter__(self):
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self.thread.join(timeout=5)

    def _run(self):
        while not self._stop.is_set():
            try:
                self.samples.append((time.time(), chaos_conns()))
            except Exception:
                pass
            self._stop.wait(self.period)

    def between(self, port, t1, t2):
        before = [c for t, c in self.samples if t <= t1]
        after = [c for t, c in self.samples if t <= t2]
        if not before or not after:
            return None
        return after[-1].get(port, 0) - before[-1].get(port, 0)


def download(host, path, timeout=120):
    """One request through the client; returns body bytes received."""
    s = socket.create_connection(("127.0.0.1", SOCKS_PORT), timeout=timeout)
    s.settimeout(timeout)
    s.sendall(b"\x05\x01\x00")
    s.recv(2)
    hb = host.encode()
    s.sendall(b"\x05\x01\x00\x03" + bytes([len(hb)]) + hb + struct.pack(">H", TARGET_PORT))
    s.recv(10)
    s.sendall(f"GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n".encode())
    head = b""
    while b"\r\n\r\n" not in head:
        chunk = s.recv(65536)
        if not chunk:
            break
        head += chunk
    body = len(head.split(b"\r\n\r\n", 1)[1]) if b"\r\n\r\n" in head else 0
    buf = bytearray(1 << 20)
    while True:
        n = s.recv_into(buf)
        if n == 0:
            break
        body += n
    s.close()
    return body


def recovery(records, at, streak=10, hosts=None):
    """Seconds from at until streak requests in a row succeeded, and failures before that."""
    after = sorted((r[0], r[1]) for r in records if r[0] >= at and (hosts is None or r[4] in hosts))
    for i in range(len(after)):
        window = after[i:i + streak]
        if len(window) == streak and all(ok for _, ok in window):
            fails = sum(1 for t, ok in after[:i] if not ok)
            return after[i][0] - at, fails
    return None, sum(1 for _, ok in after if not ok)


def events_of(events, typ, since=0):
    return [e for e in events if e["type"] == typ and e["ts"] / 1000 >= since]


def switches(events, since=0):
    return [e for e in events if e["type"] == "switch" and e["ts"] / 1000 >= since]


def probes(events, since=0, until=None):
    return [e for e in events if e["type"] == "probe_start" and e["ts"] / 1000 >= since
            and (until is None or e["ts"] / 1000 <= until)]


def workdir():
    d = tempfile.mkdtemp(prefix="adaptive-bench-")
    return d
