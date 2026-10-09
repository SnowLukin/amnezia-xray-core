"""Bench for the health observatory and the leastCost balancer.

One machine, no root: Xray servers for five transports and an AmneziaWG
server, a TCP and UDP proxy that breaks server addresses on command, a local
HTTP target instead of the internet, and an Xray client built from this branch.

Environment:
  BENCH_PORT_OFFSET   moves every port, so two benches can run side by side.
  BENCH_CLIENT        adb:<serial> runs the client under test on an Android
                      device or emulator; everything else stays here.
  BENCH_CLIENT_BIN    the client binary for that device, built with
                      GOOS=android GOARCH=arm64 CGO_ENABLED=0 go build
                      -ldflags=-checklinkname=0 ./main
  BENCH_HOST          the address the client reaches this machine at:
                      127.0.0.1 locally, 10.0.2.2 from an emulator, the LAN
                      address for a phone.
  BENCH_LISTEN        where the chaos proxy, which stands in for every server
                      address including the AmneziaWG ones, listens: 127.0.0.1
                      by default, 0.0.0.0 for a phone on the LAN.
  BENCH_TLS=1         user traffic is TLS 1.3 to the target. Only then does
                      XTLS Vision switch to direct copy, which splices on Linux.
  BENCH_LOGLEVEL      log level of the client, debug to see splices.
  BENCH_CONCURRENT    schedule.concurrent of the client: probes at a time.
"""
import datetime
import http.client
import json
import os
import shlex
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
CERT_DIR = os.path.join(HERE, "certs")
TARGET_CERT = os.path.join(CERT_DIR, "target.crt")
TARGET_KEY = os.path.join(CERT_DIR, "target.key")

UUID = "5f88c4df-2a2d-4b68-9b27-5d21339abb60"
REALITY_PRIVATE = "ODyRd0nJyxk9us_UzqG_jJM4NLTSPPLZfkpcCzuCvUk"
REALITY_PUBLIC = "SFQq-6EUyhY12O_zOjI9pVdY1b7y3aVaevZJg8JDaEw"

CLIENT = os.environ.get("BENCH_CLIENT", "")
ADB_SERIAL = CLIENT[len("adb:"):] if CLIENT.startswith("adb:") else None
CLIENT_BIN = os.environ.get("BENCH_CLIENT_BIN", "")
HOST = os.environ.get("BENCH_HOST", "10.0.2.2" if ADB_SERIAL else "127.0.0.1")
TLS = os.environ.get("BENCH_TLS") == "1"
LOGLEVEL = os.environ.get("BENCH_LOGLEVEL", "warning")
CONCURRENT = int(os.environ.get("BENCH_CONCURRENT", "1"))

OFFSET = int(os.environ.get("BENCH_PORT_OFFSET", "0"))
TARGET_TLS_PORT = 18998 + OFFSET
TARGET_PORT = 18999 + OFFSET
CHAOS_CONTROL = 19000 + OFFSET
# Not 19002: that plus an offset of 7000 is a chaos port of a bench with offset 5000.
PROVIDER_PORT = 19050 + OFFSET
SOCKS_PORT = 10808 + OFFSET
API_PORT = 10085 + OFFSET
METRICS_PORT = 11111 + OFFSET
TRANSPORTS = {"tcp_tls": 1, "reality": 2, "ws": 3, "xhttp": 4, "grpc": 5}
ADDRESSES = 4
TARGETS = [f"t{i}.test" for i in range(1, 7)]
# The timeout of the probes of the client, in seconds.
PROBE_TIMEOUT = 6


def server_port(kind):
    return 20000 + OFFSET + TRANSPORTS[kind]


def address_port(kind, index):
    """The chaos port that stands in for address number index of a transport."""
    return 21000 + OFFSET + index * 10 + TRANSPORTS[kind]


def all_address_ports():
    return [address_port(k, i) for k in TRANSPORTS for i in range(ADDRESSES)] + [CONNECTIVITY_PORT] + AWG_ADDRESSES


# The connectivity check of the client goes through this chaos port to the
# target, so a scenario can take "the internet" away without touching servers.
CONNECTIVITY_PORT = 21099 + OFFSET

# The AmneziaWG server, an Xray process of its own, listens on AWG_PORT and is
# reached through the UDP chaos ports AWG_ADDRESSES, one for each of its peers,
# so that each looks like a server of its own and fails alone. The target
# echoes datagrams on UDP_ECHO_PORT. Each of them ends in digits no other port
# here ends in, so benches whose offsets differ by thousands never take the
# same port.
AWG_PORT = 20061 + OFFSET
AWG_ADDRESSES = [21071 + OFFSET, 21081 + OFFSET]
AWG_ADDRESS = AWG_ADDRESSES[0]
UDP_ECHO_PORT = 18997 + OFFSET
UDP_PORTS = [AWG_PORT, UDP_ECHO_PORT] + AWG_ADDRESSES

AWG_SERVER_PRIVATE = "KLWsMvV9jay7/DbNtHYRzqpRtSSp55JUhzHLmO4rWnI="
AWG_SERVER_PUBLIC = "FaIcjcrbDGj21AiVKy630Bv7q+gLHOV9JGe6abLPL2U="
# The peers of the server, one per address: private key, public key, address
# in the tunnel.
AWG_CLIENTS = [("0BgPQbtCBDnYWtZTdx/L7GYuS79kaV+RcO5TUJR8LVM=", "Qu7uu5ddbF+kuPr6h4Ur0/uMh6GIicWxq5D+nizJjCU=",
                "10.66.0.2"),
               ("yBtgVsRKgu9IxQUjrGwPjIfDf/FG51UfKVbljgKefGo=", "OpsK7Ldg4OhDl/0crBYlKS8tpnM7/LUqRp4MuTMZ/H8=",
                "10.66.0.3")]
AWG_HEADER_KEY = "CKsSn78skewyso2x1VqceKtV9MBdqz8IJ/SkUfCrWkE="
AWG_SERVER_ADDRESS = "10.66.0.1"
# Both sides: junk, paddings, header ranges, header protection and trailers,
# all away from their defaults.
AWG_PARAMS = {"jc": 4, "jmin": 40, "jmax": 70, "s1": 20, "s2": 78, "s3": 15, "s4": 25,
              "h1": "100000-100100", "h2": "200000-200100", "h3": "300000-300100", "h4": "400000-400100",
              "headerProtectionKey": AWG_HEADER_KEY, "randomTrailers": True}
# A device that gets no answer repeats its handshake every AWG_REKEY_TIMEOUT seconds
# (device.RekeyTimeout); each one is the initiation after the I-packets and jc junk packets.
AWG_REKEY_TIMEOUT = 5
AWG_HANDSHAKE_DATAGRAMS = 1 + AWG_PARAMS["jc"] + sum(1 for i in range(1, 6) if f"i{i}" in AWG_PARAMS)
# A loopback address cannot cross the tunnel: the network stacks at both ends
# drop it on an interface that is not loopback. The DNS of the AmneziaWG server
# answers every target with this address instead, and its freedom takes it back
# to loopback on the same port.
TUNNEL_TARGET = "198.18.0.1"
TUNNEL_DEAD = "198.18.0.2"

SERVER_NAMES = {"tcp_tls": "std.example.com", "reality": "example.com", "ws": "ws.example.com",
                "xhttp": "x.example.com", "grpc": "g.example.com"}


def ensure_cert(xray):
    os.makedirs(CERT_DIR, exist_ok=True)
    crt = os.path.join(CERT_DIR, "example.crt")
    if not os.path.exists(crt):
        subprocess.run([xray, "tls", "cert", "--domain=example.com", "--file=" + os.path.join(CERT_DIR, "example")],
                       check=True, stdout=subprocess.DEVNULL)
    # The key xray writes is labelled as RSA whatever it is, which OpenSSL
    # rejects, so the target gets its own pair.
    if not os.path.exists(TARGET_CERT):
        subprocess.run(["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1",
                        "-nodes", "-days", "3650", "-subj", "/CN=target.test", "-keyout", TARGET_KEY,
                        "-out", TARGET_CERT], check=True, capture_output=True)
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


def awg_server_config():
    """The AmneziaWG server. Its DNS, which the client reaches through the
    tunnel, answers every target with TUNNEL_TARGET; everything else that comes
    out of the tunnel goes to loopback on the port it was meant for."""
    hosts = {t: TUNNEL_TARGET for t in TARGETS + ["probe.test"]}
    hosts["dead.test"] = TUNNEL_DEAD
    return {
        "log": {"loglevel": "warning"},
        "dns": {"hosts": hosts, "queryStrategy": "UseIPv4"},
        "inbounds": [{"tag": "awg", "listen": "127.0.0.1", "port": AWG_PORT, "protocol": "wireguard",
                      "settings": {"secretKey": AWG_SERVER_PRIVATE, "address": [AWG_SERVER_ADDRESS], "mtu": 1420,
                                   "peers": [{"publicKey": public, "allowedIPs": [f"{address}/32"]}
                                             for _, public, address in AWG_CLIENTS],
                                   "awg": AWG_PARAMS}}],
        "outbounds": [
            {"tag": "local", "protocol": "freedom",
             "settings": {"redirect": "127.0.0.1:0", "finalRules": [{"action": "allow"}]}},
            {"tag": "dns", "protocol": "dns"},
            {"tag": "dead", "protocol": "blackhole"},
        ],
        "routing": {"rules": [{"port": "53", "outboundTag": "dns"},
                              {"ip": [TUNNEL_DEAD], "outboundTag": "dead"}]},
    }


def awg_member(tag, index=0):
    """A client outbound to the AmneziaWG server as its peer number index,
    through the chaos address of that peer. keepAlive is a range, which the
    device picks every interval from; contentPaddingAddition pads what the
    client sends, which only the client needs to know."""
    private, _, address = AWG_CLIENTS[index]
    return {"tag": tag, "protocol": "wireguard", "settings": {
        "secretKey": private, "address": [f"{address}/32"], "mtu": 1420, "noKernelTun": True,
        "peers": [{"publicKey": AWG_SERVER_PUBLIC, "endpoint": f"{HOST}:{AWG_ADDRESSES[index]}", "keepAlive": "2-3"}],
        "awg": AWG_PARAMS | {"contentPaddingAddition": "8-32"}}}


def member(tag, kind, index=0, pin=None):
    """A client outbound: transport kind through address number index; kind
    awg is the AmneziaWG server, with one address for each of its peers.

    Transports use different server names, as different variants do in
    production; the probe budget per server name depends on it.
    """
    if kind == "awg":
        return awg_member(tag, index)
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
            "settings": {"vnext": [{"address": HOST, "port": address_port(kind, index), "users": [user]}]},
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
        "tag": "fresh", "url": f"http://{HOST}:{PROVIDER_PORT}/set",
        "headers": {"Authorization": "Bearer bench-token"}, "via": via, "timeout": "3s", "allowPlainHttp": True,
        "triggers": {"interval": "0s", "minHealthy": 0, "whenExhausted": False, "maxAge": "0s",
                     "onNetworkChange": "never"} | (triggers or {}),
        "limits": limits or {},
        "apply": {"balancer": "auto", "tagPrefix": "p-", "drain": "1m"} | (apply or {}),
    }


def client_config(members, costs, health=None, strategy=None, burst=None, loglevel=LOGLEVEL, providers=None):
    """members: list of (tag, kind, address index)."""
    # direct comes first, so it is the default outbound: traffic that a
    # balancer fails to hold would end there and show in its counters. It
    # resolves the names of the targets as the servers do, so that such traffic
    # reaches a target instead of failing to find it. The network check has an
    # outbound of its own, so that direct carries nothing but traffic that went
    # around the tunnel.
    outbounds = [{"tag": "direct", "protocol": "freedom",
                  "settings": {"domainStrategy": "UseIPv4", "finalRules": [{"action": "allow"}]}},
                 {"tag": "net", "protocol": "freedom", "settings": {"finalRules": [{"action": "allow"}]}}]
    outbounds += [member(t, k, i, PIN) for t, k, i in members]
    outbounds += [{"tag": "block", "protocol": "blackhole"}]
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
        "dns": {"hosts": {h: HOST for h in TARGETS + ["probe.test", "dead.test"]}},
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
                    "expectStatus": [200], "timeout": f"{PROBE_TIMEOUT}s", "progressTimeout": "3s"}],
        "connectivity": {"outbound": "net", "destination": f"http://{HOST}:{CONNECTIVITY_PORT}/204",
                         "timeout": "2s"},
    }
    if "BENCH_CONCURRENT" in os.environ:
        h["schedule"] = {"concurrent": CONCURRENT}
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
        self.provider_bodies_log = self.provider_log + ".bodies"

    def __enter__(self):
        busy = [p for p in [TARGET_PORT, TARGET_TLS_PORT, CHAOS_CONTROL, PROVIDER_PORT, SOCKS_PORT, API_PORT,
                            METRICS_PORT]
                + [server_port(k) for k in TRANSPORTS] + all_address_ports() if port_open(p)]
        busy += [f"udp {p}" for p in UDP_PORTS if udp_bound(p)]
        if busy:
            raise RuntimeError(f"ports already in use, another bench running? {busy}")
        open(self.target_log, "w").close()
        self.procs.append(subprocess.Popen(
            ["python3", os.path.join(HERE, "target.py"), str(TARGET_PORT), self.target_log, str(TARGET_TLS_PORT),
             TARGET_CERT, TARGET_KEY, str(UDP_ECHO_PORT)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        for name, config in (("server", server_config()), ("server-awg", awg_server_config())):
            path = os.path.join(self.workdir, f"{name}.json")
            with open(path, "w") as f:
                json.dump(config, f)
            self.procs.append(subprocess.Popen([self.xray, "run", "-c", path],
                                               stdout=open(os.path.join(self.workdir, f"{name}.log"), "w"),
                                               stderr=subprocess.STDOUT))
        specs = [f"{address_port(k, i)}:{server_port(k)}" for k in TRANSPORTS for i in range(ADDRESSES)]
        specs.append(f"{CONNECTIVITY_PORT}:{TARGET_PORT}")
        specs += [f"udp:{p}:{AWG_PORT}" for p in AWG_ADDRESSES]
        self.procs.append(subprocess.Popen(["python3", os.path.join(HERE, "chaos.py"), str(CHAOS_CONTROL)] + specs,
                                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        self.answer([("n1", "reality", 2)])
        open(self.provider_log, "w").close()
        open(self.provider_bodies_log, "w").close()
        self.procs.append(subprocess.Popen(
            ["python3", os.path.join(HERE, "provider.py"), str(PROVIDER_PORT), self.provider_answer,
             self.provider_log], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        try:
            for p in [TARGET_PORT, TARGET_TLS_PORT, CHAOS_CONTROL, PROVIDER_PORT] + [server_port(k) for k in TRANSPORTS]:
                wait_port(p)
            for p in UDP_PORTS:
                wait_udp(p)
        except RuntimeError:
            self.__exit__()
            raise
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
        open(self.provider_bodies_log, "w").close()
        provider_mode("ok")

    def answer(self, members, costs=None, version=None, next_refresh=None, expires=None, server_time=None,
               base_version=None):
        """Sets what the provider serves: members as (tag, kind, address index).
        expires (by tag) and server_time take "@+Ns", the time of each
        request plus N seconds."""
        body = {"outbounds": [member(t, k, i, PIN) for t, k, i in members]}
        if costs:
            body["costs"] = costs
        if version:
            body["version"] = version
        if next_refresh:
            body["nextRefreshIn"] = next_refresh
        if expires:
            body["expiresAt"] = expires
        if server_time:
            body["serverTime"] = server_time
        if base_version:
            body["baseVersion"] = base_version
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

    def provider_bodies(self, since=0):
        """{ts, auth, type, bytes, body} of every POST the provider got."""
        with open(self.provider_bodies_log) as f:
            return [b for b in map(json.loads, f) if b["ts"] >= since]

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


def udp_bound(port):
    """Whether something holds a UDP port on loopback: binding it fails then."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.bind(("127.0.0.1", port))
        return False
    except OSError:
        return True
    finally:
        s.close()


def wait_udp(port, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        if udp_bound(port):
            return
        time.sleep(0.1)
    raise RuntimeError(f"udp port {port} did not open")


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


def chaos_stats():
    """Per chaos port: (connections accepted, bytes the client sent, bytes that reached it,
    datagrams the client sent, datagrams that reached it). A UDP port counts client
    addresses as connections; a TCP port has no datagrams."""
    out = urllib.request.urlopen(f"http://127.0.0.1:{CHAOS_CONTROL}/set", timeout=3).read().decode()
    res = {}
    for line in out.strip().splitlines():
        port, _, *fields = line.split(" ")
        f = dict(field.split("=", 1) for field in fields)
        res[int(port)] = (int(f["conns"]), int(f["up"]), int(f["down"]), int(f.get("pkts_up", 0)),
                          int(f.get("pkts_down", 0)))
    return res


class LocalProcess:
    """The client as a process on this machine."""

    splices = sys.platform.startswith("linux")

    def __init__(self, binary):
        self.binary = binary
        self.clock_offset = 0.0

    def start(self, config, log, env):
        self.proc = subprocess.Popen([self.binary, "run", "-c", config], stdout=open(log, "w"),
                                     stderr=subprocess.STDOUT, env=dict(os.environ, **env))

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()

    def cpu_seconds(self):
        out = subprocess.run(["ps", "-o", "time=", "-p", str(self.proc.pid)], capture_output=True,
                             text=True).stdout.strip()
        total = 0.0
        for p in out.replace("-", ":").split(":"):
            total = total * 60 + float(p)
        return total


class AdbProcess:
    """The client on an Android device over adb. Its socks, API and metrics
    ports are forwarded to the same ports here, so the bench talks to it as
    to a local client; the log is pulled when it stops."""

    # A Linux kernel: XTLS Vision splices there.
    splices = True
    # USER_HZ of Android kernels, the unit of utime and stime in /proc.
    CLOCK_TICKS = 100
    _pushed = {}

    def __init__(self, serial, binary):
        if not binary:
            raise RuntimeError("BENCH_CLIENT_BIN must name the client binary for the device")
        self.serial = serial
        self.binary = binary
        self.dir = f"/data/local/tmp/adaptive-bench-{OFFSET}"
        self.ports = [SOCKS_PORT, API_PORT, METRICS_PORT]

    def adb(self, *args, timeout=120, check=True):
        out = subprocess.run(["adb", "-s", self.serial, *args], capture_output=True, text=True, timeout=timeout)
        if check and out.returncode != 0:
            raise RuntimeError(f"adb {' '.join(args)}: {out.stderr.strip() or out.stdout.strip()}")
        return out.stdout

    def shell(self, command, **kw):
        return self.adb("shell", command, **kw)

    def _push_binary(self):
        st = os.stat(self.binary)
        stamp = (self.binary, st.st_mtime, st.st_size)
        if AdbProcess._pushed.get(self.serial) == stamp:
            return
        self.shell(f"mkdir -p {self.dir}")
        self.adb("push", self.binary, f"{self.dir}/xray", timeout=300)
        self.shell(f"chmod 755 {self.dir}/xray")
        AdbProcess._pushed[self.serial] = stamp

    def _measure_clock(self):
        """Device clock minus ours, from the round trip with the smallest delay."""
        best = None
        for _ in range(5):
            t0 = time.time()
            remote = float(self.shell("date +%s.%N").strip())
            t1 = time.time()
            if best is None or t1 - t0 < best[0]:
                best = (t1 - t0, remote - (t0 + t1) / 2)
        self.clock_offset = best[1]

    def start(self, config, log, env):
        self.log = log
        self._push_binary()
        self.shell(f"[ -f {self.dir}/client.pid ] && kill $(cat {self.dir}/client.pid) 2>/dev/null", check=False)
        self.adb("push", config, f"{self.dir}/client.json")
        for p in self.ports:
            self.adb("forward", f"tcp:{p}", f"tcp:{p}")
        self._measure_clock()
        envs = " ".join(f"{k}={shlex.quote(v)}" for k, v in env.items())
        command = (f"cd {self.dir} && echo $$ > client.pid && "
                   f"exec env {envs} ./xray run -c client.json > client.log 2>&1")
        self.proc = subprocess.Popen(["adb", "-s", self.serial, "shell", command], stdin=subprocess.DEVNULL,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def stop(self):
        self.shell(f"kill $(cat {self.dir}/client.pid)", check=False)
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.shell(f"kill -9 $(cat {self.dir}/client.pid)", check=False)
            self.proc.kill()
        self.adb("pull", f"{self.dir}/client.log", self.log, check=False)
        for p in self.ports:
            self.adb("forward", "--remove", f"tcp:{p}", check=False)

    def cpu_seconds(self):
        pid = self.shell(f"cat {self.dir}/client.pid").strip()
        fields = self.shell(f"cat /proc/{pid}/stat").rsplit(")", 1)[1].split()
        # utime and stime are fields 14 and 15 of the line; fields here start at 3.
        return (int(fields[11]) + int(fields[12])) / self.CLOCK_TICKS


def client_process(xray):
    if ADB_SERIAL:
        return AdbProcess(ADB_SERIAL, CLIENT_BIN)
    return LocalProcess(xray)


class Client:
    """The Xray client under test with its API, metrics and event log.

    xray is the local binary: it runs the client unless BENCH_CLIENT says
    otherwise, and always serves as the API command line."""

    def __init__(self, xray, config, workdir, name, profile=False):
        # profile records every allocation, so small heaps of the mechanism
        # show up in heap profiles (the default records one per 512 KB).
        self.env = {"GODEBUG": "memprofilerate=1"} if profile else {}
        self.xray = xray
        self.process = client_process(xray)
        self.path = os.path.join(workdir, f"client-{name}.json")
        with open(self.path, "w") as f:
            json.dump(config, f, indent=1)
        self.log_path = os.path.join(workdir, f"client-{name}.log")
        self.loglevel = config["log"]["loglevel"]
        self.events = []
        self.dropped = 0
        # Reports taken while the client ran, and the last one at the stop.
        self.reports = []
        self.final_report = None
        self.mem = []
        self._stop = threading.Event()
        self.has_health = "healthObservatory" in config
        # Bytes (up, down) that went through outbound direct by the time the
        # client stopped; None if the counters could not be read.
        self.direct_bytes = None

    def __enter__(self):
        self.process.start(self.path, self.log_path, self.env)
        wait_ready()
        self.thread = threading.Thread(target=self._poll, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self.thread.join(timeout=5)
        try:
            self.direct_bytes = self.outbound_bytes("direct")
        except Exception:
            pass
        if self.has_health:
            self.drain()
            try:
                REPORT_CHECKS.append(self._check_reports())
            except Exception as e:
                REPORT_CHECKS.append({"check": "reports add up the events", "value": repr(e), "limit": "== events",
                                      "ok": False})
            with open(self.log_path.replace(".log", "-events.json"), "w") as f:
                json.dump({"dropped": self.dropped, "events": self.events, "reports": self.reports}, f, indent=1)
        self.process.stop()
        wait_closed(SOCKS_PORT)

    def cpu_seconds(self):
        return self.process.cpu_seconds()

    def splices(self):
        """Connections the client spliced, from its debug log; None when the
        log does not tell or the platform never splices."""
        if self.loglevel != "debug" or not self.process.splices:
            return None
        with open(self.log_path, errors="replace") as f:
            return sum(1 for line in f if "CopyRawConn splice" in line)

    def api(self, *args, timeout=10, stdin=None):
        out = subprocess.run([self.xray, "api", "health", f"--server=127.0.0.1:{API_PORT}", *args],
                             capture_output=True, text=True, timeout=timeout, input=stdin)
        if out.returncode != 0:
            raise RuntimeError(f"api health {args}: {out.stderr.strip()}")
        return json.loads(out.stdout)

    def network(self, network_id=""):
        return self.api("network", network_id) if network_id else self.api("network")

    def drain(self):
        r = self.api("events")
        # Event times come from the client's clock; scenarios compare them with ours.
        shift = round(self.process.clock_offset * 1000)
        for e in r["events"]:
            e["ts"] -= shift
        self.events.extend(r["events"])
        self.dropped += r["dropped"]

    def status(self):
        return self.api("status")

    def take_report(self):
        r = self.api("report", "take")
        if r:
            self.reports.append(r)
        return r

    def _check_reports(self):
        """Takes the last report, as a host does before it stops the core, and
        checks every report of the run against the events of its window."""
        quiet_until = len(self.events)
        snapshot = self.api("report")
        final = self.api("report", "final")
        again = self.api("report", "final")
        self.drain()
        self.final_report = final
        if final:
            self.reports.append(final)
        problems = []
        if again is not None:
            problems.append("a second final report")
        if final and (not snapshot or snapshot["report_reason"] != "snapshot"
                      or snapshot["report_index"] != final["report_index"]):
            problems.append(f"snapshot {snapshot and snapshot['report_reason']} before the final report")
        if not final and fold_report(self.events[:quiet_until])[1]:
            problems.append("no final report after a decision")
        by_seq = {e["seq"]: e for e in self.events}
        last = 0
        for i, r in enumerate(self.reports):
            window = [by_seq.get(seq) for seq in range(last + 1, last + r["events"] + 1)]
            last += r["events"]
            if None in window:
                continue
            want, _ = fold_report(window)
            for k, v in want.items():
                if r[k] != v:
                    problems.append(f"report {i + 1} {k}={r[k]!r}, events add up to {v!r}")
            if r["report_index"] != i + 1:
                problems.append(f"report {i + 1} has index {r['report_index']}")
        value = "; ".join(problems[:3]) if problems else f"{len(self.reports)} reports"
        return {"check": "reports add up the events", "value": value, "limit": "== events", "ok": not problems}

    def memory(self):
        vars_ = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{METRICS_PORT}/debug/vars", timeout=3).read())
        g = urllib.request.urlopen(f"http://127.0.0.1:{METRICS_PORT}/debug/pprof/goroutine?debug=1",
                                   timeout=3).read().decode()
        goroutines = int(g.split("total ")[1].split("\n")[0])
        return vars_["memstats"]["HeapInuse"], goroutines

    def goroutines(self, function):
        """How many goroutines of the client have function in their stack."""
        dump = urllib.request.urlopen(f"http://127.0.0.1:{METRICS_PORT}/debug/pprof/goroutine?debug=1",
                                      timeout=3).read().decode()
        return sum(int(block.split(" @", 1)[0]) for block in dump.split("\n\n") if " @" in block and function in block)

    def routed(self, since, until=None):
        """User connections the client sent to each outbound between two times,
        from its access log (read after it stopped). None when the log's clock
        is not ours, as on a device."""
        if ADB_SERIAL:
            return None
        count = {}
        with open(self.log_path, errors="replace") as f:
            for line in f:
                if " accepted " not in line or "[socks -> " not in line:
                    continue
                stamp = datetime.datetime.strptime(line[:26], "%Y/%m/%d %H:%M:%S.%f").timestamp()
                if stamp >= since and (until is None or stamp <= until):
                    tag = line.rsplit("[socks -> ", 1)[1].split("]")[0]
                    count[tag] = count.get(tag, 0) + 1
        return count

    def outbound_bytes(self, tag):
        """(uplink, downlink) bytes counted for an outbound by the stats of the client."""
        vars_ = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{METRICS_PORT}/debug/vars", timeout=3).read())
        counters = vars_["stats"]["outbound"].get(tag, {})
        return counters.get("uplink", 0), counters.get("downlink", 0)

    def gc_memory(self, label=None):
        """Heap and goroutines after GC. With a label, the goroutine and heap
        profiles are saved next to the log to compare where growth sits."""
        heap = urllib.request.urlopen(f"http://127.0.0.1:{METRICS_PORT}/debug/pprof/heap?gc=1", timeout=5).read()
        if label:
            base = self.log_path.replace(".log", f"-{label}")
            with open(base + "-heap.pb.gz", "wb") as f:
                f.write(heap)
            g = urllib.request.urlopen(f"http://127.0.0.1:{METRICS_PORT}/debug/pprof/goroutine?debug=1", timeout=5).read()
            with open(base + "-goroutines.txt", "wb") as f:
                f.write(g)
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

    def mechanism_goroutines(self):
        """Goroutines whose stack runs through the health observatory. User
        connections stuck in a dial of Xray itself are not among them."""
        text = urllib.request.urlopen(f"http://127.0.0.1:{METRICS_PORT}/debug/pprof/goroutine?debug=1",
                                      timeout=5).read().decode(errors="replace")
        n = 0
        for block in text.split("\n\n"):
            head = block.split(" @", 1)[0].strip()
            if head.isdigit() and "observatory/health" in block:
                n += int(head)
        return n

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


def wait_ready(timeout=60):
    """Waits until the client answers on socks and metrics. A port forwarded
    by adb accepts connections before the client listens, so an open port
    alone proves nothing."""
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection(("127.0.0.1", SOCKS_PORT), timeout=2) as s:
                s.settimeout(2)
                s.sendall(b"\x05\x01\x00")
                if s.recv(2) == b"\x05\x00":
                    urllib.request.urlopen(f"http://127.0.0.1:{METRICS_PORT}/debug/vars", timeout=2).read()
                    return
        except (OSError, http.client.HTTPException):
            pass
        time.sleep(0.2)
    raise RuntimeError("client did not start")


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
    instead. timeout is how long a request waits, like an app would. tls
    makes every request HTTPS.
    """

    def __init__(self, rate=2.0, big=0.5, targets=None, timeout=10.0, plan=None, tls=TLS):
        self.rate = rate
        self.tls = tls
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

    def save(self, path):
        """Writes the requests as (start, ok, seconds, bytes, host) for a later look."""
        with self._lock, open(path, "w") as f:
            json.dump(sorted(self.records), f)

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
            s = open_stream(host, self.timeout, self.tls)
            if s is not None:
                s.sendall(f"GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n".encode())
                head, body = read_answer(s, t0 + self.timeout)
                ok = head.startswith(b"HTTP/1.1 20") and body >= want
                size = len(head) + body
                s.close()
        except OSError:
            pass
        with self._lock:
            self.records.append((t0, ok, time.time() - t0, size, host))


class ConnSampler:
    """Samples chaos counters, so a check can count connections and bytes
    of an address within a time window."""

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
                self.samples.append((time.time(), chaos_stats()))
            except Exception:
                pass
            self._stop.wait(self.period)

    def _delta(self, ports, t1, t2, field):
        before = [c for t, c in self.samples if t <= t1]
        after = [c for t, c in self.samples if t <= t2]
        if not before or not after:
            return None
        ports = ports if isinstance(ports, (list, tuple, set)) else [ports]
        none = (0, 0, 0, 0, 0)
        return sum(after[-1].get(p, none)[field] - before[-1].get(p, none)[field] for p in ports)

    def between(self, ports, t1, t2):
        """Connections accepted on the port (or ports) between t1 and t2."""
        return self._delta(ports, t1, t2, 0)

    def traffic(self, ports, t1, t2):
        """Bytes sent by the client and bytes that reached it, between t1 and t2."""
        return self._delta(ports, t1, t2, 1), self._delta(ports, t1, t2, 2)

    def packets(self, ports, t1, t2):
        """Datagrams sent by the client and datagrams that reached it on UDP ports, between t1 and t2."""
        return self._delta(ports, t1, t2, 3), self._delta(ports, t1, t2, 4)


def open_stream(host, timeout, tls=TLS):
    """A connection through the client's socks inbound to the target as host,
    over TLS 1.3 if tls; None when the client refused it."""
    s = socket.create_connection(("127.0.0.1", SOCKS_PORT), timeout=timeout)
    s.settimeout(timeout)
    s.sendall(b"\x05\x01\x00")
    s.recv(2)
    hb = host.encode()
    port = TARGET_TLS_PORT if tls else TARGET_PORT
    s.sendall(b"\x05\x01\x00\x03" + bytes([len(hb)]) + hb + struct.pack(">H", port))
    rep = s.recv(10)
    if len(rep) < 2 or rep[1] != 0:
        s.close()
        return None
    return TLS_CLIENT.wrap_socket(s, server_hostname=host) if tls else s


def _tls_client():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    return ctx


TLS_CLIENT = _tls_client()


def read_answer(s, deadline=None):
    """Reads one HTTP answer up to its Content-Length, as a browser does: a
    connection may end with a reset after a complete answer. Returns the head
    and how many body bytes came, whatever stopped the reading."""
    buf = bytearray(1 << 20)
    data = b""
    head, length, body = b"", None, 0
    try:
        while deadline is None or time.time() < deadline:
            n = s.recv_into(buf)
            if n == 0:
                break
            if length is None:
                data += bytes(buf[:n])
                if b"\r\n\r\n" not in data:
                    continue
                head, rest = data.split(b"\r\n\r\n", 1)
                length = next((int(line.split(b":", 1)[1]) for line in head.split(b"\r\n")
                               if line.lower().startswith(b"content-length:")), 0)
                body = len(rest)
            else:
                body += n
            if body >= length:
                break
    except OSError:
        pass
    return head, body


def udp_echo(ip, port, payload, timeout=5.0):
    """Sends one datagram through the socks inbound of the client to ip:port, and
    never again: UDP is not retried. Returns the seconds until it came back, or
    None if it did not. Only for a client on this machine: adb forwards TCP."""
    ctl = socket.create_connection(("127.0.0.1", SOCKS_PORT), timeout=timeout)
    u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        ctl.settimeout(timeout)
        ctl.sendall(b"\x05\x01\x00")
        ctl.recv(2)
        # UDP ASSOCIATE; the datagrams come from an address the client learns from the first one.
        ctl.sendall(b"\x05\x03\x00\x01" + bytes(6))
        rep = ctl.recv(10)
        if len(rep) < 10 or rep[1] != 0:
            return None
        relay = socket.inet_ntoa(rep[4:8])
        relay = ("127.0.0.1" if relay == "0.0.0.0" else relay, struct.unpack(">H", rep[8:10])[0])
        u.settimeout(timeout)
        t0 = time.time()
        u.sendto(b"\x00\x00\x00\x01" + socket.inet_aton(ip) + struct.pack(">H", port) + payload, relay)
        try:
            data, _ = u.recvfrom(65536)
        except socket.timeout:
            return None
        return time.time() - t0 if data[10:] == payload else None
    finally:
        u.close()
        ctl.close()


def download(host, path, timeout=120, tls=TLS):
    """One request through the client; returns body bytes received."""
    s = open_stream(host, timeout, tls)
    s.sendall(f"GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n".encode())
    _, body = read_answer(s)
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


# Checks of the reports of every client with a health observatory, collected
# when it stops; run.py adds them to the checks of the scenario.
REPORT_CHECKS = []

START_REASONS = ("start", "start_pinned", "start_remembered", "start_with")


def fold_report(events):
    """Adds up events the way the report of the core counts them. Returns the
    counters and whether there was a decision."""
    keys = ["events", "switches", "exhausted", "path_down", "path_suspect", "pending_closed", "idle_closed",
            "probes", "probes_failed", "probe_bytes", "host_probe_requests", "host_probes", "upgrade_attempts",
            "upgrade_failures", "pinned_down",
            "network_changes", "network_known", "network_down", "provider_fetches", "provider_fetch_failures",
            "provider_retry_after", "provider_applied", "provider_rejected", "provider_restart_needed",
            "provider_expired", "provider_renewals", "provider_auth_failed", "provider_base_changed", "pins",
            "releases", "memory_low", "outbound_sleeps"]
    c = dict.fromkeys(keys, 0)
    c.update(switch_reasons={}, switch_path=[], provider_triggers={})
    decided = False
    for e in events:
        t, f = e["type"], e.get("fields", {})
        c["events"] += 1
        if t == "switch":
            reason = f.get("reason", "")
            c["switches"] += 1
            c["switch_reasons"][reason] = c["switch_reasons"].get(reason, 0) + 1
            c["switch_path"] = (c["switch_path"] + [f"{f.get('from', '')}>{f.get('to', '')}:{reason}"])[-10:]
            decided = decided or reason not in START_REASONS
        elif t in ("exhausted", "pinned_down"):
            c[t] += 1
            decided = decided or not f.get("start_pinned")
        elif t == "path_state" and f.get("state") in ("down", "suspect"):
            c["path_" + f["state"]] += 1
        elif t == "pending_closed":
            c["pending_closed"] += f.get("connections", 0)
        elif t == "idle_closed":
            c["idle_closed"] += f.get("connections", 0)
        elif t == "probe_result":
            c["probes"] += 1
            c["probes_failed"] += f.get("ok") is not True
            c["probe_bytes"] += f.get("bytes", 0)
            c["host_probes"] += f.get("reason") == "host"
        elif t == "probe_request":
            c["host_probe_requests"] += 1
        elif t == "upgrade_start":
            c["upgrade_attempts"] += 1
        elif t == "upgrade_failed":
            c["upgrade_failures"] += 1
            decided = True
        elif t == "network_change":
            c["network_changes"] += 1
            c["network_known"] += f.get("known") is True
        elif t == "network_down":
            c["network_down"] += 1
        elif t == "provider_fetch":
            c["provider_fetches"] += 1
            c["provider_fetch_failures"] += f.get("ok") is not True
            c["provider_retry_after"] += "retry_after_ms" in f
            if "trigger" in f:
                c["provider_triggers"][f["trigger"]] = c["provider_triggers"].get(f["trigger"], 0) + 1
        elif t == "provider_apply":
            if f.get("ok") is False:
                c["provider_rejected"] += 1
                decided = True
            elif f.get("changed") is True and f.get("trigger") != "initial":
                c["provider_applied"] += 1
                decided = True
            if f.get("ok") is True and f.get("renewed", 0) > 0 and f.get("trigger") != "initial":
                c["provider_renewals"] += 1
        elif t == "provider_restart_needed":
            c["provider_restart_needed"] += 1
            decided = True
        elif t in ("provider_expired", "provider_auth_failed", "provider_base_changed"):
            c[t] += 1
            decided = decided or f.get("renewal") != "saved"
        elif t in ("pin", "release"):
            c[t + "s"] += 1
        elif t == "memory_low":
            c["memory_low"] += f.get("on") is True
        elif t == "outbound_sleep":
            c["outbound_sleeps"] += 1
    return c, decided


def pop_report_checks():
    out = list(REPORT_CHECKS)
    REPORT_CHECKS.clear()
    return out


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
