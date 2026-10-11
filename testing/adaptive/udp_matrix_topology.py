"""Additional local hops used only by the stage 1 matrix."""

import copy
import json
import os
import socket
import ssl
import subprocess
import threading
import time

import bench
from udp_matrix import HELPER_PORTS, STUN_DESTINATION, save_json


def local_server_config(work, original):
    config = original()
    config["log"]["access"] = os.path.join(work, "server-access.log")
    config["outbounds"].append({"tag": "stun-helper", "protocol": "freedom",
                                "settings": {"redirect": f"127.0.0.1:{HELPER_PORTS['stun']}",
                                             "finalRules": [{"action": "allow"}]}})
    config["routing"]["rules"].insert(0, {"port": str(STUN_DESTINATION), "outboundTag": "stun-helper"})
    config["outbounds"].append({"tag": "rtc-hub", "protocol": "freedom",
                                "settings": {"redirect": "127.0.0.1:0",
                                             "finalRules": [{"action": "allow"}]}})
    config["routing"]["rules"].insert(1, {"ip": ["198.18.0.1"], "outboundTag": "rtc-hub"})
    return config


def local_awg_server_config(work, original):
    config = original()
    config["log"]["access"] = os.path.join(work, "server-awg-access.log")
    config["outbounds"].extend([
        {"tag": "cloudflare-dns", "protocol": "freedom",
         "settings": {"finalRules": [{"action": "allow"}]}},
        {"tag": "stun-helper", "protocol": "freedom",
         "settings": {"redirect": f"127.0.0.1:{HELPER_PORTS['stun']}",
                      "finalRules": [{"action": "allow"}]}},
    ])
    config["routing"]["rules"][:0] = [
        {"ip": ["1.1.1.1"], "port": "53", "outboundTag": "cloudflare-dns"},
        {"port": str(STUN_DESTINATION), "outboundTag": "stun-helper"},
    ]
    return config


def relay_config(work):
    server = bench.server_config()
    inbound = copy.deepcopy(next(item for item in server["inbounds"] if item["tag"] == "reality"))
    inbound["tag"] = "relay-entry"
    inbound["port"] = HELPER_PORTS["relay"]
    outbound = bench.member("relay-next", "reality", 0)
    next_hop = outbound["settings"]["vnext"][0]
    next_hop["address"] = "127.0.0.1"
    next_hop["port"] = bench.server_port("reality")
    return {"log": {"loglevel": "info", "access": os.path.join(work, "relay-access.log")}, "inbounds": [inbound],
            "outbounds": [outbound],
            "routing": {"rules": [{"inboundTag": ["relay-entry"], "outboundTag": "relay-next"}]}}


def path_switch_server_config(work):
    base = bench.server_config()
    reality = next(item for item in base["inbounds"] if item["tag"] == "reality")
    inbounds, outbounds, rules = [], [], []
    for tag in ("old", "new"):
        inbound = copy.deepcopy(reality)
        inbound.update(tag=f"switch-{tag}", port=HELPER_PORTS[f"switch_{tag}"])
        inbounds.append(inbound)
        outbounds.append({"tag": f"echo-{tag}", "protocol": "freedom",
                          "settings": {"redirect": f"127.0.0.1:{HELPER_PORTS[f'switch_{tag}_echo']}",
                                       "finalRules": [{"action": "allow"}]}})
        rules.append({"inboundTag": [f"switch-{tag}"], "network": "udp",
                      "outboundTag": f"echo-{tag}"})
    return {"log": {"loglevel": "info", "access": os.path.join(work, "switch-server-access.log")},
            "dns": base["dns"], "inbounds": inbounds,
            "outbounds": [{"tag": "direct", "protocol": "freedom",
                           "settings": {"finalRules": [{"action": "allow"}]}}] + outbounds,
            "routing": {"rules": rules}}


class PathSwitchHop:
    def __init__(self, xray, work):
        self.xray, self.work = xray, work

    def __enter__(self):
        config_path = os.path.join(self.work, "switch-server.json")
        save_json(config_path, path_switch_server_config(self.work))
        self.log = open(os.path.join(self.work, "switch-server.log"), "w")
        self.process = subprocess.Popen([self.xray, "run", "-c", config_path],
                                        stdout=self.log, stderr=subprocess.STDOUT)
        try:
            for tag in ("old", "new"):
                bench.wait_port(HELPER_PORTS[f"switch_{tag}"])
        except Exception:
            self.__exit__()
            raise
        return self

    def __exit__(self, *_):
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        self.log.close()


class RelayHop:
    def __init__(self, xray, work):
        self.xray, self.work = xray, work
        self.config_path = os.path.join(work, "relay-hop.json")
        self.log_path = os.path.join(work, "relay-hop.log")
        self.process = None

    def __enter__(self):
        save_json(self.config_path, relay_config(self.work))
        self.log = open(self.log_path, "w")
        self.process = subprocess.Popen([self.xray, "run", "-c", self.config_path],
                                        stdout=self.log, stderr=subprocess.STDOUT)
        bench.wait_port(HELPER_PORTS["relay"])
        return self

    def __exit__(self, *_):
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
        self.log.close()


class TLSProxy:
    """Local TLS-terminating hop; forwards HTTP/1.1 WS and HTTP/2 XHTTP bytes."""

    def __init__(self, kind, work):
        self.kind, self.work = kind, work
        self.listen_port = HELPER_PORTS[f"{kind}_proxy"]
        self.target_port = bench.server_port(kind)
        self.records = []
        self.listener = None
        self.stop = threading.Event()
        self.threads = []

    def __enter__(self):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        context.load_cert_chain(os.path.join(bench.CERT_DIR, "example.crt"),
                                os.path.join(bench.CERT_DIR, "example.key"))
        context.set_alpn_protocols(["h2", "http/1.1"])
        self.context = context
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", self.listen_port))
        self.listener.listen(16)
        self.listener.settimeout(0.2)
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=3)
        self.listener.close()
        for worker in self.threads:
            worker.join(timeout=3)
        save_json(os.path.join(self.work, f"{self.kind}-proxy.json"), self.records)

    def _accept(self):
        while not self.stop.is_set():
            try:
                incoming, address = self.listener.accept()
            except socket.timeout:
                continue
            worker = threading.Thread(target=self._connection, args=(incoming, address), daemon=True)
            worker.start()
            self.threads.append(worker)

    def _connection(self, incoming, address):
        row = {"opened_ns": time.monotonic_ns(), "source": address, "up_bytes": 0,
               "down_bytes": 0, "kind": self.kind}
        self.records.append(row)
        try:
            with self.context.wrap_socket(incoming, server_side=True) as client:
                row["client_alpn"] = client.selected_alpn_protocol()
                upstream = ssl.create_default_context()
                upstream.check_hostname = False
                upstream.verify_mode = ssl.CERT_NONE
                upstream.set_alpn_protocols([row["client_alpn"] or "http/1.1"])
                with socket.create_connection(("127.0.0.1", self.target_port), 5) as plain:
                    with upstream.wrap_socket(plain, server_hostname=bench.SERVER_NAMES[self.kind]) as server:
                        row["server_alpn"] = server.selected_alpn_protocol()
                        if (row["client_alpn"] or "http/1.1") != (row["server_alpn"] or "http/1.1"):
                            raise RuntimeError("ALPN differs across local proxy")
                        done = threading.Event()
                        copy = threading.Thread(target=self._pipe, args=(client, server, row, "up_bytes", done),
                                                daemon=True)
                        copy.start()
                        self._pipe(server, client, row, "down_bytes", done)
                        copy.join(timeout=2)
        except Exception as error:
            row["error"] = repr(error)
        finally:
            row["closed_ns"] = time.monotonic_ns()
            try:
                incoming.close()
            except OSError:
                pass

    @staticmethod
    def _pipe(source, target, row, counter, done):
        try:
            while not done.is_set():
                data = source.recv(65536)
                if not data:
                    return
                target.sendall(data)
                row[counter] += len(data)
        except OSError:
            pass
        finally:
            done.set()


def matrix_config(path):
    tag = f"matrix-{path}"
    kind = {"reality": "reality", "relay": "reality", "ws_local_proxy": "ws",
            "xhttp_local_proxy": "xhttp", "awg": "awg"}[path]
    config = bench.client_config([(tag, kind, 0)], {tag: 1}, health={"report": {"quietShare": 1}},
                                 loglevel="info")
    outbound = next(item for item in config["outbounds"] if item.get("tag") == tag)
    if path == "relay":
        outbound["settings"]["vnext"][0]["port"] = HELPER_PORTS["relay"]
    elif path in ("ws_local_proxy", "xhttp_local_proxy"):
        outbound["settings"]["vnext"][0]["port"] = HELPER_PORTS[f"{kind}_proxy"]
    return config, tag


def destination_host(path):
    return bench.TUNNEL_TARGET if path == "awg" else "127.0.0.1"
