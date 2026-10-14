"""The shape of the requests to the backend when an entry of many clients dies.

    python3 reserve_herd.py <xray binary> <out dir> <clients> <scheme:delay> [...]

scheme is A or B1s of reserve_ab.py, delay "0s" or "default". Every client is
a process of its own with its own ports and its own key at the backend; all
start on entry 1, each sends a request a second with a random phase. The
entry dies 80 s after the start, as in scheme B1s.
"""
import json
import os
import random
import socket
import struct
import subprocess
import sys
import threading
import time

import bench
import reserve_ab
from api_v2_recipe import config as api_config

FAULT_AT = 80
OBSERVE = 120
# Below the ports of benches with offsets 26000 and up.
BASE_PORT = 41000


def request(socks, timeout=4):
    t0 = time.time()
    ok = False
    try:
        s = socket.create_connection(("127.0.0.1", socks), timeout=timeout)
        s.settimeout(timeout)
        s.sendall(b"\x05\x01\x00")
        s.recv(2)
        host = random.choice(bench.TARGETS[:3]).encode()
        s.sendall(b"\x05\x01\x00\x03" + bytes([len(host)]) + host + struct.pack(">H", bench.TARGET_PORT))
        rep = s.recv(10)
        if len(rep) >= 2 and rep[1] == 0:
            s.sendall(b"GET /204 HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
            ok = s.recv(64).startswith(b"HTTP/1.1 20")
        s.close()
    except OSError:
        pass
    return t0, ok


def one(stand, backend, xray, out, clients, spec):
    scheme, delay = spec.split(":")
    stand.reset()
    backend.reset("1", scheme)
    procs, stop, records = [], threading.Event(), {}
    for i in range(clients):
        cfg = reserve_ab.client_config(scheme, "entries", delay, key=f"Bearer c{i}")
        socks, api, metrics = BASE_PORT + i * 3, BASE_PORT + i * 3 + 1, BASE_PORT + i * 3 + 2
        cfg["inbounds"][0]["port"] = socks
        cfg["api"]["listen"] = f"127.0.0.1:{api}"
        cfg["metrics"]["listen"] = f"127.0.0.1:{metrics}"
        cfg["log"]["loglevel"] = "none"
        path = os.path.join(stand.workdir, f"herd-{i}.json")
        with open(path, "w") as f:
            json.dump(api_config(cfg), f)
        procs.append(subprocess.Popen([xray, "run", "-c", path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        records[i] = []
    for i in range(clients):
        bench.wait_port(BASE_PORT + i * 3, timeout=30)

    def user(i):
        time.sleep(random.random())
        while not stop.is_set():
            records[i].append(request(BASE_PORT + i * 3))
            stop.wait(max(0, 1 - (time.time() - records[i][-1][0])))
    threads = [threading.Thread(target=user, args=(i,), daemon=True) for i in range(clients)]
    started = time.time()
    for t in threads:
        t.start()
    time.sleep(max(0, started + FAULT_AT - time.time()))
    fault = time.time()
    bench.chaos(bench.address_port("reality", 0), "blackhole")
    time.sleep(OBSERVE)
    stop.set()
    for t in threads:
        t.join(6)
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(5)
        except subprocess.TimeoutExpired:
            p.kill()
    rows = [dict(r, at=round(r["at"] - fault, 3)) for r in backend.log if r["at"] >= fault - 1]
    recoveries = []
    for i in range(clients):
        rec, _ = bench.recovery([(t, ok, 0, 0, "") for t, ok in records[i]], fault, streak=5)
        recoveries.append(rec and round(rec, 2))
    result = {"spec": spec, "clients": clients, "requests": rows, "recoveries": recoveries}
    with open(os.path.join(out, f"herd_{scheme}_{delay}.json"), "w") as f:
        json.dump(result, f, indent=1)
    return result


def main():
    xray = os.path.abspath(sys.argv[1])
    out = os.path.abspath(sys.argv[2])
    clients = int(sys.argv[3])
    os.makedirs(out, exist_ok=True)
    reserve_ab.patch_server_config()
    backend = reserve_ab.Backend().serve()
    work = bench.workdir()
    with bench.Stand(xray, work, probe_credentials=True) as stand:
        for spec in sys.argv[4:]:
            r = one(stand, backend, xray, out, clients, spec)
            times = sorted(x["at"] for x in r["requests"])
            rec = sorted(x for x in r["recoveries"] if x is not None)
            print(json.dumps({"spec": spec, "requests": len(times),
                              "firstLast": times and [times[0], times[-1]],
                              "recovered": len(rec), "recoveryMedian": rec and rec[len(rec) // 2],
                              "recoveryMax": rec and rec[-1]}), flush=True)


if __name__ == "__main__":
    main()
