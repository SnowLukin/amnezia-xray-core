#!/usr/bin/env python3
"""Private --network none container tc setup. Never invokes sudo/modprobe/sysctl.

All command output is preserved. No network commands run at import time.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import subprocess
import time

OWNER = Path("/tmp/xray-quality-tc-owner.json")
DEVICES = ("ifbqup", "ifbqdown")


def run(argv, output, *, check=True):
    start_wall = time.time_ns()
    start_mono = time.monotonic_ns()
    proc = subprocess.run(argv, capture_output=True, text=True, timeout=20)
    item = {"argv": argv, "start_wall_ns": start_wall,
            "start_monotonic_ns": start_mono,
            "end_wall_ns": time.time_ns(), "end_monotonic_ns": time.monotonic_ns(),
            "returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
    with (output / "commands.jsonl").open("a") as f:
        f.write(json.dumps(item) + "\n")
        f.flush()
    if check and proc.returncode:
        raise RuntimeError(f"{argv!r}: {proc.returncode}: {proc.stderr.strip()}")
    return proc


def guard():
    if not Path("/.dockerenv").exists() or os.environ.get("XRAY_QUALITY_ISOLATED") != "1":
        raise RuntimeError("Run only in dedicated Docker --network none container with XRAY_QUALITY_ISOLATED=1")
    if platform.machine() not in ("aarch64", "arm64"):
        raise RuntimeError(f"Native arm64 required; machine={platform.machine()}")
    status = Path("/proc/self/status").read_text()
    caps = int(next(line.split()[1] for line in status.splitlines() if line.startswith("CapEff:")), 16)
    if os.geteuid() != 0 or not caps & (1 << 12):
        raise RuntimeError("Container root with CAP_NET_ADMIN required")
    links = json.loads(subprocess.run(["ip", "-j", "link"], check=True,
                                    capture_output=True, text=True).stdout)
    unexpected = [link["ifname"] for link in links if link["ifname"] not in ("lo", *DEVICES)]
    if unexpected:
        raise RuntimeError(f"Expected isolated none namespace; unexpected interfaces: {unexpected}")
    return links


def dump(output, port):
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S") + f"-{time.time_ns()}"
    snap = output / ("dump-" + stamp)
    snap.mkdir()
    (snap / "proc-self-status.txt").write_text(Path("/proc/self/status").read_text())
    (snap / "identity.json").write_text(json.dumps({
        "machine": platform.machine(), "kernel": platform.release(),
        "network_namespace": os.readlink("/proc/self/ns/net"), "pid": os.getpid(),
        "wall_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns(), "port": port}, indent=2))
    commands = {
        "links": ["ip", "-d", "link", "show"],
        "lo-offloads": ["ethtool", "-k", "lo"],
        "qdisc-all": ["tc", "-s", "qdisc", "show"],
        "filters": ["tc", "-s", "filter", "show", "dev", "lo", "ingress"],
        "client-sockets": ["ss", "-tinp", f"( dport = :{port} )"],
        "chaos-sockets": ["ss", "-tinp", f"( sport = :{port} )"],
        "iproute-version": ["tc", "-V"],
    }
    for label, argv in commands.items():
        proc = run(argv, output, check=False)
        (snap / (label + ".txt")).write_text(proc.stdout + proc.stderr)
    return snap


def setup(output, port):
    links = guard()
    if any(link["ifname"] in DEVICES for link in links) or OWNER.exists():
        raise RuntimeError("Own IFB/state already exists; cleanup before setup")
    qdiscs = run(["tc", "-j", "qdisc", "show", "dev", "lo"], output)
    if any(q["kind"] == "clsact" for q in json.loads(qdiscs.stdout)):
        raise RuntimeError("Existing lo clsact; refusing replacement")
    OWNER.write_text(json.dumps({"namespace": os.readlink("/proc/self/ns/net"), "port": port,
                                 "original_lo": links[0], "created_wall_ns": time.time_ns()}))
    try:
        run(["ip", "link", "set", "lo", "mtu", "1500"], output)
        run(["ethtool", "-K", "lo", "tso", "off", "gso", "off", "gro", "off"], output)
        flags = run(["ethtool", "-k", "lo"], output).stdout
        for name in ("tcp-segmentation-offload", "generic-segmentation-offload", "generic-receive-offload"):
            line = next((line.strip() for line in flags.splitlines() if line.strip().startswith(name + ":")), "")
            if ": off" not in line:
                raise RuntimeError(f"Per-segment impairment needs {name}=off; got {line!r}")
        for dev in DEVICES:
            run(["ip", "link", "add", dev, "type", "ifb"], output)
            run(["ip", "link", "set", dev, "mtu", "1500", "up"], output)
        run(["tc", "qdisc", "add", "dev", "lo", "clsact"], output)
        for preference, key, dev in ((10, "dst_port", DEVICES[0]), (20, "src_port", DEVICES[1])):
            run(["tc", "filter", "add", "dev", "lo", "ingress", "protocol", "ip",
                 "pref", str(preference), "flower", "skip_hw", "src_ip", "127.0.0.1/32",
                 "dst_ip", "127.0.0.1/32", "ip_proto", "tcp", key, str(port),
                 "action", "mirred", "egress", "redirect", "dev", dev], output)
        profile(output, "clean")
    except BaseException:
        cleanup(output)
        raise


def profile(output, name):
    guard()
    if not OWNER.exists():
        raise RuntimeError("Call setup first")
    configs = {
        "clean": (["netem", "limit", "10000"], ["netem", "limit", "10000"]),
        "baseline": (["netem", "delay", "10ms", "limit", "10000"],
                     ["netem", "delay", "10ms", "limit", "10000"]),
        "rtt": (["netem", "delay", "50ms", "limit", "10000"],
                ["netem", "delay", "50ms", "limit", "10000"]),
        "loss": (["netem", "delay", "10ms", "loss", "random", "2%", "limit", "10000"],
                 ["netem", "delay", "10ms", "loss", "random", "2%", "limit", "10000"]),
        "queue": (["netem", "delay", "10ms", "limit", "10000"],
                  ["tbf", "rate", "5mbit", "burst", "16kb", "limit", "512kb"]),
    }
    up, down = configs[name]
    for dev, handle, options in ((DEVICES[0], "10:", up), (DEVICES[1], "20:", down)):
        run(["tc", "qdisc", "replace", "dev", dev, "root", "handle", handle, *options], output)


def cleanup(output):
    guard()
    if not OWNER.exists():
        return
    owner = json.loads(OWNER.read_text())
    if owner["namespace"] != os.readlink("/proc/self/ns/net"):
        raise RuntimeError("State belongs to a different namespace")
    run(["tc", "qdisc", "del", "dev", "lo", "clsact"], output, check=False)
    for dev in DEVICES:
        run(["ip", "link", "del", dev], output, check=False)
    remaining_links = json.loads(run(["ip", "-j", "link"], output).stdout)
    remaining_qdiscs = json.loads(run(["tc", "-j", "qdisc", "show", "dev", "lo"], output).stdout)
    remaining = {"own_ifb": [link["ifname"] for link in remaining_links if link["ifname"] in DEVICES],
                 "lo_clsact": any(q["kind"] == "clsact" for q in remaining_qdiscs)}
    (output / "cleanup-state.json").write_text(json.dumps(remaining, indent=2) + "\n")
    if remaining["own_ifb"] or remaining["lo_clsact"]:
        raise RuntimeError(f"Own tc cleanup incomplete; container namespace deletion remains final cleanup: {remaining}")
    OWNER.unlink()
    # Original MTU/offloads are intentionally restored by namespace deletion.
    # This container must not be reused for cost/acceptance measurements.


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("verb", choices=("preflight", "setup", "profile", "dump", "cleanup"))
    parser.add_argument("--port", type=int, default=21002)
    parser.add_argument("--profile", choices=("clean", "baseline", "rtt", "loss", "queue"), default="clean")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("invalid TCP port")
    args.output.mkdir(parents=True, exist_ok=True)
    guard()
    if args.verb == "preflight":
        dump(args.output, args.port)
        try:
            setup(args.output, args.port)
            profile(args.output, "rtt")
            dump(args.output, args.port)
            (args.output / "preflight-success.json").write_text(json.dumps({
                "kernel": platform.release(), "native_machine": platform.machine(),
                "ifb_netem_flower": "created successfully", "network_traffic_smoke": "not run"}, indent=2))
        finally:
            cleanup(args.output)
    elif args.verb == "setup":
        setup(args.output, args.port)
        dump(args.output, args.port)
    elif args.verb == "profile":
        profile(args.output, args.profile)
        dump(args.output, args.port)
    elif args.verb == "dump":
        print(dump(args.output, args.port))
    else:
        cleanup(args.output)
        dump(args.output, args.port)


if __name__ == "__main__":
    main()
