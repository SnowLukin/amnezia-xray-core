"""Runs bench scenarios and checks them against the acceptance thresholds.

    python3 run.py <xray binary> [scenario ...]

Prints one line per check and writes a JSON report next to the logs.
"""
import json
import os
import sys
import time
import traceback

import bench
import scenarios
import cost_scenarios
import return_interval_scenarios
import path_scenarios
import phase_timeout_scenarios
import probe_only_scenarios
import udp_probe_scenarios


def main():
    xray = os.path.abspath(sys.argv[1])
    wanted = sys.argv[2:]
    unknown = sorted(set(wanted) - {s.name for s in scenarios.ALL})
    if unknown:
        raise SystemExit(f"unknown scenarios: {', '.join(unknown)}")
    work = bench.workdir()
    print(f"workdir {work}", flush=True)
    results = []
    with bench.Stand(xray, work, probe_credentials=not wanted or any(
            name.startswith("probe_only_") or name == "udp_probe_credentials" for name in wanted),
            udp_observation=os.environ.get("BENCH_UDP_PROBE") == "1" or not wanted or any(
                name.startswith("udp_probe_") for name in wanted)) as stand:
        for sc in scenarios.ALL:
            if wanted and sc.name not in wanted:
                continue
            if not wanted and sc.slow and not os.environ.get("BENCH_SLOW"):
                print(f"skip  {sc.name} (slow, set BENCH_SLOW=1)", flush=True)
                continue
            stand.reset()
            t0 = time.time()
            try:
                checks = sc.run(stand, xray, work)
            except Exception as e:
                traceback.print_exc()
                checks = [{"check": "scenario ran", "value": repr(e), "limit": "", "ok": False}]
            checks += bench.pop_report_checks()
            for c in checks:
                mark = "PASS" if c["ok"] else "FAIL"
                print(f"{mark}  {sc.name:32s} {c['check']:44s} {c['value']!s:>12} {c['limit']}", flush=True)
            results.append({"scenario": sc.name, "seconds": round(time.time() - t0, 1), "checks": checks})
    path = os.path.join(work, "report.json")
    with open(path, "w") as f:
        json.dump(results, f, indent=1)
    failed = sum(1 for r in results for c in r["checks"] if not c["ok"])
    total = sum(len(r["checks"]) for r in results)
    print(f"\n{total - failed}/{total} checks passed, report {path}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
