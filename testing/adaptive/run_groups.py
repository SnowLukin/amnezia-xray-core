"""Run the 24-relay group bench with the ordinary adaptive report runner.

BENCH_GROUPS=0 measures the existing per-outbound behavior. BENCH_GROUPS=1
checks the grouped behavior. BENCH_PORT_OFFSET keeps this stand independent
of another adaptive bench on the same host.
"""

import bench
import group_scenarios
import run


if __name__ == "__main__":
    bench.ADDRESSES = 24
    run.scenarios.ALL = group_scenarios.ALL
    run.main()
