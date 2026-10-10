"""Run the 24-relay group bench with the ordinary adaptive report runner.

BENCH_GROUPS=0 measures the existing per-outbound behavior. BENCH_GROUPS=1
checks the grouped behavior. BENCH_PORT_OFFSET keeps this stand independent
of another adaptive bench on the same host.
"""

import os

import bench
import group_handshake_warmup
import group_scenarios
import run


class WarmScenario:
    def __init__(self, scenario):
        self.scenario = scenario
        self.name = scenario.name
        self.slow = scenario.slow

    def run(self, stand, xray, work):
        if not bench.SERVER_BIN:
            if group_scenarios.GROUPS:
                return [group_scenarios.check("server TLS setup settled", "BENCH_SERVER_BIN unset", False,
                                              "instrumented server required in AFTER")]
            return self.scenario.run(stand, xray, work)
        try:
            ready = group_handshake_warmup.wait_for_server_tls_idle(os.path.join(work, "server.log"))
        except (RuntimeError, TimeoutError) as exc:
            return [group_scenarios.check("server TLS setup settled", str(exc), False,
                                          "all begun handshakes ended, then trace quiet")]
        checks = self.scenario.run(stand, xray, work)
        return [group_scenarios.check("server TLS setup settled", ready, True,
                                      "all begun handshakes ended, then trace quiet")] + checks


if __name__ == "__main__":
    bench.ADDRESSES = 24
    run.scenarios.ALL = [WarmScenario(s) for s in group_scenarios.ALL]
    run.main()
