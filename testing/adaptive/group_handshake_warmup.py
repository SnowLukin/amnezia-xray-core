"""Wait for server setup handshakes before a group scenario starts.

The server trace is cumulative across scenarios. Every begin must have an end;
no SNI or error category is excluded. The caller starts its measurement window
only after all observed calls have ended and the trace has stayed quiet.
"""

import os
import time

from group_scenarios import parse_server_handshakes


def wait_for_server_tls_idle(path, timeout=8.0, quiet=0.3, poll=0.05,
                             clock=time.monotonic, sleep=time.sleep):
    start = clock()
    deadline = start + timeout
    stable_since = None
    previous = None
    observed = 0
    open_ids = []
    while True:
        if os.path.exists(path):
            with open(path, errors="replace") as f:
                intervals, problems = parse_server_handshakes(f)
            if problems:
                raise RuntimeError(f"server TLS setup trace malformed: {problems[:3]}")
            observed = len(intervals)
            open_ids = [h["id"] for h in intervals if h["end_ns"] is None]
            signature = (observed, observed - len(open_ids))
            if observed and not open_ids:
                if signature != previous:
                    stable_since = clock()
                elif stable_since is not None and clock() - stable_since >= quiet:
                    return {"handshakes": observed, "waited_s": round(clock() - start, 3)}
            else:
                stable_since = None
            previous = signature
        if clock() >= deadline:
            raise TimeoutError(f"server TLS setup did not settle in {timeout:g}s: "
                               f"{observed} observed, open IDs {open_ids}")
        sleep(min(poll, max(0, deadline - clock())))
