"""Build a bench-only Xray server with Go's native TLS handshakes traced.

The client passed to run_groups.py stays the ordinary Xray binary. This script
uses go build -overlay, so it never edits GOROOT, the module cache, or the
source checkout. The replacement source exists only during build.
"""

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
from pathlib import Path


SOURCE = Path(__file__).resolve().parents[2]

TRACE_HELPER = '''var benchHandshakeTraceID atomic.Uint64

type benchHandshakeTrace struct {
    Marker bool `json:"bench_tls_handshake"`
    TimeNS int64 `json:"ts_ns"`
    ID uint64 `json:"id"`
    Remote string `json:"remote"`
    ServerName string `json:"serverName"`
    Phase string `json:"phase"`
    Error string `json:"error"`
}

func benchTraceHandshake(c *Conn, id uint64, phase string, err error) {
    remote := ""
    if c.conn != nil && c.conn.RemoteAddr() != nil {
        remote = c.conn.RemoteAddr().String()
    }
    message := ""
    if err != nil {
        message = err.Error()
    }
    line, _ := json.Marshal(benchHandshakeTrace{
        Marker: true, TimeNS: time.Now().UnixNano(), ID: id,
        Remote: remote, ServerName: c.serverName, Phase: phase, Error: message,
    })
    _, _ = os.Stderr.Write(append(line, '\\n'))
}

'''

HANDSHAKE_CALL = '''\tbenchTraceID := uint64(0)
\tif !c.isClient && c.quic == nil && c.handshakes == 0 {
\t\tbenchTraceID = benchHandshakeTraceID.Add(1)
\t\tbenchTraceHandshake(c, benchTraceID, "begin", nil)
\t}
\tc.handshakeErr = c.handshakeFn(handshakeCtx)
\tif benchTraceID != 0 {
\t\tbenchTraceHandshake(c, benchTraceID, "end", c.handshakeErr)
\t}
'''

HANDSHAKE_REGION = '''\tc.handshakeMutex.Lock()
\tdefer c.handshakeMutex.Unlock()

\tif err := c.handshakeErr; err != nil {
\t\treturn err
\t}
\tif c.isHandshakeComplete.Load() {
\t\treturn nil
\t}

\tc.in.Lock()
\tdefer c.in.Unlock()

\tc.handshakeErr = c.handshakeFn(handshakeCtx)
\tif c.handshakeErr == nil {
\t\tc.handshakes++
'''


def one_replace(source, old, new):
    count = source.count(old)
    if count != 1:
        raise RuntimeError(f"unsupported Go crypto/tls source layout: expected one anchor, "
                           f"found {count}: {old[:100]!r}")
    return source.replace(old, new, 1)


def overlay_source(source):
    source = one_replace(source, '\t"context"\n', '\t"context"\n\t"encoding/json"\n')
    source = one_replace(source, '\t"net"\n', '\t"net"\n\t"os"\n')
    source = one_replace(source, '// A Conn represents a secured connection.\n',
                         TRACE_HELPER + '// A Conn represents a secured connection.\n')
    return one_replace(source, HANDSHAKE_REGION,
                       HANDSHAKE_REGION.replace('\tc.handshakeErr = c.handshakeFn(handshakeCtx)\n',
                                                HANDSHAKE_CALL))


def command(*args, cwd=None):
    return subprocess.run(args, cwd=cwd, check=True, text=True, capture_output=True).stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path,
                        help="path to the instrumented server binary outside the source worktree")
    parser.add_argument("--source", default=SOURCE, type=Path,
                        help="clean Xray source checkout; defaults to this script's repository")
    args = parser.parse_args()
    source_dir = args.source.resolve()
    output = args.output.resolve()
    if output.is_relative_to(source_dir):
        parser.error("--output must be outside the immutable source worktree")
    head = command("git", "rev-parse", "HEAD", cwd=source_dir)
    if command("git", "status", "--porcelain", "--untracked-files=no", cwd=source_dir):
        raise RuntimeError("source worktree has tracked changes; build from a clean checkout")
    goroot = Path(command("go", "env", "GOROOT"))
    goversion = command("go", "env", "GOVERSION")
    original = goroot / "src" / "crypto" / "tls" / "conn.go"
    content = original.read_text()
    replacement = overlay_source(content)
    original_sha = hashlib.sha256(content.encode()).hexdigest()
    overlay_sha = hashlib.sha256(replacement.encode()).hexdigest()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="xray-bench-tls-overlay-") as temp:
        patched = Path(temp) / "conn.go"
        patched.write_text(replacement)
        overlay = Path(temp) / "overlay.json"
        overlay.write_text(json.dumps({"Replace": {str(original): str(patched)}}))
        build = ["go", "build", "-overlay", str(overlay), "-o", str(output), "./main"]
        subprocess.run(build, cwd=source_dir, check=True)
    metadata = {
        "source": str(source_dir), "source_head": head,
        "goroot": str(goroot), "goversion": goversion,
        "stdlib_file": str(original), "stdlib_sha256": original_sha,
        "overlay_sha256": overlay_sha, "binary": str(output),
        "build": "go build -overlay <temporary-overlay.json> -o <binary> ./main",
    }
    Path(str(output) + ".tls-overlay.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
