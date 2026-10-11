#!/bin/sh
# Only client `run` is traced. Server is BENCH_SERVER_BIN and remains untraced.
# A wrapper's PID is strace, so CPU measurements through this file are invalid.
set -eu
: "${XRAY_TRACE_BINARY:?Actual native arm64 Xray binary must be supplied}"
: "${XRAY_TRACE_DIRECTORY:?Dedicated trace output directory must be supplied}"
if [ "${1:-}" = run ]; then
  exec strace --kill-on-exit -f -yy -s 4096 -e trace=execve,splice,getsockopt \
    -o "$XRAY_TRACE_DIRECTORY/client-splice-$$.trace" \
    "$XRAY_TRACE_BINARY" "$@"
fi
exec "$XRAY_TRACE_BINARY" "$@"
