#!/usr/bin/env python3
"""Plain HTTP duplex target for isolated quality experiments.

POST /duplex?download=N returns headers and one body byte before reading the
Content-Length request body. Its first byte must be an 'A' acknowledgement;
the remaining bytes are the upload. It then sends the rest of N response bytes.
The caller must receive the first byte before acknowledging, so S1 precedes uplink
loss and the late download milestones (S2). No TLS is used on this target.
Importing this file starts no server and performs no I/O.
"""
import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
import time
from urllib.parse import parse_qs, urlsplit

CHUNK = b"D" * (64 * 1024)
LIMIT = 128 * 1024 * 1024


class DuplexServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = False

    def __init__(self, address, log_path, timeout=240, end_hold=2):
        self.log_path = Path(log_path)
        self.log_lock = threading.Lock()
        self.timeout = timeout
        self.end_hold = end_hold
        super().__init__(address, DuplexHandler)

    def record(self, **data):
        data.update(wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns())
        with self.log_lock, self.log_path.open("a") as f:
            f.write(json.dumps(data, sort_keys=True) + "\n")
            f.flush()


class DuplexHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_POST(self):
        token = self.headers.get("X-Experiment-Token", "")
        uploaded = 0
        sent = 0
        try:
            parsed = urlsplit(self.path)
            if parsed.path != "/duplex":
                self.send_error(404)
                return
            download = int(parse_qs(parsed.query)["download"][0])
            upload = int(self.headers["Content-Length"]) - 1
            if not 1 <= download <= LIMIT or not 0 <= upload <= LIMIT:
                self.send_error(400, "invalid transfer size")
                return
            self.connection.settimeout(self.server.timeout)
            self.close_connection = True
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(download))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(b"!")
            self.wfile.flush()
            sent = 1
            self.server.record(token=token, phase="initial-byte-sent", upload_expected=upload,
                               download_expected=download, peer=list(self.client_address))
            # Also gate upload=0 responses. Without the acknowledgement a
            # short or queue response could coalesce its entire body with S1.
            if self.rfile.read(1) != b"A":
                raise RuntimeError("missing initial response acknowledgement")
            self.server.record(token=token, phase="initial-byte-acknowledged")
            while uploaded < upload:
                chunk = self.rfile.read(min(len(CHUNK), upload - uploaded))
                if not chunk:
                    raise EOFError(f"upload ended at {uploaded}/{upload}")
                uploaded += len(chunk)
            self.server.record(token=token, phase="upload-complete", uploaded=uploaded)
            while sent < download:
                chunk = CHUNK[:min(len(CHUNK), download - sent)]
                self.wfile.write(chunk)
                sent += len(chunk)
            self.wfile.flush()
            self.server.record(token=token, phase="download-complete", uploaded=uploaded, sent=sent)
            # Keep the established underlying socket available for a late ss
            # snapshot. This interval is excluded from transfer elapsed time.
            time.sleep(self.server.end_hold)
        except Exception as exc:
            self.server.record(token=token, phase="error", uploaded=uploaded, sent=sent,
                               error=repr(exc))


def create_server(port, log_path, timeout=240, end_hold=2):
    return DuplexServer(("127.0.0.1", port), log_path, timeout, end_hold)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=240)
    parser.add_argument("--end-hold", type=float, default=2)
    args = parser.parse_args()
    args.log.parent.mkdir(parents=True, exist_ok=True)
    with create_server(args.port, args.log, args.timeout, args.end_hold) as server:
        server.serve_forever(poll_interval=0.2)


if __name__ == "__main__":
    main()
