"""Stands in for the internet behind the bench servers.

GET /204             empty answer
GET /bytes/N         N bytes
GET /hold/S          one byte per second for S seconds
Every request is logged as "host path" lines to the file given as argv[2],
so the bench can count probes separately from user traffic.
"""
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG_LOCK = threading.Lock()
LOG = None


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        host = (self.headers.get("Host") or "").split(":")[0]
        if LOG is not None:
            with LOG_LOCK:
                LOG.write(f"{time.time():.3f} {host} {self.path}\n")
                LOG.flush()
        parts = self.path.strip("/").split("/")
        if parts[0] == "204":
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif parts[0] == "bytes":
            n = int(parts[1])
            self.send_response(200)
            self.send_header("Content-Length", str(n))
            self.end_headers()
            chunk = b"x" * 65536
            while n > 0:
                k = min(n, len(chunk))
                self.wfile.write(chunk[:k])
                n -= k
        elif parts[0] == "hang":
            time.sleep(600)
        elif parts[0] == "hold":
            s = int(parts[1])
            self.send_response(200)
            self.send_header("Content-Length", str(s))
            self.end_headers()
            for _ in range(s):
                self.wfile.write(b"x")
                self.wfile.flush()
                time.sleep(1)
        else:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()


def main():
    global LOG
    port = int(sys.argv[1])
    if len(sys.argv) > 2:
        LOG = open(sys.argv[2], "a", buffering=1)
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
