"""Stands in for the internet behind the bench servers.

    target.py <port> <log> [<tls port> <cert> <key>]

The same answers come over plain HTTP and, on the TLS port, over TLS 1.3.

GET /204             empty answer
GET /bytes/N         N bytes
GET /hold/S          one byte per second for S seconds
GET /slow/N/S        N bytes spread over S seconds
Every request is logged as "host path" lines to the file given as argv[2],
so the bench can count probes separately from user traffic.
"""
import ssl
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
        elif parts[0] == "slow":
            n, seconds = int(parts[1]), float(parts[2])
            self.send_response(200)
            self.send_header("Content-Length", str(n))
            self.end_headers()
            chunk = b"x" * 65536
            pause = seconds / max(1, n // len(chunk))
            while n > 0:
                k = min(n, len(chunk))
                self.wfile.write(chunk[:k])
                self.wfile.flush()
                n -= k
                time.sleep(pause)
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


class TLSServer(ThreadingHTTPServer):
    """Handshakes in the request thread, so a frozen client does not stall
    the accept loop."""

    def __init__(self, address, handler, context):
        super().__init__(address, handler)
        self.context = context

    def finish_request(self, request, client_address):
        with self.context.wrap_socket(request, server_side=True) as tls:
            super().finish_request(tls, client_address)


def main():
    global LOG
    port = int(sys.argv[1])
    if len(sys.argv) > 2:
        LOG = open(sys.argv[2], "a", buffering=1)
    ThreadingHTTPServer.daemon_threads = True
    if len(sys.argv) > 5:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3
        ctx.load_cert_chain(sys.argv[4], sys.argv[5])
        tls = TLSServer(("127.0.0.1", int(sys.argv[3])), Handler, ctx)
        threading.Thread(target=tls.serve_forever, daemon=True).start()
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
