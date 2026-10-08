"""Serves a set of outbounds the way a backend would, for provider scenarios.

Run:      provider.py <port> <answer file> <log file>
GET /set                     the answer file as JSON, read on every request
GET /control?mode=<mode>     changes how /set answers:
  ok          the answer file
  garbage     a cut JSON document
  status:N    HTTP status N with an empty body
  hang        never answers
Every /set request is logged as "time authorization path" to the log file.
"""
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

MODE = ["ok"]
LOCK = threading.Lock()
ANSWER = None
LOG = None


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def reply(self, code, body=b""):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/control":
            MODE[0] = parse_qs(url.query)["mode"][0]
            self.reply(200, b"{}")
            return
        auth = (self.headers.get("Authorization") or "-").replace(" ", "_")
        with LOCK:
            LOG.write(f"{time.time():.3f} {auth} {self.path}\n")
            LOG.flush()
        mode = MODE[0]
        if mode == "hang":
            time.sleep(600)
        elif mode.startswith("status:"):
            self.reply(int(mode.split(":")[1]))
        elif mode == "garbage":
            self.reply(200, b'{"outbounds": [{"tag": ')
        else:
            with open(ANSWER, "rb") as f:
                self.reply(200, f.read())


def main():
    global ANSWER, LOG
    port = int(sys.argv[1])
    ANSWER = sys.argv[2]
    LOG = open(sys.argv[3], "a", buffering=1)
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
