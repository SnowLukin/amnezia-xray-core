"""Serves a set of outbounds the way a backend would, for provider scenarios.

Run:      provider.py <port> <answer file> <log file>
GET /set                     the answer file as JSON, read on every request; a string
                             "@+Ns" or "@-Ns" in it becomes the time N seconds from now
                             in RFC 3339, for expiresAt and serverTime
POST /set                    the same; the body of the request goes to <log file>.bodies
GET /control?mode=<mode>     changes how /set answers:
  ok            the answer file
  garbage       a cut JSON document
  status:N[:R]  HTTP status N with an empty body; with R, Retry-After: R
  error:N:code  HTTP status N with an error body of the protocol
  rotate:T      the answer file with "headers": {"Authorization": "Bearer T"}
  require:T     401 with the error body unauthorized unless the request
                carries Authorization: Bearer T; with it the answer file
  hang          never answers
Every /set request is logged as "time authorization path" to the log file,
and the body of a POST as a JSON line {ts, auth, type, bytes, body} to the bodies
file; a body that is not JSON is kept as a string.
"""
import json
import re
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

MODE = ["ok"]
LOCK = threading.Lock()
ANSWER = None
LOG = None
BODIES = None


MOMENT = re.compile(rb'"@([+-]\d+)s"')


def moments(body):
    """Puts the time of the request into "@+Ns" strings of an answer."""
    now = time.time()
    return MOMENT.sub(lambda m: b'"' + time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now + int(m.group(1))))
                      .encode() + b'"', body)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def reply(self, code, body=b"", headers=None):
        self.send_response(code)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
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
            parts = mode.split(":")
            self.reply(int(parts[1]), headers={"Retry-After": parts[2]} if len(parts) > 2 else None)
        elif mode.startswith("error:"):
            _, status, code = mode.split(":")
            body = json.dumps({"protocol": 1, "error": code, "message": "bench"}).encode()
            self.reply(int(status), body)
        elif mode == "garbage":
            self.reply(200, b'{"outbounds": [{"tag": ')
        elif mode.startswith("require:") and self.headers.get("Authorization") != "Bearer " + mode.split(":", 1)[1]:
            body = json.dumps({"protocol": 1, "error": "unauthorized", "message": "bench"}).encode()
            self.reply(401, body)
        else:
            with open(ANSWER, "rb") as f:
                answer = moments(f.read())
            if mode.startswith("rotate:"):
                doc = json.loads(answer)
                doc["headers"] = {"Authorization": "Bearer " + mode.split(":", 1)[1]}
                answer = json.dumps(doc).encode()
            self.reply(200, answer)

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        try:
            body = json.loads(raw)
        except ValueError:
            body = raw.decode(errors="replace")
        line = {"ts": round(time.time(), 3), "auth": self.headers.get("Authorization"),
                "type": self.headers.get("Content-Type"), "bytes": len(raw), "body": body}
        with LOCK:
            BODIES.write(json.dumps(line) + "\n")
            BODIES.flush()
        self.do_GET()


def main():
    global ANSWER, LOG, BODIES
    port = int(sys.argv[1])
    ANSWER = sys.argv[2]
    LOG = open(sys.argv[3], "a", buffering=1)
    BODIES = open(sys.argv[3] + ".bodies", "a", buffering=1)
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
