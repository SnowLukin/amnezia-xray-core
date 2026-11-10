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
  error:N:code[:R]  HTTP status N with an error body of the protocol; with R, Retry-After: R
  rotate:T      the answer file with "headers": {"Authorization": "Bearer T"}
  require:T     401 with the error body unauthorized unless the request
                carries Authorization: Bearer T; with it the answer file
  hang          never answers
A fixture may contain _probeOnlyBench: generation, capacity, promote,
probeUUID, userUUID, responseDelay, drainSeconds and promoteOnExhausted. This private fixture
section never goes to the core. A passed probe in the POST promotes that
same tag, a down real tag is demoted when capacity is full. An exhausted
balancer may ask without candidate proof; promoteOnExhausted supplies its slot. Old real slots
are retained for drainSeconds; response audits go to <log file>.responses.
Every /set request is logged as "time authorization path" to the log file,
and the body of a POST as a JSON line {ts, auth, type, bytes, body} to the bodies
file; a body that is not JSON is kept as a string.
"""
import copy

from api_v2_recipe import provider_answer
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
RESPONSES = None
SLOTS = None


MOMENT = re.compile(rb'"@([+-]\d+)s"')


def moments(body):
    """Puts the time of the request into "@+Ns" strings of an answer."""
    now = time.time()
    return MOMENT.sub(lambda m: b'"' + time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now + int(m.group(1))))
                      .encode() + b'"', body)


class ProbeOnlySlots:
    """A backend promotes proof from the POST, or supplies a slot on exhaustion."""
    def __init__(self, document):
        self.policy = document["_probeOnlyBench"]
        self.generation = self.policy["generation"]
        self.answer = copy.deepcopy(document)
        self.answer.pop("_probeOnlyBench")
        self.held = {}
        self.retired = set()
        self.promotions = []
        self.capacity = self.policy["capacity"]
        if not 1 <= self.capacity <= 4 or self.real_count() > self.capacity:
            raise ValueError("invalid real-slot capacity")

    def real_count(self):
        return sum(o["path"]["isProbeOnly"] is False for o in self.answer["outbounds"])

    def respond(self, body, now):
        inventory = {o["tag"]: o for o in (body or {}).get("outbounds", [])}
        self.held = {tag: until for tag, until in self.held.items() if until > now}
        for outbound in self.answer["outbounds"]:
            tag = outbound["tag"]
            if (outbound["path"]["isProbeOnly"] is True and inventory.get(tag, {}).get("isProbeOnly") is False
                    and tag not in self.retired):
                self.held[tag] = now + self.policy.get("drainSeconds", 30)
                self.retired.add(tag)
        promoted = None
        exhausted_bypass = self.policy.get("promoteOnExhausted") is True and (body or {}).get("balancer", {}).get("isExhausted") is True
        if self.policy.get("promote"):
            for outbound in self.answer["outbounds"]:
                tag = outbound["tag"]
                evidence = inventory.get(tag, {})
                if (outbound["path"]["isProbeOnly"] is not True or evidence.get("isProbeOnly") is not True
                        or (evidence.get("probe", {}).get("isPassed") is not True and not exhausted_bypass)):
                    continue
                real = [o for o in self.answer["outbounds"] if o["path"]["isProbeOnly"] is False]
                if len(real) >= self.capacity:
                    retiring = next((o for o in real if inventory.get(o["tag"], {}).get("state") in ("suspect", "down")), None)
                    if retiring is None:
                        break
                    self.held[retiring["tag"]] = now + self.policy.get("drainSeconds", 30)
                    self.retired.add(retiring["tag"])
                    retiring["path"]["isProbeOnly"] = True
                    retiring["settings"]["vnext"][0]["users"][0]["id"] = self.policy["probeUUID"]
                self.retired.discard(tag)
                self.held.pop(tag, None)
                outbound["path"]["isProbeOnly"] = False
                outbound["settings"]["vnext"][0]["users"][0]["id"] = self.policy["userUUID"]
                promoted = tag
                self.promotions.append({"tag": tag, "at": now, "proof": copy.deepcopy(evidence)})
                self.answer["version"] = self.generation + "-promoted-" + str(len(self.promotions))
                break
        if self.real_count() > self.capacity:
            raise ValueError("backend exceeded real-slot capacity")
        answer = copy.deepcopy(self.answer)
        audit = {"at": now, "promoted": promoted, "capacity": self.capacity,
                 "realSlots": self.real_count(), "heldForDrain": dict(self.held),
                 "isExhaustedBypass": exhausted_bypass,
                 "outbounds": copy.deepcopy(answer["outbounds"])}
        return answer, audit


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
        global SLOTS
        url = urlparse(self.path)
        if url.path == "/control":
            MODE[0] = parse_qs(url.query)["mode"][0]
            if MODE[0] == "ok":
                with LOCK:
                    SLOTS = None
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
            parts = mode.split(":")
            body = json.dumps({"protocol": 1, "code": parts[2], "message": "bench"}).encode()
            self.reply(int(parts[1]), body, headers={"Retry-After": parts[3]} if len(parts) > 3 else None)
        elif mode == "garbage":
            self.reply(200, b'{"outbounds": [{"tag": ')
        elif mode.startswith("require:") and self.headers.get("Authorization") != "Bearer " + mode.split(":", 1)[1]:
            body = json.dumps({"protocol": 1, "code": "unauthorized", "message": "bench"}).encode()
            self.reply(401, body)
        else:
            with open(ANSWER, "rb") as f:
                answer = moments(f.read())
            try:
                doc = json.loads(answer)
            except ValueError:
                doc = None
            if isinstance(doc, dict) and "_probeOnlyBench" in doc:
                policy = doc["_probeOnlyBench"]
                time.sleep(policy.get("responseDelay", 0))
                with LOCK:
                    if SLOTS is None or SLOTS.generation != policy["generation"]:
                        SLOTS = ProbeOnlySlots(doc)
                    doc, audit = SLOTS.respond(getattr(self, "request_body", None), time.time())
                    RESPONSES.write(json.dumps(audit) + "\n")
                    RESPONSES.flush()
                answer = json.dumps(doc).encode()
            if mode.startswith("rotate:"):
                doc = json.loads(answer)
                doc["headers"] = {"Authorization": "Bearer " + mode.split(":", 1)[1]}
                answer = json.dumps(doc).encode()
            if isinstance(doc, dict):
                answer = json.dumps(provider_answer(doc)).encode()
            self.reply(200, answer)

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        try:
            body = json.loads(raw)
        except ValueError:
            body = raw.decode(errors="replace")
        self.request_body = body if isinstance(body, dict) else None
        line = {"ts": round(time.time(), 3), "auth": self.headers.get("Authorization"),
                "type": self.headers.get("Content-Type"), "bytes": len(raw), "body": body}
        with LOCK:
            BODIES.write(json.dumps(line) + "\n")
            BODIES.flush()
        self.do_GET()


def main():
    global ANSWER, LOG, BODIES, RESPONSES
    port = int(sys.argv[1])
    ANSWER = sys.argv[2]
    LOG = open(sys.argv[3], "a", buffering=1)
    BODIES = open(sys.argv[3] + ".bodies", "a", buffering=1)
    RESPONSES = open(sys.argv[3] + ".responses", "a", buffering=1)
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
