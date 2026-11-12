"""A strict stand-in for the provider endpoint of the backend.

Every request of the core is checked against docs/adaptive/provider-protocol.md
and every broken rule is kept in Backend.violations; the answer does not depend
on them, so the core gets what a backend would send. The backend serves the set
the way the real one does: a user slot (-s) on one entry, a probe credential
(-p, isProbeOnly in the first balancer) on every entry and a road credential
(-r, in the road balancer) on every entry without the slot; with awg, the entry
of the slot carries an AmneziaWG outbound (-a) as well, with probePolicy
standby: the core checks it while auto does not choose it.

Answers follow Backend.script, one item per request, then Backend.default:
  ("auto",)          the slot moves to another live entry when the request says
                     the first balancer is exhausted or the slot is down; the
                     same set with renewed leases otherwise
  ("same",)          the same set, leases renewed
  ("hold",)          the same set, leases not renewed
  ("move",)          the slot moves to another live entry
  ("busy", s)        503 with Retry-After s and an error body of the protocol
  ("unauthorized",)  401 with an error body of the protocol
  ("labels", d)      the same outbounds, the slot labelled d, a new version
  ("base", v)        this and later answers name base config v
  ("version",)       the same outbounds under a new version

With locations, a set of countries, the backend reads the header
X-Location: country:<c> before the script. A country of the set labels the
slot with it (a new version when it changes), any other country gets 409
location_unavailable, a value without country: gets 400 location_unknown,
both with an error body of the protocol.

Importing the module opens no sockets.
"""
import calendar
import ipaddress
import json
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import bench

TRIGGERS = {"host", "exhausted", "expiry", "min_healthy", "cost_above", "network_change", "next_refresh",
            "max_age", "interval"}
STATES = {"up", "degraded", "suspect", "down", "unknown"}
REASONS = {"traffic", "probe", "return_probe", "host_probe", "network_change", "network_down", "network_up",
           "replaced", "rescued", "chosen", "probe_changed", "candidate"}
KINDS = {"dial_failed", "dial_timeout", "server_unverified", "no_response", "closed", "stall", "partial_stall"}
ATTEMPT_CODES = {"timeout", "network", "status", "too_large", "broken_json", "skipped", "invalid_answer",
                 "invalid_set"}
UDP_STATES = {"ok", "lossy", "dead", "unsupported"}
ALWAYS = ("protocol", "provider", "run", "refreshNumber", "trigger", "retryLevel", "clientTime", "network",
          "isMemoryLow", "balancers", "outbounds")
TOP = set(ALWAYS) | {"lastRefresh", "version", "baseVersion", "groups", "witnessQuality", "qualitySamplesDropped",
                     "isTruncated"}
BALANCER_KEYS = {"tag", "current", "isOwn", "cost", "isExhausted", "isPinned", "heldMs"}
OUTBOUND_KEYS = {"tag", "isProbeOnly", "state", "reason", "stateMs", "seenMs", "quarantineLevel", "quarantineMs",
                 "failures", "probe", "udp", "connsOk", "connsFailed", "bytesUp", "bytesDown", "activeMs",
                 "ttfbP50Ms", "quality", "expiresInMs"}
PROBE_KEYS = {"profile", "reason", "state", "steps", "isPassed", "rttMs", "error", "agoMs"}
STEP_KEYS = {"name", "isPassed", "connectMs", "ms", "bytes", "destination", "error"}
GROUP_KEYS = {"balancer", "group", "state", "members", "healthy", "quarantined", "clientPosition", "failed",
              "quarantineMs", "lastFailure"}
ATTEMPT_KEYS = {"via", "outbound", "mirror", "isOk", "status", "code", "ms"}
NETWORK_KEYS = {"id", "isKnown", "isUp", "labels", "ipLabels"}
HEX16 = re.compile(r"[0-9a-f]{16}")
UTC = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z")
HEADER_ITEM = re.compile(r'\s*([a-z*][a-z0-9_.*-]*)=("(?:[^"\\]|\\.)*"|-?\d+)\s*')
DOMAIN = re.compile(r"(?i)\b[a-z0-9-]+(\.[a-z0-9-]+)*\.(test|com|net|org|invalid|example|local|localhost|ru|io)\b")
BODY_CEILING = 256 << 10

PATH = "/v1/client/provider"
ENTRY_IDS = [f"e{n}" + str(n) * 31 for n in range(1, 5)]
DAY = 86400


def is_int(v):
    return type(v) is int


def is_number(v):
    return type(v) in (int, float)


def parse_adaptive_header(value):
    """The RFC 8941 dictionary of the Xray-Adaptive header, or None."""
    out = {}
    for part in (value or "").split(","):
        m = HEADER_ITEM.fullmatch(part)
        if not m:
            return None
        key, raw = m.groups()
        out[key] = raw[1:-1] if raw.startswith('"') else int(raw)
    return out or None


def leaks(value, secrets, where="body"):
    """Strings of a body that name an address, a domain, a URL or a secret."""
    found = []
    if isinstance(value, dict):
        for k, v in value.items():
            found += leaks(k, secrets, where + ".<key>")
            found += leaks(v, secrets, f"{where}.{k}")
    elif isinstance(value, list):
        for i, v in enumerate(value):
            found += leaks(v, secrets, f"{where}[{i}]")
    elif isinstance(value, str):
        lower = value.lower()
        if "://" in value:
            found.append(f"{where}: URL {value!r}")
        for s in secrets:
            if s and s.lower() in lower:
                found.append(f"{where}: secret {s!r}")
        if DOMAIN.search(value):
            found.append(f"{where}: domain {value!r}")
        for token in re.split(r"[\s\[\]/,;=@()]+", value):
            for candidate in (token, token.rsplit(":", 1)[0] if token.count(":") == 1 else ""):
                try:
                    ipaddress.ip_address(candidate)
                except ValueError:
                    continue
                found.append(f"{where}: address {value!r}")
                break
    return found


class Expect:
    """What the backend knows about the client it serves."""

    def __init__(self, provider, balancers, via, prefix, base_version, max_per_hour=6, path=PATH):
        self.provider = provider
        self.balancers = list(balancers)
        self.via = list(via)
        self.prefix = prefix
        self.base_version = base_version
        self.max_per_hour = max_per_hour
        self.path = path
        # tag -> isProbeOnly, over every set the client could hold.
        self.known = {}
        self.versions = set()
        self.tag_sets = []
        self.secrets = set()

    def served(self, answer):
        tags = {o["tag"]: o["selection"].get("isProbeOnly") is True for o in answer["outbounds"]}
        self.known.update(tags)
        self.versions.add(answer.get("version"))
        if set(tags) not in self.tag_sets:
            self.tag_sets.append(set(tags))


def check_request(method, path, headers, raw, expect, now=None):
    """Checks one request of the core. headers by lower-case name. Returns the
    body (None when it is not a JSON object) and the broken rules."""
    now = time.time() if now is None else now
    bad = []
    if method != "POST":
        bad.append(f"method: {method}, the config says POST")
    if path.split("?", 1)[0] != expect.path:
        bad.append(f"path: {path}")
    if "?" in path:
        bad.append("query string: credentials and state go in headers and body only")
    if (headers.get("accept") or "") != "application/json":
        bad.append(f"Accept: {headers.get('accept')!r}")
    if not (headers.get("content-type") or "").startswith("application/json"):
        bad.append(f"Content-Type: {headers.get('content-type')!r}")
    adaptive = parse_adaptive_header(headers.get("xray-adaptive"))
    if adaptive is None:
        bad.append(f"Xray-Adaptive: {headers.get('xray-adaptive')!r} is not a dictionary of RFC 8941")
    else:
        for key in ("api", "rev", "provider"):
            if not is_int(adaptive.get(key)) or adaptive[key] < 1:
                bad.append(f"Xray-Adaptive: {key} is {adaptive.get(key)!r}")
        if adaptive.get("provider") not in (None, 1):
            bad.append(f"Xray-Adaptive: provider={adaptive['provider']}, protocol 1 expected")
        if not isinstance(adaptive.get("xray"), str):
            bad.append("Xray-Adaptive: no xray version")
    if len(raw) > BODY_CEILING:
        bad.append(f"body of {len(raw)} bytes is over 256 KiB")
    try:
        body = json.loads(raw)
    except ValueError:
        return None, bad + ["body is not JSON"]
    if not isinstance(body, dict):
        return None, bad + ["body is not a JSON object"]
    bad += check_body(body, expect, now)
    bad += leaks(body, expect.secrets)
    return body, bad


def check_body(body, expect, now):
    bad = []
    for key in ALWAYS:
        if key not in body:
            bad.append(f"{key}: missing, the protocol sends it always")
    for key in sorted(set(body) - TOP):
        bad.append(f"{key}: not a field of the protocol")
    if "protocol" in body and (not is_int(body["protocol"]) or body["protocol"] != 1):
        bad.append(f"protocol: {body['protocol']!r}")
    if "provider" in body and body["provider"] != expect.provider:
        bad.append(f"provider: {body['provider']!r}, the tag is {expect.provider!r}")
    if "run" in body and not (isinstance(body["run"], str) and HEX16.fullmatch(body["run"])):
        bad.append(f"run: {body['run']!r} is not 16 hex digits")
    number = body.get("refreshNumber")
    if "refreshNumber" in body and (not is_int(number) or number < 1):
        bad.append(f"refreshNumber: {number!r}")
    if "trigger" in body and body["trigger"] not in TRIGGERS:
        bad.append(f"trigger: {body['trigger']!r}")
    if "retryLevel" in body and (not is_int(body["retryLevel"]) or body["retryLevel"] < 0):
        bad.append(f"retryLevel: {body['retryLevel']!r}")
    if is_int(number) and number > 1 and "lastRefresh" not in body:
        bad.append(f"lastRefresh: missing in refresh {number}")
    if "lastRefresh" in body:
        bad += check_last_refresh(body["lastRefresh"], expect)
    if "version" in body:
        if not isinstance(body["version"], str) or not body["version"]:
            bad.append(f"version: {body['version']!r}")
        elif expect.versions and body["version"] not in expect.versions:
            bad.append(f"version: {body['version']!r} was never served")
    elif expect.versions:
        bad.append("version: missing although the client holds a set")
    if expect.base_version and body.get("baseVersion") != expect.base_version:
        bad.append(f"baseVersion: {body.get('baseVersion')!r}, the core runs {expect.base_version!r}")
    t = body.get("clientTime")
    if "clientTime" in body:
        if not (isinstance(t, str) and UTC.fullmatch(t)):
            bad.append(f"clientTime: {t!r} is not RFC 3339 in UTC")
        else:
            at = calendar.timegm(time.strptime(t[:19], "%Y-%m-%dT%H:%M:%S"))
            if abs(at - now) > 120:
                bad.append(f"clientTime: {t} is {round(at - now)} s off")
    if "network" in body:
        bad += check_network(body["network"])
    if "isMemoryLow" in body and not isinstance(body["isMemoryLow"], bool):
        bad.append(f"isMemoryLow: {body['isMemoryLow']!r}")
    if "balancers" in body:
        bad += check_balancers(body["balancers"], expect)
    if "outbounds" in body:
        bad += check_outbounds(body["outbounds"], expect)
    if "groups" in body:
        groups = body["groups"]
        if not isinstance(groups, list) or not groups:
            bad.append(f"groups: {groups!r}, present only with groups")
        else:
            for g in groups:
                if not isinstance(g, dict) or set(g) - GROUP_KEYS:
                    bad.append(f"groups: {g!r}")
    if "isTruncated" in body and body["isTruncated"] is not True:
        bad.append(f"isTruncated: {body['isTruncated']!r}, only true is sent")
    if "qualitySamplesDropped" in body and not (is_int(body["qualitySamplesDropped"])
                                                and body["qualitySamplesDropped"] > 0):
        bad.append(f"qualitySamplesDropped: {body['qualitySamplesDropped']!r}")
    return bad


def check_last_refresh(last, expect):
    if not isinstance(last, dict):
        return [f"lastRefresh: {last!r}"]
    bad = []
    for key, test in (("isOk", lambda v: isinstance(v, bool)), ("agoMs", lambda v: is_int(v) and v >= 0),
                      ("trigger", lambda v: v in TRIGGERS), ("attempts", lambda v: isinstance(v, list))):
        if not test(last.get(key)):
            bad.append(f"lastRefresh.{key}: {last.get(key)!r}")
    for key in sorted(set(last) - {"isOk", "agoMs", "trigger", "attempts"}):
        bad.append(f"lastRefresh.{key}: not a field of the protocol")
    attempts = last.get("attempts") if isinstance(last.get("attempts"), list) else []
    if len(attempts) > 16:
        bad.append(f"lastRefresh.attempts: {len(attempts)}, at most 16")
    for i, a in enumerate(attempts):
        where = f"lastRefresh.attempts[{i}]"
        if not isinstance(a, dict):
            bad.append(f"{where}: {a!r}")
            continue
        for key in sorted(set(a) - ATTEMPT_KEYS):
            bad.append(f"{where}.{key}: not a field of the protocol")
        if a.get("via") not in expect.via:
            bad.append(f"{where}.via: {a.get('via')!r} is not in via of the config")
        for key in ("mirror", "status", "ms"):
            if not is_int(a.get(key)) or a[key] < 0:
                bad.append(f"{where}.{key}: {a.get(key)!r}")
        if not isinstance(a.get("isOk"), bool):
            bad.append(f"{where}.isOk: {a.get('isOk')!r}")
        if "code" in a and a["code"] not in ATTEMPT_CODES:
            bad.append(f"{where}.code: {a['code']!r}")
        # The outbound the attempt went through: the choice of a balancer
        # or a probe credential, both as served, or the via itself.
        if "outbound" in a and a["outbound"] != a.get("via"):
            bad += check_tag(a["outbound"], expect, f"{where}.outbound")
    return bad


def check_network(network):
    if not isinstance(network, dict):
        return [f"network: {network!r}"]
    bad = [f"network.{k}: not a field of the protocol" for k in sorted(set(network) - NETWORK_KEYS)]
    for key in ("isKnown", "isUp"):
        if not isinstance(network.get(key), bool):
            bad.append(f"network.{key}: {network.get(key)!r}, sent always")
    if "id" in network and not (isinstance(network["id"], str) and HEX16.fullmatch(network["id"])):
        bad.append(f"network.id: {network['id']!r} is not a 16-digit hash")
    for key in ("labels", "ipLabels"):
        if key in network and not isinstance(network[key], dict):
            bad.append(f"network.{key}: {network[key]!r}")
    return bad


def check_tag(tag, expect, where):
    if not isinstance(tag, str) or not tag:
        return [f"{where}: {tag!r}"]
    if expect.prefix and tag.startswith(expect.prefix):
        return [f"{where}: {tag} carries the prefix {expect.prefix!r}"]
    if expect.known and tag not in expect.known:
        return [f"{where}: {tag} is not a tag of any served set"]
    return []


def check_balancers(balancers, expect):
    if not isinstance(balancers, list):
        return [f"balancers: {balancers!r}"]
    bad = []
    tags = [b.get("tag") if isinstance(b, dict) else None for b in balancers]
    if tags != expect.balancers:
        bad.append(f"balancers: {tags}, apply.balancers in order is {expect.balancers}")
    for b in balancers:
        if not isinstance(b, dict):
            continue
        where = f"balancers[{b.get('tag')}]"
        for key in sorted(set(b) - BALANCER_KEYS):
            bad.append(f"{where}.{key}: not a field of the protocol")
        for key in ("isOwn", "isExhausted", "isPinned"):
            if not isinstance(b.get(key), bool):
                bad.append(f"{where}.{key}: {b.get(key)!r}, sent always")
        if "current" in b:
            bad += check_tag(b["current"], expect, f"{where}.current")
            if b.get("isOwn") is not True:
                bad.append(f"{where}.current: set while isOwn is {b.get('isOwn')!r}")
            if expect.known.get(b["current"]) is True:
                bad.append(f"{where}.current: {b['current']} is a probe credential")
        elif b.get("isOwn") is True:
            bad.append(f"{where}: isOwn without current")
        if b.get("isExhausted") is True and (b.get("current") or b.get("isOwn")):
            bad.append(f"{where}: exhausted yet holds {b.get('current')!r}")
        if "cost" in b and not (is_number(b["cost"]) and b["cost"] > 0):
            bad.append(f"{where}.cost: {b['cost']!r}")
        if "heldMs" in b and not (is_int(b["heldMs"]) and b["heldMs"] >= 0):
            bad.append(f"{where}.heldMs: {b['heldMs']!r}")
    return bad


def check_outbounds(outbounds, expect):
    if not isinstance(outbounds, list):
        return [f"outbounds: {outbounds!r}, an array always"]
    bad = []
    tags = []
    for o in outbounds:
        if not isinstance(o, dict):
            bad.append(f"outbounds: {o!r}")
            continue
        tag = o.get("tag")
        tags.append(tag)
        where = f"outbounds[{tag}]"
        bad += check_tag(tag, expect, where + ".tag")
        for key in sorted(set(o) - OUTBOUND_KEYS):
            bad.append(f"{where}.{key}: not a field of the protocol")
        if not isinstance(o.get("isProbeOnly"), bool):
            bad.append(f"{where}.isProbeOnly: {o.get('isProbeOnly')!r}, sent always")
        elif tag in expect.known and o["isProbeOnly"] != expect.known[tag]:
            bad.append(f"{where}.isProbeOnly: {o['isProbeOnly']}, served as {expect.known[tag]}")
        if o.get("state") not in STATES:
            bad.append(f"{where}.state: {o.get('state')!r}")
        if "reason" in o and o["reason"] not in REASONS:
            bad.append(f"{where}.reason: {o['reason']!r}")
        for key in ("stateMs", "seenMs", "quarantineLevel", "quarantineMs", "connsOk", "connsFailed", "bytesUp",
                    "bytesDown", "activeMs", "ttfbP50Ms"):
            if key in o and not (is_int(o[key]) and o[key] > 0):
                bad.append(f"{where}.{key}: {o[key]!r}, omitted when zero")
        if "expiresInMs" in o and not is_int(o["expiresInMs"]):
            bad.append(f"{where}.expiresInMs: {o['expiresInMs']!r}")
        if tag in expect.known and "expiresInMs" not in o:
            bad.append(f"{where}.expiresInMs: missing for a leased outbound")
        failures = o.get("failures", {})
        if not isinstance(failures, dict) or any(k not in KINDS or not is_int(v) for k, v in failures.items()):
            bad.append(f"{where}.failures: {failures!r}")
        if "probe" in o:
            bad += check_probe(o["probe"], where + ".probe")
        if "udp" in o:
            udp = o["udp"]
            if not isinstance(udp, dict) or udp.get("state") not in UDP_STATES:
                bad.append(f"{where}.udp: {udp!r}")
        if "quality" in o and not isinstance(o["quality"], dict):
            bad.append(f"{where}.quality: {o['quality']!r}")
    if len(tags) != len(set(tags)):
        bad.append(f"outbounds: repeated tags {tags}")
    if expect.tag_sets and set(tags) not in expect.tag_sets:
        bad.append(f"outbounds: {sorted(t for t in tags if t)} is not a set the backend served")
    return bad


def check_probe(probe, where):
    if not isinstance(probe, dict):
        return [f"{where}: {probe!r}"]
    bad = [f"{where}.{k}: not a field of the protocol" for k in sorted(set(probe) - PROBE_KEYS)]
    if not isinstance(probe.get("isPassed"), bool):
        bad.append(f"{where}.isPassed: {probe.get('isPassed')!r}")
    if not is_int(probe.get("agoMs")) or probe["agoMs"] < 0:
        bad.append(f"{where}.agoMs: {probe.get('agoMs')!r}")
    if "state" in probe and probe["state"] not in STATES:
        bad.append(f"{where}.state: {probe['state']!r}")
    if probe.get("isPassed") is True and "error" in probe:
        bad.append(f"{where}.error: {probe['error']!r} on a passed probe")
    if probe.get("isPassed") is False and not probe.get("error"):
        bad.append(f"{where}.error: missing on a failed probe")
    for i, step in enumerate(probe.get("steps", [])):
        if not isinstance(step, dict) or set(step) - STEP_KEYS or not isinstance(step.get("isPassed"), bool):
            bad.append(f"{where}.steps[{i}]: {step!r}")
    return bad


class Backend:
    """The backend of one scenario. Thread safe; the HTTP server calls handle."""

    def __init__(self, provider="api", balancers=("auto", "roads"), via=("auto", "roads", "direct"), prefix="p-",
                 base_version="base-4", slot=0, awg=False, leases=None, slot_labels=None, max_per_hour=6,
                 next_refresh="2h", locations=None):
        self.lock = threading.Lock()
        self.expect = Expect(provider, balancers, via, prefix, base_version, max_per_hour)
        self.base = base_version
        self.slot = slot
        self.awg = awg
        self.alive = set(range(len(ENTRY_IDS)))
        self.leases = {"s": 14 * DAY, "a": 14 * DAY, "p": 30 * DAY, "r": 30 * DAY} | (leases or {})
        self.slot_labels = slot_labels
        self.next_refresh = next_refresh
        self.generation = 0
        self.ends = {}
        self.script = []
        self.default = ("auto",)
        self.requests = []
        self.violations = []
        self.runs = {}
        self.not_before = 0.0
        self.refused = None
        self.locations = locations

    @staticmethod
    def tag(entry, role):
        return f"{ENTRY_IDS[entry]}-{role}"

    def version(self):
        return f"s-{self.generation:04d}"

    def outbound(self, entry, role, cost):
        tag = self.tag(entry, role)
        if role == "a":
            item = bench.awg_member(tag, entry)
            item["settings"]["remoteDNS"] = [bench.AWG_SERVER_ADDRESS]
            selection = {"balancer": self.expect.balancers[0], "cost": cost, "probe": "full",
                         "probePolicy": "standby"}
        else:
            item = bench.member(tag, "ws", entry, bench.PIN)
            name = bench.SERVER_NAMES["ws"]
            item["streamSettings"]["tlsSettings"].update(fingerprint="chrome", allowInsecure=False)
            item["streamSettings"]["wsSettings"]["headers"] = {"Host": name}
            user = item["settings"]["vnext"][0]["users"][0]
            if role == "s":
                user["level"] = 8
            else:
                user["id"] = bench.PROBE_UUID
            balancer = self.expect.balancers[1] if role == "r" else self.expect.balancers[0]
            selection = {"balancer": balancer, "cost": cost, "probe": "full" if role == "s" else "candidate"}
            if role == "p":
                selection["isProbeOnly"] = True
            if role == "s" and self.slot_labels:
                selection["labels"] = dict(self.slot_labels)
        item["selection"] = selection
        return item

    def outbounds(self):
        n = len(ENTRY_IDS)
        others = [(self.slot + k) % n for k in range(1, n)]
        items = [self.outbound(self.slot, "s", 1)]
        if self.awg:
            items.append(self.outbound(self.slot, "a", 2))
        for cost, entry in enumerate(others, 1):
            items += [self.outbound(entry, "p", 1), self.outbound(entry, "r", cost)]
        items.append(self.outbound(self.slot, "p", 1))
        return items

    def set_answer(self, now, renew=True):
        items = self.outbounds()
        for o in items:
            if renew or o["tag"] not in self.ends:
                self.ends[o["tag"]] = now + self.leases[o["tag"][-1]]
        doc = {"protocol": 1, "version": self.version(), "serverTime": rfc3339(now), "nextRefreshIn": self.next_refresh,
               "outbounds": items, "groups": [], "expiresAt": {o["tag"]: rfc3339(self.ends[o["tag"]]) for o in items},
               "baseVersion": self.base}
        self.expect.served(doc)
        return doc

    def initial_answer(self):
        with self.lock:
            return self.set_answer(time.time())

    def kill(self, *entries):
        with self.lock:
            self.alive -= set(entries)

    def revive(self, *entries):
        with self.lock:
            self.alive |= set(entries)

    def pick_slot(self, body):
        """Another live entry, the one the client proved first."""
        states = {o.get("tag"): o for o in (body or {}).get("outbounds", []) if isinstance(o, dict)}

        def score(entry):
            p = states.get(self.tag(entry, "p"), {})
            r = states.get(self.tag(entry, "r"), {})
            if p.get("probe", {}).get("isPassed") is True and p.get("state") == "up":
                return 3
            if "up" in (p.get("state"), r.get("state")):
                return 2
            if "down" in (p.get("state"), r.get("state")):
                return 0
            return 1
        n = len(ENTRY_IDS)
        order = [(self.slot + k) % n for k in range(1, n)]
        live = [e for e in order if e in self.alive]
        return max(live, key=score) if live else None

    def slot_is_dead(self, body):
        if not body:
            return False
        first = next((b for b in body.get("balancers", []) if isinstance(b, dict)
                      and b.get("tag") == self.expect.balancers[0]), {})
        slot = next((o for o in body.get("outbounds", []) if isinstance(o, dict)
                     and o.get("tag") == self.tag(self.slot, "s")), {})
        return first.get("isExhausted") is True or slot.get("state") == "down" or self.slot not in self.alive

    def locate(self, location):
        """The answer to X-Location when it refuses one, else None."""
        if self.locations is None or not location:
            return None
        if not location.startswith("country:"):
            return 400, {}, {"protocol": 1, "code": "location_unknown", "message": location}
        country = location.removeprefix("country:")
        if country not in self.locations:
            return 409, {}, {"protocol": 1, "code": "location_unavailable", "message": location}
        if (self.slot_labels or {}).get("country") != country:
            self.slot_labels = dict(self.slot_labels or {}, country=country)
            self.generation += 1
        return None

    def respond(self, action, body, auth, now, location=None):
        refusal = self.locate(location)
        if refusal is not None:
            return refusal
        kind = action[0]
        if kind == "busy":
            self.not_before = now + action[1]
            return 503, {"Retry-After": str(action[1])}, {"protocol": 1, "code": "unavailable", "message": "busy"}
        if kind == "unauthorized":
            self.refused = (auth,)
            return 401, {}, {"protocol": 1, "code": "unauthorized", "message": "token"}
        if kind == "base":
            self.base = action[1]
        if kind == "labels":
            self.slot_labels = action[1]
            self.generation += 1
        if kind == "version":
            self.generation += 1
        if kind == "move" or (kind == "auto" and self.slot_is_dead(body)):
            entry = self.pick_slot(body)
            if entry is None:
                self.not_before = now + 30
                return 503, {"Retry-After": "30"}, {"protocol": 1, "code": "unavailable", "message": "no entry"}
            self.slot = entry
            self.generation += 1
        return 200, {}, self.set_answer(now, renew=kind != "hold")

    def session(self, body, raw, auth, now):
        bad = []
        if now < self.not_before - 0.2:
            bad.append(f"request {round(self.not_before - now, 1)} s before the end of Retry-After")
        if self.refused is not None and auth == self.refused[0]:
            bad.append("request with the headers the backend refused, before the host changed them")
        if not body or not isinstance(body.get("run"), str) or not is_int(body.get("refreshNumber")):
            return bad
        run = self.runs.setdefault(body["run"], {"last": None, "raw": None, "answered": False, "starts": []})
        number = body["refreshNumber"]
        if run["last"] is not None and number < run["last"]:
            bad.append(f"refreshNumber {number} after {run['last']}")
        elif number == run["last"]:
            if raw != run["raw"]:
                bad.append(f"refresh {number} repeated with another body")
            if run["answered"]:
                bad.append(f"refresh {number} asked again after an answer of the protocol ended it")
        else:
            run["starts"].append(now)
            recent = [t for t in run["starts"] if t > now - 3600]
            if len(recent) > self.expect.max_per_hour:
                bad.append(f"{len(recent)} refreshes within an hour, maxRefreshesPerHour {self.expect.max_per_hour}")
        run.update(last=number, raw=raw, answered=True)
        return bad

    def handle(self, method, path, headers, raw, now=None):
        now = time.time() if now is None else now
        with self.lock:
            body, bad = check_request(method, path, headers, raw, self.expect, now)
            auth = headers.get("authorization")
            bad += self.session(body, raw, auth, now)
            action = self.script.pop(0) if self.script else self.default
            location = headers.get("x-location")
            status, extra, doc = self.respond(action, body, auth, now, location)
            self.requests.append({"at": now, "method": method, "path": path, "auth": auth, "location": location,
                                  "adaptive": headers.get("xray-adaptive"), "bytes": len(raw), "body": body,
                                  "action": list(action), "status": status, "version": doc.get("version")})
            for rule in bad:
                self.violations.append({"at": now, "refresh": (body or {}).get("refreshNumber"),
                                        "trigger": (body or {}).get("trigger"), "rule": rule})
        return status, extra, json.dumps(doc).encode()

    def bodies(self, since=0):
        return [r["body"] for r in self.requests if r["at"] >= since and r["body"] is not None]


def rfc3339(t):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


class Server:
    """Serves whatever backend the owner holds at the moment of a request."""

    def __init__(self, port, owner):
        self.port = port
        self.owner = owner

    def __enter__(self):
        owner = self.owner

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def serve(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                backend = owner.backend
                if backend is None:
                    status, extra, body = 503, {}, b'{"protocol": 1, "code": "unavailable"}'
                else:
                    headers = {k.lower(): v for k, v in self.headers.items()}
                    status, extra, body = backend.handle(self.command, self.path, headers, raw)
                self.send_response(status)
                for k, v in extra.items():
                    self.send_header(k, v)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            do_GET = do_POST = serve

        ThreadingHTTPServer.daemon_threads = True
        self.httpd = ThreadingHTTPServer(("127.0.0.1", self.port), Handler)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        return self

    def __exit__(self, *exc):
        self.httpd.shutdown()
        self.httpd.server_close()
