import json
import time
import unittest

import strict_provider
from strict_provider import Backend, check_request, parse_adaptive_header

HEADERS = {"accept": "application/json", "content-type": "application/json",
           "xray-adaptive": 'xray="26.10.31", api=2, rev=3, provider=1'}
NOW = 1_800_000_000.0


def utc(t):
    return time.strftime("%Y-%m-%dT%H:%M:%S.123Z", time.gmtime(t))


def body(backend, number=1, trigger="host", exhausted=False, states=None):
    """A request the way the core builds it for the set the backend served last."""
    tags = [o for o in backend.expect.tag_sets[-1]]
    slot = backend.tag(backend.slot, "s")
    first = {"tag": "auto", "isOwn": not exhausted, "isExhausted": exhausted, "isPinned": False}
    if not exhausted:
        first.update(current=slot, cost=1, heldMs=1000)
    out = {"protocol": 1, "provider": "api", "run": "0123456789abcdef", "refreshNumber": number, "trigger": trigger,
           "retryLevel": 0, "version": backend.version(), "baseVersion": "base-4", "clientTime": utc(NOW),
           "network": {"isKnown": False, "isUp": True}, "isMemoryLow": False,
           "balancers": [first, {"tag": "roads", "isOwn": False, "isExhausted": False, "isPinned": False}],
           "outbounds": [{"tag": t, "isProbeOnly": backend.expect.known[t], "state": (states or {}).get(t, "up"),
                          "expiresInMs": 1000} for t in sorted(tags)]}
    if number > 1:
        out["lastRefresh"] = {"isOk": True, "agoMs": 5000, "trigger": "host",
                              "attempts": [{"via": "auto", "mirror": 1, "isOk": True, "status": 200, "ms": 40}]}
    return out


def ask(backend, doc, headers=HEADERS, now=NOW):
    return backend.handle("POST", strict_provider.PATH, dict(headers), json.dumps(doc).encode(), now)


def rules(backend):
    return [v["rule"] for v in backend.violations]


class HeaderTest(unittest.TestCase):
    def test_dictionary(self):
        self.assertEqual(parse_adaptive_header('xray="26.10.31", api=2, rev=3, provider=1'),
                         {"xray": "26.10.31", "api": 2, "rev": 3, "provider": 1})

    def test_broken(self):
        for value in (None, "", "api=2; rev=3", "api=two", 'xray=26.10.31'):
            self.assertIsNone(parse_adaptive_header(value), value)


class RequestTest(unittest.TestCase):
    def setUp(self):
        self.backend = Backend()
        self.backend.initial_answer()

    def problems(self, doc, headers=HEADERS, path=strict_provider.PATH, method="POST"):
        return check_request(method, path, headers, json.dumps(doc).encode(), self.backend.expect, NOW)[1]

    def test_request_of_the_core_passes(self):
        self.assertEqual(self.problems(body(self.backend)), [])
        self.assertEqual(self.problems(body(self.backend, number=2)), [])

    def test_headers_and_transport(self):
        headers = dict(HEADERS, **{"xray-adaptive": "api=2, rev=3, provider=2"})
        found = self.problems(body(self.backend), headers=headers, path=strict_provider.PATH + "?token=x", method="GET")
        for part in ("method", "query string", "provider=2", "no xray version"):
            self.assertTrue(any(part in f for f in found), (part, found))
        found = self.problems(body(self.backend), headers={k: v for k, v in HEADERS.items() if k != "accept"})
        self.assertTrue(any(f.startswith("Accept") for f in found))

    def test_fields_and_types(self):
        doc = body(self.backend, number=3)
        del doc["isMemoryLow"]
        del doc["lastRefresh"]
        doc["retryLevel"] = True
        doc["extra"] = 1
        doc["clientTime"] = "2026-11-10T10:00:00+03:00"
        found = self.problems(doc)
        for part in ("isMemoryLow: missing", "lastRefresh: missing", "retryLevel", "extra: not a field",
                     "clientTime"):
            self.assertTrue(any(part in f for f in found), (part, found))

    def test_tags_go_without_prefix_and_keep_their_kind(self):
        doc = body(self.backend)
        slot = self.backend.tag(0, "s")
        doc["balancers"][0]["current"] = "p-" + slot
        probe = next(o for o in doc["outbounds"] if o["tag"].endswith("-p"))
        probe["isProbeOnly"] = False
        doc["outbounds"].pop()
        found = self.problems(doc)
        for part in ("carries the prefix", "served as True", "not a set the backend served"):
            self.assertTrue(any(part in f for f in found), (part, found))

    def test_balancers_in_the_order_of_apply(self):
        doc = body(self.backend)
        doc["balancers"].reverse()
        self.assertTrue(any("apply.balancers in order" in f for f in self.problems(doc)))
        doc = body(self.backend, exhausted=True)
        doc["balancers"][0]["current"] = self.backend.tag(0, "s")
        doc["balancers"][0]["isOwn"] = True
        self.assertTrue(any("exhausted yet holds" in f for f in self.problems(doc)))

    def test_attempt_outbound_names_only_a_probe_credential(self):
        doc = body(self.backend, number=2)
        doc["lastRefresh"]["attempts"] = [
            {"via": "roads", "outbound": self.backend.tag(1, "r"), "mirror": 1, "isOk": True, "status": 200, "ms": 9},
            {"via": "elsewhere", "mirror": 0, "isOk": False, "status": 0, "code": "skipped", "ms": 0},
            {"via": "auto", "mirror": 1, "isOk": False, "status": 0, "code": "dns", "ms": 0}]
        found = self.problems(doc)
        for part in ("is not a probe credential", "not in via of the config", "code: 'dns'"):
            self.assertTrue(any(part in f for f in found), (part, found))

    def test_no_addresses_domains_or_secrets(self):
        self.backend.expect.secrets.add("home-wifi")
        doc = body(self.backend)
        doc["network"]["labels"] = {"gw": "192.168.1.1:443", "asn": "as-home-wifi", "dns": "dns.example.com",
                                    "v6": "[2a02::1]:443", "url": "https://x"}
        found = self.problems(doc)
        for part in ("address '192.168.1.1:443'", "secret 'home-wifi'", "domain 'dns.example.com'",
                     "address '[2a02::1]:443'", "URL"):
            self.assertTrue(any(part in f for f in found), (part, found))
        self.assertEqual(strict_provider.leaks({"version": "s-0001", "t": "2026-11-10T10:00:00Z",
                                                "v": "sha256:0123456789abcdef"}, set()), [])


class SessionTest(unittest.TestCase):
    def setUp(self):
        self.backend = Backend()
        self.backend.initial_answer()

    def test_numbers_go_up_and_a_repeat_is_the_same_body(self):
        ask(self.backend, body(self.backend, number=2))
        ask(self.backend, body(self.backend, number=2, trigger="exhausted"), now=NOW + 40)
        ask(self.backend, body(self.backend, number=1), now=NOW + 80)
        found = rules(self.backend)
        for part in ("repeated with another body", "asked again after an answer", "refreshNumber 1 after 2"):
            self.assertTrue(any(part in f for f in found), (part, found))

    def test_retry_after_holds_every_request(self):
        self.backend.script = [("busy", 20)]
        status, headers, raw = ask(self.backend, body(self.backend))
        self.assertEqual((status, headers, json.loads(raw)["code"]), (503, {"Retry-After": "20"}, "unavailable"))
        ask(self.backend, body(self.backend, number=2), now=NOW + 10)
        ask(self.backend, body(self.backend, number=3), now=NOW + 30)
        self.assertEqual([r for r in rules(self.backend) if "Retry-After" in r], ["request 10.0 s before the end of Retry-After"])

    def test_refused_headers_are_not_sent_again(self):
        self.backend.script = [("unauthorized",)]
        status, _, raw = ask(self.backend, body(self.backend))
        self.assertEqual((status, json.loads(raw)["code"]), (401, "unauthorized"))
        ask(self.backend, body(self.backend, number=2), now=NOW + 40)
        ask(self.backend, body(self.backend, number=3), headers=dict(HEADERS, authorization="Bearer new"), now=NOW + 80)
        self.assertEqual(sum("refused" in r for r in rules(self.backend)), 1)

    def test_refreshes_per_hour(self):
        for n in range(1, 9):
            ask(self.backend, body(self.backend, number=n), now=NOW + 40 * n)
        self.assertEqual(sum("within an hour" in r for r in rules(self.backend)), 2)


class SlotTest(unittest.TestCase):
    def test_shape_of_the_set(self):
        backend = Backend(awg=True)
        doc = backend.initial_answer()
        roles = [o["tag"][-1] for o in doc["outbounds"]]
        self.assertEqual(roles, ["s", "a", "p", "r", "p", "r", "p", "r", "p"])
        self.assertEqual(set(doc["expiresAt"]), {o["tag"] for o in doc["outbounds"]})
        roads = [o["selection"] for o in doc["outbounds"] if o["tag"].endswith("-r")]
        self.assertEqual([(s["balancer"], s["cost"]) for s in roads], [("roads", 1), ("roads", 2), ("roads", 3)])
        probes = [o for o in doc["outbounds"] if o["tag"].endswith("-p")]
        self.assertTrue(all(o["selection"]["isProbeOnly"] is True for o in probes))
        self.assertTrue(all(o["settings"]["vnext"][0]["users"][0]["id"] == strict_provider.bench.PROBE_UUID
                            for o in probes))

    def test_exhaustion_moves_the_slot_to_a_proven_live_entry(self):
        backend = Backend()
        backend.initial_answer()
        backend.kill(0, 1)
        states = {backend.tag(3, "p"): "up"}
        doc = body(backend, trigger="exhausted", exhausted=True, states=states)
        doc["outbounds"] = [dict(o, probe={"isPassed": True, "agoMs": 1}) if o["tag"] == backend.tag(3, "p") else o
                            for o in doc["outbounds"]]
        status, _, raw = ask(backend, doc)
        answer = json.loads(raw)
        self.assertEqual((status, backend.slot, answer["version"]), (200, 3, "s-0001"))
        tags = {o["tag"] for o in answer["outbounds"]}
        self.assertIn(backend.tag(3, "s"), tags)
        self.assertIn(backend.tag(0, "r"), tags)
        self.assertNotIn(backend.tag(3, "r"), tags)

    def test_no_live_entry_is_503(self):
        backend = Backend()
        backend.initial_answer()
        backend.kill(0, 1, 2, 3)
        status, headers, _ = ask(backend, body(backend, trigger="exhausted", exhausted=True))
        self.assertEqual((status, headers), (503, {"Retry-After": "30"}))

    def test_labels_keep_the_outbounds_and_hold_keeps_the_leases(self):
        backend = Backend(slot_labels={"country": "nl"}, leases={"s": 40})
        first = backend.initial_answer()
        backend.script = [("labels", {"country": "de"}), ("hold",)]
        _, _, raw = ask(backend, body(backend), now=time.time() + 5)
        second = json.loads(raw)
        strip = lambda doc: [{k: v for k, v in o.items() if k != "selection"} for o in doc["outbounds"]]
        self.assertEqual(strip(first), strip(second))
        self.assertEqual(second["outbounds"][0]["selection"]["labels"], {"country": "de"})
        self.assertNotEqual(first["expiresAt"], second["expiresAt"])
        _, _, raw = ask(backend, body(backend, number=2), now=time.time() + 10)
        self.assertEqual(json.loads(raw)["expiresAt"], second["expiresAt"])

    def test_answer_names_the_base(self):
        backend = Backend()
        backend.initial_answer()
        backend.script = [("base", "base-5")]
        _, _, raw = ask(backend, body(backend))
        _, _, again = ask(backend, body(backend, number=2), now=NOW + 40)
        self.assertEqual([json.loads(raw)["baseVersion"], json.loads(again)["baseVersion"]], ["base-5", "base-5"])
        self.assertEqual(rules(backend), [])


if __name__ == "__main__":
    unittest.main()
