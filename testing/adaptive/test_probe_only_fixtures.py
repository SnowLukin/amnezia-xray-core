"""Pure fixtures and counter oracles; no binary, sockets or processes."""
import copy
import unittest

import bench
import probe_only_scenarios as scenes
import provider


class ProbeOnlyBackendTest(unittest.TestCase):
    def test_no_credentials_promoted_without_passed_probe(self):
        slots = provider.ProbeOnlySlots(scenes.fixture("proof", {"a"}, 1))
        body = {"outbounds": [{"tag": "a", "isProbeOnly": False, "state": "down"},
                              {"tag": "b", "isProbeOnly": True}]}
        for proof in ({}, {"isPassed": False}):
            body["outbounds"][1]["probe"] = proof
            answer, audit = slots.respond(body, 100)
            self.assertIsNone(audit["promoted"])
            self.assertEqual(slots.real_count(), 1)
            self.assertTrue(answer["outbounds"][1]["path"]["isProbeOnly"])
        body["outbounds"][1]["probe"] = {"isPassed": True, "rttMs": 12, "agoMs": 2}
        answer, audit = slots.respond(body, 101)
        self.assertEqual(audit["promoted"], "b")
        self.assertEqual([o["tag"] for o in answer["outbounds"]], ["a", "b", "c"])
        modes = {o["tag"]: o["path"]["isProbeOnly"] for o in answer["outbounds"]}
        self.assertEqual(modes, {"a": True, "b": False, "c": True})
        self.assertEqual(slots.real_count(), 1)
        self.assertEqual(audit["heldForDrain"], {"a": 131})
        self.assertNotIn("_probeOnlyBench", answer)
        self.assertEqual(answer["outbounds"][1]["settings"]["vnext"][0]["users"][0]["id"], bench.UUID)
        self.assertEqual(answer["outbounds"][0]["settings"]["vnext"][0]["users"][0]["id"], bench.PROBE_UUID)
        _, retained = slots.respond(body, 130)
        self.assertIn("a", retained["heldForDrain"])
        _, expired = slots.respond(body, 131)
        self.assertNotIn("a", expired["heldForDrain"])

    def test_exhaustion_may_request_a_slot_without_candidate_proof(self):
        document = scenes.fixture("cold-one", {"a"}, 1)
        document["_probeOnlyBench"]["promoteOnExhausted"] = True
        slots = provider.ProbeOnlySlots(document)
        body = {"balancer": {"isExhausted": True}, "outbounds": [
            {"tag": "a", "isProbeOnly": False, "state": "down"},
            {"tag": "b", "isProbeOnly": True}, {"tag": "c", "isProbeOnly": True}]}
        answer, audit = slots.respond(body, 100)
        self.assertEqual(audit["promoted"], "b")
        self.assertTrue(audit["isExhaustedBypass"])
        self.assertEqual(slots.real_count(), 1)
        self.assertFalse(answer["outbounds"][1]["path"]["isProbeOnly"])

    def test_capacity_one_can_answer_unchanged_and_keep_verified(self):
        slots = provider.ProbeOnlySlots(scenes.fixture("one", {"a"}, 1, promote=False))
        body = {"trigger": "min_healthy", "outbounds": [{"tag": "b", "isProbeOnly": True,
                                                        "probe": {"isPassed": True}}]}
        original = copy.deepcopy(slots.answer)
        answer, audit = slots.respond(body, 100)
        self.assertEqual(answer, original)
        self.assertIsNone(audit["promoted"])
        self.assertEqual(audit["realSlots"], 1)

    def test_access_rules_require_probe_user_domain_and_port_before_block(self):
        cfg = bench.server_config(probe_credentials=True, access_log="access.log")
        self.assertNotEqual(bench.UUID, bench.PROBE_UUID)
        for inbound in cfg["inbounds"]:
            clients = inbound["settings"]["clients"]
            self.assertEqual([(c["id"], c["email"]) for c in clients],
                             [(bench.UUID, bench.USER_EMAIL), (bench.PROBE_UUID, bench.PROBE_EMAIL)])
        rules = cfg["routing"]["rules"]
        self.assertEqual(rules[0], {"user": [bench.PROBE_EMAIL], "domain": ["full:probe.test"],
                                   "port": str(bench.TARGET_PORT), "outboundTag": "allow"})
        self.assertEqual(rules[1], {"user": [bench.PROBE_EMAIL], "outboundTag": "dead"})
        self.assertEqual(cfg["log"]["access"], "access.log")

    def test_leak_oracle_rejects_user_target_even_if_server_blocked_it(self):
        accesses = [{"email": bench.PROBE_EMAIL, "destination": f"tcp:probe.test:{bench.TARGET_PORT}"},
                    {"email": bench.USER_EMAIL, "destination": f"tcp:t1.test:{bench.TARGET_PORT}"}]
        self.assertEqual(scenes.probe_credential_leaks(accesses), [])
        wrong = {"email": bench.PROBE_EMAIL, "destination": f"tcp:t1.test:{bench.TARGET_PORT}"}
        self.assertEqual(scenes.probe_credential_leaks(accesses + [wrong]), [wrong])

    def test_request_inventory_requires_explicit_bool_for_every_tag(self):
        body = {"outbounds": [{"tag": "a", "isProbeOnly": False}, {"tag": "b", "isProbeOnly": True}]}
        self.assertTrue(scenes.inventory_check(body, ["a", "b"]))
        body["outbounds"][1].pop("isProbeOnly")
        self.assertFalse(scenes.inventory_check(body, ["a", "b"]))
        body["outbounds"][1]["isProbeOnly"] = 1
        self.assertFalse(scenes.inventory_check(body, ["a", "b"]))

    def test_scene_credentials_match_modes_and_provider_uses_direct(self):
        cfg = scenes.client_config(scenes.fixture("cfg", {"a", "b"}, 2), 2)
        for outbound in cfg["outbounds"]:
            if "path" in outbound:
                expected = bench.PROBE_UUID if outbound["path"]["isProbeOnly"] else bench.UUID
                self.assertEqual(outbound["settings"]["vnext"][0]["users"][0]["id"], expected)
        self.assertEqual(cfg["healthObservatory"]["providers"][0]["via"], ["direct"])
        steps = cfg["healthObservatory"]["probes"][0]["steps"]
        self.assertEqual([step["name"] for step in steps], ["transport", "transport-second", "egress"])
        self.assertNotIn("isOptional", steps[0])
        self.assertTrue(steps[2]["isOptional"])


class ProbeOnlyReportTest(unittest.TestCase):
    def test_starts_failures_promotion_and_fresh_down_are_distinct(self):
        events = [{"type": typ, "fields": fields} for typ, fields in [
            ("probe_start", {"reason": "candidate"}),
            ("probe_start", {"reason": "candidate"}),
            ("probe_result", {"reason": "candidate", "is_ok": True, "bytes": 40960}),
            ("probe_result", {"reason": "candidate", "is_ok": False, "bytes": 0}),
            ("probe_result", {"reason": "host", "is_ok": False}),
            ("probe_only_promoted", {"outbound": "p-b"}),
            ("fresh_path_down", {"outbound": "p-b"}),
        ]]
        counters, _ = bench.fold_report(events)
        self.assertEqual({key: counters[key] for key in (
            "candidate_probes", "candidate_probes_failed", "probe_only_promoted_count", "fresh_path_down_count")},
            {"candidate_probes": 2, "candidate_probes_failed": 1, "probe_only_promoted_count": 1,
             "fresh_path_down_count": 1})
        self.assertEqual(counters["probes"], 3)
        self.assertEqual(counters["probes_failed"], 2)
        self.assertEqual(counters["probe_bytes"], 40960)


if __name__ == "__main__":
    unittest.main()
