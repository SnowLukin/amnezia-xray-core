import unittest

import api_v2_recipe
import bench
import probe_only_scenarios
import provider
import scenarios


class StandGreenOraclesTest(unittest.TestCase):
    def test_proxy_samples_do_not_count_a_retry_before_the_window(self):
        sampler = bench.ConnSampler()
        sampler.connection_history = {"events": [{"at": 2.999, "port": 7}], "dropped": 0}
        self.assertEqual(sampler.between(7, 3.0, 4.0), 0)
        sampler.connection_history["events"].append({"at": 3.001, "port": 7})
        self.assertEqual(sampler.between(7, 3.0, 4.0), 1)
        sampler.connection_history["dropped"] = 1
        with self.assertRaises(RuntimeError):
            sampler.between(7, 2.0, 4.0)


    def test_quiet_packet_and_byte_checks_include_the_boundary_bucket(self):
        sampler = bench.ConnSampler()
        sampler.samples = [(2.75, {7: (0, 0, 0, 0, 0)}),
                           (3.01, {7: (1, 99, 0, 1, 0)}),
                           (4.0, {7: (1, 99, 0, 1, 0)})]
        self.assertEqual(sampler.traffic(7, 3.0, 4.0), (99, 0))
        self.assertEqual(sampler.packets(7, 3.0, 4.0), (1, 0))

    def test_return_oracle_rejects_three_instant_successes(self):
        def history(spacing):
            events = []
            for index in range(3):
                for kind, extra in [("probe_start", 0), ("probe_result", 10)]:
                    events.append({"type": kind, "ts": 1000 + index * spacing + extra,
                                   "fields": {"outbound": "v1", "reason": "return", "is_ok": True}})
            return events, {"ts": events[-1]["ts"] + 1}
        events, away = history(20000)
        self.assertTrue(all(row["ok"] for row in scenarios.return_sequence_checks(events, 0, away)))
        events, away = history(20)
        checks = scenarios.return_sequence_checks(events, 0, away)
        self.assertFalse(next(row["ok"] for row in checks if "spacing" in row["check"]))

    def test_local_initial_set_does_not_make_a_quiet_session_reportable(self):
        events = [{"type": "provider_apply", "fields": {"is_ok": True,
                   "is_changed": True, "trigger": "initial"}}]
        self.assertFalse(bench.fold_report(events)[1])
        events[0]["fields"]["is_ok"] = False
        self.assertTrue(bench.fold_report(events)[1])
        events[0]["fields"].update(is_ok=True, trigger="host")
        self.assertTrue(bench.fold_report(events)[1])

    def test_provider_can_declare_groups_without_static_group_members(self):
        value = api_v2_recipe.strategy({"groups": []})
        self.assertEqual(api_v2_recipe.presence_config(value)["groups"], [])
        self.assertNotIn("groups", api_v2_recipe.presence_config(api_v2_recipe.strategy({})))

    def test_backend_replaces_an_unhealthy_slot_only_with_verified_credentials(self):
        slots = provider.ProbeOnlySlots(probe_only_scenarios.fixture("suspect", {"a", "b"}, 2))
        body = {"outbounds": [{"tag": "a", "isProbeOnly": False, "state": "suspect"},
                              {"tag": "b", "isProbeOnly": False, "state": "up"},
                              {"tag": "c", "isProbeOnly": True}]}
        self.assertIsNone(slots.respond(body, 100)[1]["promoted"])
        body["outbounds"][2]["probe"] = {"isPassed": True}
        _, audit = slots.respond(body, 101)
        self.assertEqual(audit["promoted"], "c")
        self.assertEqual(audit["realSlots"], 2)
        self.assertEqual(audit["heldForDrain"], {"a": 131})


if __name__ == "__main__":
    unittest.main()
