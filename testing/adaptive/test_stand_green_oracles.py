import unittest

import api_v2_recipe
import bench
import probe_only_scenarios
import provider


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
