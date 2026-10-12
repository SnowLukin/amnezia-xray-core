"""Pure STUN/config/observation oracles. No sockets, binaries, or timers are started."""
import struct
import unittest
from unittest import mock

import bench
import scenarios
import udp_probe_scenarios as udp


def request(transaction=b"abcdefghijkl"):
    return udp.HEADER.pack(0x0001, 0, udp.COOKIE, transaction)


class STUNFixtureTest(unittest.TestCase):
    def test_binding_request_rejects_truncated_cookie_type_and_length(self):
        valid = request()
        self.assertEqual(udp.parse_binding_request(valid), b"abcdefghijkl")
        for malformed in (valid[:19], b"\x01\x01" + valid[2:], valid[:4] + b"\x00" * 4 + valid[8:],
                          valid[:2] + b"\x00\x04" + valid[4:], valid + b"\x00\x00\x00\x00"):
            self.assertIsNone(udp.parse_binding_request(malformed))
            self.assertIsNone(udp.fixture_reply("all", 1, malformed))

    def test_success_contains_own_id_and_well_formed_attributes(self):
        transaction = b"abcdefghijkl"
        response = udp.binding_success(transaction, ("192.0.2.7", 49152))
        self.assertTrue(udp.valid_success(response, {transaction}))
        self.assertFalse(udp.valid_success(response, {b"foreign12345"}))
        kind, length, cookie, own = udp.HEADER.unpack_from(response)
        self.assertEqual((kind, length, cookie, own), (0x0101, 12, udp.COOKIE, transaction))
        attribute, size = struct.unpack_from("!HH", response, 20)
        self.assertEqual((attribute, size), (0x0020, 8))
        malformed = response[:22] + b"\x00\xff" + response[24:]
        self.assertFalse(udp.valid_success(malformed, {transaction}))
        self.assertFalse(udp.valid_success(response + b"tail", {transaction}))
        self.assertFalse(udp.valid_success(response[:4] + b"\x00" * 4 + response[8:], {transaction}))

    def test_alternating_loss_is_finite_pattern_not_fixed_two_reply_quota(self):
        replies = [udp.fixture_reply("alternate", ordinal, request()) is not None
                   for ordinal in range(1, 11)]
        self.assertEqual(replies, [False, True] * 5)
        self.assertEqual(sum(replies[:5]), 2)
        self.assertEqual(sum(replies[5:]), 3)
        self.assertTrue(all(udp.fixture_reply("all", i, request()) for i in range(1, 6)))
        self.assertTrue(all(udp.fixture_reply("zero", i, request()) is None for i in range(1, 6)))
        self.assertEqual(udp.fixture_reply("all", 1, b"live-user"), b"live-user")
        self.assertIsNone(udp.fixture_reply("zero", 1, b"live-user"))

    def test_construct_reset_and_modes_open_no_socket(self):
        with mock.patch("socket.socket", side_effect=AssertionError("unexpected socket")):
            target = udp.STUNTargets("/unused")
            target.set_mode(0, "zero")
            target.set_mode(1, "alternate")
            target.reset()
        self.assertEqual(target.modes, dict.fromkeys(bench.UDP_STUN_PORTS, "all"))
        self.assertEqual(target.ordinals, dict.fromkeys(bench.UDP_STUN_PORTS, 0))
        self.assertEqual(target.listeners, [])
        self.assertEqual(target.threads, [])
        self.assertEqual(target.snapshot(), [])


class UDPScenarioFixtureTest(unittest.TestCase):
    def test_vless_server_dns_trap_and_awg_ip_targets(self):
        cfg, tag = udp.observation_config()
        health = cfg["healthObservatory"]
        self.assertEqual(health["udpProbe"], {"destinations": ["stun-one.test:19302", "stun-two.test:3478"],
            "requests": 5, "interval": "200ms", "timeout": "2s", "perHour": 6})
        self.assertEqual(cfg["dns"]["hosts"]["stun-one.test"], "198.18.0.2")
        self.assertEqual(health["passive"]["udpReplyTimeout"], "5s")
        self.assertEqual(health["schedule"]["hostProbes"]["reuseWindow"], "0s")
        awg, _ = udp.observation_config("awg")
        self.assertEqual(awg["healthObservatory"]["udpProbe"]["destinations"],
                         [f"{bench.TUNNEL_TARGET}:19302", f"{bench.TUNNEL_TARGET}:3478"])
        outbound = next(o for o in cfg["outbounds"] if o.get("tag") == tag)
        self.assertEqual(outbound["settings"]["vnext"][0]["users"][0]["id"], bench.UUID)

    def test_blackhole_is_server_udp_rule_for_same_real_user_credentials(self):
        server = bench.server_config(probe_credentials=True, udp_observation=True)
        self.assertEqual(server["routing"]["rules"][0],
            {"user": [bench.UDP_DROP_EMAIL], "network": "udp", "outboundTag": "dead"})
        self.assertTrue(all(any(c["id"] == bench.UDP_DROP_UUID and c["email"] == bench.UDP_DROP_EMAIL
                               for c in inbound["settings"]["clients"]) for inbound in server["inbounds"]))
        cfg, tag = udp.observation_config(blackhole=True)
        outbound = next(o for o in cfg["outbounds"] if o.get("tag") == tag)
        self.assertFalse(outbound["path"].get("isProbeOnly", False))
        self.assertEqual(outbound["settings"]["vnext"][0]["users"][0]["id"], bench.UDP_DROP_UUID)
        self.assertEqual(cfg["healthObservatory"]["udpProbe"]["destinations"],
                         ["stun-one.test:19302", "stun-two.test:3478"])
        self.assertFalse(any(rule.get("user") == [bench.UDP_DROP_EMAIL] and rule.get("network") != "udp"
                             for rule in server["routing"]["rules"]))

    def test_both_destinations_have_domain_and_awg_ip_redirects(self):
        server = bench.server_config(udp_observation=True)
        for i, port in enumerate((19302, 3478)):
            tag = f"udp-stun-{i}"
            outbound = next(o for o in server["outbounds"] if o["tag"] == tag)
            self.assertEqual(outbound["settings"]["redirect"], f"127.0.0.1:{bench.UDP_STUN_PORTS[i]}")
            rules = [r for r in server["routing"]["rules"] if r.get("outboundTag") == tag]
            self.assertIn({"domain": [f"full:{bench.UDP_STUN_DOMAINS[i]}"], "network": "udp",
                           "port": str(port), "outboundTag": tag}, rules)
            self.assertIn({"ip": [bench.TUNNEL_TARGET], "network": "udp", "port": str(port), "outboundTag": tag}, rules)

    def test_host_uses_production_cli_bool_and_returns_udp_only(self):
        client = mock.Mock()
        client.api.return_value = {"request": {"is_udp": True, "outbounds": [{"outbound": "tested",
            "result": "lossy", "udp": {"state": "lossy", "answered": 2, "sent": 5}}]}}
        result = udp.host_udp(client, "tested")
        client.api.assert_called_once_with("-t", "15", "probe", "--udp", "auto", "tested", timeout=18)
        self.assertEqual(result["udp"]["answered"], 2)
        client.api.return_value = {"request": {"outbounds": [{"outbound": "tested", "result": "ok", "rtt_ms": 1}]}}
        self.assertIsNone(udp.host_udp(client, "tested")["udp"])

    def test_all_functional_scenes_registered_once(self):
        expected = {"udp_probe_" + name for name in ("matrix", "blackhole", "lossy", "second_destination",
                    "noise", "sleep", "budget", "idle", "credentials", "public", "priority", "switch")}
        names = [scene.name for scene in scenarios.ALL if scene.name in expected]
        self.assertEqual(set(names), expected)
        self.assertEqual(len(names), len(expected))


if __name__ == "__main__":
    unittest.main()
