import ast
import copy
import json
import tempfile
import unittest
from pathlib import Path

import bench
import group_scenarios
import path_scenarios
import scenarios


def provider_answer_calls(tree):
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
        if not isinstance(call.func, ast.Attribute) or call.func.attr != "answer":
            continue
        if isinstance(call.func.value, ast.Name) and call.func.value.id == "self":
            owner = parents.get(call)
            while owner is not None and not isinstance(owner, ast.ClassDef):
                owner = parents.get(owner)
            if owner is not None and any(isinstance(base, ast.Name) and base.id == "BaseHTTPRequestHandler"
                                         for base in owner.bases):
                continue
        yield call


def candidates(cfg):
    return [item["tag"] for item in cfg["outbounds"] if item.get("path", {}).get("balancer") == "auto"]


class PathConfigTest(unittest.TestCase):
    def test_awg_config_keeps_both_paths_and_their_prices(self):
        cfg = scenarios.awg_config()
        paths = {item["tag"]: item["path"] for item in cfg["outbounds"] if "path" in item}
        self.assertEqual(list(paths), ["v1", "w1"])
        self.assertEqual([paths[tag]["cost"] for tag in paths], [1, 3])
        self.assertTrue(all(path["probe"] == "default" for path in paths.values()))

    def test_client_paths_and_named_probe(self):
        cfg = bench.client_config([("a1", "reality", 0), ("b1", "tcp_tls", 0)], {"a1": 1, "b1": 3})
        self.assertEqual([item["path"]["cost"] for item in cfg["outbounds"] if "path" in item], [1, 3])
        self.assertEqual(cfg["healthObservatory"]["probes"][0]["name"], "default")
        self.assertNotIn("selector", cfg["routing"]["balancers"][0])
        self.assertNotIn("subjectSelector", cfg["healthObservatory"])
        self.assertNotIn("costs", cfg["routing"]["balancers"][0]["strategy"]["settings"])

    def test_missing_path_is_excluded_and_leak_is_detected(self):
        cfg = path_scenarios.pathless_config()
        self.assertEqual(candidates(cfg), ["chosen"])
        leaked = copy.deepcopy(cfg)
        path_scenarios.outbound(leaked, "forgotten")["path"] = {"balancer": "auto", "cost": 1}
        with self.assertRaises(AssertionError):
            self.assertEqual(candidates(leaked), ["chosen"])

    def test_both_declared_orders_reject_alphabetical_sort(self):
        orders = (["relay-10", "relay-1", "relay-2"], ["relay-2", "relay-10", "relay-1"])
        for order in orders:
            with self.subTest(order=order):
                cfg = bench.client_config([(tag, "reality", i) for i, tag in enumerate(order)],
                                          {tag: 1 for tag in order})
                self.assertEqual(candidates(cfg), order)
                sorted_outbounds = copy.deepcopy(cfg)
                sorted_outbounds["outbounds"].sort(key=lambda item: item.get("tag", ""))
                with self.assertRaises(AssertionError):
                    self.assertEqual(candidates(sorted_outbounds), order)

    def test_provider_answer_paths_are_raw_and_ordered(self):
        members = [("relay-10", "reality", 0), ("relay-2", "reality", 1), ("relay-1", "reality", 2)]
        order = [tag for tag, _, _ in members]
        items = bench.path_members(members, {tag: 1 for tag in order})
        self.assertEqual([item["tag"] for item in items], order)
        self.assertEqual([item["path"]["balancer"] for item in items], ["auto"] * 3)
        for position in range(len(order)):
            expected = path_scenarios.group_position_tags(order, position)
            sorted_core = path_scenarios.group_position_tags(sorted(order), position)
            with self.assertRaises(AssertionError):
                self.assertEqual(sorted_core, expected)

    def test_exact_prices_reject_prefix_collision(self):
        members = [("relay-1", "reality", 0), ("relay-10", "reality", 1)]
        with self.assertRaisesRegex(ValueError, "relay-10"):
            bench.path_members(members, {"relay-1": 1})
        items = bench.path_members(members, {"relay-1": 1, "relay-10": 3})
        self.assertEqual([item["path"]["cost"] for item in items], [1, 3])

    def test_provider_answer_requires_explicit_price_for_every_member(self):
        members = [("x1", "reality", 0), ("x2", "reality", 1)]
        stand = object.__new__(bench.Stand)
        with tempfile.TemporaryDirectory() as work:
            stand.provider_answer = f"{work}/answer.json"
            with self.assertRaises(TypeError):
                stand.answer(members)
            with self.assertRaisesRegex(ValueError, "x1"):
                stand.answer(members, {})
            with self.assertRaisesRegex(ValueError, "x2"):
                stand.answer(members, {"x1": 1})

    def test_bench_answer_callsites_pass_explicit_prices(self):
        directory = Path(bench.__file__).parent
        for path in directory.glob("*.py"):
            if path.name.startswith("test_"):
                continue
            tree = ast.parse(path.read_text())
            for call in provider_answer_calls(tree):
                with self.subTest(file=path.name, line=call.lineno):
                    self.assertTrue(len(call.args) >= 2 or any(key.arg == "costs" for key in call.keywords),
                                    f"{path.name}:{call.lineno} omits exact path costs")

    def test_http_answer_is_distinct_from_provider_answer(self):
        source = "class Handler(BaseHTTPRequestHandler):\n def do_GET(self): self.answer(32768)\n"
        self.assertEqual(list(provider_answer_calls(ast.parse(source))), [])

    def test_missing_prices_are_still_detected_on_stand_and_self(self):
        source = "stand.answer(members)\nclass Stand:\n def run(self): self.answer(members)\n"
        calls = list(provider_answer_calls(ast.parse(source)))
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(len(call.args) == 1 and not call.keywords for call in calls))

    def test_stand_starting_answer_has_exact_path(self):
        stand = object.__new__(bench.Stand)
        with tempfile.TemporaryDirectory() as work:
            stand.provider_answer = f"{work}/answer.json"
            stand.answer([("n1", "reality", 2)], {"n1": 1})
            body = json.loads(Path(stand.provider_answer).read_text())
        self.assertEqual([item["tag"] for item in body["outbounds"]], ["n1"])
        self.assertEqual(body["outbounds"][0]["selection"],
                         {"balancer": "auto", "cost": 1, "probe": "default"})

    def test_provider_only_config_has_no_base_candidate(self):
        provider = bench.provider_config(["direct"])
        cfg = bench.client_config([], {}, providers=[provider])
        self.assertEqual(candidates(cfg), [])
        self.assertEqual(cfg["routing"]["balancers"][0]["tag"], provider["apply"]["balancer"])
        self.assertNotIn("selector", cfg["routing"]["balancers"][0])
        self.assertEqual(cfg["healthObservatory"]["providers"], [provider])

    def test_one_late_rejection_cannot_pass_both_invalid_answers(self):
        late = {"seq": 7, "ts": 11_000, "fields": {"version": "override", "is_ok": False,
                                                 "error": "outbound x1 path.cost is set by group relay"}}
        self.assertIsNone(path_scenarios.rejected_apply([late], "missing", 6, 10))
        self.assertIs(path_scenarios.rejected_apply([late], "override", 10, 14), late)

    def test_group_config_has_only_group_owned_properties(self):
        previous = group_scenarios.GROUPS
        group_scenarios.GROUPS = True
        try:
            cfg = group_scenarios.config()
        finally:
            group_scenarios.GROUPS = previous
        settings = cfg["routing"]["balancers"][0]["strategy"]["settings"]
        self.assertEqual(settings["groups"], [
            {"name": "relay", "cost": 1, "probe": "default", "failedInRowToLeave": 3}])
        relay = [item for item in cfg["outbounds"] if item.get("tag", "").startswith("relay-")]
        self.assertEqual([item["path"] for item in relay],
                         [{"balancer": "auto", "group": "relay"}] * 24)
        self.assertNotIn("costs", settings)

    def test_plain_config_removes_paths_with_balancer(self):
        cfg = bench.plain(bench.client_config([("a1", "reality", 0)], {"a1": 1}), "a1")
        self.assertNotIn("path", path_scenarios.outbound(cfg, "a1"))


if __name__ == "__main__":
    unittest.main()
