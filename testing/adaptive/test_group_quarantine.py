"""Event-level checks for the group's quarantine and one-member recovery."""

import unittest

from group_scenarios import group_quarantine_checks


def event(when, kind, **fields):
    return {"ts": round(when * 1000), "type": kind, "fields": fields}


def valid_events():
    return [
        event(119, "group_state", group="relay-", state="down", failed=3, quarantine_ms=12000),
        event(131.5, "probe_start", outbound="relay-04", reason="group_recheck"),
        event(131.9, "probe_result", outbound="relay-04", reason="group_recheck", is_ok=True),
        event(132, "group_state", group="relay-", state="up"),
        event(132.2, "probe_start", outbound="relay-04", reason="return"),
        event(132.5, "probe_result", outbound="relay-04", reason="return", is_ok=True),
        event(132.6, "switch", to="relay-04"),
    ]


def checks(events):
    return group_quarantine_checks(events, "relay-01", 110, 140, 128, events[-1])


class GroupQuarantineTest(unittest.TestCase):
    def test_one_recheck_then_normal_return_probe(self):
        self.assertTrue(all(c["ok"] for c in checks(valid_events())))

    def test_return_start_in_same_millisecond_as_up(self):
        events = valid_events()
        events[4]["ts"] = events[3]["ts"]
        self.assertTrue(all(c["ok"] for c in checks(events)))

    def test_early_probe_is_rejected(self):
        events = valid_events()
        events.insert(1, event(120, "probe_start", outbound="relay-07", reason="return"))
        self.assertFalse(checks(events)[0]["ok"])

    def test_recheck_must_target_next_after_last_failed(self):
        events = valid_events()
        events[1]["fields"]["outbound"] = "relay-07"
        self.assertFalse(checks(events)[1]["ok"])

    def test_second_recheck_is_rejected_but_return_is_allowed(self):
        events = valid_events()
        events.insert(-1, event(132.55, "probe_start", outbound="relay-05", reason="group_recheck"))
        self.assertFalse(checks(events)[1]["ok"])

    def test_group_recheck_does_not_replace_return_probe(self):
        events = valid_events()
        events = [e for e in events if e["fields"].get("reason") != "return"]
        self.assertFalse(checks(events)[3]["ok"])


if __name__ == "__main__":
    unittest.main()
