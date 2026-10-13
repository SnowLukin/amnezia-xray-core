import unittest
from presence_recipe import config


class PresenceRecipeTest(unittest.TestCase):
    def test_default_on_opt_out_survives_and_optional_opt_out_disappears(self):
        self.assertEqual(config({'evidence': {'dialFailures': {'isEnabled': False}, 'udpReplies': {'isEnabled': False}, 'earlyClose': {'isEnabled': True}}}),
                         {'evidence': {'dialFailures': {'isEnabled': False}, 'earlyClose': {}}})

    def test_groups_become_array_and_override_false_survives(self):
        got = config({'groups': {'isEnabled': True, 'items': [{'name': 'g', 'returnToCheaper': {'isEnabled': False}}]}})
        self.assertEqual(got, {'groups': [{'name': 'g', 'returnToCheaper': {'isEnabled': False}}]})

    def test_provider_and_network_addresses_become_lists(self):
        got = config({'providers': [{'isEnabled': True, 'url': 'https://api.test/set'}, {'isEnabled': False, 'url': 'https://off.test/set'}],
                      'network': {'connectivity': {'isEnabled': True, 'destination': 'https://a.test/204'}}})
        self.assertEqual(got, {'providers': [{'urls': ['https://api.test/set']}], 'network': {'connectivity': {'destinations': ['https://a.test/204']}}})

    def test_presence_config_is_idempotent_and_unknown_fields_survive(self):
        value = {'telemetry': {'quality': {}}, 'selection': {'returnToCheaper': {'interval': '10m'}}, 'unknown': {'isEnabled': True}}
        self.assertEqual(config(config(value)), value)


if __name__ == '__main__':
    unittest.main()
