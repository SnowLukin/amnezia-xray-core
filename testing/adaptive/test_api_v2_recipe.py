import json
import unittest

import api_v2_recipe as v2


class RecipeBoundaryTest(unittest.TestCase):
    def test_real_minimal_v2_is_not_given_legacy_defaults(self):
        value = {'healthObservatory': {}, 'outbounds': []}
        self.assertEqual(v2.config(value), value)

    def test_old_duration_disablement_becomes_explicit_flags(self):
        value = {'healthObservatory': {'_standRecipeV1': True, 'passive': {'udpReplyTimeout': '0s', 'loneFailureAfter': '0s'}, 'sleep': {'afterLastUse': '0s'}}, 'outbounds': []}
        got = v2.config(value)['healthObservatory']
        self.assertNotIn('udpReplies', got['evidence'])
        self.assertNotIn('loneFailureProbe', got['evidence'])
        self.assertNotIn('tunnelSleep', got['resources'])

    def test_negative_closure_flags_and_internal_marker_do_not_leak(self):
        value = {'healthObservatory': {'_standRecipeV1': True, 'passive': {'shouldKeepPendingOnDown': True, 'shouldSkipQuality': True}},
                 'outbounds': [{'path': {'balancer': 'auto', 'cost': 1, 'isLazy': True}}],
                 'routing': {'balancers': [{'strategy': {'type': 'leastCost'}}]}}
        got = v2.config(value)
        self.assertFalse(got['routing']['balancers'][0]['strategy']['settings']['failover']['closePending']['isEnabled'])
        self.assertNotIn('quality', got['healthObservatory']['telemetry'])
        self.assertEqual(got['outbounds'][0]['selection']['probePolicy'], 'onDemand')
        raw = json.dumps(got)
        for old in ('_standRecipeV1', 'shouldKeepPendingOnDown', 'shouldSkipQuality', 'isLazy', '"path"'):
            self.assertNotIn(old, raw)

    def test_unknown_input_is_left_for_real_loader_to_reject(self):
        value = {'healthObservatory': {'_standRecipeV1': True, 'unsupportedField': 7}, 'outbounds': []}
        self.assertEqual(v2.config(value)['healthObservatory']['unsupportedField'], 7)

    def test_provider_routing_is_not_silently_removed(self):
        value = {'routing': {'rules': []}, 'outbounds': [{'path': {'balancer': 'auto', 'cost': 1}}]}
        got = v2.provider_answer(value)
        self.assertIn('routing', got)
        self.assertIn('selection', got['outbounds'][0])

    def test_disabled_periods_preserve_zero_cost_feedback_delay(self):
        got = v2.provider({'triggers': {'interval': '0s', 'maxAge': '0s', 'minHealthyOutbounds': 0, 'shouldRefreshWhenExhausted': False, 'whenCostAbove': {'cost': 10, 'after': '0s'}}})
        self.assertNotIn('periodic', got['triggers'])
        self.assertNotIn('maxAge', got['triggers'])
        self.assertNotIn('minHealthy', got['triggers'])
        self.assertEqual(got['triggers']['whenCostAbove']['after'], '0s')
        self.assertNotIn('isEnabled', got['triggers']['whenCostAbove'])

    def test_zero_provider_limits_take_the_loosest_allowed(self):
        got = v2.provider({'limits': {'minInterval': '0s', 'exhaustedMinInterval': '0s', 'maxRefreshesPerHour': 0}})
        self.assertEqual(got['limits'], {'minInterval': '1ms', 'urgentMinInterval': '1ms', 'maxRefreshesPerHour': 60})

    def test_forget_on_network_change_turns_off_the_return_on_change(self):
        got = v2.strategy({'networkChangeAction': 'forget'})
        self.assertEqual(got['networkChanges'], {'isEnabled': True, 'action': 'forget'})
        self.assertEqual(got['returnToCheaper']['onNetworkChange'], {'isEnabled': False})

    def test_result_data_and_errors_are_unwrapped_once(self):
        self.assertEqual(v2.result_payload({'success': True, 'data': {'seq': 1, 'set': {}}}), {'seq': 1, 'set': {}})
        self.assertIsNone(v2.result_payload({'success': True}))
        self.assertIs(v2.result_payload(True), True)
        with self.assertRaises(RuntimeError): v2.result_payload({'success': False, 'error': {'code': 'feature_disabled'}})


if __name__ == '__main__': unittest.main()
