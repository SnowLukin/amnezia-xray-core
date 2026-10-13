"""Migrate recorded stand recipes to the presence-based adaptive JSON boundary.

This helper is test tooling, not a public core compatibility adapter. Unknown
keys remain visible to the real loader. Historical proof artifacts are untouched.
"""
import copy

DEFAULT_ON = {'dialFailures', 'dialTimeout', 'serverUnverified', 'firstReply',
              'earlyClose', 'stalledRequests', 'closePending', 'closeSilent',
              'exhausted', 'leaseRenewal', 'backendSchedule', 'expiredLeaseFallback'}
OPTIONAL = {'partialStalls', 'udpReplies', 'loneFailureProbe', 'downAudit',
            'returnToCheaper', 'onNetworkChange', 'networkChanges', 'pin',
            'stepProfiles', 'candidates', 'requests', 'udp', 'connectivity',
            'witnesses', 'memory', 'hostLabels', 'labelRequest', 'pinning',
            'userPresent', 'memoryPressure', 'tunnelSleep', 'closeUnchosenIdle',
            'events', 'report', 'quality', 'pathLabels', 'probeMeasurements',
            'metrics', 'sessionLabels', 'periodic', 'minHealthy', 'whenCostAbove',
            'maxAge', 'networkChange'}


def migrate(value, parent=''):
    if isinstance(value, list):
        return [migrate(item, parent) for item in value]
    if not isinstance(value, dict):
        return value
    result = {}
    for key, item in value.items():
        if key == 'providers' and isinstance(item, list):
            result[key] = [migrate({k: v for k, v in p.items() if k != 'isEnabled'}, 'provider')
                           for p in item if not isinstance(p, dict) or p.get('isEnabled', True)]
            continue
        if key == 'groups' and isinstance(item, dict) and 'items' in item:
            if item.get('isEnabled', False):
                result[key] = migrate(item['items'], 'group')
            continue
        if key in DEFAULT_ON | OPTIONAL and isinstance(item, dict):
            enabled = item.get('isEnabled')
            # Per-path return overrides inherit the balancer's enabled policy;
            # a false override is the explicit default-on opt-out.
            override = key == 'returnToCheaper' and parent in ('selection', 'group')
            if enabled is False and key not in DEFAULT_ON and not override:
                continue
            item = {k: v for k, v in item.items() if k != 'isEnabled' or enabled is False}
        if key == 'url' and parent == 'provider':
            result['urls'] = [item]
            continue
        if key == 'destination' and parent in ('connectivity', 'labelRequest'):
            result['destinations'] = [item]
            continue
        result[key] = migrate(item, key)
    return result


def config(value):
    return migrate(copy.deepcopy(value))


def answer(value):
    return migrate(copy.deepcopy(value))
