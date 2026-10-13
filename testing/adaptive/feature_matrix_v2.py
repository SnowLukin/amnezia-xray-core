"""LIVE API v2 feature matrix; native v2 client configs and real network traffic.

Run from testing/adaptive, or pass --bench-dir for a scratch copy. Network counts
prove the observed interval only; flag echoes never count as feature behavior.
"""
import argparse
import copy
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import threading
import time
import urllib.request

FEATURES = {
    'dial_failures': 'evidence.dialFailures', 'dial_timeout': 'evidence.dialTimeout',
    'server_unverified': 'evidence.serverUnverified', 'first_reply': 'evidence.firstReply',
    'early_close': 'evidence.earlyClose', 'stalled_requests': 'evidence.stalledRequests',
    'partial_stalls': 'evidence.partialStalls', 'udp_replies': 'evidence.udpReplies',
    'lone_failure_probe': 'evidence.loneFailureProbe', 'step_profiles': 'probes.stepProfiles',
    'candidates': 'probes.candidates', 'probe_requests': 'probes.requests', 'udp_probe': 'probes.udp',
    'connectivity': 'network.connectivity', 'witnesses': 'network.witnesses',
    'network_memory': 'network.memory', 'host_labels': 'network.hostLabels', 'label_request': 'network.labelRequest',
    'pinning': 'host.pinning', 'user_present': 'host.userPresent', 'memory_pressure': 'host.memoryPressure',
    'tunnel_sleep': 'resources.tunnelSleep', 'close_unchosen_idle': 'resources.closeUnchosenIdle',
    'events': 'telemetry.events', 'report': 'telemetry.report', 'quality': 'telemetry.quality',
    'path_labels': 'telemetry.pathLabels', 'probe_measurements': 'telemetry.probeMeasurements',
    'metrics': 'telemetry.metrics', 'session_labels': 'telemetry.report.sessionLabels',
}
# These are contractual dependencies. Additional flags used to stimulate/observe
# a behavior are recorded separately by behavioral scenarios.
DEPENDENCIES = {'lone_failure_probe': {'dial_failures'}, 'session_labels': {'report'}}
bench = None
LABEL_URL = None
CASE_FILTER = set()
FEATURE_FILTER = set()
AVAILABLE_CASES = {}


def setv(root, path, value):
    parts = path.split('.')
    for part in parts[:-1]:
        root = root.setdefault(part, {})
    root[parts[-1]] = value


def effective_features(requested):
    effective = set(requested)
    for flag in requested:
        effective.update(DEPENDENCIES.get(flag, ()))
    return effective


def select_case(name, requested=(), primary=None):
    """Filter by exact case or by its primary tested feature, including controls."""
    if primary is None and len(requested) == 1:
        primary = next(iter(requested))
    AVAILABLE_CASES[name] = primary
    return not (CASE_FILTER or FEATURE_FILTER) or name in CASE_FILTER or primary in FEATURE_FILTER


def request(host='t1.test', size=65536, path=None, timeout=5):
    stream = bench.open_stream(host, timeout, False)
    if stream is None:
        raise AssertionError('SOCKS connection refused')
    payload = f'GET {path or "/bytes/" + str(size)} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n'.encode()
    try:
        stream.sendall(payload)
        head, received = bench.read_answer(stream, time.time() + timeout)
        assert head.startswith(b'HTTP/1.1 200') and received >= size, (head, received)
        return len(payload), received
    finally:
        stream.close()


def raw_api(xray, *args):
    result = subprocess.run([xray, 'api', 'health', f'--server=127.0.0.1:{bench.API_PORT}', *args],
                            capture_output=True, text=True, timeout=12)
    assert result.returncode == 0, (args, result.stdout, result.stderr)
    return json.loads(result.stdout)


def config(requested):
    # Stand helper constructs upstream transport/DNS topology. The observer
    # block sent to Xray is built below from native v2 fields only.
    cfg = bench.api_config(bench.client_config([('a1', 'tcp_tls', 0), ('b1', 'tcp_tls', 1)], {'a1': 1, 'b1': 2}))
    profiles = cfg['healthObservatory']['probes']['profiles']
    h = {'evidence': {key: {'isEnabled': False} for key in
                     ['dialFailures', 'dialTimeout', 'serverUnverified', 'firstReply', 'earlyClose', 'stalledRequests']}}
    h['probes'] = {'profiles': profiles, 'udp': {
        'destinations': [f'{bench.TUNNEL_TARGET}:19302', f'{bench.TUNNEL_TARGET}:3478']}}
    h['network'] = {
        'connectivity': {'outbound': 'net', 'destination': f'http://127.0.0.1:{bench.CONNECTIVITY_PORT}/204/matrix-connectivity'},
        'witnesses': {'outbounds': ['net']},
        'labelRequest': {'outbound': 'net', 'destination': LABEL_URL or f'http://127.0.0.1:{bench.TARGET_PORT}/204/matrix-label',
                         'labels': {'asn': 'X-ASN'}},
    }
    h['telemetry'] = {'report': {'sessionLabels': {'labels': {'cohort': 'matrix'}}}}
    effective = effective_features(requested)
    for name in FEATURES:
        setv(h, FEATURES[name] + '.isEnabled', name in effective)
    if 'report' in effective:
        h['telemetry']['report'].update(quietSampleFraction=1, interval='100ms')
    cfg['healthObservatory'] = h
    cfg['routing']['rules'].insert(0, {'type': 'field', 'domain': ['full:t6.test'], 'outboundTag': 'net'})
    cfg['routing']['balancers'][0]['strategy']['settings'] = {
        'failover': {'closePending': {'isEnabled': False}, 'closeSilent': {'isEnabled': False}}}
    for outbound in cfg['outbounds']:
        if outbound['tag'] == 'net':
            # Stand destinations such as t6.test are synthetic DNS hosts. A
            # direct witness must use Xray's DNS, just like outbound direct.
            outbound['settings']['domainStrategy'] = 'UseIPv4'
        if 'selection' in outbound:
            outbound['selection']['labels'] = {'node': outbound['tag']}
    return bench.api_config(cfg), effective


class LabelFixture:
    """A real local HTTP label endpoint with independently observed requests."""
    def __enter__(self):
        self.hits = []
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                fixture.hits.append(time.time())
                self.send_response(204)
                self.send_header('Content-Length', '0')
                self.send_header('X-ASN', 'fixture-asn')
                self.end_headers()

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.worker = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.worker.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}/labels'
        return self

    def __exit__(self, *args):
        self.server.shutdown()
        self.server.server_close()
        self.worker.join(2)


def target_counts(stand, labels):
    rows = []
    for line in Path(stand.target_log).read_text().splitlines():
        timestamp, host, path = line.split(' ', 2)
        rows.append((float(timestamp), host, path))
    return {
        'tcp_probe': sum(host == 'probe.test' for _, host, _ in rows),
        'connectivity': sum(path == '/204/matrix-connectivity' for _, _, path in rows),
        'label': len(labels.hits),
        'udp_probe': sum(row['kind'] == 'stun' and row['epoch'] == stand.udp_targets.epoch
                         for row in stand.udp_targets.snapshot()),
        'provider': len(stand.provider_requests()),
        'user': sum(host in ('t1.test', 't2.test') and path == '/bytes/65536' for _, host, path in rows),
    }


def path_status(status, outbound='a1'):
    return next(path for path in status['paths'] if path['outbound'] == outbound)


def poll_status(client, predicate, timeout=6):
    deadline = time.monotonic() + timeout
    latest = client.status()
    while not predicate(latest) and time.monotonic() < deadline:
        time.sleep(.05)
        latest = client.status()
    return latest


def poll_probe_result(xray, ticket, timeout=12):
    """CLI RequestProbe has a bounded wait; ProbeResult itself never waits."""
    deadline = time.monotonic() + timeout
    latest = raw_api(xray, 'probe-result', ticket['run'], str(ticket['id']))
    while latest.get('success') and not (latest.get('data') or {}).get('is_done') and time.monotonic() < deadline:
        time.sleep(.05)
        latest = raw_api(xray, 'probe-result', ticket['run'], str(ticket['id']))
    return latest


def gate(check, label, result, enabled):
    check(label, result.get('success') is enabled and
          (enabled or result.get('error', {}).get('code') == 'feature_disabled'), result)


def run_case(xray, stand, labels, out, name, requested, quiet_seconds):
    stand.reset()
    labels.hits.clear()
    cfg, enabled = config(requested)
    checks, behavior = [], set()

    def check(label, condition, detail=None, features=()):
        checks.append({'check': label, 'ok': bool(condition), 'detail': detail})
        if condition:
            behavior.update(features)

    observation = {}
    try:
        with bench.Client(xray, cfg, str(out), 'v2-' + name) as client:
            request(); request()
            status = poll_status(client, lambda st: not st['probes_running'] and
                                 ('quality' not in enabled or (path_status(st).get('quality', {}).get('ttfb_count', 0) >= 2 and
                                  path_status(st).get('quality', {}).get('bytes_down', 0) >= 131072)) and
                                 ('report' not in enabled or (path_status(st).get('conns_ok', 0) >= 2 and
                                  path_status(st).get('bytes_down', 0) >= 131072)))
            actual = {flag for flag in FEATURES if status['features'].get(flag) is True}
            check('Status exposes every observer feature under its normative key', set(FEATURES) <= set(status['features']),
                  sorted(set(FEATURES) - set(status['features'])))
            check('effective flags match requested plus explicit dependencies', actual == enabled, sorted(actual))
            check('real traffic chooses cheapest tunnel', status['balancers'][0]['active'] == 'a1', status['balancers'])
            check('Status is marked local', status['is_local_only'] is True)
            tcp_quality = any(path.get('quality', {}).get('ttfb_count', 0) or
                              path.get('quality', {}).get('dial_count', 0) for path in status['paths'])
            check('TCP quality sample gate', bool(tcp_quality) == ('quality' in enabled))
            if 'quality' in enabled:
                quality = path_status(status).get('quality', {})
                check('quality records real reply samples independently', quality.get('ttfb_count', 0) >= 2 and
                      quality.get('bytes_down', 0) >= 131072, quality, ('quality',))
            vars_ = json.loads(urllib.request.urlopen(f'http://127.0.0.1:{bench.METRICS_PORT}/debug/vars', timeout=3).read())
            exported = vars_.get('observatory') or {}
            check('metrics export gate matches actual exported paths', set(exported) == ({'a1', 'b1'} if 'metrics' in enabled else set()),
                  sorted(exported), ('metrics',) if 'metrics' in enabled else ())

            before = target_counts(stand, labels)
            started = time.monotonic()
            time.sleep(quiet_seconds)
            after = target_counts(stand, labels)
            observation = {'duration_seconds': round(time.monotonic() - started, 3), 'before': before, 'after': after}
            check('healthy traffic causes no unsolicited TCP probes', after['tcp_probe'] == 0, observation)
            if 'udp_probe' not in enabled:
                check('disabled UDP probe sends no STUN requests', after['udp_probe'] == 0, observation)
            if 'label_request' not in enabled:
                check('disabled labelRequest sends no HTTP requests', after['label'] == 0, observation)
            if 'connectivity' not in enabled:
                check('disabled connectivity sends no HTTP requests', after['connectivity'] == 0, observation)
            check('no provider configured sends no provider requests', after['provider'] == 0, observation)
            check('target independently saw both user requests', after['user'] == 2, after)

            for cmd, flag in [('events', 'events'), ('report', 'report')]:
                result = raw_api(xray, cmd)
                gate(check, cmd + ' command gate', result, flag in enabled)
                if cmd == 'events' and flag in enabled:
                    # Preserve drained entries for Client's final reconciliation.
                    client.events.extend(result['data']['events'])
                    client.dropped += result['data']['dropped']
                    check('journal records real initial choice', any(event['type'] == 'switch' for event in client.events),
                          client.events, ('events',))
                if cmd == 'report' and flag in enabled:
                    summary = result.get('data')
                    path = summary['paths']['a1'] if summary else {}
                    check('report accounts real user connections and bytes without events dependency',
                          path.get('conns_ok', 0) == 2 and path.get('bytes_down', 0) >= 131072,
                          path, ('report',))
                    again = raw_api(xray, 'report').get('data')
                    check('PeekReport does not close or double-count window', again is not None and summary is not None and
                          again['report_index'] == summary['report_index'] and
                          all(again['paths']['a1'][key] == summary['paths']['a1'][key]
                              for key in ('bytes_up', 'bytes_down', 'conns_ok', 'conns_failed', 'probes')),
                          {'first': summary, 'second': again})

            result = raw_api(xray, 'memory', '--low=true')
            gate(check, 'memory command gate', result, 'memory_pressure' in enabled)
            check('memory signal state changes only when enabled', client.status()['is_memory_low'] == ('memory_pressure' in enabled),
                  features=('memory_pressure',) if 'memory_pressure' in enabled else ())
            if 'memory_pressure' in enabled:
                raw_api(xray, 'memory', '--low=false')

            result = raw_api(xray, 'pin', 'auto', 'b1', status['run'])
            gate(check, 'pin command gate', result, 'pinning' in enabled)
            if 'pinning' in enabled:
                check('pin selects requested outbound', client.status()['balancers'][0]['active'] == 'b1', features=('pinning',))
                raw_api(xray, 'release', 'auto', status['run'])

            before_probe = target_counts(stand, labels)['tcp_probe']
            result = raw_api(xray, 'probe', 'auto', 'b1')
            gate(check, 'probe command gate', result, 'probe_requests' in enabled)
            if 'probe_requests' in enabled:
                ticket = result['data']
                check('probe ticket belongs to current run', ticket['run'] == status['run'])
                stale = raw_api(xray, 'probe-result', 'prior-run', str(ticket['id']))
                check('prior-run ticket refused', stale.get('error', {}).get('code') == 'stale_run')
                final = poll_probe_result(xray, ticket)
                answer = final.get('data') or {}
                check('host probe completes with real successful path result', final['success'] and answer.get('is_done') is True and
                      any(row.get('outbound') == 'b1' and row.get('result') == 'ok' for row in answer.get('outbounds', [])),
                      answer, ('probe_requests',))
                check('host probe reaches independent HTTP target', target_counts(stand, labels)['tcp_probe'] > before_probe)
            else:
                check('disabled host probe sends no HTTP probe', target_counts(stand, labels)['tcp_probe'] == before_probe)

            if 'pinning' in enabled:
                # After the explicit-probe oracle: successful pin traffic would
                # otherwise legitimately let its fresh result be reused.
                raw_api(xray, 'pin', 'auto', 'b1', status['run'])
                request('t2.test')
                check('pin routes real user bytes to requested outbound', client.outbound_bytes('b1')[1] >= 65536)
                raw_api(xray, 'release', 'auto', status['run'])

            # NetworkLost is a core signal. Enabled witnesses and connectivity
            # restore it by different independently counted network actions.
            lost_result = raw_api(xray, 'lost')
            lost = client.status()
            check('NetworkLost signal is accepted', lost_result['success'] is True)
            check('NetworkLost pauses decisions before any witness', lost['is_network_up'] is False)
            if 'witnesses' in enabled:
                request('t6.test')
                restored = poll_status(client, lambda st: st['is_network_up'])
                check('network recovers after real direct witness traffic', restored['is_network_up'] is True,
                      {'connectivity_also_enabled': 'connectivity' in enabled},
                      features=('witnesses',) if 'connectivity' not in enabled else ())
            else:
                # An authoritative host-loss signal does not create polling.
                # Active connectivity is tested separately using real suspicion.
                connectivity_before = target_counts(stand, labels)['connectivity']
                request('t6.test')
                time.sleep(.6)
                check('disabled witnesses do not restore network from direct traffic', client.status()['is_network_up'] is False)
                check('host NetworkLost does not itself start active polling',
                      target_counts(stand, labels)['connectivity'] == connectivity_before)
            raw_api(xray, 'network', 'matrix-network')

            # Final window semantics are checked directly when the event journal
            # is absent; Client reconciles journal windows when both are enabled.
            if 'report' in enabled and 'events' not in enabled:
                snapshot = raw_api(xray, 'report').get('data')
                final = raw_api(xray, 'report', 'final').get('data')
                duplicate = raw_api(xray, 'report', 'final').get('data')
                check('standalone report final closes exactly one window', final is not None and snapshot is not None and
                      final['report_index'] == snapshot['report_index'] and final['report_reason'] == 'stop' and duplicate is None)
    except Exception as exc:
        check('matrix case ran', False, repr(exc))
    report_checks = bench.pop_report_checks()
    if {'events', 'report'} <= enabled:
        checks.extend(report_checks)
    return {'case': name, 'kind': 'configuration_matrix', 'requested': sorted(requested), 'enabled': sorted(enabled),
            'dependencies': sorted(enabled - set(requested)), 'stimulus_features': [],
            'behavior_verified': sorted(behavior), 'config_sha256': hashlib.sha256(json.dumps(cfg, sort_keys=True).encode()).hexdigest(),
            'observed_network_work': observation, 'checks': checks}


def dependency_rejections(xray, out):
    recipes = []
    cfg, _ = config({'lone_failure_probe'})
    setv(cfg, 'healthObservatory.evidence.dialFailures.isEnabled', False)
    recipes.append(('loneFailureProbe without path detector', cfg, 'loneFailureProbe'))
    cfg, _ = config({'session_labels'})
    setv(cfg, 'healthObservatory.telemetry.report.isEnabled', False)
    cfg['healthObservatory']['telemetry']['report'].pop('quietSampleFraction', None)
    recipes.append(('optional report opt-out rejected', cfg, 'report'))
    cfg, _ = config({'metrics'})
    cfg.pop('metrics')
    recipes.append(('metrics without exporter', cfg, 'metrics'))
    cfg, _ = config(set())
    next(outbound for outbound in cfg['outbounds'] if outbound['tag'] == 'b1')['selection']['isProbeOnly'] = True
    recipes.append(('probe-only without candidates', cfg, 'candidates'))
    cfg, _ = config(set())
    profile = cfg['healthObservatory']['probes']['profiles'][0]
    profile['steps'].append(copy.deepcopy(profile['steps'][0]))
    profile['steps'][-1]['name'] = 'second'
    recipes.append(('two steps without stepProfiles', cfg, 'stepProfiles'))
    result = []
    primary_features = {'loneFailureProbe': 'lone_failure_probe', 'sessionLabels': 'session_labels', 'report': 'session_labels',
                        'metrics': 'metrics', 'candidates': 'candidates', 'stepProfiles': 'step_profiles'}
    for name, cfg, required in recipes:
        if not select_case(name, primary=primary_features[required]):
            continue
        path = out / ('invalid-' + required + '.json')
        path.write_text(json.dumps(cfg, indent=2))
        proc = subprocess.run([xray, 'run', '-test', '-c', str(path)], capture_output=True, text=True, timeout=10)
        error = proc.stdout + proc.stderr
        result.append({'case': name, 'kind': 'invalid_dependency', 'checks': [
            {'check': 'config rejects explicit missing dependency with feature path',
             'ok': proc.returncode != 0 and required in error, 'detail': error[-1600:]}]})
    return result


def collector_scenarios(xray, stand, labels, out, metadata_ready):
    """Public operations stimulate collectors without enabling TCP detectors."""
    rows = []

    def scenario(name, requested, stimuli, execute, primary=None):
        if not select_case(name, requested, primary):
            return
        stand.reset()
        labels.hits.clear()
        cfg, effective = config(set(requested) | set(stimuli))
        checks, verified = [], set()

        def check(label, condition, detail=None, features=()):
            checks.append({'check': label, 'ok': bool(condition), 'detail': detail})
            if condition:
                verified.update(features)

        try:
            execute(cfg, check)
        except Exception as exc:
            check('behavioral scenario ran', False, repr(exc))
        rows.append({'case': name, 'kind': 'behavioral_scenario', 'requested': sorted(requested),
                     'enabled': sorted(effective), 'dependencies': sorted(effective - set(requested) - set(stimuli)),
                     'stimulus_features': sorted(stimuli), 'behavior_verified': sorted(verified), 'checks': checks,
                     'config_sha256': hashlib.sha256(json.dumps(cfg, sort_keys=True).encode()).hexdigest()})
        bench.pop_report_checks()
        print(f'{name}: {sum(check["ok"] for check in checks)}/{len(checks)}', flush=True)

    for enabled in (False, True):
        def wake(cfg, check, enabled=enabled):
            setv(cfg, 'healthObservatory.host.userPresent.freshness', '100ms')
            setv(cfg, 'healthObservatory.host.userPresent.minInterval', '2s')
            with bench.Client(xray, cfg, str(out), f'v2-wake-{enabled}') as client:
                request()
                time.sleep(.3)
                before = len(stand.probe_hits(0))
                result = raw_api(xray, 'present')
                gate(check, 'UserPresent command gate', result, enabled)
                if enabled:
                    status = poll_status(client, lambda st: path_status(st).get('probe', {}).get('reason') == 'wake')
                    check('UserPresent executes stale selected-path probe',
                          path_status(status).get('probe', {}).get('reason') == 'wake' and len(stand.probe_hits(0)) > before,
                          path_status(status), ('user_present',))
                    first = len(stand.probe_hits(0))
                    raw_api(xray, 'present')
                    time.sleep(.6)
                    check('repeated UserPresent respects minimum interval', len(stand.probe_hits(0)) == first)
                else:
                    time.sleep(.6)
                    check('disabled UserPresent creates no probe request', len(stand.probe_hits(0)) == before)
        scenario(f'user-present-{"on" if enabled else "off"}', {'user_present'} if enabled else set(), set(), wake, 'user_present')

    for enabled in (False, True):
        def memory(cfg, check, enabled=enabled):
            with bench.Client(xray, cfg, str(out), f'v2-network-memory-{enabled}') as client:
                client.network('matrix-first')
                request()
                client.network('matrix-other')
                client.network('matrix-first')
                client.drain()
                changes = [event['fields'] for event in client.events if event['type'] == 'network_change']
                final = changes[-1] if changes else {}
                check('return to traffic-proven network is known only with memory', final.get('is_known') is enabled,
                      final, ('network_memory',) if enabled else ())
                check('memory does not enable TCP quality', not path_status(client.status()).get('quality', {}).get('ttfb_count'))
        scenario(f'network-memory-{"on" if enabled else "off"}', {'network_memory'} if enabled else set(), {'events'}, memory, 'network_memory')

    for enabled in (False, True):
        def connectivity(cfg, check, enabled=enabled):
            setv(cfg, 'healthObservatory.network.connectivity.timeout', '2s')
            with bench.Client(xray, cfg, str(out), f'v2-connectivity-suspect-{enabled}') as client:
                request()
                control_port = bench.CONNECTIVITY_PORT
                before = bench.chaos_stats()[control_port][0]
                # Reset accepts and independently counts the active-check dial;
                # refuse closes the listener and cannot count SYN attempts.
                bench.chaos(control_port, 'reset')
                bench.chaos(bench.address_port('tcp_tls', 0), 'refuse')
                failures = []
                for host in bench.TARGETS[:3]:
                    try:
                        request(host, timeout=3)
                        failures.append(False)
                    except (AssertionError, OSError):
                        failures.append(True)
                check('active connectivity stimulus is three real refused transport requests', all(failures), failures)
                if enabled:
                    paused = poll_status(client, lambda st: st['is_network_up'] is False, timeout=8)
                    attempts = bench.chaos_stats()[control_port][0] - before
                    check('suspect path triggers an actual independent connectivity dial', attempts > 0, attempts)
                    check('failed active check pauses network decisions', paused['is_network_up'] is False and
                          paused['balancers'][0]['active'] == 'a1', paused)
                else:
                    paused = poll_status(client, lambda st: st['balancers'][0]['active'] == 'b1', timeout=8)
                    check('disabled connectivity sends no independent check even after suspicion',
                          bench.chaos_stats()[control_port][0] == before)
                    check('disabled active check does not pause failover', paused['is_network_up'] is True and
                          paused['balancers'][0]['active'] == 'b1', paused)
                bench.chaos(bench.address_port('tcp_tls', 0), 'pass')
                bench.chaos(control_port, 'pass')
                if enabled:
                    restored = poll_status(client, lambda st: st['is_network_up'] and
                                           target_counts(stand, labels)['connectivity'] > 0, timeout=8)
                    check('successful configured HTTP endpoint restores active-check pause',
                          restored['is_network_up'] is True and target_counts(stand, labels)['connectivity'] > 0,
                          {'status': restored, 'endpoint_hits': target_counts(stand, labels)['connectivity']}, ('connectivity',))
                else:
                    check('disabled connectivity endpoint receives no HTTP request', target_counts(stand, labels)['connectivity'] == 0)
                request()
                check('real user traffic succeeds after connectivity observation', True)
        scenario(f'connectivity-suspect-{"on" if enabled else "off"}', {'connectivity'} if enabled else set(),
                 {'dial_failures'}, connectivity, 'connectivity')

    def steps(cfg, check):
        profile = cfg['healthObservatory']['probes']['profiles'][0]
        profile['steps'] = [
            {'name': 'empty', 'destinations': [f'http://probe.test:{bench.TARGET_PORT}/204/matrix-step-one'],
             'expectedStatuses': [204], 'timeout': '3s'},
            {'name': 'body', 'destinations': [f'http://probe.test:{bench.TARGET_PORT}/bytes/32768'],
             'expectedStatuses': [200], 'minBytes': 32768, 'timeout': '3s'},
        ]
        with bench.Client(xray, cfg, str(out), 'v2-two-steps') as client:
            request()
            ticket = raw_api(xray, 'probe', 'auto', 'b1')['data']
            status = poll_status(client, lambda st: path_status(st, 'b1').get('probe') is not None)
            probe = path_status(status, 'b1').get('probe', {})
            outcomes = probe.get('steps', [])
            check('multi-step profile executes both required real HTTP steps',
                  len(outcomes) == 2 and {row['name'] for row in outcomes} == {'empty', 'body'} and
                  all(row['is_passed'] for row in outcomes), probe, ('step_profiles',))
            check('independent target sees two profile requests', len(stand.probe_hits(0)) == 2)
            check('probe measurement driver completes', poll_probe_result(xray, ticket)['data']['is_done'])
    scenario('step-profiles-real-two-steps', {'step_profiles'}, {'probe_requests'}, steps)

    for enabled in (False, True):
        def udp(cfg, check, enabled=enabled):
            # A host request is an explicit stimulus, not an implicit dependency
            # of the automatic UDP measurement feature.
            with bench.Client(xray, cfg, str(out), f'v2-udp-request-{enabled}') as client:
                request()
                before = target_counts(stand, labels)['udp_probe']
                result = raw_api(xray, 'probe', '--udp', 'auto', 'b1')
                gate(check, 'UDP host request requires its own measurement flag', result, enabled)
                if enabled:
                    ticket = result['data']
                    answer = poll_probe_result(xray, ticket)['data']
                    check('UDP request completes with successful real STUN measurement',
                          answer['is_done'] and answer['is_udp'] and
                          any(item['result'] == 'ok' and item.get('udp', {}).get('sent', 0) > 0 and
                              item['udp']['answered'] == item['udp']['sent'] for item in answer['outbounds']),
                          answer, ('udp_probe',))
                    check('independent UDP target saw STUN request', target_counts(stand, labels)['udp_probe'] > before)
                else:
                    time.sleep(.6)
                    check('disabled UDP measurement makes no STUN request', target_counts(stand, labels)['udp_probe'] == before)
                check('UDP measurement leaves TCP route unchanged', client.status()['balancers'][0]['active'] == 'a1')
    
        scenario(f'udp-request-{"on" if enabled else "off"}', {'udp_probe'} if enabled else set(), {'probe_requests'}, udp, 'udp_probe')

    if metadata_ready:
        for enabled in (False, True):
            def measurements(cfg, check, enabled=enabled):
                with bench.Client(xray, cfg, str(out), f'v2-probe-measurements-{enabled}') as client:
                    request()
                    ticket = raw_api(xray, 'probe', 'auto', 'b1')['data']
                    final = poll_probe_result(xray, ticket)
                    check('probe measurement stimulus completes', final['success'] and final['data']['is_done'])
                    status = client.status()
                    quality = path_status(status, 'b1').get('quality', {})
                    check('real reserve probe sample exported only with probeMeasurements',
                          (quality.get('probe_count', 0) > 0) is enabled, quality,
                          ('probe_measurements',) if enabled else ())
                    check('probe-only measurements carry no TCP timing histograms',
                          all(not quality.get(key, 0) for key in ('ttfb_count', 'dial_count', 'tcp_count', 'bulk_count')), quality)
                    check('probe collector adds no extra network requests', len(stand.probe_hits(0)) == 1)
            scenario(f'probe-measurements-{"on" if enabled else "off"}', {'probe_measurements'} if enabled else set(),
                     {'probe_requests'}, measurements, 'probe_measurements')
        for enabled in (False, True):
            def host(cfg, check, enabled=enabled):
                with bench.Client(xray, cfg, str(out), f'v2-host-labels-{enabled}') as client:
                    request()
                    before = client.status()
                    result = raw_api(xray, 'network', 'matrix-labels', '--label', 'type=wifi')
                    after = client.status()
                    check('labels signal always applies base network change', result['success'] and after.get('network_id') == 'matrix-labels', result)
                    check('host labels present only with hostLabels flag',
                          after.get('network', {}).get('host', {}).get('type') == ('wifi' if enabled else None), after.get('network'),
                          ('host_labels',) if enabled else ())
                    invalid = raw_api(xray, 'network', 'matrix-invalid', '--label', 'BADKEY=invalid')
                    current = client.status()
                    if enabled:
                        # CLI/gRPC carries a typed map and cannot express malformed
                        # JSON. Native malformed-object atomicity has a separate
                        # bridge regression; invalid entries here may normalize.
                        check('enabled host labels never export invalid keys',
                              'BADKEY' not in current.get('network', {}).get('host', {}), current.get('network'))
                    else:
                        check('disabled host labels ignore invalid values while applying base signal',
                              invalid['success'] and current.get('network_id') == 'matrix-invalid' and
                              not current.get('network', {}).get('host'), current)
                    check('host labels create no label endpoint query', len(labels.hits) == 0)
                    check('host labels do not change chosen route', before['balancers'][0]['active'] == current['balancers'][0]['active'])
            scenario(f'host-labels-{"on" if enabled else "off"}', {'host_labels'} if enabled else set(), set(), host, 'host_labels')

        for enabled in (False, True):
            def ip_labels(cfg, check, enabled=enabled):
                with bench.Client(xray, cfg, str(out), f'v2-ip-labels-{enabled}') as client:
                    client.network('matrix-ip')
                    request()
                    if enabled:
                        status = poll_status(client, lambda st: st.get('network', {}).get('ip', {}).get('asn') == 'fixture-asn')
                        check('real whoami HTTP reply populates independent IP labels',
                              status.get('network', {}).get('ip', {}).get('asn') == 'fixture-asn' and len(labels.hits) > 0,
                              {'network': status.get('network'), 'requests': len(labels.hits)}, ('label_request',))
                    else:
                        time.sleep(.6)
                        status = client.status()
                        check('disabled labelRequest exports no IP labels and sends no query',
                              not status.get('network', {}).get('ip') and len(labels.hits) == 0, status.get('network'))
                    check('IP labels work without hostLabels, memory or TCP quality',
                          all(status['features'][flag] is False for flag in ('host_labels', 'network_memory', 'quality')))
                    check('whoami does not count as user quality', not path_status(status).get('quality', {}).get('ttfb_count'))
            scenario(f'ip-labels-{"on" if enabled else "off"}', {'label_request'} if enabled else set(), set(), ip_labels, 'label_request')

        for feature, key in [('path_labels', 'path_labels'), ('session_labels', 'labels')]:
            for enabled in (False, True):
                def report_labels(cfg, check, enabled=enabled, feature=feature, key=key):
                    with bench.Client(xray, cfg, str(out), f'v2-{feature}-{enabled}') as client:
                        request()
                        summary = raw_api(xray, 'report').get('data')
                        expected = {'a1': {'node': 'a1'}} if feature == 'path_labels' else {'cohort': 'matrix'}
                        actual = summary.get(key, {}) if summary else {}
                        check('report metadata appears only under its own gate',
                              all(actual.get(k) == v for k, v in expected.items()) if enabled else not actual,
                              actual, (feature,) if enabled else ())
                        check('metadata report is independent of TCP quality and events',
                              client.status()['features']['quality'] is False and client.status()['features']['events'] is False)
                # Report is a normative dependency only for sessionLabels; for
                # pathLabels it merely exposes the report-level observable.
                stimuli = {'report'} if feature == 'path_labels' or not enabled else set()
                scenario(f'{feature}-{"on" if enabled else "off"}', {feature} if enabled else set(), stimuli, report_labels, feature)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('xray')
    parser.add_argument('output', type=Path)
    parser.add_argument('--bench-dir', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--quiet-seconds', type=float, default=1.1)
    parser.add_argument('--metadata-ready', action='store_true')
    parser.add_argument('--matrix-only', action='store_true', help='Omit behavioral scenarios; records explicit coverage gaps')
    parser.add_argument('--case', action='append', help='Run exact named cases across matrix, dependencies and behavioral scenarios')
    parser.add_argument('--feature', action='append', choices=sorted(FEATURES),
                        help='Run primary single-feature matrix/dependency/scenario cases, including their off controls')
    args = parser.parse_args()
    if args.quiet_seconds < .5:
        parser.error('--quiet-seconds must cover at least one fixed 500ms engine cadence')
    sys.path.insert(0, str(args.bench_dir.resolve()))
    global bench, LABEL_URL, CASE_FILTER, FEATURE_FILTER, AVAILABLE_CASES
    CASE_FILTER, FEATURE_FILTER, AVAILABLE_CASES = set(args.case or ()), set(args.feature or ()), {}
    bench = importlib.import_module('bench')
    xray = os.path.abspath(args.xray)
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    cases = [('none', set())] + [(name, {name}) for name in FEATURES] + [('all', set(FEATURES))]
    rng = random.Random(20261013)
    cases += [(f'combination-{i}', set(rng.sample(list(FEATURES), 8))) for i in range(4)]
    results = []
    with LabelFixture() as labels:
        LABEL_URL = labels.url
        # Test-server opt-in: restricted PROBE_UUID may reach only probe.test;
        # ordinary real members continue using their unchanged USER_UUID.
        with bench.Stand(xray, str(out), probe_credentials=True, udp_observation=True) as stand:
            for name, requested in cases:
                if not select_case(name, requested):
                    continue
                row = run_case(xray, stand, labels, out, name, requested, args.quiet_seconds)
                results.append(row)
                print(f'{name}: {sum(check["ok"] for check in row["checks"])}/{len(row["checks"])}', flush=True)
            results.extend(dependency_rejections(xray, out))
            if not args.matrix_only:
                results.extend(collector_scenarios(xray, stand, labels, out, args.metadata_ready))
                # Kept separate so the configuration matrix still means one
                # requested feature plus its contractual dependencies.
                helper_dir = Path(__file__).resolve().parent / 'api-live-matrix-review'
                if (helper_dir / 'feature_scenarios.py').exists():
                    sys.path.insert(0, str(helper_dir))
                helper = importlib.import_module('feature_scenarios')
                results.extend(helper.run_scenarios(xray, stand, out, config, raw_api, select_case=select_case))
    if CASE_FILTER - set(AVAILABLE_CASES):
        parser.error('unknown or disabled case: ' + ', '.join(sorted(CASE_FILTER - set(AVAILABLE_CASES))))
    (out / 'matrix.json').write_text(json.dumps(results, indent=2))
    failures = [(row['case'], check) for row in results for check in row['checks'] if not check['ok']]
    behavior = sorted({flag for row in results for flag in row.get('behavior_verified', [])})
    coverage_notes = [
        'server_unverified: standalone live exact reason needs trusted REALITY decoy; core feature tests remain separate evidence.',
        'candidates: this runner proves probe-only user exclusion; automatic standalone upkeep has separate live/core evidence.',
        'host_labels: CLI/gRPC typed map cannot represent malformed JSON; native malformed-input atomicity is bridge evidence.',
        'tunnel_sleep/close_unchosen_idle: live verifies real AWG only, other transport capabilities need their own coverage.',
    ]
    if not args.metadata_ready:
        coverage_notes.append('Metadata payload scenarios were omitted pending combined runtime source; their flag gates do not prove implementation.')
    if args.matrix_only:
        coverage_notes.append('Behavioral fault/resource/metadata scenarios were explicitly omitted by --matrix-only.')
    summary = {'cases': len(results), 'checks': sum(len(row['checks']) for row in results), 'failures': failures,
               'selected_cases': sorted(CASE_FILTER), 'selected_features': sorted(FEATURE_FILTER),
               'behavior_verified': behavior, 'behavior_missing': sorted(set(FEATURES) - set(behavior)),
               'coverage_notes': coverage_notes,
               'checks_passed': not failures, 'live_behavior_coverage_complete': set(behavior) == set(FEATURES),
               'metadata_scenarios_enabled': args.metadata_ready,
               'negative_proof_scope': 'Only independently counted endpoint work in each recorded interval; no zero-allocation claim.'}
    (out / 'summary.json').write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    return bool(failures)


if __name__ == '__main__':
    sys.exit(main())
