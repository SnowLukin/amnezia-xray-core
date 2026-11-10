"""Path census LIVE receipts: real tunnels, endpoints and controller traces.

The one-second startAfter is public feature config. Production quarantine,
probe spacing, cohort draw and 60-second budget windows are unchanged.
"""
import copy
import json
from pathlib import Path
import time

import bench
from feature_scenarios import _active, _path, _poll, _request
from scenarios import check, leak_check, scenario


def _outbound(cfg, tag):
    return next(o for o in cfg['outbounds'] if o.get('tag') == tag)


def _config(members=None, census=True, sample=1, limits=None):
    members = members or [('a1', 'reality', 0), ('b1', 'tcp_tls', 0)]
    costs = {tag: index + 1 for index, (tag, _, _) in enumerate(members)}
    cfg = bench.api_config(bench.client_config(members, costs, loglevel='info'))
    h = cfg['healthObservatory']
    h['network'] = {}
    h['resources'] = {}
    h['host'] = {'memoryPressure': {}}
    h['probes'].pop('candidates', None)
    h['probes'].pop('udp', None)
    h['probes']['limits'] = limits or {'maxConcurrent': 1, 'perMinute': 60,
                                     'perServerNamePerMinute': 10}
    h['telemetry'] = {'events': {'maxEvents': 4096, 'maxBytes': 262144},
                      'report': {'quietSampleFraction': 1}, 'quality': {}}
    if census:
        h['telemetry']['census'] = {'sampleFraction': sample, 'startAfter': '1s'}
    cfg['routing']['balancers'][0]['strategy']['settings'] = {
        'failover': {'confirmWithProbe': 'never'},
    }
    return cfg


def _slow(cfg, tag):
    _outbound(cfg, tag)['selection']['probe'] = 'slow'
    cfg['healthObservatory']['probes']['profiles'].append({'name': 'slow', 'steps': [
        {'name': 'slow', 'destinations': [f'http://probe.test:{bench.TARGET_PORT}/hold/20'],
         'minBytes': 20, 'timeout': '30s', 'progressTimeout': '5s'}]})


def _events(client, kind, **fields):
    client.drain()
    return [e for e in client.events if e['type'] == kind and
            all(e['fields'].get(k) == v for k, v in fields.items())]


def _census(client, count=1, seconds=40):
    # User demand keeps the session eligible; probe responses are never used
    # as the user-traffic prerequisite.
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        rows = _events(client, 'census')
        if len(rows) >= count:
            return rows[count - 1]['fields']
        _request()
        time.sleep(.15)
    return {}


def _hits(stand, since):
    rows = []
    for line in Path(stand.target_log).read_text().splitlines():
        at, host, path = line.split(' ', 2)
        if float(at) >= since and host == 'probe.test':
            rows.append({'at': float(at), 'path': path.strip()})
    return rows


def _save(work, name, **values):
    Path(work, name + '-proof.json').write_text(json.dumps(values, indent=2))


def _trace(client):
    return [{k: e['fields'].get(k) for k in ('from', 'to', 'reason')}
            for e in _events(client, 'switch')]


@scenario()
def census_basic(stand, xray, work, name):
    members = [('a1', 'reality', 0), ('relay-a', 'tcp_tls', 0),
               ('relay-b', 'tcp_tls', 1), ('demand', 'ws', 0),
               ('secret', 'grpc', 0), ('standby', 'xhttp', 0), ('b1', 'grpc', 1)]
    cfg = _config(members)
    settings = cfg['routing']['balancers'][0]['strategy']['settings']
    settings['groups'] = [{'name': 'relay', 'cost': 2, 'probe': 'default'}]
    for tag in ('relay-a', 'relay-b'):
        _outbound(cfg, tag)['selection'] = {'balancer': 'auto', 'group': 'relay'}
    for tag in ('demand', 'secret'):
        _outbound(cfg, tag)['selection']['probePolicy'] = 'onDemand'
    _outbound(cfg, 'standby')['selection']['isProbeOnly'] = True
    _outbound(cfg, 'standby')['settings']['vnext'][0]['users'][0]['id'] = bench.PROBE_UUID
    _outbound(cfg, 'standby')['selection']['probe'] = 'candidate-full'
    h = cfg['healthObservatory']
    h['telemetry']['census']['onDemand'] = ['demand']
    h['probes']['stepProfiles'] = {}
    h['probes']['profiles'].append({'name': 'candidate-full', 'steps': [
        {'name': 'required-32k', 'destinations': [f'http://probe.test:{bench.TARGET_PORT}/bytes/32768'],
         'minBytes': 32768, 'timeout': '4s', 'progressTimeout': '2s'},
        {'name': 'required-8k', 'destinations': [f'http://probe.test:{bench.TARGET_PORT}/bytes/8192'],
         'minBytes': 8192, 'timeout': '4s', 'progressTimeout': '2s'},
        {'name': 'optional', 'isOptional': True,
         'destinations': [f'http://dead.test:{bench.TARGET_PORT}/204'], 'timeout': '2s'}]})
    began = time.time()
    checks = []
    with bench.Client(xray, cfg, work, name) as client:
        first = _request()
        snapshot = _census(client)
        status, report = client.status(), client.api('report')
        paths = snapshot.get('paths', {})
        starts = _events(client, 'probe_start', reason='census')
        hits = _hits(stand, began)
        group_starts = [e for e in starts if e['fields'].get('outbound') in ('relay-a', 'relay-b')]
        secret_starts = [e for e in starts if e['fields'].get('outbound') == 'secret']
        checks += [check('session selected and census complete', snapshot,
                         first['is_ok'] and status.get('census', {}).get('is_selected') is True and
                         snapshot.get('is_complete') is True, 'selected / complete'),
                   check('all paths and group have one snapshot entry', sorted(paths),
                         set(paths) == {'a1', 'relay', 'demand', 'secret', 'standby', 'b1'}, '6 distinct path keys'),
                   check('selected path uses recent user success', paths.get('a1'),
                         paths.get('a1', {}).get('source') == 'recent' and
                         paths.get('a1', {}).get('result') == 'ok', 'recent / ok'),
                   check('group has one real probed representative', group_starts,
                         len(group_starts) == 1 and paths.get('relay', {}).get('member') in ('relay-a', 'relay-b'), '1'),
                   check('explicit onDemand is probed and secret stays skipped', paths,
                         paths.get('demand', {}).get('result') == 'ok' and
                         paths.get('secret', {}).get('result') == 'skipped' and
                         paths.get('secret', {}).get('reason') == 'on_demand' and not secret_starts, 'demand ok; secret skipped'),
                   check('probe-only full mandatory profile really reaches endpoints', hits,
                         paths.get('standby', {}).get('result') == 'ok' and
                         len(hits) == 5 and sum(r['path'] == '/bytes/8192' for r in hits) == 1, '4 x 32KiB + 1 x 8KiB'),
                   check('probe-only stays unavailable for user selection', _path(client, 'standby'),
                         _path(client, 'standby').get('is_probe_only') is True and _active(client) == 'a1', 'true / a1'),
                   check('report retains census and separate actual probe counters', report.get('census'),
                         report.get('census', {}).get('paths') == paths and
                         report.get('census_probes') == 4 and report.get('census_probe_bytes', 0) >= 4 * 32768 + 8192,
                         '4 probes / >=139264 bytes')]
        _save(work, name, snapshot=snapshot, status=status, report=report, events=client.events, hits=hits)
    checks.append(leak_check(client))
    return checks


@scenario()
def census_no_effect(stand, xray, work, name):
    receipts = []
    checks = []
    for enabled in (False, True):
        stand.reset()
        bench.chaos(str(bench.address_port('tcp_tls', 0)), 'refuse')
        cfg = _config(census=enabled)
        with bench.Client(xray, cfg, work, name + ('-on' if enabled else '-off')) as client:
            _request()
            if enabled:
                snapshot = _census(client)
            else:
                time.sleep(2.5); snapshot = {}
            reserve = _path(client, 'b1')
            before = client.status()
            quality = client.api('report').get('quality', {})
            checks.append(check(('census' if enabled else 'control') + ' reserve stays unknown without evidence or quarantine', reserve,
                                reserve.get('state') == 'unknown' and reserve.get('failures') == 0 and
                                reserve.get('quarantine_level', 0) == 0 and reserve.get('probes') == 0 and
                                reserve.get('conns_failed') == 0, 'unknown / zero counters'))
            if enabled:
                checks.append(check('failed real census is observation only', snapshot,
                                    snapshot.get('paths', {}).get('b1', {}).get('result') == 'failed' and _active(client) == 'a1', 'failed / active a1'))
            bench.chaos(str(bench.address_port('tcp_tls', 0)), 'pass')
            bench.chaos(str(bench.address_port('reality', 0)), 'refuse')
            faults = [_request(host) for host in ('t1.test', 't2.test', 't3.test')]
            restored = _poll(lambda: _active(client) == 'b1', 15)
            reply = _request()
            final = client.status()
            receipts.append({'enabled': enabled, 'snapshot': snapshot, 'reserve': reserve,
                             'before': before, 'quality': quality, 'faults': faults,
                             'restored': restored, 'reply': reply, 'final': final,
                             'trace': _trace(client), 'events': client.events, 'provider_requests': stand.provider_requests()})
            checks.append(check(('census' if enabled else 'control') + ' actual failure selects working backup',
                                {'active': _active(client), 'reply': reply}, bool(restored) and reply['is_ok'], 'b1 / 64KiB reply'))
        checks.append(leak_check(client))
    checks += [check('same controlled failures produce identical selection trace', [r['trace'] for r in receipts],
                     receipts[0]['trace'] == receipts[1]['trace'], 'exact trace match'),
               check('census never asks provider', [r['provider_requests'] for r in receipts],
                     all(not r['provider_requests'] for r in receipts), '0')]
    _save(work, name, runs=receipts)
    return checks


@scenario()
def census_interrupt(stand, xray, work, name):
    cfg = _config(); _slow(cfg, 'b1')
    checks = []
    with bench.Client(xray, cfg, work, name) as client:
        _request()
        started = _poll(lambda: _events(client, 'probe_start', reason='census', outbound='b1'), 15)
        began = time.monotonic()
        bench.chaos(str(bench.address_port('tcp_tls', 0)), 'refuse')
        client.network('census-new-network')
        first = _census(client, seconds=4)
        interrupted_after = time.monotonic() - began
        # TakeReport follows the ordinary due interval; PeekReport observes
        # this window without fabricating an interval decision.
        first_report = client.api('report')
        _request()
        # The same SNI was already probed on the old network. Its real
        # one-minute quota survives NetworkChanged, so wait for eligibility.
        second = _census(client, count=2, seconds=70)
        second_report = client.api('report')
        checks += [check('actual old-network census was running', bool(started), bool(started), 'true'),
                   check('network change emits partial snapshot promptly', first,
                         first.get('is_complete') is False and interrupted_after < 3, 'incomplete / <3s'),
                   check('report window keeps interrupted snapshot', first_report and first_report.get('census'),
                         bool(first_report) and first_report.get('census', {}).get('is_complete') is False, 'incomplete'),
                   check('new network receives one complete census after traffic and delay', second,
                         second.get('is_complete') is True and len(_events(client, 'census')) == 2, 'complete / 2 total'),
                   check('both network snapshots survive the same report window', second_report.get('census'),
                         second_report.get('census', {}).get('is_complete') is False and
                         len(second_report.get('census', {}).get('additional', [])) == 1 and
                         second_report['census']['additional'][0].get('is_complete') is True, 'first partial + additional complete'),
                   check('census interruption leaves backup unknown', _path(client, 'b1'),
                         _path(client, 'b1').get('state') == 'unknown' and _path(client, 'b1').get('failures') == 0, 'unknown / 0')]
        _save(work, name, first=first, second=second, first_report=first_report, interrupted_after=interrupted_after, second_report=second_report,
              status=client.status(), events=client.events)
    checks.append(leak_check(client))
    return checks


@scenario()
def census_priority(stand, xray, work, name):
    cfg = _config([('a1', 'reality', 0), ('b1', 'tcp_tls', 0), ('slow', 'ws', 0)])
    _slow(cfg, 'slow')
    checks = []
    with bench.Client(xray, cfg, work, name) as client:
        _request()
        running = _poll(lambda: _events(client, 'probe_start', reason='census', outbound='slow'), 18)
        began = time.monotonic()
        bench.chaos(str(bench.address_port('reality', 0)), 'refuse')
        faults = [_request(host) for host in ('t1.test', 't2.test', 't3.test')]
        switched = _poll(lambda: _active(client) == 'b1', 6)
        elapsed = time.monotonic() - began
        reply = _request()
        downs = _events(client, 'path_state', outbound='a1', state='down')
        switched_rows = _events(client, 'switch', to='b1', reason='failover')
        start_rows = _events(client, 'probe_start', reason='failover')
        decision_ms = switched_rows[-1]['ts'] - downs[-1]['ts'] if switched_rows and downs else None
        preempt_ms = start_rows[0]['ts'] - downs[-1]['ts'] if start_rows and downs else None
        checks += [check('slow census probe had really started', bool(running), bool(running), 'true'),
                   check('failover starts promptly after real down evidence, before census body',
                         {'decision_ms': decision_ms, 'probe_start_ms': preempt_ms, 'request_schedule_seconds': elapsed},
                         bool(switched) and decision_ms is not None and 0 <= decision_ms < 2000 and
                         preempt_ms is not None and 0 <= preempt_ms < 1000, 'start<1s / switch<2s after down'),
                   check('new selected path carries actual user response', reply, reply['is_ok'], '64KiB'),
                   check('ordinary failover probe started while census pending',
                         _events(client, 'probe_start', reason='failover'),
                         bool(_events(client, 'probe_start', reason='failover')), 'nonempty')]
        _save(work, name, faults=faults, elapsed=elapsed, decision_ms=decision_ms, preempt_ms=preempt_ms, reply=reply, status=client.status(), events=client.events,
              report=client.api('report'))
    checks.append(leak_check(client))
    return checks


@scenario()
def census_budget(stand, xray, work, name):
    members = [('a1', 'reality', 0), ('b1', 'tcp_tls', 0), ('c1', 'ws', 0), ('same-name', 'tcp_tls', 1)]
    cfg = _config(members, limits={'maxConcurrent': 1, 'perMinute': 2, 'perServerNamePerMinute': 1})
    began = time.time()
    checks = []
    with bench.Client(xray, cfg, work, name) as client:
        _request()
        ready = _poll(lambda: len(_events(client, 'probe_start', reason='census')) == 2, 18)
        time.sleep(2)
        starts = _events(client, 'probe_start', reason='census')
        hits = _hits(stand, began)
        by_name = [e['fields'].get('outbound') for e in starts if e['fields'].get('outbound') in ('b1', 'same-name')]
        census = client.status().get('census', {})
        checks += [check('two budgeted real probes reach HTTP fixture', hits,
                         bool(ready) and len(starts) == 2 and len(hits) == 2, '2'),
                   check('same server name starts once inside its actual minute', by_name,
                         len(by_name) == 1, '1; no shortened minute'),
                   check('whole census is still paused on remaining quota', census,
                         census.get('is_running') is True and not _events(client, 'census'), 'running / no complete snapshot'),
                   check('fixed common spacing separates real starts', starts,
                         len(starts) == 2 and starts[1]['ts'] > starts[0]['ts'], 'positive spacing')]
        client.api('memory', '--low=true')
        snapshot = _census(client, seconds=4)
        checks.append(check('memory pressure flushes budget-paused partial snapshot', snapshot,
                            snapshot.get('is_complete') is False, 'incomplete'))
        _save(work, name, snapshot=snapshot, starts=starts, hits=hits, status=census, events=client.events,
              report=client.api('report'))
    checks.append(leak_check(client))
    return checks


@scenario()
def census_not_sampled(stand, xray, work, name):
    cfg = _config(sample=1e-12)
    began = time.time()
    checks = []
    with bench.Client(xray, cfg, work, name) as client:
        replies = []
        for _ in range(12):
            replies.append(_request()); time.sleep(.2)
        status = client.status().get('census', {})
        starts = _events(client, 'probe_start', reason='census')
        hits = _hits(stand, began)
        checks += [check('real cohort draw reports session not selected', status,
                         status.get('is_selected') is False, 'false; actual draw'),
                   check('healthy control still routes user traffic', replies,
                         all(r['is_ok'] for r in replies), '12 real 64KiB replies'),
                   check('not sampled creates no census probes or endpoint requests', {'starts': starts, 'hits': hits},
                         not starts and not hits and not _events(client, 'census'), '0'),
                   check('not sampled report contains zero census count', client.api('report').get('census_probes'),
                         client.api('report').get('census_probes') == 0, '0')]
        _save(work, name, status=status, events=client.events, hits=hits, report=client.api('report'))
    checks.append(leak_check(client))
    return checks
