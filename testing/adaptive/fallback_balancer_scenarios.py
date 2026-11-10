"""Balancer fallback LIVE scenarios using real TCP/UDP and public health APIs.

No clocks, controller thresholds, jitter or quarantine constants are replaced.
"""
import copy
import json
import os
from pathlib import Path
import socket
import time

import bench
from feature_scenarios import _request, _poll
from scenarios import check, scenario, leak_check
import udp_matrix


def _balancer(client, tag):
    return next(row for row in client.status()['balancers'] if row['balancer'] == tag)


def _config(udp=False, pinned=False, provider=False):
    source = 'udp' if udp else 'ai'
    members = [('special', 'awg' if udp else 'tcp_tls', 0), ('normal', 'reality', 0)]
    cfg = bench.api_config(bench.client_config(members, {'special': 1, 'normal': 1}))
    h = cfg['healthObservatory']
    # Observe only what these scenarios need. Network probing cannot race the
    # route proof, and per-balancer freshness follows explicit networkChanges.
    h['network'] = {}
    h['resources'] = {}
    h['probes'].pop('candidates', None)
    h['telemetry'] = {'events': {}, 'report': {'quietSampleFraction': 1}, 'quality': {}}
    h['host'] = {'pinning': {}}
    for item in cfg['outbounds']:
        if item.get('tag') in ('special', 'normal'):
            item['selection']['balancer'] = source if item['tag'] == 'special' else 'main'
    settings = {'fallbackBalancer': 'main', 'networkChanges': {},
                'failover': {'confirmWithProbe': 'never'}}
    if pinned:
        settings['startup'] = {'pin': {'outbound': 'special', 'timeout': '0s'}}
    cfg['routing']['balancers'] = [
        {'tag': source, 'fallbackTag': 'block', 'strategy': {'type': 'leastCost', 'settings': settings}},
        {'tag': 'main', 'fallbackTag': 'block', 'strategy': {'type': 'leastCost', 'settings': {
            'networkChanges': {}, 'failover': {'confirmWithProbe': 'never'}}}},
    ]
    cfg['inbounds'][0]['sniffing'] = {'enabled': True, 'destOverride': ['http', 'tls']}
    if udp:
        # The AWG netstack cannot carry loopback IPs. VLESS still forwards the
        # hostname to its server, whose independent DNS maps it to loopback.
        cfg['dns']['hosts'].update(dict.fromkeys(bench.TARGETS + ['probe.test'], bench.TUNNEL_TARGET))
    rules = []
    if udp:
        rules.append({'network': 'udp', 'balancerTag': source})
        # TCP evidence uses the same AWG tunnel; UDP evidence does not change health.
        rules.append({'domain': ['full:t2.test', 'full:t3.test', 'full:t4.test'], 'balancerTag': source})
    else:
        rules.append({'domain': ['full:t1.test', 'full:t2.test', 'full:t3.test'], 'balancerTag': source})
    cfg['routing']['rules'] = rules + [{'inboundTag': ['socks'], 'balancerTag': 'main'}]
    if provider:
        p = bench.provider_config(['net'], triggers={'shouldRefreshWhenExhausted': True})
        p['apply']['balancer'] = source
        p = bench.api_config({'healthObservatory': {'_standRecipeV1': True, 'providers': [p]}})['healthObservatory']['providers'][0]
        h['providers'] = [p]
    return cfg


def _events(client, event, **fields):
    client.drain()
    return [row for row in client.events if row['type'] == event and
            all(row['fields'].get(k) == v for k, v in fields.items())]


def _receipt(work, name, **value):
    Path(work, name + '-proof.json').write_text(json.dumps(value, indent=2))


def _wait_fallback(client, source, host='t1.test', seconds=80):
    # is_falling_back becomes true on routed demand, not merely exhaustion.
    # Without network.connectivity an exhausted balancer holds its last path
    # for a minute before it falls back.
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        _request(host)
        if _balancer(client, source).get('is_falling_back'):
            return True
        time.sleep(.1)
    return False


def _down(client, source='ai', udp=False):
    port = bench.AWG_ADDRESSES[0] if udp else bench.address_port('tcp_tls', 0)
    bench.chaos(str(port), 'blackhole' if udp else 'refuse')
    for host in ('t2.test', 't3.test', 't4.test') if udp else ('t1.test', 't2.test', 't3.test'):
        _request(host, seconds=8 if udp else 3)
    return _wait_fallback(client, source, 't2.test' if udp else 't1.test')


def _fallback_checks(client, source):
    status = _balancer(client, source)
    starts = _events(client, 'fallback_balancer', balancer=source, to='main', is_active=True)
    exhausted = _events(client, 'exhausted', balancer=source)
    report = client.api('report')
    return [
        check('Status names target and active transition', status,
              status.get('fallback_balancer') == 'main' and status.get('is_falling_back') is True, 'main / true'),
        check('transition starts once on routed demand', len(starts), len(starts) == 1, '1'),
        check('exhausted names fallback balancer', exhausted,
              bool(exhausted) and exhausted[-1]['fields'].get('fallback_balancer') == 'main', 'main'),
        check('report counts transition start', report.get('fallback_balancer_count'),
              report.get('fallback_balancer_count') == 1, '1'),
    ]


@scenario()
def fallback_balancer_ai(stand, xray, work, name):
    # The dedicated node is unavailable before core start, not just later.
    bench.chaos(str(bench.address_port('tcp_tls', 0)), 'refuse')
    checks = []
    with bench.Client(xray, _config(), work, name) as client:
        initial = [_request(host) for host in ('t1.test', 't2.test', 't3.test')]
        falling = _wait_fallback(client, 'ai')
        replies = [_request() for _ in range(3)]
        checks += [check('dead-from-start node reaches fallback', falling, falling, 'true'),
                   check('fallback carries three real 64KiB HTTP replies', replies,
                         all(r['is_ok'] for r in replies), 'all true'),
                   check('target path accounts fallback user bytes', client.outbound_bytes('normal'),
                         client.outbound_bytes('normal')[1] >= 3 * 65536, '>= 196608')]
        checks += _fallback_checks(client, 'ai')
        bench.chaos(str(bench.address_port('tcp_tls', 0)), 'pass')
        client.api('network', 'recovered-ai')
        restored = _request()
        state = _balancer(client, 'ai')
        checks.append(check('reacting network reset returns new connections to dedicated node',
                            {'reply': restored, 'status': state}, restored['is_ok'] and state['active'] == 'special', 'special / real reply'))
        ends = _events(client, 'fallback_balancer', balancer='ai', to='main', is_active=False)
        checks.append(check('transition end event emitted', len(ends), len(ends) == 1, '1'))
        _receipt(work, name, initial=initial, replies=replies, final=client.status(), events=client.events, report=client.api('report'))
    checks.append(leak_check(client))
    return checks


@scenario()
def fallback_balancer_recheck(stand, xray, work, name):
    checks = []
    with bench.Client(xray, _config(), work, name) as client:
        _request()
        down = _down(client)
        checks.append(check('dedicated node is exhausted', down, down, 'true'))
        _request()
        checks += _fallback_checks(client, 'ai')
        bench.chaos(str(bench.address_port('tcp_tls', 0)), 'pass')
        start = time.monotonic()
        passed = attempts = 0
        # Real demand goes through the fallback until a recheck round of the
        # exhausted source finds the node alive; the rounds do not wait for
        # the end of its quarantine.
        while time.monotonic() - start < 180 and _balancer(client, 'ai')['active'] != 'special':
            result = _request()
            attempts += 1; passed += int(result['is_ok'])
            time.sleep(.5)
        restored = _balancer(client, 'ai')
        probe_results = _events(client, 'probe_result', outbound='special')
        ends = _events(client, 'fallback_balancer', balancer='ai', to='main', is_active=False)
        checks += [check('fallback user demand succeeds until the recheck', f'{passed}/{attempts}',
                         passed == attempts and attempts > 0, 'all; at least one real request'),
                   check('ordinary quarantine recheck restores dedicated path without reset', restored,
                         restored['active'] == 'special' and not restored.get('is_falling_back'), 'special / false'),
                   check('source receives real recheck probe result', probe_results, bool(probe_results), 'nonempty'),
                   check('recovery ends transition once', len(ends), len(ends) == 1, '1')]
        _receipt(work, name, attempts=attempts, passed=passed, wait_seconds=time.monotonic()-start,
                 final=client.status(), events=client.events, report=client.api('report'))
    checks.append(leak_check(client))
    return checks


@scenario()
def fallback_balancer_chain(stand, xray, work, name):
    checks = []
    with bench.Client(xray, _config(provider=True), work, name) as client:
        # Reject fresh sets, so exhausted refresh is observable without adding
        # a replacement path that would invalidate the terminal-block proof.
        bench.provider_mode('garbage')
        _request()
        _down(client)
        _request('t6.test')
        bench.chaos(str(bench.address_port('reality', 0)), 'refuse')
        for host in ('t4.test', 't5.test', 't6.test'):
            _request(host)
        # The exhausted target holds its last path for a minute, then routes to its fallbackTag.
        _poll(lambda: _balancer(client, 'main').get('exit') == 'block', 80)
        began = time.monotonic(); terminal = _request(); elapsed = time.monotonic()-began
        checks += [check('exhausted target refuses via terminal block', terminal,
                         not terminal['is_ok'] and elapsed < 2, '<2s; failure'),
                   check('both selectors have no usable path', client.status()['balancers'],
                         all(_balancer(client,t)['active'] == '' for t in ('ai','main')), 'empty active'),
                   check('source exhaustion still asks provider', stand.provider_requests(),
                         bool(stand.provider_requests()), '>=1 real HTTP request')]
        checks += _fallback_checks(client, 'ai')
        report=client.api('report'); times=report.get('fallback_balancer_ms', {})
        checks.append(check('report measures source transition duration', times,
                            isinstance(times,dict) and times.get('ai',0)>0, 'ai > 0ms'))
        _receipt(work,name,terminal=terminal,elapsed=elapsed,final=client.status(),events=client.events,report=report,
                 provider_requests=stand.provider_requests())
    checks.append(leak_check(client))
    return checks


@scenario()
def fallback_balancer_pin(stand, xray, work, name):
    checks = []
    with bench.Client(xray, _config(pinned=True), work, name) as client:
        _request(); bench.chaos(str(bench.address_port('tcp_tls', 0)), 'refuse')
        results=[_request(host) for host in ('t1.test','t2.test','t3.test')]
        _poll(lambda: _events(client,'pinned_down',balancer='ai'),20)
        state=_balancer(client,'ai');starts=_events(client,'fallback_balancer',balancer='ai',is_active=True)
        checks += [check('pin stays on failed dedicated node',state,
                         state.get('pinned')=='special' and state['active']=='special','special'),
                   check('pinned requests fail on chosen node',results,all(not r['is_ok'] for r in results),'all fail'),
                   check('pinned down is diagnosed',len(_events(client,'pinned_down',balancer='ai')),
                         bool(_events(client,'pinned_down',balancer='ai')),'>=1'),
                   check('pin never starts balancer fallback',starts,not starts and not state.get('is_falling_back'),'none'),
                   check('target carries no pinned user bytes',client.outbound_bytes('normal'),
                         client.outbound_bytes('normal')[1]==0,'0')]
        _receipt(work,name,final=client.status(),events=client.events,report=client.api('report'))
    checks.append(leak_check(client))
    return checks


class _UDP:
    """Minimal real SOCKS association; every probe packet has a unique nonce."""
    def __enter__(self):
        self.control=socket.create_connection(('127.0.0.1',bench.SOCKS_PORT),3);self.control.settimeout(3)
        self.control.sendall(b'\x05\x01\x00');assert udp_matrix.recv_exact(self.control,2)==b'\x05\x00'
        self.packet=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);self.packet.bind(('127.0.0.1',0));self.packet.settimeout(2)
        self.control.sendall(b'\x05\x03\x00'+udp_matrix.socks_address('0.0.0.0',self.packet.getsockname()[1]))
        head=udp_matrix.recv_exact(self.control,4);assert head[:3]==b'\x05\x00\x00'
        host,port=udp_matrix.read_socks_address(self.control,head[3]);self.relay=('127.0.0.1' if host=='0.0.0.0' else host,port)
        return self

    def exchange(self):
        nonce=os.urandom(24);frame=b'\x00\x00\x00'+udp_matrix.socks_address('t1.test',bench.UDP_ECHO_PORT)+nonce
        self.packet.sendto(frame,self.relay)
        try:
            response,_=self.packet.recvfrom(65535)
            return response.endswith(nonce)
        except socket.timeout:
            return False

    def __exit__(self,*_):
        self.packet.close();self.control.close()


@scenario()
def fallback_balancer_udp(stand,xray,work,name):
    checks=[]
    with bench.Client(xray,_config(udp=True),work,name) as client, _UDP() as original:
        healthy=original.exchange()
        initial=client.outbound_bytes('special')
        checks.append(check('healthy UDP reaches real echo through AWG',{'echo':healthy,'bytes':initial},healthy and initial[1]>=24,'AWG real echo'))
        _request('t2.test');down=_down(client,'udp',udp=True)
        checks.append(check('TCP evidence through same AWG exhausts its selector',down,down,'true'))
        with _UDP() as fresh:
            forwarded=fresh.exchange()
        target=client.outbound_bytes('normal')
        checks.append(check('new UDP flow uses current main path',{'echo':forwarded,'bytes':target},forwarded and target[1]>=24,'main real echo'))
        checks += _fallback_checks(client,'udp')
        silent = _poll(lambda: _events(client,'pending_closed',outbound='special',is_silent=True),12)
        before_old = client.outbound_bytes('special')
        bench.chaos(str(bench.AWG_ADDRESSES[0]),'pass')
        # Restore the server before testing the old association. Blackhole
        # silence alone cannot prove that the down transition closed it. A
        # SOCKS datagram may create a fresh flow through main after closure.
        old_answer=original.exchange()
        after_old = client.outbound_bytes('special')
        checks.append(check('silent down closes old AWG flow; it cannot resume there',
                            {'closed':silent,'echo':old_answer,'before':before_old,'after':after_old},
                            bool(silent) and after_old == before_old,'silent closure + no AWG bytes'))
        client.api('network','recovered-awg')
        _request('t2.test')
        before=client.outbound_bytes('special')[1]
        with _UDP() as recovered:
            recovery=recovered.exchange()
        after=client.outbound_bytes('special')[1]
        checks.append(check('new UDP flows return to recovered AWG',{'echo':recovery,'delta':after-before},recovery and after-before>=24,'AWG echo + bytes'))
        _receipt(work,name,final=client.status(),events=client.events,report=client.api('report'))
    checks.append(leak_check(client))
    return checks
