"""Isolated REALITY certificate fixture; native API v2 and production verifier.

Only the client process receives a one-CA fallback trust pool. The Go overlay
changes main startup, never the REALITY verifier, observer, or any thresholds.
All stand endpoints bind loopback. No public destination or system trust edit.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument('--repo', required=True)
parser.add_argument('--production-binary', required=True)
parser.add_argument('--test-binary', required=True)
parser.add_argument('--out', required=True)
parser.add_argument('--cert-dir', required=True)
args = parser.parse_args()
repo = Path(args.repo).resolve()
out = Path(args.out).resolve()
out.mkdir(parents=True, exist_ok=True)
certs = Path(args.cert_dir).resolve()
assert os.environ.get('BENCH_PORT_OFFSET') == '26000'
assert os.environ.get('BENCH_HOST') == '127.0.0.1'
assert os.environ.get('BENCH_LISTEN') == '127.0.0.1'
assert not os.environ.get('BENCH_CLIENT')
assert not os.environ.get('BENCH_CHAOS_SCRIPT')
assert not os.environ.get('XRAY_TEST_FALLBACK_CA')
assert 'x509usefallbackroots' not in os.environ.get('GODEBUG', '')
os.environ['BENCH_SERVER_BIN'] = args.production_binary
sys.path.insert(0, str(repo/'testing/adaptive'))
import bench
import feature_matrix_v2 as matrix
import feature_scenarios as scenario
matrix.bench = bench
bench.CERT_DIR = str(certs)
bench.TARGET_CERT = str(certs/'target.crt')
bench.TARGET_KEY = str(certs/'target.key')

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

rows = []
with bench.Stand(args.production_binary, str(out)) as stand:
    for enabled in (False, True):
        name = 'live-server-unverified-' + ('on' if enabled else 'off')
        stand.reset()
        cfg, effective = matrix.config({'server_unverified'} if enabled else set())
        # Retain the final VLESS error wrapper for the exact error-class oracle.
        cfg['log']['loglevel'] = 'info'
        scenario._replace_transport(cfg, 'a1', 'reality', 0)
        checks = []
        row = {'case': name, 'kind': 'behavioral_scenario',
               'requested': sorted(effective), 'enabled': sorted(effective),
               'dependencies': [], 'stimulus_features': [], 'checks': checks,
               'instrumentation': ['one scratch test CA through main startup overlay; client process only'],
               'config_sha256': hashlib.sha256(json.dumps(cfg, sort_keys=True).encode()).hexdigest()}

        def check(label, condition, detail=None):
            checks.append({'check': label, 'ok': bool(condition), 'detail': detail})

        try:
            client = bench.Client(args.test_binary, cfg, str(out), name)
            client.env = {'GODEBUG': 'x509usefallbackroots=1', 'XRAY_TEST_FALLBACK_CA': str(certs/'ca.crt')}
            with client:
                status = client.status()
                actual = {k for k,v in status['features'].items() if v and not k.startswith(('balancers.', 'providers.'))}
                check('only declared observer features are effective', actual == effective, sorted(actual))
                scenario._traffic_prime(check)
                row['warm_status'] = client.status()
                check('warm real request selects verified REALITY a1', scenario._active(client) == 'a1', scenario._active(client))
                port = bench.address_port('reality', 0)
                before = bench.chaos_stats()[port][0]
                bench.chaos(port, f'redirect:{bench.server_port("tcp_tls")}')
                fault_at = time.time()
                failed = [scenario._request(host, seconds=3) for host in bench.TARGETS[:3]]
                after = bench.chaos_stats()[port][0]
                check('three real user requests fail against TLS decoy', all(not r['is_ok'] for r in failed), failed)
                check('three independent transport connections reached local decoy', after-before >= 3, after-before)
                if enabled:
                    scenario._poll(lambda: scenario._active(client) == 'b1', 10)
                else:
                    scenario._hold(1)
                count = scenario._count(client, 'server_unverified')
                check('server_unverified evidence gate', count >= 3 if enabled else count == 0, count)
                check('strong evidence selects reserve' if enabled else 'disabled detector keeps prior choice',
                      scenario._active(client) == ('b1' if enabled else 'a1'), scenario._active(client))
                if enabled:
                    request = scenario._request()
                    check('reserve carries real successful user request', request['is_ok'], request)
                    hits = scenario._logged(stand, 't1.test', '/bytes/65536', fault_at)
                    check('independent local HTTP target received reserve request', bool(hits), hits)
                row['final_status'] = client.status()
                log = Path(client.log_path).read_text()
                invalid = [line for line in log.splitlines() if 'REALITY: processed invalid connection' in line]
                x509_errors = [line for line in log.splitlines() if 'x509:' in line]
                check('production verifier generated exact invalid-connection error', len(invalid) >= 3, invalid)
                check('trusted decoy did not fail x509 verification', not x509_errors, x509_errors)
                path = scenario._path(client)
                check('unrelated evidence classes remain empty', all(v == 0 for k,v in path.get('failures_by_kind', {}).items() if k != 'server_unverified'), path.get('failures_by_kind'))
            check('direct fallback carries no user bytes', client.direct_bytes == (0,0), client.direct_bytes)
        except Exception as exc:
            check('real scenario completed', False, repr(exc))
        row['behavior_verified'] = ['server_unverified'] if enabled and checks and all(c['ok'] for c in checks) else []
        row['coverage'] = 'verified' if checks and all(c['ok'] for c in checks) else 'failed'
        (out/(name+'-checks.json')).write_text(json.dumps(row, indent=2))
        rows.append(row)
        print(name, sum(c['ok'] for c in checks), '/', len(checks), flush=True)

summary = {'cases':len(rows), 'checks':sum(len(r['checks']) for r in rows),
           'failures':[(r['case'],c) for r in rows for c in r['checks'] if not c['ok']],
           'checks_passed':all(c['ok'] for r in rows for c in r['checks']),
           'behavior_verified': sorted({f for r in rows for f in r['behavior_verified']}),
           'test_only_boundary':'main startup overlay registers exactly one scratch CA; no production verifier/observer/controller edit or threshold override',
           'production_binary_sha256':sha(args.production_binary), 'test_binary_sha256':sha(args.test_binary),
           'ca_sha256':sha(certs/'ca.crt'), 'leaf_sha256':sha(certs/'example.crt'),
           'port_offset':bench.OFFSET}
(out/'matrix.json').write_text(json.dumps(rows, indent=2))
(out/'summary.json').write_text(json.dumps(summary, indent=2))
print(json.dumps(summary, indent=2), flush=True)
sys.exit(0 if summary['checks_passed'] else 1)
