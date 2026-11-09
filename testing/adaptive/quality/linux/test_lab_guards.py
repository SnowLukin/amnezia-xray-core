#!/usr/bin/env python3
"""Synthetic regression guards for evidence validators; no network or benchmark.

Prepare/read safely. Invoke only in the manager's agreed test window. Fixtures
are invented parser inputs, never proof of real splice, RTT, provider or CPU.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent
SCHEMA = None
RTT = None
PROOF = None


class LabGuards(unittest.TestCase):
    def body(self, tag='wanted', quality=None):
        identity = 'opaque-synthetic-private-id'
        url = 'http://127.0.0.1:18998/set'
        body = {'network':{'id':SCHEMA.provider_hash(url, identity)},
                'outbounds':[{'tag':tag,'quality': {'bytesDown':0,'dialCount':1,'dialP50Ms':10,'dialP90Ms':10} if quality is None else quality}]}
        return body,url,identity

    def provider_ok(self, body, url, identity):
        return all(x['ok'] for x in SCHEMA.provider_checks(body,url,identity,expected_tag='wanted'))

    def test_provider_rejects_quality_of_other_tag(self):
        self.assertFalse(self.provider_ok(*self.body(tag='other')), 'quality of another tag satisfied the expected path')

    def test_provider_rejects_empty_quality(self):
        self.assertFalse(self.provider_ok(*self.body(quality={})), 'empty quality satisfied a measured path')

    def test_provider_accepts_known_zero(self):
        self.assertTrue(self.provider_ok(*self.body()), 'known-zero bytes with successful dial became unknown')

    def test_network_ids_are_opaque(self):
        for raw in ['cell::fp','cell:１２３４５:fp','cell:1234:fp','cell:25001:','wifi:','WiFi:fp','cell:00101:fp']:
            with self.subTest(raw=raw):
                body,url,_ = self.body()
                expected = hashlib.sha256((url + '\0' + raw).encode()).hexdigest()[:16]
                self.assertEqual(SCHEMA.provider_hash(url, raw), expected)
                body['network']['id'] = expected
                self.assertTrue(self.provider_ok(body, url, raw), 'opaque ID was parsed or rejected')

    def test_provider_rejects_obsolete_network_metadata(self):
        for field,value in [('networkType','cell'),('networkOperator','00101')]:
            with self.subTest(field=field):
                body,url,identity = self.body()
                body[field] = value
                self.assertFalse(self.provider_ok(body, url, identity), 'obsolete quality metadata accepted')

    def test_provider_rejects_raw_opaque_id(self):
        body,url,identity = self.body()
        body['unexpectedIdentity'] = identity
        self.assertFalse(self.provider_ok(body, url, identity), 'raw opaque host ID escaped in provider body')

    def rtt_pair(self, exported=True):
        cases=[]
        for name,minimum,p50,dial in [('clean',1,10,20),('delayed',101,125,250)]:
            q={'dial_p50_ms':dial}
            if exported:
                q.update(tcp_rtt_min_ms=minimum,tcp_rtt_p50_ms=p50,tcp_count=1)
            cases.append({'case':name,'quality':q,'transfers':[{'initial_socket_snapshot':{'sockets':[{'minrtt':minimum}]}}],
                          'tc':{'drops':{'up':0,'down':0}}})
        return cases

    def test_rtt_rejects_missing_exported_metrics(self):
        self.assertFalse(all(x['ok'] for x in RTT.compare_rtt(*self.rtt_pair(False))),
                         'raw ss/dial accepted absent exported RTT metrics')

    def test_rtt_accepts_bucket_quantization(self):
        self.assertTrue(all(x['ok'] for x in RTT.compare_rtt(*self.rtt_pair(True))))

    def proof_case(self, traced_runtime):
        with tempfile.TemporaryDirectory(prefix='quality-synthetic-proof-') as folder:
            root=Path(folder);run=root/'host-mounted-run';trace=root/'traces';run.mkdir();trace.mkdir()
            plans=[{'id':f'attempt-{i}','label':'P' if i%2==0 else 'O'} for i in range(4)]
            runtime='/runtime/unique-current-run';binary='/binaries/synthetic-xray'
            manifest={'mode':'splice-proof','cpu_valid':False,'run_id':'unique-current-run',
                      'output_runtime_path':runtime,'actual_xray':binary,'fixed_schedule':plans}
            attempts=[]
            for i,plan in enumerate(plans):
                filename=f"client-{plan['id']}.json"
                config=f"{runtime}/{plan['id']}/{filename}"
                attempts.append({**plan,'complete':True,'transferred_bytes':16*(1<<20), 'client_config_runtime_path':config})
                argv_config=f"{traced_runtime}/{plan['id']}/{filename}"
                argv=json.dumps([binary,'run','-c',argv_config])
                content=f'100 execve({json.dumps(binary)}, {argv}, 0x0 /* 0 vars */) = 0\n'
                content+='100 splice(7<TCP:[127.0.0.1:12345->127.0.0.1:21002]>, NULL, 8<pipe:[1]>, NULL, 65536, SPLICE_F_MOVE) = 65536\n'
                (trace/f'client-splice-{i}.trace').write_text(content)
            (run/'manifest.json').write_text(json.dumps(manifest));(run/'attempts.json').write_text(json.dumps(attempts))
            result=subprocess.run([sys.executable,str(PROOF),'--run-dir',str(run),'--trace-dir',str(trace)],capture_output=True,text=True)
            summary=json.loads((run/'splice-proof.json').read_text())
            return result.returncode,summary

    def test_splice_rejects_previous_run_same_basenames(self):
        status,summary=self.proof_case('/runtime/previous-run')
        self.assertEqual(status,1,'trace from another run satisfied current splice proof')
        self.assertFalse(summary['accepted'])

    def test_splice_accepts_current_runtime_paths_on_host(self):
        status,summary=self.proof_case('/runtime/unique-current-run')
        self.assertEqual(status,0)
        self.assertTrue(summary['accepted'])


def module(path,name):
    spec=importlib.util.spec_from_file_location(name,path)
    value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value)
    return value


def load_defaults():
    global SCHEMA,RTT,PROOF
    sys.path.insert(0,str(ROOT))
    SCHEMA=module(ROOT/'schema_privacy.py','quality_guard_schema')
    RTT=module(ROOT/'quality_experiments.py','quality_guard_rtt')
    PROOF=ROOT/'splice_proof.py'


def setUpModule():
    if SCHEMA is None:
        load_defaults()


def main():
    global SCHEMA,RTT,PROOF
    root=ROOT
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--schema-source',type=Path,default=root/'schema_privacy.py')
    parser.add_argument('--rtt-source',type=Path,default=root/'quality_experiments.py')
    parser.add_argument('--proof-source',type=Path,default=root/'splice_proof.py')
    args,rest=parser.parse_known_args()
    print(json.dumps({'kind':'synthetic acceptance guard inputs; no real kernel/benchmark result',
                      'source_sha256':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [args.schema_source,args.rtt_source,args.proof_source]}},indent=2),flush=True)
    sys.path.insert(0,str(root))
    SCHEMA=module(args.schema_source,'quality_guard_schema');RTT=module(args.rtt_source,'quality_guard_rtt');PROOF=args.proof_source
    unittest.main(argv=[sys.argv[0],*rest])


if __name__ == '__main__':
    main()
