"""Prepare a process-local test CA and Go main overlay, without repo edits."""
import argparse
import json
import os
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('--repo', required=True)
parser.add_argument('--out', required=True)
args = parser.parse_args()
repo = Path(args.repo).resolve()
out = Path(args.out).resolve()
out.mkdir(parents=True, exist_ok=False)
certs = out/'certs'
certs.mkdir()
source = Path(__file__).resolve().parent
main = (repo/'main/main.go').read_text()
assert main.count('"flag"') == 1
main = main.replace('"flag"', '"flag"\n\t"crypto/x509"') + '\n' + (source/'fixture_init.go.txt').read_text()
(out/'main_overlay.go').write_text(main)
(out/'overlay.json').write_text(json.dumps({'Replace':{str(repo/'main/main.go'):str(out/'main_overlay.go')}}, indent=2))
(certs/'leaf.ext').write_text('basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:example.com\n')

def run(command, **kwargs):
    subprocess.run(command, check=True, **kwargs)

run(['openssl', 'req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-nodes',
     '-days', '3650', '-subj', '/CN=server-unverified-test-CA', '-addext', 'basicConstraints=critical,CA:TRUE',
     '-addext', 'keyUsage=critical,keyCertSign,cRLSign', '-keyout', str(certs/'ca.key'), '-out', str(certs/'ca.crt')])
run(['openssl', 'req', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-nodes', '-subj', '/CN=example.com',
     '-keyout', str(certs/'example.key'), '-out', str(certs/'example.csr')])
run(['openssl', 'x509', '-req', '-in', str(certs/'example.csr'), '-CA', str(certs/'ca.crt'), '-CAkey', str(certs/'ca.key'),
     '-CAcreateserial', '-days', '3650', '-extfile', str(certs/'leaf.ext'), '-out', str(certs/'example.crt')])
env = dict(os.environ, GOPROXY='off', GOWORK='off')
run(['go', 'build', '-buildvcs=false', '-overlay='+str(out/'overlay.json'), '-ldflags=-checklinkname=0',
     '-o', str(out/'xray-test-ca'), './main'], cwd=repo, env=env)
run(['go', 'build', '-o', str(out/'verify_fixture'), str(source/'verify_fixture.go')], env=env)
for enabled in ('0', '1'):
    with (out/('x509-'+('trusted' if enabled == '1' else 'untrusted')+'.json')).open('w') as output:
        run([str(out/'verify_fixture'), str(certs/'ca.crt'), str(certs/'example.crt')],
            env=dict(os.environ, GODEBUG='x509usefallbackroots='+enabled), stdout=output)
