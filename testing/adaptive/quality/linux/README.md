# Linux quality runners

The runners live in `testing/adaptive/quality/linux/`. Run the commands below from the repository root against the recorded candidate and frozen baseline in an agreed shared bench slot. Source installation does not establish acceptance; preserve actual inputs, hashes and results for every run. Experiments use a dedicated `--network none` namespace. Do not place source/binary copies in the storage repository. The scratch package is not a runnable substitute for the repository tree.

Files:

- `Dockerfile`: pinned Alpine OCI index, explicit native Linux arm64, Python/OpenSSL/iproute2/procps/strace/ethtool.
- `tc_netem.py`: `preflight`, `setup`, `profile`, `dump`, `cleanup`; private namespace guard, CAP_NET_ADMIN, IFB creation, 21002 filters, per-segment offload check, raw commands and dumps. Preflight creates IFB/netem/flower but deliberately does not claim a traffic smoke result.
- `cpu_clock.py`: public libc `clock_getcpuclockid(clientPID)` + `clock_gettime` process CPU clock, capturing `clock_getres`. This clock counts the whole client process across its threads. It has no hardcoded clock ID and no silent fallback when unavailable. Separate future preflight starts a controlled foreign-PID CPU workload whose worker threads exit before the second snapshot.
- `throughput_runner.py`: default acceptance 1 GiB, both `throughput` and `throughput_spliced`; each transport has an upfront fixed schedule of 10 blocks of 4 attempts, five POPO then five OPOP. Uses the public process CPU clock as primary and retains `/proc/PID/stat` raw utime/stime as parallel legacy evidence. Retains every raw attempt, cfg/log/events, target/provider logs, wall/monotonic timestamps, CPU nanoseconds, clock resolution, legacy ticks, winner IDs and unchanged <=2% gates. A block winner is the faster legacy-wall attempt in each arm, and CPU comes from that same attempt. All 10 blocks are reported; a red block is retained, never replaced by an adaptive extra run.
- `xray_trace_wrapper.sh`, `splice_proof.py`: separate 16 MiB syscall proof. Only the client run command is traced; server remains untraced. Positive socket→pipe splice bytes are required at client→21002. Wrapper PID is strace, so proof CPU is explicitly invalid. Total splice bytes from both legs are not counted as application bytes.
- `duplex_target.py`, `quality_experiments.py`: standalone stage-4 helpers with a separate duplex target; no production code is changed by these helpers.
- `schema_privacy.py`: exact Stage3 status/report paths, camelCase provider quality, per-provider salted network/run hashes and report/provider identity privacy. No recursive fallback to unrelated quality objects. A provider capture is a separate diagnostic; kernel experiments configure no provider and explicitly do not claim provider verification.

`poll-parity` and `poll-off` are explicitly diagnostic modes in separate series. Acceptance polling is unchanged. Poll-parity matches the plain/observed launch cadence of event API subprocesses; the plain HealthService response can reject the call, so its service-side workload is not identical. Neither diagnostic can replace an acceptance result. `splice-proof` is also separate from acceptance.

Set `QBASE` below to the manager's existing frozen baseline source tree, which already contains bench certs. The frozen source and certs mount read-only; output paths mount writable. Any missing cert fails the read-only mount instead of editing the baseline tree. Baseline binary must be compiled from this frozen source, not from the changing worktree. The current session's baseline label is `8b3d839f`; use its verified full ID in `--source-commit` when available. Candidate comparisons must use the final stage-1-rebased tree and record its own commit/hash. The baseline tree and binaries are external inputs, not contents of this source package.

## Concrete command checklist after GO

Run from zsh; commands are separate phases and must not run alongside a native/peer full stand or load. Variables are task-specific, and Docker context is explicit rather than changed globally.

```sh
QREPO="$PWD"
QSTAGE="$QREPO/testing/adaptive/quality/linux"
QBASE=/absolute/path/to/existing-frozen-baseline-source
QBIN="$QREPO/work/quality-lab/binaries"
QRESULTS="$QREPO/work/quality-lab/results"
mkdir -p "$QBIN" "$QRESULTS/preflight" "$QRESULTS/acceptance" "$QRESULTS/splice-proof/traces" "$QRESULTS/splice-proof/runs"
```

Check disk before adding an image; do not independently prune caches. Pull only native arm64, then build the native image and capture metadata:

```sh
docker --context colima-bang-deploy pull --platform linux/arm64 alpine@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6
docker --context colima-bang-deploy build --platform linux/arm64 -t xray-quality-linux:20261011 "$QSTAGE"
docker --context colima-bang-deploy image inspect --platform linux/arm64 xray-quality-linux:20261011 > "$QRESULTS/image-inspect.json"
docker --context colima-bang-deploy info --format '{{json .}}' > "$QRESULTS/docker-info.json"
```

The cached Alpine/Node images were amd64-only; their top-level OCI index metadata had empty architecture. Explicit `--platform linux/arm64`, nonempty native image inspect, ELF `aarch64`, and runtime `platform.machine()==aarch64` are required. Do not use a mixed-architecture fallback for CPU comparison.

Build frozen base after the compile slot is free:

```sh
cd "$QBASE"
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags=-checklinkname=0 -o "$QBIN/xray-base-linux-arm64" ./main
file "$QBIN/xray-base-linux-arm64" > "$QRESULTS/binary-file.txt"
shasum -a 256 "$QBIN/xray-base-linux-arm64" > "$QRESULTS/binary-sha256.txt"
go version > "$QRESULTS/go-version.txt"
```

Short module/capability preflight only; namespace exit restores MTU/offloads and all link state:

```sh
docker --context colima-bang-deploy run --rm \
  --name xray-quality-preflight-20261011 \
  --platform linux/arm64 --network none --cap-add NET_ADMIN \
  --mount type=bind,src="$QSTAGE",dst=/runner,readonly \
  --mount type=bind,src="$QRESULTS/preflight",dst=/results \
  -e XRAY_QUALITY_ISOLATED=1 \
  xray-quality-linux:20261011 \
  python3 /runner/tc_netem.py preflight --port 21002 --output /results
```

Failure to create IFB/netem/flower is a blocker for these experiments, not a reason to add `--privileged`, load VM modules, or change VM-global sysctl. Read preserved `commands.jsonl` and dumps. Ordinary acceptance can still run unshaped if appropriate.

Separate CPU precision preflight after execution GO, in a fresh namespace with a controlled child process; no bench starts. It captures nanosecond CPU samples/resolution and parallel raw `/proc` stats before and after two exited worker threads:

```sh
mkdir -p "$QRESULTS/cpu-preflight"
docker --context colima-bang-deploy run --rm \
  --name xray-quality-cpu-preflight-20261011 \
  --platform linux/arm64 --network none --cap-add NET_ADMIN \
  --mount type=bind,src="$QSTAGE",dst=/runner,readonly \
  --mount type=bind,src="$QRESULTS/cpu-preflight",dst=/results \
  -e XRAY_QUALITY_ISOLATED=1 \
  xray-quality-linux:20261011 \
  python3 /runner/cpu_clock.py preflight --output /results
```

For a live Xray client PID in the same container, `python3 /runner/cpu_clock.py sample --pid <CLIENT_PID>` captures the process CPU clock and its resolution without summing live threads. Acceptance takes two such snapshots while the client is open, retaining `/proc` stats in parallel. Missing API or invalid CPU sample fails the attempt; it never substitutes jiffies for the2% CPU gate. The public foreign-PID API is documented with an example in [Linux clock_getcpuclockid](https://www.man7.org/linux/man-pages/man3/clock_getcpuclockid.3.html).

Full baseline acceptance: default 80 x 1 GiB downloads across both transports; fixed upfront schedule is written to manifest before traffic. This is an exclusive load slot. Standard `throughput` has a tcp+tls outbound and uses `bench.TLS` for optional TLS to the final target, default0. `throughput_spliced` always forces TLS1.3 to the target independently, exactly as the original scenario. Match the native baseline input manifest's BENCH_TLS value; the command below uses standard default0. Set1 only when the native comparison also did so.

```sh
docker --context colima-bang-deploy run --rm \
  --name xray-quality-throughput-20261011 \
  --platform linux/arm64 --network none --cap-add NET_ADMIN \
  --mount type=bind,src="$QBASE",dst=/source,readonly \
  --mount type=bind,src="$QSTAGE",dst=/runner,readonly \
  --mount type=bind,src="$QBIN",dst=/binaries,readonly \
  --mount type=bind,src="$QRESULTS/acceptance",dst=/results \
  -e XRAY_QUALITY_ISOLATED=1 -e BENCH_TLS=0 -e TMPDIR=/results \
  xray-quality-linux:20261011 \
  python3 /runner/throughput_runner.py \
  --bench-dir /source/testing/adaptive \
  --xray /binaries/xray-base-linux-arm64 \
  --server-xray /binaries/xray-base-linux-arm64 \
  --source-commit 8b3d839f --output /results --transport both --mode acceptance
```

Read printed unique output directory. `manifest.json` contains fixed schedule, measurement-helper and input hashes, `attempts.json` all raw attempts, `blocks.json` each winner pair and acceptance checks, `summary.json` count/completion/red blocks. Per-attempt directories preserve config/log/events and status after the measured interval. The report does not infer a single aggregate green result from a subset of blocks. Wall discontinuity beyond30 ms is separately marked invalid. Keep libc clock resolution and parallel `/proc` tick resolution visible; primary CPU gates use the high-resolution process clock, without rounding to whole seconds or ticks.

Diagnostic polling series use a fresh container and output root, the exact same command with `--mode poll-parity` or `--mode poll-off`. Do not delete/overwrite acceptance output. These modes still run the fixed10blocks by default.

Stage4 quality price uses a **separate** comparison, both arms with health enabled:

```
--comparison quality-off-on
P = quality_off, passive.shouldSkipQuality=true
O = quality_on,  passive.shouldSkipQuality=false
```

Both arms use the same binary, health configuration, API services, polling, log level, outbound and `healthObservatory.report.quietShare=1`; only `passive.shouldSkipQuality` differs. The runner verifies this by comparing normalized configs after deleting that one field and retains the common hash with each raw attempt. Stage0 remains default `--comparison plain-observed` and retains legacy P/O attempt IDs. The Stage4 IDs explicitly name quality_off/quality_on and use a new unique result directory. Both comparisons retain the fixed10x4 schedule and the original2% wall/CPU gates with same-wall-winner CPU. Post-measurement status/report checks verify that the switch actually removes/produces quality observations. Poll-parity/off remain separate diagnostic `--mode` choices and never replace acceptance.

After the final Stage1-rebased candidate with Stage3 exports is built, execute quality price in its own exclusive slot:

```sh
mkdir -p "$QRESULTS/quality-on-off"
QCANDIDATECOMMIT="$(git -C "$QREPO" rev-parse HEAD)"
docker --context colima-bang-deploy run --rm \
  --name xray-quality-on-off-20261011 \
  --platform linux/arm64 --network none --cap-add NET_ADMIN \
  --mount type=bind,src="$QBASE",dst=/source,readonly \
  --mount type=bind,src="$QSTAGE",dst=/runner,readonly \
  --mount type=bind,src="$QBIN",dst=/binaries,readonly \
  --mount type=bind,src="$QRESULTS/quality-on-off",dst=/results \
  -e XRAY_QUALITY_ISOLATED=1 -e BENCH_TLS=0 -e TMPDIR=/results \
  xray-quality-linux:20261011 \
  python3 /runner/throughput_runner.py \
  --bench-dir /source/testing/adaptive \
  --xray /binaries/xray-candidate-linux-arm64 \
  --server-xray /binaries/xray-base-linux-arm64 \
  --source-commit "$QCANDIDATECOMMIT" --output /results \
  --transport both --mode acceptance --comparison quality-off-on
```

Separate syscall splice proof (four16MiB attempts; no CPU acceptance):

```sh
docker --context colima-bang-deploy run --rm \
  --name xray-quality-spliceproof-20261011 \
  --platform linux/arm64 --network none --cap-add NET_ADMIN \
  --mount type=bind,src="$QBASE",dst=/source,readonly \
  --mount type=bind,src="$QSTAGE",dst=/runner,readonly \
  --mount type=bind,src="$QBIN",dst=/binaries,readonly \
  --mount type=bind,src="$QRESULTS/splice-proof",dst=/results \
  -e XRAY_QUALITY_ISOLATED=1 -e BENCH_TLS=1 -e TMPDIR=/results \
  -e XRAY_TRACE_BINARY=/binaries/xray-base-linux-arm64 \
  -e XRAY_TRACE_DIRECTORY=/results/traces \
  xray-quality-linux:20261011 \
  python3 /runner/throughput_runner.py \
  --bench-dir /source/testing/adaptive \
  --xray /runner/xray_trace_wrapper.sh \
  --actual-xray /binaries/xray-base-linux-arm64 \
  --server-xray /binaries/xray-base-linux-arm64 \
  --source-commit 8b3d839f --output /results/runs --transport throughput_spliced \
  --mode splice-proof --size-mib 16
```

For the printed unique proof run ID, evaluate evidence (no network/bench operation):

```sh
python3 "$QSTAGE/splice_proof.py" \
  --run-dir "$QRESULTS/splice-proof/runs/<printed-run-id>" \
  --trace-dir "$QRESULTS/splice-proof/traces" --port 21002
```

The parser checks source FD annotations `127.0.0.1:ephemeral->127.0.0.1:21002` in successful `splice`, including unfinished/resumed strace lines. It preserves exact evidence. `-s4096` retains config argv for attempt matching; `--kill-on-exit` closes the traced client when the wrapper process is stopped. EAGAIN, EOF/zero, debug log alone, and server syscalls never prove client splice. If ptrace is blocked, retry only the isolated proof container with CAP_SYS_PTRACE after reading the exact failure; never replace throughput acceptance by a traced result.

For stage4, after the final candidate binary is available in `$QBIN/xray-candidate-linux-arm64`, use a fresh namespace. The candidate implements quality; the server remains frozen base. Use the candidate commit supplied by the manager in the helper manifest. The helper has a separate plain HTTP target on18996, explicitly sends response/S1 before upload, and never enables inner TLS:

```sh
QQUALITYRESULT="$QRESULTS/stage4-$(date -u +%Y%m%dT%H%M%SZ)"
QCANDIDATECOMMIT="$(git -C "$QREPO" rev-parse HEAD)"
mkdir -p "$QQUALITYRESULT"
docker --context colima-bang-deploy run --rm \
  --name xray-quality-stage4-20261011 \
  --platform linux/arm64 --network none --cap-add NET_ADMIN \
  --mount type=bind,src="$QBASE",dst=/source,readonly \
  --mount type=bind,src="$QSTAGE",dst=/runner,readonly \
  --mount type=bind,src="$QBIN",dst=/binaries,readonly \
  --mount type=bind,src="$QQUALITYRESULT",dst=/results \
  -e XRAY_QUALITY_ISOLATED=1 -e BENCH_TLS=0 -e BENCH_PORT_OFFSET=0 \
  -e BENCH_SERVER_BIN=/binaries/xray-base-linux-arm64 -e TMPDIR=/results \
  xray-quality-linux:20261011 \
  python3 /runner/quality_experiments.py \
  --bench-dir /source/testing/adaptive \
  --xray /binaries/xray-candidate-linux-arm64 \
  --server-xray /binaries/xray-base-linux-arm64 \
  --source-commit "$QCANDIDATECOMMIT" \
  --output /results --scenario all --timeout 240 --short-count 12
```

`tc_netem.py` profile commands inside that same quality-only namespace are:

```sh
python3 /runner/tc_netem.py setup --port 21002 --output /results/tc
python3 /runner/tc_netem.py profile --profile clean --port 21002 --output /results/tc
python3 /runner/tc_netem.py profile --profile rtt --port 21002 --output /results/tc
python3 /runner/tc_netem.py profile --profile baseline --port 21002 --output /results/tc
python3 /runner/tc_netem.py profile --profile loss --port 21002 --output /results/tc
python3 /runner/tc_netem.py profile --profile queue --port 21002 --output /results/tc
python3 /runner/tc_netem.py dump --port 21002 --output /results/tc
python3 /runner/tc_netem.py cleanup --port 21002 --output /results/tc
```

`clean`: zero delay; `rtt`:50 ms each direction; loss control `baseline`:10 ms each direction; `loss`:same10 ms +2% random per direction; `queue`:uplink10 ms, downlink TBF5 Mbit, burst16 KiB, FIFO limit512 KiB. Queue64MiB transfer timeout is >=180s. Only IPv4 TCP src/dst21002 is redirected at lo ingress to its respective IFB; upstream20002, local target, API and direct witness stay outside these filters. Same-port probes are also shaped; their observations must stay outside quality aggregates.

Capture raw client `ss -tinp '( dport = :21002 )'`, client PID/config association, two filter counters, both qdisc dumps, actual transferred bytes, statuses/reports/provider body. Missing raw SS counter fields are unknown, not zero. GET alone does not ensure client Bytes_retrans; duplex upload happens after first response/S1 and before late download/S2. Do not claim tc loss rate equals the reported retransmit/out-of-order fraction.

Kernel cases set singular `healthObservatory.report.quietShare=1`, the canonical JSON key in infra/conf/health.go. They name a synthetic `cell:25001:quality-synthetic-<UUID>` network before traffic to validate metadata and export privacy. Status shape is `paths[].outbound` with snake_case `quality`; report shape is `quality[].paths[tag]`, with network metadata and at most4 segments/paths. Local status's legacy `network` may contain the host ID; report quality and provider bodies must not expose the ID/fingerprint. No provider apply/refresh is inserted into the kernel cases.

Once a **separate provider diagnostic** has captured a POST after traffic, validate its artifacts without running traffic:

```sh
python3 "$QSTAGE/schema_privacy.py" \
  --status <captured-status.json> --report <captured-report.json> --tag a1 \
  --raw-network-id <exact-synthetic-host-id> \
  --provider-bodies <captured-provider.log.bodies> \
  --provider-tag <exact-provider-display-tag-of-measured-path> \
  --provider-url http://127.0.0.1:19050/set \
  --output <schema-privacy-result.json>
```

Provider shape is `outbounds[].tag` with camelCase quality plus root `networkType`, optional `networkOperator`, optional `witnessQuality`. Existing provider hash is exactly `sha256(providerURL + NUL + complete raw ID).hex[:16]`, and `run` uses the same function over report `health_run`. Use the exact configured URL, including any bench port offset. The helper validates the POST body inside each capture wrapper; local wrapper Authorization metadata is not an outbound JSON field. Capture rows before synthetic network/traffic must be separated from the post-traffic diagnostic. Without such a capture provider verification remains explicitly pending.

Addressed abort/cleanup only for our own container, for example:

```sh
docker --context colima-bang-deploy stop -t 10 xray-quality-throughput-20261011
docker --context colima-bang-deploy stop -t 10 xray-quality-spliceproof-20261011
```

Do not stop either unless that specific container is ours. `--rm` destroys namespace and its qdisc/interfaces; writable bind outputs survive. Never use global kill/prune/network cleanup.

Splice proof now requires all planned attempts and exact runtime config argv receipts from its unique run, not repeated basenames; host/container mount path differences are handled by stored runtime paths. Exported RTT min and p50 bucket deltas are required in addition to raw ss and dial checks. Provider validation requires the explicit measured display tag and a meaningful quality object; empty/flag-only/zero-count objects or another tag cannot satisfy acceptance. Prefix validation follows the core strict parser.

## Synthetic regression guards after test GO

From the final repository root, run the preserved evidence-validator regressions only in the manager's agreed test window:

```sh
python3 testing/adaptive/quality/linux/test_lab_guards.py
```

These fixtures exercise rejection of missing exported RTT metrics, provider quality from another tag, empty provider measurements and traces from a previous run with repeated basenames. Positive synthetic controls remain alongside the rejection cases. They are invented parser inputs and cannot establish real kernel, traffic, splice, provider or CPU acceptance. Optional `--schema-source`, `--rtt-source` and `--proof-source` overrides retain support for checking a selected source or RED regression variant. Keep the failing result and its source hashes when demonstrating a regression; do not substitute it for acceptance evidence.

The Linux runners use the explicitly supplied `--bench-dir` and their documented `/source`, `/binaries`, `/results` mounts. Sibling helper imports remain local to this directory. `quality_experiments.py` retains `/runner/tc_netem.py` as its container default and allows an explicit `--tc-helper`; the commands above mount this final helper directory at `/runner`. The traced client runtime config paths stored in manifests/attempts remain the receipt used by `splice_proof.py`, including when evidence is evaluated on the host.
