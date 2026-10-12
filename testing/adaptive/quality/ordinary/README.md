# Ordinary legacy stand with final-candidate quality on/off

These helper sources are prepared for `testing/adaptive/quality/ordinary/`. Preparation used file edits and static reading only; no imports, syntax checks, tests, builds or stand runs establish acceptance yet. This wrapper preserves the old full stand's `run.py`, scenario functions, durations, thresholds, polling, report checks and original PASS/FAIL terminal output. It delegates them directly. The root CPU queue currently covers only short Stage2/Stage3 checks; these files do not queue or authorize a full ordinary stand.

The explicit `--bench-dir` may point to an existing frozen benchmark tree outside the final repository. It must contain original `run.py`, `bench.py`, their scenario/helper imports and existing certificates. The candidate must already be built; the same `--xray` binary is used for both arms, server and local clients. No historical source or binary is copied into storage Git by these helpers. Evidence goes to new unique directories outside the benchmark source tree.

The wrapper imports the explicit bench, then installs a local `Client.__init__` delegate before importing/calling the original `run.main()`. For every final scenario config with `healthObservatory`, it deep-copies the config and sets exactly `healthObservatory.passive.shouldSkipQuality`: `false` for `on`, `true` for `off`. Intentional plain/burst configs without the observatory stay unchanged. It does not force `quietShare` or change log/API/health settings. An absent `passive` object is created only as the parent of this flag; leaf mutation checks permit only the flag. Normalized final-config SHA-256 removes only that leaf and retains all other keys, including an empty `passive` object. Original and written final configs are retained separately.

## Future execution after separate full-stand GO

Run from the final repository root. Use the same benchmark source, candidate, environment, scenario selection and port offset sequentially for both arms. Obtain a separate exclusive full-stand slot first; `--go` is the caller's acknowledgement of that GO, not a scheduler or permission request. The following commands are future examples and have not been run:

```sh
QORDINARY="$PWD/testing/adaptive/quality/ordinary"
QBENCH=/absolute/path/to/existing-frozen-benchmark/testing/adaptive
QCANDIDATE=/absolute/path/to/already-built-final-candidate
QEVIDENCE=/absolute/path/to/evidence-parent
QPAIR=unique-pair-label

python3 "$QORDINARY/ordinary_runner.py" \
  --bench-dir "$QBENCH" --xray "$QCANDIDATE" \
  --mode off --port-offset 24000 --output "$QEVIDENCE/$QPAIR-off" \
  --source-commit final-candidate-commit --go
```

Wait for the off arm's terminal result and cleanup before starting on. Preserve all FAIL output and evidence even if the first arm fails; a later comparator cannot turn a failed original check into acceptance.

```sh
python3 "$QORDINARY/ordinary_runner.py" \
  --bench-dir "$QBENCH" --xray "$QCANDIDATE" \
  --mode on --port-offset 24000 --output "$QEVIDENCE/$QPAIR-on" \
  --source-commit final-candidate-commit --go
```

With no scenario arguments, the original default scenario set is used and original slow cases are skipped. Append exact original scenario names for a selected series; explicitly naming a slow case retains `run.py`'s original semantics. `--slow` explicitly sets the original `BENCH_SLOW=1` option for the full set. An inherited nonempty `BENCH_SLOW` requires that explicit flag. No durations or thresholds are shortened by this wrapper; a selected series is not a full-stand result.

Preflight requires local macOS/Linux clients, no remote `BENCH_CLIENT`/`BENCH_CLIENT_BIN`, local `BENCH_HOST`/`BENCH_LISTEN`, no rate-proxy override, existing executables/certificates, a valid explicit bench tree and a new output directory. It rejects a different `BENCH_SERVER_BIN`. Existing legacy environment knobs are retained and recorded. The effective environment has a canonical SHA-256 and includes explicit present/absent values for `PATH`, Python selectors, `GOWORK`, `GOFLAGS`, `GOOS`, `GOARCH`, `CGO_ENABLED`, Go paths, `XRAY_LOCATION_ASSET`/config and library-path selectors, plus the stand's `BENCH_*` knobs. Unrelated environment credentials are not dumped. Use an identical environment for both arms; the comparator recomputes the recorded digest and fails differing effective environment/source/binary inputs instead of normalizing them away.

## Evidence and comparison

Each run keeps `manifest.json` with candidate, benchmark/certificate/helper hashes and effective environment; `terminal.log` with complete original Python PASS/FAIL output and tracebacks; the original `stand/report.json`; all original stand outputs; scenario/client receipts; and unique archived original/final configs, client logs and raw event files for every client occurrence. The original throughput cases reuse names and overwrite ordinary paths, so occurrence archives are captured after each original `Client.__exit__`. They bind the active original scenario, client name and ordinal. Logs for server/target/provider remain in the original stand directory. Transparent API/memory delegates preserve behavior and record polling exceptions the old stand otherwise swallows; these errors block evidence acceptance.

The separate comparator reads existing evidence only. It starts no stand and creates a new comparison JSON outside both input directories:

```sh
python3 "$QORDINARY/compare_events.py" \
  --off "$QEVIDENCE/$QPAIR-off" --on "$QEVIDENCE/$QPAIR-on" \
  --output "$QEVIDENCE/$QPAIR-comparison.json"
```

It requires identical scenario order/sets and complete per-occurrence artifact/file sets, no dropped events, contiguous sequence numbers from 1, valid raw timestamps/schema, exact source/binary/environment inputs, single-flag config mutation and every original per-scenario acceptance check passing. Each archive role has its canonical occurrence path and exactly one reference; aliased or renamed archives fail before set comparison. All config/log/ledger/event relative file sets are compared across arms. Each scenario's ordered client receipt IDs must exactly match that scenario's client rows, and these bindings also match across arms. Missing files, missing/empty expected health event streams, wrapper/API/capture errors, malformed data, partial receipts or no observed semantic health data fail. Empty `reports` arrays are allowed for valid quiet/start-only sessions. Intentional no-health or config-validation scenarios retain original config/check gates and are explicitly identified as having no live health-behavior comparison coverage.

The comparator checks the combined ordered semantic stream without removing start switches, sorting transitions or renaming tags:

| Event | Exact semantic fields |
| --- | --- |
| `switch` | `from`, `to`, `reason`, `balancer` |
| `path_state` | `outbound` tag, `state`, `reason`, quarantine level and provisional-field presence/value |
| `group_state` | `balancer`, `group` tag, `state`, exact reason-field absence/value, `members`, `failed` |
| `exhausted` | `balancer`, `from`, `candidates` |
| `pinned_down` | `balancer`, `outbound`, exact start-pin-field presence/value |
| `return_failed` | `balancer`, `outbound`, exact reason-field absence/value, `successes` |

Path states include `down` and `suspect`; quarantine is represented by down events and discrete quarantine levels, not an invented separate state. Legacy group events have no reason key, which is preserved as absent. All raw events, their original timestamps, all reports and raw pairing/elapsed timing differences remain in evidence. Expected failed-probe/provider `fields.error` values are legitimate scenario observations; they are retained rather than treated as wrapper failures.

Wall timing is nondeterministic between real stand runs. `held_ms`, `quarantine_ms`, raw timestamps and elapsed differences are reported without a new timing tolerance or timing-equality PASS. Original scenario timeranges remain the only stand timing acceptance gates. Tags and random choices are not normalized; any selected semantic mismatch fails and remains visible even when original checks passed. Frozen fake-clock core invariance tests cover exact timestamp equality separately. This comparator cannot establish equality of unselected observational fields, exact real-wall timestamps or unobserved no-health behavior.

The original `provider_saved_set` and `provider_slots_flow` scenarios build a second client's `initialAnswer` from runtime provider data, including lease/time inputs. These values may differ between real arms. Exact original/normalized final config comparison is retained: a difference is a failure with a non-comparable-input note, even if selected decision semantics match. No timestamp, tag or randomness normalization makes it green. Preparing these helpers does not promise green outcomes for the complete old stand.

## Planned short synthetic guards

Only in an agreed short test window, after test GO:

```sh
python3 testing/adaptive/quality/ordinary/test_ordinary_guards.py -v
```

These invented inputs check single-flag mutation/quietShare preservation, dropped/gapped/empty/type-invalid evidence, absent group reason, quarantine level, nonfinite/duplicate JSON, retained timing differences and semantic mismatch rejection. Complete invented on/off arm files additionally plan a valid load/comparison control and rejection of occurrence aliases with the redundant file removed, renamed non-event files, differing complete cross-arm file sets, redistributed/reordered scenario bindings, cross-arm binding mismatch, missing/tampered environment digests, missing `PATH` and differing valid effective-environment digests. They have not been executed during source preparation and cannot establish real traffic or quality invariance. Root's separate Stage4 RED/GREEN orchestrator and its fixture provenance are reviewed/executed independently; they are not copied into this standalone ordinary helper folder.
