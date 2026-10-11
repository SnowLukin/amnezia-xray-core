#!/usr/bin/env python3
"""Delegate the unchanged ordinary adaptive run.py with quality on/off.

Execute only after explicit GO in an exclusive sequential stand slot. No build,
certificate generation or benchmark-source copy is performed by this wrapper.
Bench/scenario imports and subprocess/network operations happen only in main().
"""
import argparse
import copy
import importlib
import inspect
import json
import os
from pathlib import Path
import platform
import shutil
import sys
import threading
import time
import traceback

sys.dont_write_bytecode = True

from ordinary_common import (FLAG_PATH, RUNTIME_ENV_KEYS, archive_paths, file_sha256, leaf_changes, quality_config,
                             save, value_sha256, without_flag)

def stamp():
    return {"wall_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns()}


def bench_environment():
    # Relevant stand/runtime inputs only; do not dump unrelated credentials.
    result = {key: os.environ.get(key) for key in RUNTIME_ENV_KEYS}
    result.update({key: value for key, value in sorted(os.environ.items()) if key.startswith("BENCH_")})
    return result


class TerminalTee:
    def __init__(self, terminal, record, lock):
        self.terminal, self.record, self.lock = terminal, record, lock

    def write(self, text):
        with self.lock:
            self.record.write(text)
            self.record.flush()
            self.terminal.write(text)
            self.terminal.flush()
        return len(text)

    def flush(self):
        with self.lock:
            self.record.flush()
            self.terminal.flush()

    def isatty(self):
        return False

    def fileno(self):
        return self.terminal.fileno()


def preflight(args):
    if platform.system() not in ("Darwin", "Linux"):
        raise RuntimeError("ordinary wrapper supports local macOS/Linux clients")
    if os.environ.get("BENCH_CLIENT") or os.environ.get("BENCH_CLIENT_BIN"):
        raise RuntimeError("local clients only; BENCH_CLIENT/BENCH_CLIENT_BIN must be unset")
    for name in ("BENCH_HOST", "BENCH_LISTEN"):
        if os.environ.get(name, "127.0.0.1") != "127.0.0.1":
            raise RuntimeError(f"{name} must be 127.0.0.1")
    if os.environ.get("BENCH_CHAOS_SCRIPT"):
        raise RuntimeError("ordinary stand must retain its original chaos proxy")
    if os.environ.get("BENCH_SLOW") and not args.slow:
        raise RuntimeError("request --slow explicitly; inherited BENCH_SLOW is not implicit GO")
    if not 1000 <= args.port_offset <= 44000:
        raise RuntimeError("--port-offset must be in [1000, 44000]")
    args.bench_dir = args.bench_dir.resolve(strict=True)
    args.xray = args.xray.resolve(strict=True)
    args.output = args.output.resolve()
    if not args.bench_dir.is_dir() or not args.xray.is_file() or not os.access(args.xray, os.X_OK):
        raise RuntimeError("existing explicit bench directory and executable candidate required")
    if args.output.exists():
        raise RuntimeError("--output must be a new unique directory, not an existing directory")
    if args.output == args.bench_dir or args.bench_dir in args.output.parents:
        raise RuntimeError("evidence output must be outside the benchmark source tree")
    required = ("run.py", "bench.py", "scenarios.py", "cost_scenarios.py",
                "return_interval_scenarios.py", "path_scenarios.py", "target.py",
                "chaos.py", "provider.py", "certs/example.crt", "certs/example.key",
                "certs/target.crt", "certs/target.key")
    missing = [name for name in required if not (args.bench_dir / name).is_file()]
    if missing:
        raise RuntimeError(f"existing original stand/certificates required: {missing}")
    server = os.environ.get("BENCH_SERVER_BIN")
    if server and Path(server).resolve(strict=True) != args.xray:
        raise RuntimeError("this on/off comparison uses the same candidate for server and client")
    os.environ["BENCH_HOST"] = "127.0.0.1"
    os.environ["BENCH_LISTEN"] = "127.0.0.1"
    os.environ["BENCH_PORT_OFFSET"] = str(args.port_offset)
    os.environ["BENCH_SERVER_BIN"] = str(args.xray)
    if args.slow:
        os.environ["BENCH_SLOW"] = "1"
    sys.path.insert(0, str(args.bench_dir))
    for name in ("bench", "run", "scenarios", "cost_scenarios", "return_interval_scenarios", "path_scenarios"):
        if name in sys.modules:
            raise RuntimeError(f"unexpected module already imported: {name}")
    bench = importlib.import_module("bench")
    if Path(bench.__file__).resolve() != args.bench_dir / "bench.py" or bench.CLIENT or bench.HOST != "127.0.0.1":
        raise RuntimeError("explicit local bench import contract failed")
    expected_init = ("self", "xray", "config", "workdir", "name", "profile")
    if tuple(inspect.signature(bench.Client.__init__).parameters) != expected_init:
        raise RuntimeError("legacy Client.__init__ API changed; review before running")
    return bench


def install_receipts(bench, args, state):
    original_init = bench.Client.__init__
    original_exit = bench.Client.__exit__
    original_api = bench.Client.api
    original_memory = bench.Client.memory
    work = args.output / "stand"
    receipts = args.output / "receipts"
    receipts.mkdir()

    def workdir():
        work.mkdir(exist_ok=False)
        return str(work)

    def client_init(client, xray, config, workdir, name, profile=False):
        if Path(xray).resolve() != args.xray or bench.CLIENT:
            raise RuntimeError("client must use the same local candidate binary")
        scenario = state["active_scenario"]
        if scenario is None:
            raise RuntimeError("client constructed outside the original scenario scope")
        original = copy.deepcopy(config)
        final, has_health, changes = quality_config(original, args.mode)
        receipt_id = f"receipt-{len(state['clients']) + 1:06d}"
        row = {"id": receipt_id, "scenario": scenario, "client_name": name,
               "ordinal": len(state["clients"]) + 1, "mode": args.mode,
               "has_health": has_health, "profile": profile, "started": stamp(),
               **archive_paths(receipt_id, has_health),
               "normalized_final_sha256": value_sha256(without_flag(final)),
               "changed_leaf_paths": [list(path) for path in changes],
               "exited": False, "api_errors": [], "memory_errors": []}
        state["clients"].append(row)
        save(args.output / row["original_config"], original)
        save(args.output / row["final_config"], final)
        row["archived_original_config_sha256"] = file_sha256(args.output / row["original_config"])
        row["archived_final_config_sha256"] = file_sha256(args.output / row["final_config"])
        save(receipts / f"{receipt_id}.json", row)
        try:
            original_init(client, xray, final, workdir, name, profile=profile)
            written = Path(client.path).resolve()
            if work.resolve() not in written.parents:
                raise RuntimeError("legacy client config escaped the evidence work directory")
            disk_config = json.loads(written.read_text())
            if disk_config != final or config != original or any(
                    path != FLAG_PATH for path in leaf_changes(original, disk_config)):
                raise RuntimeError("written config differs beyond the single quality flag")
            log_path = Path(client.log_path).resolve()
            events_path = Path(client.log_path.replace(".log", "-events.json")).resolve()
            if work.resolve() not in log_path.parents or work.resolve() not in events_path.parents:
                raise RuntimeError("legacy client log/event path escaped the evidence work directory")
            row.update(config_runtime_path=str(written),
                       log_runtime_path=str(log_path), events_runtime_path=str(events_path),
                       runtime_final_config_sha256=file_sha256(written),
                       flag_only_mutation_verified=True)
            client._quality_ordinary_receipt = row
        except BaseException as error:
            row["init_error"] = repr(error)
            raise
        finally:
            save(receipts / f"{receipt_id}.json", row)

    def client_exit(client, *exc):
        row = client._quality_ordinary_receipt
        try:
            return original_exit(client, *exc)
        except BaseException as error:
            row["exit_error"] = repr(error)
            raise
        finally:
            row["finished"] = stamp()
            row["exited"] = True
            if row.get("exit_error"):
                state["evidence_errors"].append(f"{row['id']}: {row['exit_error']}")
            try:
                destination = args.output / row["client_log"]
                shutil.copy2(Path(row["log_runtime_path"]), destination)
                row["client_log_sha256"] = file_sha256(destination)
            except BaseException as error:
                row["log_capture_error"] = repr(error)
                state["evidence_errors"].append(f"{row['id']}: {error!r}")
            if row["has_health"]:
                source = Path(row["events_runtime_path"])
                try:
                    body = json.loads(source.read_text())
                    expected = {"dropped": client.dropped, "events": client.events, "reports": client.reports}
                    if body != expected:
                        raise RuntimeError("event file is stale or differs from this exact Client occurrence")
                    destination = args.output / row["events_file"]
                    shutil.copy2(source, destination)
                    row["events_sha256"] = file_sha256(destination)
                    row["event_count"] = len(body["events"])
                    row["dropped"] = body["dropped"]
                except BaseException as error:
                    row["event_capture_error"] = repr(error)
                    state["evidence_errors"].append(f"{row['id']}: {error!r}")
            save(receipts / f"{row['id']}.json", row)

    def api(client, *args, **kwargs):
        try:
            return original_api(client, *args, **kwargs)
        except Exception as error:
            client._quality_ordinary_receipt["api_errors"].append(
                {"args": list(args), "error": repr(error), **stamp()})
            raise

    def memory(client, *args, **kwargs):
        try:
            return original_memory(client, *args, **kwargs)
        except Exception as error:
            client._quality_ordinary_receipt["memory_errors"].append(
                {"error": repr(error), **stamp()})
            raise

    bench.workdir = workdir
    # Installed after the explicit bench import, before importing/calling run.main.
    bench.Client.__init__ = client_init
    bench.Client.__exit__ = client_exit
    # Transparent delegates preserve legacy polling cadence/exception behavior,
    # while recording exceptions the original _poll would otherwise swallow.
    bench.Client.api = api
    bench.Client.memory = memory


def scenario_receipts(run, state):
    for scenario in run.scenarios.ALL:
        original_run = scenario.run

        def scoped(*args, _name=scenario.name, _run=original_run, **kwargs):
            row = {"scenario": _name, "started": stamp()}
            first_client = len(state["clients"])
            state["scenarios"].append(row)
            state["active_scenario"] = _name
            try:
                checks = _run(*args, **kwargs)
                row["returned_checks"] = copy.deepcopy(checks)
                return checks
            except BaseException as error:
                row["error"] = repr(error)
                raise
            finally:
                row["finished"] = stamp()
                row["client_receipt_ids"] = [item["id"] for item in state["clients"][first_client:]]
                state["active_scenario"] = None

        scenario.run = scoped  # Delegate every original check/duration/scenario unchanged.


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bench-dir", type=Path, required=True)
    parser.add_argument("--xray", type=Path, required=True)
    parser.add_argument("--mode", choices=("on", "off"), required=True)
    parser.add_argument("--port-offset", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True, help="new unique exact output directory")
    parser.add_argument("--go", action="store_true", required=True, help="explicit execution GO acknowledgement")
    parser.add_argument("--slow", action="store_true", help="explicitly include legacy slow scenarios")
    parser.add_argument("--source-commit", default="", help="optional final candidate identity label")
    parser.add_argument("scenarios", nargs="*", help="original scenario names; empty retains original default set")
    args = parser.parse_args()
    before_environment = bench_environment()
    try:
        bench = preflight(args)
    except Exception as error:
        parser.error(str(error))
    args.output.mkdir(parents=True, exist_ok=False)
    state = {"active_scenario": None, "clients": [], "scenarios": [], "evidence_errors": []}
    manifest = {"kind": "ordinary unchanged run.py quality on/off wrapper", "mode": args.mode,
                "bench_dir": str(args.bench_dir), "xray": str(args.xray),
                "xray_sha256": file_sha256(args.xray), "source_commit_label": args.source_commit,
                "port_offset": args.port_offset, "requested_scenarios": args.scenarios,
                "slow_requested": args.slow, "environment_before": before_environment,
                "environment": bench_environment(), "argv": sys.argv, "created": stamp(),
                "python": sys.version, "platform": platform.platform(),
                "bench_source_sha256": {str(path.relative_to(args.bench_dir)): file_sha256(path)
                                        for path in sorted(args.bench_dir.rglob("*"))
                                        if path.is_file() and path.suffix in (".py", ".crt", ".key")},
                "helper_sha256": {path.name: file_sha256(path) for path in
                                  (Path(__file__).resolve(), Path(__file__).with_name("ordinary_common.py"))},
                "only_config_change": list(FLAG_PATH), "forces_quietShare": False,
                "timing_equality_claimed": False}
    manifest["environment_sha256"] = value_sha256(manifest["environment"])
    manifest["environment_before_sha256"] = value_sha256(manifest["environment_before"])
    save(args.output / "manifest.json", manifest)
    save(args.output / "client-receipts.json", [])
    exit_code = 1
    old_argv, old_stdout, old_stderr = sys.argv, sys.stdout, sys.stderr
    with (args.output / "terminal.log").open("w", encoding="utf-8", buffering=1) as terminal:
        lock = threading.Lock()
        sys.stdout, sys.stderr = TerminalTee(old_stdout, terminal, lock), TerminalTee(old_stderr, terminal, lock)
        try:
            install_receipts(bench, args, state)
            run = importlib.import_module("run")
            if Path(run.__file__).resolve() != args.bench_dir / "run.py" or run.bench is not bench:
                raise RuntimeError("run.py must import the explicit bench instance")
            known = {item.name for item in run.scenarios.ALL}
            if set(args.scenarios) - known:
                raise RuntimeError(f"unknown original scenarios: {sorted(set(args.scenarios) - known)}")
            manifest["planned_scenarios"] = [item.name for item in run.scenarios.ALL
                if (item.name in args.scenarios if args.scenarios else
                    not item.slow or bool(os.environ.get("BENCH_SLOW")))]
            if not manifest["planned_scenarios"]:
                raise RuntimeError("no original scenarios selected")
            save(args.output / "manifest.json", manifest)
            scenario_receipts(run, state)
            sys.argv = [str(args.bench_dir / "run.py"), str(args.xray), *args.scenarios]
            try:
                run.main()
                exit_code = 0
            except SystemExit as result:
                exit_code = result.code if isinstance(result.code, int) else 1
                if result.code is not None and not isinstance(result.code, int):
                    print(result.code, file=sys.stderr)
        except BaseException as error:
            state["evidence_errors"].append(repr(error))
            traceback.print_exc()
        finally:
            sys.argv = old_argv
            save(args.output / "client-receipts.json", state["clients"])
            save(args.output / "scenario-receipts.json", state["scenarios"])
            report = args.output / "stand/report.json"
            if not report.is_file():
                state["evidence_errors"].append("original report.json missing")
            for row in state["clients"]:
                if not row.get("flag_only_mutation_verified") or not row["exited"]:
                    state["evidence_errors"].append(f"incomplete client receipt: {row['id']}")
                for key in ("api_errors", "memory_errors"):
                    if row.get(key):
                        state["evidence_errors"].append(f"{row['id']}: {key}={row[key]!r}")
            summary = {"original_exit_code": exit_code, "original_report": "stand/report.json",
                       "evidence_errors": state["evidence_errors"], "finished": stamp(),
                       "wrapper_exit_code": exit_code if exit_code else (1 if state["evidence_errors"] else 0),
                       "note": "Original per-scenario checks/FAIL output remain gates. Event comparator is separate."}
            save(args.output / "summary.json", summary)
            sys.stdout, sys.stderr = old_stdout, old_stderr
    return exit_code if exit_code else (1 if state["evidence_errors"] else 0)


if __name__ == "__main__":
    raise SystemExit(main())
