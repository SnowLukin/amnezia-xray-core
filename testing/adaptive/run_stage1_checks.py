"""Отдельный runner A-D и probe-steps; сеть включается только с явным --go.

Acceptance требует отдельный binary/manifest базы 8b3d839f и отчёты полного
старого стенда BEFORE/AFTER. --smoke не выдаёт acceptance PASS.
"""
import argparse
import contextlib
import hashlib
import json
import os
import re
import time
import traceback

import bench
import scenarios
import cost_scenarios
import return_interval_scenarios
import path_scenarios
import phase_timeout_scenarios
import stage1_helpers as h
import stage1_check_scenarios
import stage1_probe_step_scenarios


PHASE = "stage1_steps_slow_network_before_after"
LEGACY = "stage1_steps_legacy_full_regression"
RUN_WORK = None


def sha(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@contextlib.contextmanager
def environment(key, value):
    previous = os.environ.get(key)
    os.environ[key] = value
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = previous


def phase_pair(stand, xray, work, name):
    checks = []
    for label, binary in (("before", h.BASELINE), ("after", xray)):
        stand.reset()
        with environment("BENCH_PHASE_EXPECT", label):
            rows = phase_timeout_scenarios.phase_network_800ms_2pct(
                stand, binary, work, name + "-" + label)
        checks += [{**row, "check": label.upper() + ": " + row["check"]} for row in rows]
        checks += [{**row, "check": label.upper() + ": " + row["check"]}
                   for row in bench.pop_report_checks()]
    return checks


LEGACY_TIMINGS = {"BENCH_IDLE_MIN": 10, "BENCH_FLAP_MIN": 10, "BENCH_HEALTHY_MIN": 30,
                  "BENCH_EXPENSIVE_MIN": 60, "BENCH_SOAK_MIN": 120, "BENCH_DEAD_SITE_SEC": 300,
                  "BENCH_AWG_WAKES": 6, "BENCH_THROUGHPUT_MB": 1024}
LEGACY_HARNESS_FILES = ("run.py", "bench.py", "chaos.py", "target.py", "provider.py", "scenarios.py",
                 "cost_scenarios.py", "return_interval_scenarios.py", "path_scenarios.py")


def legacy_reference_checks(reference, actual_harness, phase_exists):
    harness, catalog = reference.get("harness"), reference.get("catalog")
    valid_catalog = isinstance(catalog, list) and len(catalog) == 85 and len(set(catalog)) == 85
    valid_catalog = valid_catalog and catalog == sorted(catalog) and all(isinstance(name, str) and not name.startswith("phase_") for name in catalog)
    return [h.check("Reference исходного 8b", reference.get("source_commit"),
                    isinstance(reference.get("source_commit"), str) and len(reference["source_commit"]) == 40
                    and reference["source_commit"].startswith("8b3d839f"), "полный commit 8b3d839f"),
            h.check("Original harness содержит ровно девять файлов", harness,
                    isinstance(harness, dict) and set(harness) == set(LEGACY_HARNESS_FILES)
                    and all(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) for value in harness.values()),
                    list(LEGACY_HARNESS_FILES)),
            h.check("Frozen original файлы сохранили reference SHA", actual_harness,
                    isinstance(harness, dict) and actual_harness == harness, harness),
            h.check("В original harness нет нового P1-файла", phase_exists,
                    phase_exists is False, "phase_timeout_scenarios.py отсутствует"),
            h.check("Reference содержит обычный каталог 85", catalog,
                    valid_catalog and h.strict_count(reference.get("scenario_count"), 85), "sorted 85 unique ordinary"),
            h.check("Reference defaults и список очищенного environment", reference.get("defaults"),
                    isinstance(reference.get("defaults"), dict) and len(reference["defaults"]) == 8
                    and isinstance(reference.get("cleared_environment"), list)
                    and len(reference["cleared_environment"]) == 19
                    and len(set(reference["cleared_environment"])) == 19, "8 defaults, 19 unique cleared vars")]


def load_legacy_reference(path):
    with open(path) as stream:
        reference = json.load(stream)
    root = reference.get("harness_root")
    if not isinstance(root, str) or not os.path.isabs(root):
        raise ValueError("Legacy reference требует абсолютный harness_root исходного 8b")
    actual = {name: sha(os.path.join(root, name)) for name in LEGACY_HARNESS_FILES
              if os.path.isfile(os.path.join(root, name))}
    checks = legacy_reference_checks(reference, actual, os.path.exists(os.path.join(root, "phase_timeout_scenarios.py")))
    failures = [row["check"] for row in checks if row["ok"] is not True]
    if failures:
        raise ValueError("Неверный legacy reference: " + "; ".join(failures))
    return reference, sha(path)


def legacy_proof_checks(data, meta, expected, report_sha, terminal):
    if not isinstance(data, list) or not isinstance(meta, dict):
        return [h.check("Legacy JSON и sidecar существуют", None, False, "list/object")]
    rows = []
    binary = meta.get("binary", {})
    rows.append(h.check("Legacy binary и исходники соответствуют роли", binary,
        binary.get("sha256") == expected["binary_sha256"] and binary.get("commit") == expected["commit"]
        and meta.get("source_manifest_sha256") == expected["source_manifest_sha256"], expected))
    rows.append(h.check("Sidecar связан с точным SHA отчёта", meta.get("report_sha256"),
                        meta.get("report_sha256") == report_sha, report_sha))
    rows.append(h.check("Оба legacy прогона используют явный original reference",
        meta.get("legacy_reference_sha256"), meta.get("legacy_reference_sha256") == expected["legacy_reference_sha256"]
        and meta.get("harness_source_commit") == expected["harness_source_commit"]
        and meta.get("defaults") == expected["defaults"]
        and meta.get("cleared_environment") == expected["cleared_environment"],
        {key: expected[key] for key in ("legacy_reference_sha256", "harness_source_commit", "defaults", "cleared_environment")}))
    rows.append(h.check("Хеши использованного harness совпадают", meta.get("harness"),
                        meta.get("harness") == expected["harness"], expected["harness"]))
    catalog = expected["catalog"]
    names = [entry.get("scenario") for entry in data]
    rows.append(h.check("Ровно полный обычный каталог, без повторов", names,
                        sorted(names) == catalog and meta.get("catalog") == catalog, catalog))
    env, timings, command = meta.get("environment"), meta.get("timings"), meta.get("command")
    valid_env = isinstance(env, dict) and env.get("BENCH_SLOW") in (None, "") and timings == LEGACY_TIMINGS
    if valid_env:
        valid_env = all(key not in env for key in expected["cleared_environment"])
        for key, value in LEGACY_TIMINGS.items():
            if key in env:
                try:
                    valid_env = valid_env and float(env[key]) == value
                except (TypeError, ValueError):
                    valid_env = False
        valid_env = valid_env and all(key not in env for key in ("BENCH_EXPENSIVE_RETURN", "BENCH_AWG_DELAY", "BENCH_SOAK_PLAIN", "PYTHONPATH"))
    rows.append(h.check("Default timings обычного прогона", {"environment": env, "timings": timings},
                        valid_env, LEGACY_TIMINGS))
    valid_command = isinstance(command, list) and len(command) == 3 + len(catalog)
    if valid_command:
        valid_command = (os.path.abspath(command[1]) == os.path.join(expected["harness_root"], "run.py")
                         and command[2] == binary.get("path")
                         and sorted(command[3:]) == catalog and len(set(command[3:])) == len(catalog))
    rows.append(h.check("Команда вызвала этот binary и весь обычный каталог", command, valid_command,
                        "python run.py <binary> <85 ordinary scenes>"))
    checks = [row for entry in data for row in entry.get("checks", [])]
    empty = [entry.get("scenario") for entry in data if not entry.get("checks")]
    failures = [(entry.get("scenario"), row.get("check"), row.get("value"), row.get("limit"))
                for entry in data for row in entry.get("checks", []) if row.get("ok") is not True]
    checks_count, failed = len(checks), len(failures)
    receipt = meta.get("terminal", {})
    marks = re.findall(r"^(PASS|FAIL)\s+([^\s]+)\s+", terminal, re.MULTILINE)
    summary = re.findall(r"^(\d+)/(\d+) checks passed, report (.+)$", terminal, re.MULTILINE)
    valid_terminal = len(marks) == checks_count and sum(mark == "FAIL" for mark, _ in marks) == failed
    valid_terminal = valid_terminal and set(name for _, name in marks) == set(catalog)
    valid_terminal = valid_terminal and len(summary) == 1 and int(summary[0][0]) == checks_count - failed
    valid_terminal = valid_terminal and int(summary[0][1]) == checks_count
    if summary:
        valid_terminal = valid_terminal and os.path.realpath(summary[0][2]) == os.path.realpath(meta.get("report_path", ""))
    valid_terminal = valid_terminal and h.strict_count(receipt.get("checks_total"), checks_count)
    valid_terminal = valid_terminal and h.strict_count(receipt.get("checks_failed"), failed)
    valid_terminal = valid_terminal and h.strict_count(receipt.get("scenario_count"), len(catalog))
    valid_terminal = valid_terminal and h.strict_count(receipt.get("exit_code"), 1 if failed else 0)
    rows.append(h.check("Терминал, exit и точное число выполненных проверок", receipt,
                        valid_terminal and not empty and checks_count > 0, "terminal = report = sidecar"))
    rows.append(h.check("Каждая проверка содержит boolean ok", checks,
                        all(type(row.get("ok")) is bool for row in checks) and not empty, "bool, непустые checks"))
    rows.append(h.check("Все красные обычного стенда сохранены", failures, not failures, "0 FAIL; без whitelist"))
    return rows


def legacy_reports(before_path, after_path, before_meta, after_meta, expected_before, expected_after):
    rows = []
    for label, path, sidecar, expected in (("BEFORE", before_path, before_meta, expected_before),
                                          ("AFTER", after_path, after_meta, expected_after)):
        if not path or not sidecar:
            rows.append(h.check(label + ": отчёт с provenance sidecar", None, False, "report.json + sidecar"))
            continue
        with open(path) as stream:
            data = json.load(stream)
        with open(sidecar) as stream:
            meta = json.load(stream)
        terminal_info = meta.get("terminal", {})
        terminal_path = terminal_info.get("path")
        if not terminal_path or not os.path.isfile(terminal_path):
            rows.append(h.check(label + ": терминальный журнал существует", terminal_path, False, "raw terminal log"))
            continue
        with open(terminal_path) as stream:
            terminal = stream.read()
        proof = legacy_proof_checks(data, meta, expected, sha(path), terminal)
        command = meta.get("command") or []
        source_dir = os.path.dirname(command[1]) if len(command) >= 2 else ""
        actual_harness = {name: sha(os.path.join(source_dir, name)) for name in expected["harness"]
                          if os.path.isfile(os.path.join(source_dir, name))}
        proof.append(h.check("Файлы harness по пути исполнявшейся команды", actual_harness,
                             actual_harness == expected["harness"], expected["harness"]))
        binary_path = (meta.get("binary") or {}).get("path")
        proof.append(h.check("Исполнявшийся binary ещё соответствует receipt SHA", binary_path,
            bool(binary_path) and os.path.isfile(binary_path) and sha(binary_path) == expected["binary_sha256"],
            expected["binary_sha256"]))
        proof.append(h.check("Точный SHA терминального журнала", terminal_info.get("sha256"),
                             terminal_info.get("sha256") == sha(terminal_path), sha(terminal_path)))
        proof.append(h.check("Sidecar называет этот report path", meta.get("report_path"),
            os.path.abspath(meta.get("report_path", "")) == os.path.abspath(path), os.path.abspath(path)))
        rows += [{**row, "check": label + ": " + row["check"]} for row in proof]
    return rows


def source_manifest_data(manifest):
    path = manifest.get("source_manifest")
    expected = manifest.get("source_manifest_sha256")
    if not path or not isinstance(expected, str) or sha(path) != expected:
        raise ValueError("Нужен точный SHA frozen source manifest для binary")
    with open(path) as stream:
        source = json.load(stream)
    if not isinstance(source, dict):
        raise ValueError("Source manifest должен быть object")
    commit = source.get("commit", source.get("metadata", {}).get("git_head"))
    root = source.get("source_root", source.get("source"))
    files = source.get("files")
    if (not isinstance(commit, str) or not commit or commit != manifest.get("commit")
            or not isinstance(root, str) or not os.path.isabs(root)):
        raise ValueError("Source manifest не содержит commit/root этого binary")
    if isinstance(files, list):
        if not all(isinstance(row, dict) and set(row) >= {"path", "sha256"} for row in files):
            raise ValueError("Source files требуют path/sha256")
        pairs = [(row["path"], row["sha256"]) for row in files]
    elif isinstance(files, dict):
        pairs = list(files.items())
    else:
        raise ValueError("Source files должны быть list или object")
    declared = {}
    for relative, digest in pairs:
        if (not isinstance(relative, str) or not relative or os.path.isabs(relative)
                or relative == ".." or os.path.normpath(relative) != relative or relative.startswith("../")
                or relative in declared or not isinstance(digest, str)
                or not re.fullmatch(r"[0-9a-f]{64}", digest)):
            raise ValueError("Source path/SHA некорректен или повторяется")
        actual = os.path.realpath(os.path.join(root, relative))
        if os.path.commonpath((os.path.realpath(root), actual)) != os.path.realpath(root) or sha(actual) != digest:
            raise ValueError("Source file не соответствует frozen SHA: " + relative)
        declared[relative] = digest
    if not declared:
        raise ValueError("Source files пусты")
    return root, declared


def source_manifest(manifest):
    source_manifest_data(manifest)
    return manifest["source_manifest_sha256"]


REUSE_SCOPE = "historical-original85-routing-default-evidence"
ROOT_ADDRESS = "a_336357aa53394595a0d83776b8e78d4a"
EXPORT_DELTA = {
    "app/observatory/health/engine.go": ["\t\tr.reason = j.reason.String()"],
    "app/observatory/health/probe_result.go": ["\treason   string"],
    "app/observatory/health/probe_views.go": [
        "\tReason  string                \x60json:\x22reason,omitempty\x22\x60", "\t\tReason:  r.reason,"],
    "app/observatory/health/provider_request.go": [
        "\tReason   string              \x60json:\x22reason,omitempty\x22\x60", "\t\to.Probe.Reason = r.reason"],
}


def legacy_after_scope(args, current, current_sha, current_source, reference_sha):
    old_path, decision_path = args.legacy_after_manifest, args.legacy_reuse_disposition
    if bool(old_path) != bool(decision_path):
        raise ValueError("Legacy reuse требует обе парные опции")
    identity = {"binary_sha256": current_sha, "commit": current["commit"],
                "source_manifest_sha256": current_source}
    metadata = {"scope": "current-binary-measurement", **identity}
    if not old_path:
        return identity, metadata
    with open(old_path) as stream:
        old = json.load(stream)
    old_source = source_manifest(old)
    old_sha = old.get("sha256")
    if old_sha == current_sha or sha(old["binary_path"]) != old_sha:
        raise ValueError("Historical binary должен отличаться от current и совпадать с SHA")
    with open(decision_path) as stream:
        decision = json.load(stream)
    if (decision.get("schema") != "stage1-legacy-reuse-disposition/v1"
            or decision.get("root_address") != ROOT_ADDRESS or decision.get("scope") != REUSE_SCOPE
            or decision.get("current_measurement") is not False
            or decision.get("stage_acceptance") is not False
            or decision.get("raw_policy") != "preserve-all-failures-zero-failed-gate"
            or not isinstance(decision.get("rationale"), str) or not decision["rationale"].strip()):
        raise ValueError("Disposition должен явно ограничивать историческое evidence без acceptance")
    expected = {"current_binary_sha256": current_sha, "current_source_manifest_sha256": current_source,
                "original_binary_sha256": old_sha, "original_source_manifest_sha256": old_source,
                "legacy_reference_sha256": reference_sha}
    if any(decision.get(key) != value for key, value in expected.items()):
        raise ValueError("Disposition не соответствует current/original identity")
    paths = {"original_manifest": old_path, "original_report": args.legacy_after_report,
             "original_meta": args.legacy_after_meta}
    with open(args.legacy_after_meta) as stream:
        meta = json.load(stream)
    paths["original_terminal"] = meta.get("terminal", {}).get("path")
    pins = decision.get("pins", {})
    for key in ("reason_review", "source_delta_proof"):
        paths[key] = pins.get(key, {}).get("path")
    for key, path in paths.items():
        pin = pins.get(key, {})
        if not path or pin.get("path") != path or pin.get("sha256") != sha(path):
            raise ValueError("Disposition pin не совпадает: " + key)
    if (meta.get("binary", {}).get("sha256") != old_sha
            or meta.get("source_manifest_sha256") != old_source
            or meta.get("report_sha256") != pins["original_report"]["sha256"]):
        raise ValueError("Historical sidecar не соответствует original binary/source/report")
    with open(paths["source_delta_proof"]) as stream:
        proof = json.load(stream)
    if proof.get("identities") != expected or proof.get("added_lines") != EXPORT_DELTA:
        raise ValueError("Source delta proof не соответствует разрешённым шести строкам")
    old_root, old_files = source_manifest_data(old)
    current_root, current_files = source_manifest_data(current)
    production = lambda path: path.endswith(".go") and not path.endswith("_test.go") and not path.startswith("testing/")
    changed = {path for path in set(old_files) | set(current_files)
               if production(path) and old_files.get(path) != current_files.get(path)}
    if changed != set(EXPORT_DELTA):
        raise ValueError("Production delta содержит другие изменения")
    for path, additions in EXPORT_DELTA.items():
        with open(os.path.join(old_root, path)) as stream:
            before = stream.read()
        with open(os.path.join(current_root, path)) as stream:
            after = stream.read()
        lines = after.split("\n")
        for addition in additions:
            if lines.count(addition) != 1 or addition in before.split("\n"):
                raise ValueError("Export delta не является точной добавкой: " + path)
            lines.remove(addition)
        if "\n".join(lines) != before:
            raise ValueError("После удаления export delta исходники различаются: " + path)
    original = {"binary_sha256": old_sha, "commit": old["commit"], "source_manifest_sha256": old_source}
    return original, {"scope": REUSE_SCOPE, "original": original, "current": identity,
                      "current_measurement": False, "stage_acceptance": False,
                      "disposition": {"path": decision_path, "sha256": sha(decision_path), "body": decision}}


def arguments():
    parser = argparse.ArgumentParser(description="Стенд этапа 1 с отдельной baseline и реальными окнами")
    parser.add_argument("--xray")
    parser.add_argument("--baseline-bin")
    parser.add_argument("--baseline-manifest", help="JSON: commit=8b3d839f..., sha256=<binary SHA>")
    parser.add_argument("--after-manifest", help="JSON: commit, sha256, source_manifest, source_manifest_sha256")
    parser.add_argument("--legacy-harness-manifest", help="Явный reference ORIGINAL 8b: harness9, catalog85, defaults/environment")
    parser.add_argument("--legacy-after-manifest", help="Исторический AFTER manifest; только вместе с disposition")
    parser.add_argument("--legacy-reuse-disposition", help="ROOT JSON: ограниченный перенос original85 evidence")
    parser.add_argument("--legacy-before-meta")
    parser.add_argument("--legacy-after-meta")
    parser.add_argument("--legacy-before-report")
    parser.add_argument("--legacy-after-report")
    parser.add_argument("--workdir")
    parser.add_argument("--go", action="store_true", help="Явный GO владельца эксклюзивного стенда")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--smoke", action="store_true", help="Отдельный прогон, никогда acceptance PASS")
    parser.add_argument("--scenario", action="append", default=[])
    return parser.parse_args()


def main():
    global RUN_WORK
    args = arguments()
    names = [fn.__name__ for fn in h.ALL] + [PHASE, LEGACY]
    if args.list:
        print("\n".join(names))
        return 0
    if not args.go:
        raise SystemExit("Runtime запрещён без явного --go владельца стенда")
    if not args.xray or not args.baseline_bin or not args.baseline_manifest or not args.after_manifest:
        raise SystemExit("Нужны --xray, отдельный --baseline-bin, --baseline-manifest и --after-manifest")
    xray, baseline = os.path.abspath(args.xray), os.path.abspath(args.baseline_bin)
    with open(args.baseline_manifest) as stream:
        manifest = json.load(stream)
    before_sha, after_sha = sha(baseline), sha(xray)
    if os.path.samefile(xray, baseline) or before_sha == after_sha:
        raise SystemExit("BEFORE и AFTER не могут быть одним binary")
    if not str(manifest.get("commit", "")).startswith("8b3d839f") or manifest.get("sha256") != before_sha:
        raise SystemExit("Manifest базы 8b3d839f не соответствует SHA baseline binary")
    with open(args.after_manifest) as stream:
        after_manifest = json.load(stream)
    if after_manifest.get("sha256") != after_sha or not after_manifest.get("commit"):
        raise SystemExit("AFTER manifest не соответствует проверяемому binary")
    before_source, after_source = source_manifest(manifest), source_manifest(after_manifest)
    reference_path = args.legacy_harness_manifest or manifest.get("legacy_harness_manifest")
    if not reference_path:
        raise SystemExit("Нужен --legacy-harness-manifest ORIGINAL 8b либо ссылка в baseline manifest")
    reference, reference_sha = load_legacy_reference(reference_path)
    legacy_expected = {"harness": reference["harness"], "harness_root": reference["harness_root"],
                       "catalog": reference["catalog"], "defaults": reference["defaults"],
                       "cleared_environment": reference["cleared_environment"],
                       "harness_source_commit": reference["source_commit"], "legacy_reference_sha256": reference_sha}
    expected_before = {"binary_sha256": before_sha, "commit": manifest["commit"],
                       "source_manifest_sha256": before_source, **legacy_expected}
    legacy_identity, legacy_scope = legacy_after_scope(args, after_manifest, after_sha, after_source, reference_sha)
    expected_after = {**legacy_identity, **legacy_expected}
    h.BASELINE = baseline
    wanted = args.scenario or (['stage1_a_auto_burst', 'stage1_d_fresh',
                               'stage1_steps_reserve_without_endpoint'] if args.smoke else names)
    unknown = set(wanted) - set(names)
    if unknown:
        raise SystemExit("Неизвестные сценарии: " + ", ".join(sorted(unknown)))
    work = os.path.abspath(args.workdir) if args.workdir else bench.workdir()
    os.makedirs(work, exist_ok=True)
    RUN_WORK = work
    metadata = {"legacy_reference": {"path": os.path.abspath(reference_path), "sha256": reference_sha,
                                     "source_commit": reference["source_commit"], "harness_root": reference["harness_root"]},
                "client": {"path": xray, "sha256": after_sha, "manifest": after_manifest},
                "legacy_after_evidence": legacy_scope,
                "baseline": {"path": baseline, "sha256": before_sha, "manifest": manifest},
                "port_offset": bench.OFFSET, "mode": "smoke" if args.smoke else "acceptance",
                "expected_scenarios": names, "requested_scenarios": wanted,
                "strict_times": {"periodic_s": 90, "before_window_s": 600, "lone_min_interval_s": 300},
                "iphone_memory": "не измеряется локальным стендом; heap/RSS относятся к Xray на Mac"}
    h.save(work, "stage1-run-metadata", metadata)
    print("Артефакты: " + work, flush=True)
    result = []
    h.save(work, "stage1-matrix", {"metadata": metadata, "results": [], "acceptance": "RUNNING"})
    functions = {fn.__name__: fn for fn in h.ALL}
    with bench.Stand(xray, work) as stand:
        for name in wanted:
            stand.reset()
            started = time.time()
            try:
                if name == PHASE:
                    checks = phase_pair(stand, xray, work, name)
                elif name == LEGACY:
                    checks = legacy_reports(args.legacy_before_report, args.legacy_after_report,
                                            args.legacy_before_meta, args.legacy_after_meta,
                                            expected_before, expected_after)
                else:
                    checks = functions[name](stand, xray, work, name)
            except Exception as exc:
                traceback.print_exc()
                checks = [h.check("Сценарий завершился", repr(exc), False, "без исключений")]
            checks += bench.pop_report_checks()
            if not checks:
                checks = [h.check("Есть реальные проверки", [], False, "непустой список")]
            for row in checks:
                print(("CHECK_PASS" if row.get("ok") is True else "FAIL") + "  " + name + "  " + row["check"], flush=True)
            result.append({"scenario": name, "seconds": time.time() - started, "checks": checks})
            h.save(work, "stage1-matrix", {"metadata": metadata, "results": result,
                   "auto_always": h.METRICS, "acceptance": "RUNNING"})
    missing = sorted(set(names) - {r["scenario"] for r in result})
    failed = sum(row.get("ok") is not True for entry in result for row in entry["checks"])
    complete = not args.smoke and not missing and not failed
    verdict = "PASS" if complete else "FAIL" if failed else "INCOMPLETE_SMOKE" if args.smoke else "INCOMPLETE_SUBSET"
    matrix = {"metadata": metadata, "results": result, "auto_always": h.METRICS,
              "missing": missing, "failed_checks": failed, "acceptance": verdict}
    h.save(work, "stage1-matrix", matrix)
    with open(os.path.join(work, "stage1-auto-always.md"), "w") as stream:
        stream.write("| Сцена | Режим | Switch, с | Ложные down | Rescue | Switches | Ошибки запросов | Heap, байт | RSS, байт |\n")
        stream.write("| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
        for name in sorted(h.METRICS):
            m = h.METRICS[name]
            stream.write("| %s | %s | %s | %s | %s | %s | %s/%s | %s | %s |\n" % (
                m["scene"], m["mode"], m["switch_delay_s"], m["false_down"], m["rescue"], m["switches"],
                m["user_errors"], m["user_requests"], m["peak_heap_bytes"], m["peak_rss_bytes"]))
        stream.write("\nПамять измерена у локального Xray. Пик памяти iPhone требует отдельного устройства.\n")
    print(verdict + ": " + os.path.join(work, "stage1-matrix.json"), flush=True)
    return 0 if complete else 1 if failed else 2


if __name__ == "__main__":
    try:
        code = main()
    except Exception as exc:
        traceback.print_exc()
        if RUN_WORK:
            filename = os.path.join(RUN_WORK, "stage1-matrix.json")
            with open(filename) as stream:
                matrix = json.load(stream)
            matrix.update(acceptance="FAIL", infrastructure_error=repr(exc))
            h.save(RUN_WORK, "stage1-matrix", matrix)
        code = 1
    raise SystemExit(code)
