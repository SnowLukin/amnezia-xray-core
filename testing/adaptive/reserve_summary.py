"""Folds results of reserve_ab.py and reserve_herd.py into tables.

    python3 reserve_summary.py <results.jsonl>... [--herd <herd dir>]
"""
import collections
import glob
import json
import os
import statistics
import sys

CAP = 90


def fmt(values):
    """Median and worst of recoveries; None is no recovery within CAP s."""
    got = sorted(v for v in values if v is not None)
    lost = sum(1 for v in values if v is None)
    if not got:
        return f"> {CAP} (все {lost})"
    text = f"{statistics.median(got):.1f} / {got[-1]:.1f}"
    if lost:
        text += f", без связи > {CAP} с: {lost} из {len(values)}"
    return text


def main():
    args = sys.argv[1:]
    herd = None
    if "--herd" in args:
        i = args.index("--herd")
        herd = args[i + 1]
        args = args[:i] + args[i + 2:]
    rows = collections.defaultdict(list)
    extra = collections.defaultdict(list)
    for path in args:
        for line in open(path):
            r = json.loads(line)
            if r.get("error"):
                print("error", r["spec"], r["error"])
                continue
            key = (r["scheme"], r["backend"], r["road"], r["fault"], r["delay"], r.get("net", ""))
            rows[key].append(r["recovery"])
            extra[key].append((r["s1DownAfter"], r["firstBackendRequestAfter"], r["backendRequests"], r["provenBy"]))
    print("| Схема | Бэк | Дорога | Отказ | Задержка повода | Сеть | Без связи, с: медиана / худший | Отказ замечен, с | Запросов к бэку |")
    print("|---|---|---|---|---|---|---|---|---|")
    for key in sorted(rows):
        down = [d for d, _, _, _ in extra[key] if d is not None]
        reqs = [n for _, _, n, _ in extra[key]]
        print(f"| {' | '.join(key)} | {fmt(rows[key])} | {statistics.median(down):.1f} | "
              f"{min(reqs)}-{max(reqs)} |")
    if herd:
        print()
        print("| Всплеск | Запросов | Первый, с | Последний, с | Секунд с 80% запросов | Пик в секунду | Без связи, с: медиана / худший |")
        print("|---|---|---|---|---|---|---|")
        for path in sorted(glob.glob(os.path.join(herd, "herd_*.json"))):
            h = json.load(open(path))
            times = sorted(x["at"] for x in h["requests"] if x["at"] >= 0)
            if not times:
                continue
            per = collections.Counter(int(t) for t in times)
            k = max(1, int(len(times) * 0.8))
            span = min(times[i + k - 1] - times[i] for i in range(len(times) - k + 1))
            print(f"| {h['spec']} | {len(times)} | {times[0]:.1f} | {times[-1]:.1f} | {span:.1f} | "
                  f"{max(per.values())} | {fmt(h['recoveries'])} |")


if __name__ == "__main__":
    main()
