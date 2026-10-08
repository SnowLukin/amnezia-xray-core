"""Prints the decision timeline of one bench run from its events file.

    python3 timeline.py <client-*-events.json> [since_unix_seconds]
"""
import json
import sys


def main():
    data = json.load(open(sys.argv[1]))
    events = data["events"]
    if not events:
        print("no events")
        return
    base = float(sys.argv[2]) if len(sys.argv) > 2 else events[0]["ts"] / 1000
    for e in events:
        f = e.get("fields") or {}
        brief = " ".join(f"{k}={v}" for k, v in f.items() if v not in ("", None, {}))
        print(f"{e['ts'] / 1000 - base:8.2f}  {e['type']:16s} {brief}")
    if data.get("dropped"):
        print(f"dropped {data['dropped']}")


if __name__ == "__main__":
    main()
