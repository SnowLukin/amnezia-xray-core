"""Source/evidence utilities; importing this module starts no bench or network."""
import copy
import hashlib
import json

FLAG_PATH = ("healthObservatory", "passive", "shouldSkipQuality")
RUNTIME_ENV_KEYS = ("PATH", "TZ", "LANG", "LC_ALL", "PYTHONHASHSEED", "PYTHONPATH", "PYTHONHOME",
                    "VIRTUAL_ENV", "GODEBUG", "TMPDIR", "GOWORK", "GOFLAGS", "GOOS", "GOARCH",
                    "CGO_ENABLED", "GOROOT", "GOPATH", "GOMODCACHE", "GOCACHE", "GOTMPDIR",
                    "XRAY_LOCATION_ASSET", "XRAY_LOCATION_CONFIG", "LD_LIBRARY_PATH",
                    "DYLD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH")


def archive_paths(receipt_id, has_health):
    """One canonical path for each role of one numbered Client occurrence."""
    return {"ledger_file": f"receipts/{receipt_id}.json",
            "original_config": f"receipts/{receipt_id}-original.json",
            "final_config": f"receipts/{receipt_id}-final.json",
            "client_log": f"receipts/{receipt_id}-client.log",
            "events_file": f"receipts/{receipt_id}-events.json" if has_health else None}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def value_sha256(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False,
                               allow_nan=False) + "\n", encoding="utf-8")


def without_flag(config):
    """Remove exactly the leaf flag; retain every other key, including passive={}."""
    result = copy.deepcopy(config)
    health = result.get("healthObservatory")
    if isinstance(health, dict) and isinstance(health.get("passive"), dict):
        health["passive"].pop("shouldSkipQuality", None)
    return result


def leaf_changes(before, after, path=()):
    """Expand a newly added parent into its leaves, without ignoring empty objects."""
    if isinstance(before, dict) and isinstance(after, dict):
        result = []
        for key in sorted(before.keys() | after.keys()):
            if key not in before:
                result.extend(added_leaves(after[key], path + (key,)))
            elif key not in after:
                result.extend(added_leaves(before[key], path + (key,)))
            else:
                result.extend(leaf_changes(before[key], after[key], path + (key,)))
        return result
    return [] if before == after else [path]


def added_leaves(value, path):
    if isinstance(value, dict) and value:
        return [leaf for key in sorted(value) for leaf in added_leaves(value[key], path + (key,))]
    return [path]


def quality_config(original, mode):
    result = copy.deepcopy(original)
    health = result.get("healthObservatory")
    if health is None and "healthObservatory" not in result:
        return result, False, []  # Preserve intentional plain/burst controls.
    if not isinstance(health, dict):
        raise ValueError("healthObservatory must be an object")
    passive = health.setdefault("passive", {})
    if not isinstance(passive, dict):
        raise ValueError("healthObservatory.passive must be an object")
    passive["shouldSkipQuality"] = mode == "off"
    changes = leaf_changes(original, result)
    if any(path != FLAG_PATH for path in changes):
        raise ValueError(f"non-flag config mutation: {changes}")
    return result, True, changes
