"""Translate historical test recipes at the stand's JSON boundary.

This is test tooling, not a core compatibility adapter. Invalid and unknown
properties survive translation and are still rejected by the real v2 loader.
Retired timing knobs are removed; scenarios run against the core constants.
"""
import copy
from presence_recipe import config as presence_config, answer as presence_answer


def on(value=True):
    return {"isEnabled": bool(value)}


def nonzero(value):
    return value not in ("0", "0s", "0ms", "0ns", 0, None)


def period(value):
    result = on(nonzero(value))
    if nonzero(value):
        result["interval"] = value
    return result


def selection(recipe):
    result = copy.deepcopy(recipe)
    if "isLazy" in result:
        result["probePolicy"] = "onDemand" if result.pop("isLazy") else "regular"
    if "returnInterval" in result:
        result["returnToCheaper"] = period(result.pop("returnInterval"))
    return result


def provider_answer(recipe):
    if not isinstance(recipe, dict):
        return recipe
    result = copy.deepcopy(recipe)
    for outbound in result.get("outbounds", []):
        if isinstance(outbound, dict) and "path" in outbound:
            outbound["selection"] = selection(outbound.pop("path"))
    if isinstance(result.get("groups"), list):
        result["groups"] = [selection(g) for g in result["groups"]]
    return presence_answer(result)


def provider(recipe):
    result = copy.deepcopy(recipe)
    result["isEnabled"] = result.get("isEnabled", True)
    old = result.pop("triggers", {})
    fresh = {}
    fresh["exhausted"] = on(old.pop("shouldRefreshWhenExhausted", True))
    fresh["periodic"] = period(old.pop("interval", "2h"))
    threshold = old.pop("minHealthyOutbounds", 2)
    fresh["minHealthy"] = on(bool(threshold))
    if threshold: fresh["minHealthy"]["outbounds"] = threshold
    fresh["maxAge"] = on(nonzero(old.get("maxAge", "6h")))
    age = old.pop("maxAge", "6h")
    if nonzero(age): fresh["maxAge"]["age"] = age
    mode = old.pop("networkChangeMode", "unknownNotCheapest")
    fresh["networkChange"] = on(mode != "never")
    if mode != "never": fresh["networkChange"]["mode"] = mode
    if "networkSettleDelay" in old: fresh["networkChange"]["settleDelay"] = old.pop("networkSettleDelay")
    cost = old.pop("whenCostAbove", None)
    fresh["whenCostAbove"] = on(cost is not None)
    if cost: fresh["whenCostAbove"].update(cost)
    expiry = old.pop("expiry", {})
    fresh["leaseRenewal"] = on(expiry.get("renewAtFraction", .5) != 0)
    for key in ("renewAtFraction", "urgentAtFraction"):
        if key in expiry and fresh["leaseRenewal"]["isEnabled"]:
            fresh["leaseRenewal"][key] = expiry[key]
    fresh["backendSchedule"] = on()
    old.pop("jitterFraction", None)
    fresh.update(old)
    result["triggers"] = fresh
    limits = result.setdefault("limits", {})
    if "exhaustedMinInterval" in limits:
        limits["urgentMinInterval"] = limits.pop("exhaustedMinInterval")
    for key in ("backoff", "authRetryDelay"): limits.pop(key, None)
    result.pop("maxBytes", None)
    apply = result.setdefault("apply", {})
    apply.pop("isRoutingAccepted", None)
    apply["expiredLeaseFallback"] = on()
    if isinstance(result.get("initialAnswer"), dict): result["initialAnswer"] = provider_answer(result["initialAnswer"])
    return presence_config({"providers": [result]})["providers"][0]


def strategy(recipe):
    result = copy.deepcopy(recipe)
    failover = {
        "shouldProbeBeforeSwitch": result.pop("shouldProbeBeforeSwitch", True),
        "confirmWithProbe": result.pop("confirmWithProbe", "auto"),
        "downAudit": on(not result.pop("shouldSkipDownAudit", False)),
    }
    if failover["confirmWithProbe"] != "auto": failover["downAudit"] = on(False)
    result["failover"] = failover
    groups_present = "groups" in result
    groups = result.pop("groups", [])
    result["groups"] = {"isEnabled": groups_present, "items": [selection(g) for g in groups]}
    old_return = result.pop("returnToCheaper", {})
    returns = on(True)
    returns["interval"] = old_return.pop("interval", "30m")
    returns["minHoldTime"] = result.pop("minHoldTime", "2m")
    if not nonzero(returns["interval"]): returns = on(False)
    returns["onNetworkChange"] = on(old_return.pop("shouldTryOnNetworkChange", True))
    if returns["onNetworkChange"]["isEnabled"]:
        returns["onNetworkChange"]["scope"] = "unknown" if old_return.pop("isUnknownNetworksOnly", True) else "all"
    else: old_return.pop("isUnknownNetworksOnly", None)
    for key in ("hasRandomPhase", "probesToReturn", "probeInterval", "penaltyWindow", "penaltyMultiplier", "maxPenaltyDelay"):
        old_return.pop(key, None)
    returns.update(old_return)
    action = result.pop("networkChangeAction", "recheck")
    result["networkChanges"] = on(action != "ignore")
    if action != "ignore": result["networkChanges"]["action"] = action
    if not returns["isEnabled"] or action == "ignore": returns["onNetworkChange"] = on(False)
    result["returnToCheaper"] = returns
    startup = {"outbound": result.pop("startOutbound", "")}
    pin = result.pop("startPinnedOutbound", "")
    startup["pin"] = on(bool(pin))
    if pin: startup["pin"].update(outbound=pin, timeout=result.pop("startPinTimeout", "0s"))
    else: result.pop("startPinTimeout", None)
    result["startup"] = startup
    return result


def config(recipe):
    result = copy.deepcopy(recipe)
    if "healthObservatory" not in result: return result
    old = result["healthObservatory"]
    if not old.pop("_standRecipeV1", False): return presence_config(result)
    for outbound in result.get("outbounds", []):
        if "path" in outbound: outbound["selection"] = selection(outbound.pop("path"))
    p = old.pop("passive", {}) or {}
    dial_only = p.pop("isDialOnly", False)
    evidence = {
        "dialFailures": on(), "dialTimeout": {**on(), "timeout": p.pop("dialTimeout", "6s")}, "serverUnverified": on(),
        "firstReply": {**on(not dial_only), "timeout": p.pop("firstByteTimeout", "5s")}, "earlyClose": on(not dial_only),
        "stalledRequests": {**on(not dial_only), "timeout": p.pop("stallTimeout", "8s")},
        "partialStalls": {**on(not dial_only and not p.pop("shouldIgnorePartialStalls", False)), "maxAge": p.pop("partialStallMaxAge", "30s")},
    }
    udp_timeout = p.pop("udpReplyTimeout", "5s")
    evidence["udpReplies"] = on(nonzero(udp_timeout))
    if nonzero(udp_timeout): evidence["udpReplies"]["timeout"] = udp_timeout
    lone = p.pop("loneFailureAfter", "2m")
    evidence["loneFailureProbe"] = on(nonzero(lone))
    if nonzero(lone): evidence["loneFailureProbe"]["after"] = lone
    pending = not p.pop("shouldKeepPendingOnDown", False)
    silent = not p.pop("shouldKeepSilentOnDown", False)
    quality = not p.pop("shouldSkipQuality", False)
    for key in ("window", "failuresToSuspect", "minDistinctTargets", "successBytes", "sweepInterval", "stallMinWriteBytes", "partialStallMinBytes"):
        p.pop(key, None)
    evidence.update(p)
    profiles = []
    for profile in old.pop("probes", []):
        profile = copy.deepcopy(profile)
        name = profile.pop("name", "default")
        steps = profile.pop("steps", None)
        if steps is None:
            destination = profile.pop("destination", None)
            steps = [{"name": name, "destinations": [destination], **profile}]
        profiles.append({"name": name, "steps": steps})
    schedule = old.pop("schedule", {}) or {}
    hosts = schedule.pop("hostProbes", {}) or {}
    hosts.pop("reuseWindow", None)
    probes = {
        "profiles": profiles,
        "stepProfiles": on(any(len(p["steps"]) > 1 or any(s.get("isOptional") for s in p["steps"]) for p in profiles)),
        "requests": {**on(), **hosts},
        "candidates": {**on(), **schedule.pop("probeOnly", {})},
    }
    limits = {}
    for a,b in [("maxConcurrentProbes","maxConcurrent"),("probesPerMinute","perMinute"),("probesPerServerNamePerMinute","perServerNamePerMinute")]:
        if a in schedule: limits[b] = schedule.pop(a)
    if "probesPerMinute" in probes["requests"]: limits["hostPerMinute"] = probes["requests"].pop("probesPerMinute")
    probes["limits"] = limits
    for key in ("idleAfter", "quarantine", "minProbeInterval", "probeJitterFraction", "suspectCooldown"): schedule.pop(key, None)
    probes.update(schedule)
    udp = old.pop("udpProbe", None)
    probes["udp"] = {**on(udp is not None), **(udp or {})}
    network = old.pop("network", {}) or {}
    memory = {**on(network.pop("shouldRememberNetworks", True))}
    if "maxNetworks" in network: memory["maxNetworks"] = network.pop("maxNetworks")
    connectivity = old.pop("connectivity", None)
    witnesses = (connectivity or {}).pop("witnesses", [])
    if connectivity:
        for key in ("recheckBackoff", "maxPause"): connectivity.pop(key, None)
    network.update(memory=memory, connectivity={**on(connectivity is not None), **(connectivity or {})}, witnesses={**on(bool(witnesses)), "outbounds": witnesses})
    wake = old.pop("wake", {}) or {}
    host = {"pinning": on(), "userPresent": {**on(), **wake}, "memoryPressure": on()}
    sleep = old.pop("sleep", {}) or {}
    after = sleep.pop("afterLastUse", "5m"); close_idle = sleep.pop("closeIdleAfter", "0s")
    resources = {"tunnelSleep": on(nonzero(after)), "closeUnchosenIdle": on(nonzero(close_idle))}
    if nonzero(after): resources["tunnelSleep"]["afterLastUse"] = after
    if nonzero(close_idle): resources["closeUnchosenIdle"]["after"] = close_idle
    resources.update(sleep)
    report = old.pop("report", {}) or {}; quiet = report.pop("quietShare", 0)
    report = {**on(), "quietSampleFraction": quiet, **report}
    if "interval" in report and not nonzero(report["interval"]): report.pop("interval")
    telemetry = {"events": {**on(), **(old.pop("events", {}) or {})}, "report": report, "quality": on(quality), "metrics": on(), "pathLabels": on(False), "probeMeasurements": on(False)}
    providers = [provider(p) for p in old.pop("providers", [])]
    result["healthObservatory"] = {"evidence": evidence, "probes": probes, "network": network, "host": host, "resources": resources, "telemetry": telemetry, "providers": providers, **old}
    for balancer in result.get("routing", {}).get("balancers", []):
        if balancer.get("strategy", {}).get("type") != "leastCost": continue
        settings = strategy(balancer["strategy"].get("settings", {}))
        settings["failover"]["closePending"] = on(pending)
        settings["failover"]["closeSilent"] = {**on(silent), "after": evidence["stalledRequests"]["timeout"]}
        balancer["strategy"]["settings"] = settings
    return presence_config(result)


def result_payload(result):
    if isinstance(result, dict) and isinstance(result.get("success"), bool):
        if not result["success"]: raise RuntimeError(result.get("error", "health command failed"))
        return result.get("data")
    return result
