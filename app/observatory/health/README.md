# Health observatory and the leastCost balancer

The health observatory judges outbounds by the connections users already
make and chooses outbounds for balancers with the `leastCost` strategy. It
probes only when there is a reason. The core knows nothing about what an
outbound means to the product: everything is set by tags, costs and rules in
the config.

## How it decides

1. **Evidence from traffic.** Every connection an observed outbound dials is
   followed through its traffic counters, so readv, writev and XTLS splice
   are seen as well. Failures are of two kinds:
   - about the path, whatever the destination: the dial failed, the dial did
     not finish within `dialTimeout`, REALITY did not verify the server;
   - possibly about one destination: no first byte within
     `firstByteTimeout`, a written request without an answer for
     `stallTimeout`, or a young connection that received less than
     `successBytes` and went silent for `stallTimeout`. The last one is what a
     censor freezing connections after the first kilobytes looks like.
2. **Suspicion.** `failures` path failures, or `failures` failures over at
   least `distinctTargets` destinations, since the last proof that the path
   works. A connection that downloads `successBytes` is such a proof and
   clears the evidence. One dead site never makes a path suspect, and silence
   is not a failure.
3. **Confirmation.** If `connectivity` is set, the observatory first checks
   the network through that outbound (usually one that bypasses the tunnel);
   without a network it pauses all decisions. A pause lasts at most
   `maxPause`. After that decisions resume, but outbounds ruled out while the
   check keeps failing are provisional: they come back as soon as a check
   passes. A working outbound while the check fails shows that the check
   cannot reach its destination in this network; it is then not used until
   the next network change. Pick a destination that stays reachable where
   the outbounds get blocked, otherwise failover waits `maxPause` there.
   Connections of `witnesses` outbounds (such as `direct` for sites outside
   the tunnel) that receive `successBytes` prove the network works the same
   way, without waiting. Path failures need no further
   proof; destination failures are confirmed by a probe. A probe that clears
   the suspicion starts a cooldown that doubles with every false alarm in a
   row.
4. **Failover.** The chosen outbound goes down and rests in quarantine with a
   growing, jittered backoff. The balancer moves new connections to the
   cheapest usable candidate, after probing it if `switchAfterProbe`. When
   nothing is left, the balancer returns no tag and traffic goes to
   `fallbackTag`; quarantined candidates are rechecked while users keep
   trying.
5. **Return to cheaper.** Not before `minHold`, on `upgrade.interval` (with a
   random phase per device) or after a network change. The cheaper candidate
   must pass `upgrade.successes` probes in a row, `upgrade.spacing` apart. A
   return that fails within `penaltyWindow` multiplies the wait before the
   next one; a return that holds clears it.
6. **Networks.** On `NetworkChanged(id)` results of the previous network are
   forgotten. If the network was seen before, the outbound that last worked
   there is tried first, with one probe and no round over all candidates.

Probes run one at a time (`concurrent`), at least `spacing` apart, within
`perMinute` in total and `perServerNamePerMinute` per TLS or REALITY server
name: several near-simultaneous handshakes to one server name are a known
censor signal. Without user traffic for `pauseWhenIdle` there are no probes
and no timers.

Existing connections stay where they are; only new connections move.

## Config

```json
{
  "healthObservatory": {
    "subjectSelector": ["a", "b", "c"],
    "passive": {
      "window": "60s", "dialTimeout": "6s", "firstByteTimeout": "5s", "stallTimeout": "8s",
      "failures": 3, "distinctTargets": 2, "successBytes": 24576, "sweepInterval": "500ms",
      "partialStallMaxAge": "30s", "ignorePartialStalls": false, "dialOnly": false
    },
    "probes": [
      { "selector": ["a", "b"], "destination": "https://example.com/bytes/32768", "method": "GET",
        "expectStatus": [200], "minBytes": 32768, "timeout": "8s", "progressTimeout": "4s" }
    ],
    "connectivity": { "outbound": "direct", "destination": "https://example.com/204", "timeout": "4s",
                      "recheck": { "initial": "2s", "multiplier": 2, "max": "30s", "jitter": 0.2 },
                      "maxPause": "60s", "witnesses": ["direct"] },
    "schedule": {
      "pauseWhenIdle": "2m",
      "quarantine": { "initial": "2m", "multiplier": 2, "max": "30m", "jitter": 0.3 },
      "concurrent": 1, "spacing": "2s", "spacingJitter": 0.3,
      "perServerNamePerMinute": 3, "perMinute": 12, "suspectCooldown": "1m"
    },
    "events": { "maxEvents": 1000, "maxBytes": 65536 },
    "network": { "remember": true, "maxNetworks": 16 }
  },
  "routing": {
    "balancers": [{
      "tag": "auto",
      "selector": ["a", "b", "c"],
      "strategy": { "type": "leastCost", "settings": {
        "costs": [ { "match": "a", "value": 1 }, { "match": "b", "value": 3 }, { "match": "c", "value": 50 } ],
        "defaultCost": 0,
        "lazy": ["c"],
        "switchAfterProbe": true,
        "confirm": "auto",
        "minHold": "2m",
        "upgrade": { "interval": "30m", "randomPhase": true, "successes": 3, "spacing": "20s",
                     "penaltyWindow": "5m", "penaltyMultiplier": 2, "maxPenalty": "2h",
                     "onNetworkChange": true, "unknownNetworksOnly": true },
        "startPinned": "",
        "onNetworkChange": "recheck"
      } },
      "fallbackTag": "block"
    }]
  }
}
```

The values above are the defaults, except `subjectSelector`, `probes`,
`connectivity`, `costs`, `lazy` and `startPinned`, which have none. Without a
matching probe profile an outbound is probed with a GET of
`https://www.gstatic.com/generate_204`.

- `costs` uses the format of `leastLoad`: a substring or a `regexp` of the
  tag and a value above zero. Lower is preferred. Equal costs prefer
  outbounds known to work, then tag order: the outbound manager hands the
  candidates over sorted by tag.
- Every `selector` of a `leastCost` balancer must be covered by
  `subjectSelector`: an outbound nobody observes is never left.
- A mux outbound counts one failure per connection to its server, however
  many user connections ride on it, so it is ruled out later than one
  without mux.
- `lazy` outbounds are never probed in the background and are only tried
  when everything cheaper is down.
- `confirm`: `auto` probes only destination failures, `always` probes every
  suspicion, `never` trusts the evidence.
- `onNetworkChange` of the strategy: `recheck` forgets and probes the chosen
  outbound once (or the one remembered for the network), `forget` only
  forgets, `ignore` keeps everything.
- `upgrade.interval` cannot be zero; a very long interval such as `8760h`
  leaves only returns after network changes.
- `startPinned` keeps the balancer on one outbound until the host releases
  it, for example while another component verifies the connection.
- Set `fallbackTag` to a blackhole: without it, an empty choice sends traffic
  to the default outbound.

## Host API

The `HealthService` gRPC service (enable it in `api.services`) and
`xray api health` expose the same calls; embedders can call the `Observer`
methods directly (`health.FromInstance(instance)`):

| Call | Purpose |
| --- | --- |
| `NetworkChanged(id)` / `xray api health network <id>` | The device moved to another network. `id` lets the observatory remember working outbounds per network; empty when the host cannot tell networks apart. |
| `NetworkLost()` / `lost` | No network; decisions pause until the next change. |
| `Pin(balancer, outbound)` / `pin` | Keep the balancer on an outbound. |
| `Release(balancer)` / `release` | Let the balancer choose again. |
| `Status()` / `status` | Current state as JSON, see below. |
| `DrainEvents()` / `events` | Events since the previous drain as a JSON array, and how many were dropped because the buffer was full. |

## Events

Every event is `{"seq": n, "ts": unix_ms, "type": "...", "fields": {...}}`.
`seq` grows by one per event, so gaps show dropped events.

| Type | Fields | When |
| --- | --- | --- |
| `switch` | `balancer`, `from`, `to`, `reason`, `held_ms`, `cost` | New connections go to another outbound. `reason`: `start`, `start_pinned`, `start_remembered`, `failover`, `upgrade`, `recovered`, `remembered`, `network_change`, `candidates_changed`, `pin`. `held_ms` is the time on the previous one. |
| `path_state` | `outbound`, `state`, `reason`; for `suspect` also `failures`, `targets`, `kinds`, `strong`; for `down` also `quarantine_ms`, `quarantine_level` and `provisional` when it was ruled out while the network check was failing | An outbound became `up`, `suspect`, `down` or `unknown`. `reason`: `traffic`, `probe`, `upgrade_probe`, `network_change`, `network_down`, `network_up`. `kinds` counts failures by kind: `dial_failed`, `dial_timeout`, `server_unverified`, `no_response`, `stall`, `partial_stall`. |
| `probe_start` | `outbound`, `reason`, `balancer` | A probe started. `reason`: `confirm`, `failover`, `network_change`, `remembered`, `recheck`, `upgrade`. |
| `probe_result` | `outbound`, `reason`, `balancer`, `ok`, `rtt_ms`, `bytes`, `error`, `stale` | A probe finished. `bytes` counts both directions above TLS. `stale` means it was made in the previous network and was ignored. |
| `exhausted` | `balancer`, `from`, `candidates` | No usable candidate is left; traffic goes to `fallbackTag`. |
| `upgrade_start` | `balancer`, `from`, `to` | Probing a cheaper outbound to return to it. |
| `upgrade_failed` | `balancer`, `outbound`, `successes` | The return was abandoned after a failed probe. |
| `upgrade_delayed` | `balancer`, `outbound`, `delay_ms` | The return waits longer because the previous one failed. |
| `upgrade_penalty` | `balancer`, `outbound`, `multiplier` | A return failed within `penaltyWindow`. |
| `pin`, `release` | `balancer`, `outbound` | The host pinned or released a balancer. |
| `pinned_down` | `balancer`, `outbound` | The pinned outbound failed; the balancer stays on it. |
| `connectivity` | `ok`, `rtt_ms`, `error` | Result of a network check. |
| `network_down` | `error` | The network check failed; decisions paused. `error` is `host` when the host reported the loss. |
| `network_up` | `by` | Decisions resumed: `connectivity` (a check passed), `traffic` or `probe` (an outbound worked). |
| `connectivity_doubt` | `failed_checks`, `paused_ms` | The check kept failing for `maxPause`; decisions resumed provisionally. |
| `connectivity_ignored` | `failed_checks` | The check failed while an outbound worked; it is not used until the next network change. |
| `network_change` | `network`, `known`, `generation` | The host reported a network change. |

## Status

`Status()` returns the network (`network`, `network_up`, `connectivity`:
`ok`, `paused`, `doubt` or `ignored`, `generation`, `idle`), probe queue sizes, per outbound the state and its age, failures in
the window, quarantine, last probe RTT and error, connections, bytes, time to
first byte (p50, p90), failures by kind, probes and their bytes, and time as
the chosen outbound; per balancer the chosen and pinned outbound, whether it
is exhausted, the time on the chosen outbound and the next return attempt.
