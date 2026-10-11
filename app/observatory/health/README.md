# Health observatory and the leastCost balancer

The health observatory judges outbounds by the connections users already
make and chooses outbounds for balancers with the `leastCost` strategy. It
probes only when there is a reason. The core knows nothing about what an
outbound means to the product: paths, costs and rules are explicit in
the config.

## How it decides

1. **Evidence from traffic.** Every connection an observed outbound dials is
   followed through its traffic counters, so readv, writev and XTLS splice
   are seen as well. Outbounds that are neither observed, nor `witnesses`,
   nor the outbound of the network check dial as without the observatory.
   Failures are of two kinds:
   - about the path, whatever the destination: the dial failed, the dial did
     not finish within `dialTimeout`, REALITY did not verify the server;
   - possibly about one destination: no first byte within
     `firstByteTimeout` of the request, or of the dial when nothing could be
     sent, a request whose connection ended before any answer
     (a transport that connects lazily, such as XHTTP, shows a reset of its
     server this way), a written request without an answer for
     `stallTimeout`, or a young connection that received less than
     `successBytes` and went silent for `stallTimeout`. The last one is what a
     censor freezing connections after the first kilobytes looks like.

   An outbound that carries its connections inside a tunnel it keeps, such
   as WireGuard, reaches its server once for all of them. Each connection
   through the tunnel counts as a dial of the server instead, which ends
   when the server answers anything after the connection started: its own
   first packet back, traffic of other connections, or the handshake a
   connection asks for once it has waited a second, which a working server
   answers whatever the destination. A tunnel that cannot be set up is a
   failed dial, and a server that answers nothing within `dialTimeout` a
   dial that did not finish: both are about the path. A connection that
   failed inside the tunnel before the server answered waits for it a little
   longer, up to 11 s, so that a slow tunnel and a dead one are told apart;
   an answer that comes too late for it is held against nothing. Inside the
   tunnel, TCP connections are judged by their traffic like any other: a
   destination that refuses the connection shows as one that ended before
   any answer, one that drops it as no first byte. UDP flows are judged by
   their dial alone.
2. **Suspicion.** `failuresToSuspect` path failures, or `failuresToSuspect` failures over at
   least `minDistinctTargets` destinations, since the last proof that the path
   works. A connection that downloads `successBytes` is such a proof and
   clears the evidence. One dead site never makes a path suspect, and silence
   is not a failure.
3. **Confirmation.** If `connectivity` is set, the observatory first checks
   the network through that outbound (usually one that bypasses the tunnel);
   without a network it pauses all decisions. Dials that started during a
   pause and fail after it ended are not held against the outbound. A pause
   lasts at most `maxPause`. After that decisions resume, but outbounds ruled
   out while the check keeps failing are provisional: they come back as soon
   as a check passes. A working outbound while the check fails shows that the check
   cannot reach its destination in this network; it is then not used until
   the next network change. Pick a destination that stays reachable where
   the outbounds get blocked, otherwise failover waits `maxPause` there.
   Connections of `witnesses` outbounds (such as `direct` for sites outside
   the tunnel) that receive `successBytes` prove the network works the same
   way, without waiting. Path failures need no further
   proof; destination failures are confirmed by a probe. A probe that clears
   the suspicion starts a cooldown that doubles with every false alarm in a
   row. A user connection that receives `successBytes` through the outbound
   while the suspicion waits for its probe clears it the same way, and the
   probe is not sent.

   Strong evidence does not wait for a probe that confirms weak evidence:
   failures about the path that come while it runs rule the outbound out at
   once. When that probe then passes, the failures were a hiccup shorter
   than the probe, and the probe rescues the outbound: it is up again with
   reason `rescued`, out of quarantine at the quarantine level it had
   before, the down raises no penalty of a return to it, and its connections
   that go silent are kept, see below; those ended at the decision stay
   ended. The failover goes on: a balancer that moved or still moves away
   comes back by a return soon after `minHoldTime`, see below, and one left with
   nothing takes the outbound back with `recovered`. A confirming probe that had not
   started yet is dropped at the down, and one that fails changes nothing.
   A probe for any other reason brings an outbound up when it passes,
   whenever it started.

   With `confirmWithProbe: auto`, a strong down still starts failover immediately.
   The observatory then queues one `audit` probe below failover and recheck.
   It must start within 30 seconds in the same network generation. A passed
   audit rescues the down, including its
   quarantine, return penalty, and pending silence check. The balancer returns
   through its usual `minHoldTime` and `probesToReturn` rules. A failed audit
   leaves the down unchanged. A path gets at most one audit per active
   `penaltyWindow`; when several balancers select it, the longest window wins.
   Set `shouldSkipDownAudit: true` in `leastCost.settings` to disable audits.
   `audit_count` counts starts, `audit_passed_count` counts rescues, and
   `audit_skipped_count` counts audits suppressed by the setting or throttle
   and queued audits dropped before starting. The report fields
   `audits` and `audits_passed` count the same per outbound.
   A path-level failure (`dial_failed`, `dial_timeout`, `server_unverified`)
   below the usual suspicion threshold queues a `lone` probe if at least one
   balancer selects the outbound and its last traffic or probe success is
   older than `passive.loneFailureAfter` (default `"2m"`, `"0s"` disables).
   Age starts at path creation when there is no success; a network change
   or recovery starts a new age window without inventing a success.
   No probe is queued when the network is lost or in doubt, or the path is
   already being checked. If configured connectivity has no success in the
   current network, or that success is older than four connectivity timeouts,
   its check runs first and holds lone until it passes. This freshness is
   checked again after waiting for probe budgets; disabled or ignored
   connectivity adds no gate. The usual threshold of three failures stays
   the same.
   `lone` follows failover and recheck and precedes audit; it shares their
   concurrency, spacing, rate and server-name budgets and can run in idle
   because a real connection supplied its failure. A path gets at most one
   started lone probe per five minutes. Canceling a queued job spends none
   of that interval. A passed probe clears failures and records its state;
   a degraded pass keeps the path usable while unpinned balancers search
   for an up alternative under the usual probe-before-switch rule; when all
   paths are degraded, the live choice stays usable. A failed probe makes
   one strong down through the usual quarantine, failover,
   pending-close and silence-close rules, without a second audit.
   Traffic success, ordinary suspicion or down, a network change or loss,
   and handler replacement cancel lone work. Before starting and accepting
   its result, the observer checks that the path is still selected and that
   no success, down or provider retirement overtook the original failure.
   A stale result records the probe but changes no path state.
4. **Failover.** The chosen outbound goes down and rests in quarantine with a
   growing, jittered backoff. The balancer moves new connections to the
   cheapest usable candidate, after probing it if `shouldProbeBeforeSwitch`. Up to
   `maxConcurrentProbes` candidates are probed at once, cheapest first and one per
   server name, so dead candidates do not cost a probe timeout each in
   turn. The balancer takes the cheapest candidate that passed once every
   cheaper one has failed: a dearer one that answers first waits for them.
   Probes that answer after the move only record the state of their
   outbound. When nothing is left, the balancer returns no tag and traffic
   goes to `fallbackTag`; quarantined candidates are rechecked while users
   keep trying.
5. **Return to cheaper.** Not before `minHoldTime`, on `returnToCheaper.interval` (with a
   random phase per device), after a network change, or soon after `minHoldTime`
   when the outbound it failed over from turned out to work, as after a
   rescue: at a random point within 5 minutes after `minHoldTime`, or within
   `returnToCheaper.interval` when that is shorter, since one server that hiccups is
   rescued on many devices at once and their returns must not probe it in
   one wave; without `returnToCheaper.hasRandomPhase`, at `minHoldTime`. The cheaper
   candidate must pass `returnToCheaper.probesToReturn` probes in a row,
   `returnToCheaper.probeInterval` apart. A return that fails within `penaltyWindow`
   multiplies the wait before the next one; a return that holds clears it.
6. **User presence.** `UserPresent()` / `xray api health present` returns
   immediately after queuing `wake` probes of stale selected paths. A balancer
   must be unpinned and not exhausted; a shared outbound is considered once
   per call. No work is queued while the network is lost or in doubt, or
   while another probe of that path is running or other queued probe work
   is already sufficient. A deferred
   network-change or remembered probe of the selected path is replaced by
   one guarded wake probe; other paths and balancers remain idle. A success exactly
   `wake.freshness` old still counts as fresh; older or missing proof needs
   a check. The default freshness is `"60s"`, and `wake.minInterval` defaults
   to `"5m"`, counted from the actual start. Missing JSON fields keep these
   defaults; `"0s"` removes the corresponding limit. In a direct protobuf,
   zero keeps the default and a negative value removes the limit.
   `wake` has the same priority as `network_change` and can run during idle,
   with the usual concurrency, spacing, total-rate and server-name budgets.
   A pass records the profile state without moving a healthy choice;
   a degraded pass starts the ordinary search for an up alternative, keeping
   the path usable when every candidate is degraded. A failure uses the
   usual quarantine, failover, pending-close and silence-close rules,
   without an audit or a passive strong/weak down count.
   Fresh traffic, a newer down, switching or pinning the choice, provider
   changes, and every network change (including `ignore`) cancel old wake
   work. Obsolete queued work spends no interval; completed stale work counts
   as a probe but changes no path. No wake job is requeued automatically.
   Without a host call, this feature adds no probes or timers.
7. **Networks.** On `NetworkChanged(id)` results of the previous network are
   forgotten. If the network was seen before, the outbound that last worked
   there is tried first, with one probe and no round over all candidates.

At most `maxConcurrentProbes` probes run at a time and at most `maxConcurrentProbes` start
within one `minProbeInterval`: with the default of one, probes run one by one at
least `minProbeInterval` apart. They stay within `probesPerMinute` in total and
`probesPerServerNamePerMinute` per TLS or REALITY server name, and two probes of
one server name never run at once: several near-simultaneous handshakes to
one server name are a known censor signal. Without user traffic for
`idleAfter` there are no probes and no timers, except the renewal and
the end of a provider lease, see Providers, putting an outbound to sleep
after its last use and ending its idle connections, see Sleep, and the one
look at the connections of an outbound found down `stallTimeout` after the
decision, see below, and checks explicitly queued by `UserPresent`, see
User presence above. Closing
the core ends the probes still running, and their outcome is not held
against the outbound.

Existing connections stay where they are; only new connections move. The
exception is an outbound found down: its connections that have received
nothing yet, still dialing or waiting for an answer, are ended at the
decision and at the switch away from it. Applications see the failure at once
and retry on the outbound chosen now instead of waiting for their own
timeout, and the stuck connections stop holding memory. Connections that
received something keep going, since an address that refuses new
connections may still serve old ones. `shouldKeepPendingOnDown` turns this off.

Once the core has left an outbound found down, a connection from before the
decision that receives nothing on it does not progress either, whatever
ruled the outbound out. Failures about the path need no proof, and a probe
goes to a destination of the config: one that fails tells that the path is
dead, also when it confirmed failures that may have been about
destinations, and also when no balancer uses the outbound, as for a return,
a recheck or a request of the host. `stallTimeout` after the decision, the
connections from before it that have received nothing since, UDP flows too,
are ended: long-lived ones such as DNS over HTTPS or push connections would
keep a tunnel such as WireGuard awake, see Sleep, while their applications
wait for a timeout of their own. A connection that received anything since
the decision is kept, as an address that refuses new connections may still
serve old ones, and so is everything when the outbound came back, also by a
rescue, or a balancer chose it again before the look, or when the network
failed or changed meanwhile or is in doubt at the look. While a failover
away from it still probes candidates, new connections still go to it, and
the look waits for the switch. A spliced connection is judged by what its
socket received; on a platform that does not tell, it is kept.
`shouldKeepSilentOnDown` turns this off.

A first reply on a newer connection makes delayed dial and handshake
errors from older connections obsolete. That reply alone does not mark
the path up: `successBytes` still decides that. An old connection that
keeps carrying traffic does not hide failures of newer dials, and
destination-specific response and stall failures still count.

A decision made while the network check was failing ends nothing.

## Durations

Human and backend durations are Go strings parsed by `time.ParseDuration`.

| String | Meaning |
| --- | --- |
| `"500ms"`, `"6s"`, `"30m"`, `"2h"` | Units are `ms`, `s`, `m` and `h`. |
| `"30m6s"`, `"1h30m"`, `"2h45m30s"` | Several units together, without spaces. |
| `"1.5h"` | A fractional unit: 1 h 30 min. |
| `"0s"`, `"0"` | Exactly zero. |
| `"1d"` | Invalid: use `"24h"` for a day. |
| `"30m 6s"` | Invalid: no spaces. |
| `"30min"`, `"6S"` | Invalid: units and their case matter. |
| `6` | Invalid: a duration must be a string. |

An omitted key uses its default. Explicit zero disables a period or limit,
or removes a delay. A request or probe timeout and `returnToCheaper.interval` cannot be zero:
the config is refused with the name of the key. `startPinTimeout: "0s"` keeps
the start pin until the host releases it; `drainTimeout: "0s"` closes removed
outbounds without a drain delay. Durations the core measures
are integer milliseconds, with `Ms` in provider requests and `_ms` in host
outputs. `nextRefreshIn` follows this table as well; a number is refused.

## Config

```json
{
  "outbounds": [
    { "tag": "a1", "protocol": "vless", "settings": {}, "streamSettings": {},
      "path": { "balancer": "auto", "cost": 1, "probe": "large" } },
    { "tag": "b1", "protocol": "vless", "settings": {}, "streamSettings": {},
      "path": { "balancer": "auto", "cost": 3, "probe": "large" } },
    { "tag": "c1", "protocol": "vless", "settings": {}, "streamSettings": {},
      "path": { "balancer": "auto", "cost": 50, "isLazy": true, "returnInterval": "10m" } },
    { "tag": "direct", "protocol": "freedom" },
    { "tag": "block", "protocol": "blackhole" }
  ],
  "healthObservatory": {
    "wake": { "freshness": "60s", "minInterval": "5m" },
    "passive": {
      "window": "60s", "dialTimeout": "6s", "firstByteTimeout": "5s", "stallTimeout": "8s",
      "loneFailureAfter": "2m",
      "failuresToSuspect": 3, "minDistinctTargets": 2, "successBytes": 24576, "sweepInterval": "500ms",
      "partialStallMaxAge": "30s", "shouldIgnorePartialStalls": false, "isDialOnly": false,
      "shouldKeepPendingOnDown": false, "shouldKeepSilentOnDown": false
    },
    "probes": [
      { "name": "large", "destination": "https://example.com/bytes/32768", "method": "GET",
        "expectedStatuses": [200], "minBytes": 32768, "timeout": "8s", "progressTimeout": "4s" }
    ],
    "connectivity": { "outbound": "direct", "destination": "https://example.com/204", "timeout": "4s",
                      "recheckBackoff": { "initialDelay": "2s", "multiplier": 2, "maxDelay": "30s", "jitterFraction": 0.2 },
                      "maxPause": "60s", "witnesses": ["direct"] },
    "schedule": {
      "idleAfter": "2m",
      "quarantine": { "initialDelay": "2m", "multiplier": 2, "maxDelay": "30m", "jitterFraction": 0.3 },
      "maxConcurrentProbes": 1, "minProbeInterval": "2s", "probeJitterFraction": 0.3,
      "probesPerServerNamePerMinute": 3, "probesPerMinute": 12, "suspectCooldown": "1m",
      "hostProbes": { "probesPerMinute": 4, "reuseWindow": "15s", "maxOutbounds": 8, "timeout": "2m" }
    },
    "events": { "maxEvents": 1000, "maxBytes": 65536 },
    "network": { "shouldRememberNetworks": true, "maxNetworks": 16 },
    "report": { "interval": "24h" },
    "sleep": { "afterLastUse": "5m", "closeIdleAfter": "0s" }
  },
  "routing": {
    "balancers": [{
      "tag": "auto",
      "strategy": { "type": "leastCost", "settings": {
        "shouldProbeBeforeSwitch": true,
        "confirmWithProbe": "auto",
        "minHoldTime": "2m",
        "returnToCheaper": { "interval": "30m", "hasRandomPhase": true, "probesToReturn": 3, "probeInterval": "20s",
                     "penaltyWindow": "5m", "penaltyMultiplier": 2, "maxPenaltyDelay": "2h",
                     "shouldTryOnNetworkChange": true, "isUnknownNetworksOnly": true },
        "startPinnedOutbound": "", "startPinTimeout": "0s", "startOutbound": "",
        "networkChangeAction": "recheck"
      } },
      "fallbackTag": "block"
    }]
  }
}
```

The values above are the defaults, except `outbounds[].path`, `probes`,
`connectivity`, `startPinnedOutbound` and `startOutbound`, which have none. A path without `probe` uses the built-in GET of
`https://www.gstatic.com/generate_204`.

A probe profile may instead contain one to four ordered `steps`. Each step has
`name`, one to four `destinations`, optional `isOptional`, and its own `method`,
`expectedStatuses`, `minBytes`, `timeout` and `progressTimeout`. Required steps
run first and stop the probe on failure. Optional steps run afterwards even
when another optional step fails. The first successful destination in a step
wins; the others are canceled. Required steps passing and an optional step
failing leave the path `degraded`: it still carries traffic, but an `up` path
is preferred even when it costs more. If all candidates are `degraded`, cost
decides among them. A profile with `destination` keeps its former behavior
as one required step named after the profile.

Each HTTP probe uses its own connection through the outbound. On a mux
outbound the request uses an isolated mux worker, so a completed mux handshake
measures the actual dial of this probe; mux framing stays in place. User
traffic and provider requests continue using the reusable worker pool.

Values that size what the observatory keeps have ceilings, and a larger
value is taken as the ceiling: probe and network check timeouts 30 s,
`maxConcurrentProbes` 8, `maxEvents` 4096, events `maxBytes` 256 KiB, `maxNetworks`
64; for a provider `maxBytes` 1 MiB, `maxOutbounds` 128 and `drainTimeout` 30 min;
for `hostProbes` `probesPerMinute` half of `schedule.probesPerMinute`, `reuseWindow` 5 min,
`maxOutbounds` 16 and `timeout` 10 min.

- A candidate carries `path.balancer` naming this `leastCost` balancer and
  either a positive `path.cost` or `path.group` naming one of its groups.
  An outbound without `path` is neither a candidate nor observed. Lower
  cost is preferred. At equal cost, paths follow their first appearance
  in `outbounds`; group members follow that array's order. The observer
  follows paths, `connectivity.outbound` and explicit witnesses.
  A `leastCost` balancer with `selector` is rejected. Stock strategies
  keep their selectors.
- A mux outbound counts one failure per connection to its server, however
  many user connections ride on it, so it is ruled out later than one
  without mux.
- A WireGuard outbound repeats a lost handshake after 5 s, and the
  connections waiting for it count as dialing meanwhile: keep `dialTimeout`
  above that and a round trip, as the default does, or a lost packet counts
  as a failure. The first handshake of a new tunnel, at a wake, is the
  exception: once a connection or a probe has waited a second for it in
  vain, the tunnel repeats it at once, a single time. A probe that wakes the
  outbound thus gets through a lost packet within its `progressTimeout`,
  which is shorter than 5 s.
- Paths with `isLazy` are never probed in the background and are only tried
  when everything cheaper is down.
- `confirmWithProbe`: `auto` probes only destination failures, `always` probes every
  suspicion before deciding, `never` trusts the evidence. `auto` audits strong
  downs after the decision unless `shouldSkipDownAudit` is true.
- `networkChangeAction` of the strategy: `recheck` forgets and probes the chosen
  outbound once (or the one remembered for the network), `forget` only
  forgets, `ignore` keeps everything. A pinned balancer is rechecked once
  its pin is released, if the network changed meanwhile; its outbound is
  not probed when traffic or a probe has already judged it there.
- `returnToCheaper.interval` cannot be zero; a very long interval such as `8760h`
  leaves only returns after network changes.
- A single path or group may set `returnInterval`, a Go duration that replaces
  `returnToCheaper.interval` while that path is chosen. Absence keeps the global interval.
  `"0s"` disables periodic returns from that path; returns caused by a
  network change or a successful check remain possible and respect
  `minHoldTime`. The chosen interval also bounds the random first phase.
  When a return fails, the chosen interval and `minHoldTime` set the base
  of its penalty. The base plus additional delay is capped by `maxPenaltyDelay`;
  a base already longer than that cap keeps its own period and gains no
  additional delay. The cap is applied after the next outbound is chosen.
- `startPinnedOutbound` keeps the balancer on one outbound until the host releases
  it, for example while another component verifies the connection.
  `startPinTimeout` releases it by itself after that long: a host that
  verifies within a known time and stops the core when the check fails
  then needs no release call. While a balancer is pinned, the triggers of
  its provider wait, but for a host request and the first request about a
  lease that is urgent or ended, see Providers.
- `startOutbound` starts the balancer on one outbound instead of the cheapest,
  without a pin, when it is a usable candidate whose lease did not end: the
  host passes the outbound that worked last time, so a restart needs no
  probing. An outbound remembered for the network comes first.
- Set `fallbackTag` to a blackhole: without it, an empty choice sends traffic
  to the default outbound.
- `report.interval`: a window with a decision is reported once it lasts
  this long, see Report. `"0s"` leaves only the report at the stop.
- `sleep.afterLastUse`: an outbound that can release what it holds while idle
  does so this long after its last use while no balancer chooses it, and
  at once while it is down, see Sleep. At least 10 s; `"0s"` turns sleep
  off.
- `sleep.closeIdleAfter`: on such an outbound that no balancer has chosen
  for `sleep.afterLastUse`, connections that received nothing for this long are
  ended, so that it can sleep, see Sleep. `"0s"`, the default, keeps them;
  otherwise at least 10 s, as a connection between two requests is not
  idle, and at most 10 min, as the observer may stop following a connection
  without traffic after that. Nothing without sleep.
- `hostProbes` limits the probes the host asks for, see Probes on request.

## Outbound groups

A group is a path with several ordered addresses. Declare it once in the
`groups` list of a `leastCost` balancer and put only `balancer` and `group`
in each member's `path`. A member cannot override the group's `cost`,
`isLazy`, `returnInterval` or `probe`. The base config rejects duplicate or
unused groups, unknown probes and missing members. A config needs no groups
when all paths are single outbounds.

```json
{
  "outbounds": [
    { "tag": "g-10", "protocol": "vless", "settings": {}, "streamSettings": {},
      "path": { "balancer": "auto", "group": "g" } },
    { "tag": "g-2", "protocol": "vless", "settings": {}, "streamSettings": {},
      "path": { "balancer": "auto", "group": "g" } },
    { "tag": "b-1", "protocol": "vless", "settings": {}, "streamSettings": {},
      "path": { "balancer": "auto", "cost": 3 } }
  ],
  "routing": { "balancers": [{
    "tag": "auto", "fallbackTag": "block",
    "strategy": { "type": "leastCost", "settings": {
      "groups": [{ "name": "g", "cost": 1, "probe": "large", "failedInRowToLeave": 3 }]
    } }
  }] },
  "healthObservatory": { "probes": [{ "name": "large",
    "destination": "https://example.com/bytes/32768", "minBytes": 32768 }] }
}
```

Here `g-10` precedes `g-2` because it appears first in `outbounds`.
The limit `failedInRowToLeave` defaults to 3 and accepts 1 through 16.
A client's initial position is the first eight bytes of SHA-256 of
`run + "\x00" + group name`, interpreted as a big-endian uint64 modulo the
member count. `startOutbound` and the outbound remembered for the network
take precedence. After a chosen member fails, failover advances around the
array. A passed probe or `successBytes` clears consecutive failures; at the
limit the group enters `schedule.quarantine` and unvisited members are skipped.

After group quarantine, one following member is checked with reason
`group_recheck`. A passed check makes the group available; other members
keep their individual state, including unknown for members never checked.
The check does not move a working balancer by itself or replace
`probesToReturn`. Idle, a lost network and a pin postpone it; low memory
permits it only for an exhausted balancer. Lazy members and expired leases
are not checked in the background, but remain reserves. Probe budgets and
the one-probe-per-server-name rule still apply.

Status and reports retain the `groups` fields `balancer`, `group`, `state`,
`members`, `healthy`, `quarantined`, `client_position`, `failed`,
`quarantine_ms` and optional `last_failure`. The group name is the declared
name for base paths and the prefixed name for provider paths.
`client_position` indexes the declared member order. Provider requests use
the same summary with camelCase names, omit `balancer`, and count only
members owned by that provider; the position still belongs to the full ring.
A down group contributes no usable members to `minHealthyOutbounds`.

## Providers

A provider fetches a fresh set of outbounds over HTTP and puts it into a
balancer without a restart, for example when every address of the current
set is blocked. It is configured inside `healthObservatory`, because its
triggers are judged on what the observatory knows about the balancer. What
goes over the wire is protocol 1: [PROVIDER.md](PROVIDER.md) describes the
request, the answer, the errors and what a backend has to do, for backends.

```json
{
  "healthObservatory": {
    "providers": [{
      "tag": "fresh",
      "url": "https://api.example.com/set",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "via": ["auto", "backup", "direct"],
      "timeout": "10s", "maxBytes": 262144,
      "triggers": {
        "interval": "2h", "jitterFraction": 0.2, "minHealthyOutbounds": 2, "shouldRefreshWhenExhausted": true,
        "whenCostAbove": { "cost": 10, "after": "15m" }, "maxAge": "6h",
        "networkChangeMode": "unknownNotCheapest", "networkSettleDelay": "30s",
        "expiry": { "renewAtFraction": 0.5, "urgentAtFraction": 0.8, "jitterFraction": 0.1 }
      },
      "limits": {
        "minInterval": "5m", "exhaustedMinInterval": "30s", "maxRefreshesPerHour": 6,
        "backoff": { "initialDelay": "30s", "multiplier": 2, "maxDelay": "30m", "jitterFraction": 0.3 }, "authRetryDelay": "1h"
      },
      "apply": { "balancer": "auto", "tagPrefix": "p-", "drainTimeout": "5m", "maxOutbounds": 64, "isRoutingAccepted": false,
                 "acceptedHeaders": ["Authorization"] },
      "baseVersion": "", "initialAnswer": null
    }]
  }
}
```

The values above are the defaults, except `whenCostAbove`, which is off
unless set, and `maxBytes`, which is 4 KiB per outbound of `maxOutbounds`.
`initialAnswer` is an answer to start with, see Initial set, and `baseVersion` names
the config, see Base config. `tag`, `url`, `apply.balancer` and `apply.tagPrefix` are
required. An explicit zero disables a period or limit; a zero delay runs immediately. See Durations.

**Request.** A provider that serves a static file on a web server needs
`"method": "GET"`: by default the request is a POST, which a static host
answers with 405 or 403. A POST of `url` with `headers` carries the state of
the core as JSON, see below; a GET has no body. Credentials belong in a header,
the URL must not carry them. The URL must be `https`; `isPlainHttpAllowed: true`
permits `http`, and the headers then cross the network unencrypted, a token
an answer or the host set as well. The request goes through the `via` entries in
order until one brings a well-formed answer: an outbound tag, or the tag of a
`leastCost` balancer, which means its current choice. An exhausted balancer,
an outbound in quarantine and a tag with no outbound are skipped. Without `via` the request is
routed like any other connection. Each attempt has its own `timeout`;
redirects are not followed. A body that is not a JSON object, is cut short
or is larger than `maxBytes` may have broken on the way, and the next entry
is tried. An answer that is a JSON object but breaks the protocol ends the
attempts: the backend itself sent it, and another entry leads to the same
backend.

An answer other than 200 may carry an error body of the protocol, whose
code goes into `provider_fetch`. A 429 or 503 with `Retry-After` holds every
trigger, the host and an exhausted balancer too, for that long: at most
`backoff.maxDelay`, then spread by up to `backoff.jitterFraction` upward only, so no
client comes back sooner than asked and clients told the same pause come
back apart. Such a pause is no failure, and the backoff does not grow. An
error body and `Retry-After` end the attempts, as the backend itself
answered. A bare status may come from a firewall, a portal or a proxy on
one way, and the next entry is tried; a bare 401 or 403 too, which such a
box sends often, see Token.

The body of a POST tells the backend why it is asked and what broke, so that
it can answer with the right outbounds at once: the trigger and the previous
failure, the set and the base config the core runs with, the network, the
balancer, and per outbound of the set its state, failures, last probe,
traffic and lease. It never holds destinations, domains, addresses or server
names, the targets of failures, the text of an error that may name the probe
URL, or header values. `run` and the network id go out hashed for the
provider, so the raw run, which the report carries as `health_run`, never
leaves the device. A usual set takes a few kilobytes, the largest one some
60 KB.

**Answer.** An answer is a JSON object with `outbounds` and, optionally,
`protocol`, `version`, `groups`, `nextRefreshIn`, `expiresAt`, `serverTime`,
`headers`, `baseVersion` and `routing`. It is read strictly. `protocol` is 1
or left out; an answer of another protocol is refused for that, whatever
keys it has. Any other key, a section of the config such as `dns`,
`inbounds` or `policy`, and `healthObservatory` refuse the whole answer, so
a backend never takes a key for applied that the core threw away; a new key
is a new protocol. A refused answer changes nothing and is reported in
`provider_apply` with `is_ok` false and the reason, cut to 200 bytes like every
reason that quotes the answer, which the next request tells the backend.

`outbounds` use the format of the config file and need unique tags. Their
protocols must be in `apply.allowedProtocols`; by default `vless`, `vmess`,
`trojan`, `shadowsocks`, `wireguard`, `hysteria`, `socks` and `http`, so an
answer cannot send traffic straight out with `freedom`.
Each outbound must carry `path.balancer` equal to `apply.balancer` and
either its own positive cost or a group declared in this answer. The answer
may declare `groups` with raw names; the core prefixes each name and tag
with `tagPrefix`. Members cannot join groups from the base config or another
provider. A missing path or any conflicting group property rejects the
whole answer. A provider answer may contain an empty group declaration.
`nextRefreshIn` (a Go duration string) replaces `interval` until the next
refresh. `"0s"` asks for an immediate refresh subject to the provider limits;
omission uses `triggers.interval` again. `null` and numbers are refused. `version` names the set; without it the set is named by
a hash of `outbounds`, `groups` and `routing`, so an answer that differs only
in `protocol`, `nextRefreshIn`, `expiresAt`, `serverTime`, `headers` or
`baseVersion` is the same set. A set with the same name as the applied one
changes no outbound; its leases, headers and base still apply. It is kept
for `ProviderSet` all the same, so its outbounds are checked short of
building them, their number, tags, protocols and paths, and an answer that
fails is refused.

`routing` takes `rules`, `balancers` and `domainStrategy` in the format of
the config file and replaces all rules and balancers at once, and the domain
strategy when the answer has one, without a restart, when `apply.isRoutingAccepted` is
on; any other key in it refuses the answer. Rules refer to the final tags, `tagPrefix` included.
The answer is refused when a rule or a fallback goes to an outbound or a
balancer that does not exist (Xray would take its default outbound for it,
which may be outside the tunnel), when the leastCost balancer of the
provider is gone, when a rule has a webhook, which would post every matching
connection with its destination. Balancers the new routing no longer has are forgotten, with the
outbounds they remembered per network. While `apply.isRoutingAccepted` is off the
routing of an answer is ignored, which `provider_apply` tells with
`is_routing_ignored`: taking routing from a provider is the choice of the
client. Accepted routing may change strategy settings, but leastCost
membership and order still come from the assembled outbound paths.

**Apply.** Every outbound gets `tagPrefix` in front of its tag.
`apply.balancer` has to be a `leastCost` balancer; every answer path must
name it exactly. The provider owns every outbound whose
tag starts with the prefix, also ones from the config; such an outbound
should not be the first one, which the core takes as the default. A host
that starts the core with the set it saved last time passes it as `initialAnswer`
instead. All outbounds are validated and built before anything changes; a
broken answer or a set that does not build changes nothing. Outbounds whose
settings did not change are not touched and keep their connections. An
outbound whose settings changed is replaced under the same tag. An outbound
that left the set is no longer a candidate, keeps its connections for
`drainTimeout` and is then removed and closed, and what the observatory knew about
it and its traffic counters are forgotten; if the balancer uses it, the
balancer moves to another candidate the usual way and stays where it is
while there is nothing else. A pinned balancer stays on it, so it stays
too: its drain does not end while a balancer is pinned to it, and one whose
time ran out ends at the release, which moves the balancer off it. The old
handler of a replaced outbound drains the same way. A drain ends at once
when the outbound has no open connections and no balancer chooses it. An
outbound that can sleep is asked as well, since the observatory may not see
its connections, see Sleep: while one goes on it refuses, and the drain
runs its time. At most `maxOutbounds` outbounds drain at a time: beyond
that the oldest drains end early, but for pinned ones.

**Leases.** `expiresAt` tells by tag when the backend stops serving an
outbound, in RFC 3339 with a zone, and `serverTime` is the clock of the
backend when it answered. An outbound without an end has none, and an end
for a tag the answer does not list is ignored. The clock of the device may
be off by any amount, so a lease is kept as what was left of it when the
answer arrived: `expiresAt` less the later of `serverTime` and the `Date`
of the answer plus its `Age`, which a cache sets, or less the clock of the
device without either, at most 30 days. It counts down from there by the
longer of the monotonic clock, which stands while the device sleeps, and
the wall clock, so it ends early rather than late. A refresh renews it
before that, see the `expiry` trigger. Every answer is the whole truth
about the outbounds in it: a later end renews a lease, the same end only
brings a fresher clock and moves neither the end nor the renewal, an
earlier end shortens the lease without moving the renewal, so a later
answer that extends it again renews it, and an outbound that leaves the
set takes its lease along. An answer with a time the core cannot read is
refused, and a refused answer changes no lease.

An outbound whose lease ended stays a candidate, the last resort: new
connections go to it only while no other usable candidate is left, so when
every lease ended traffic goes through those outbounds rather than to
`fallbackTag`. A balancer on it moves to another candidate the usual way,
with reason `expired`, and stays while there is nothing else; it never moves
from one expired outbound to another, and a pinned balancer stays on its
pin. Such an outbound is not probed in the background, not returned to, not
taken as the outbound remembered for a network or as `startOutbound`, and not in
the plan of a probe request of the host. A failover tries it after every
other candidate. Its state, quarantine and connections stay as they are, and
failures of old connections are ignored while no balancer chooses it: a
revoked outbound is no block. One a balancer still uses is judged as usual.
`minHealthyOutbounds` counts it as gone. A network change keeps the leases.

**Token.** `headers` of an answer replace headers of the request, to rotate
the token of the provider. Only names in `apply.acceptedHeaders` may come, in any
case: `authorization` replaces the `Authorization` of the config rather than
going beside it. `[]` lets no answer set a header, and an answer with
`headers` is then refused. At most 8 headers, each name at most 256 bytes,
each value non-empty, in UTF-8, without line breaks or other control
characters and at most 4 KiB; names of the transport such as `Host`,
`Content-Type`, `Connection` or `Proxy-*` never.
The host sets headers the same way with `SetProviderHeaders`, any names but
those of the transport; an empty value removes what the host or an answer
set, and the config applies again. A value that is not UTF-8 fails in a
direct call as over gRPC: `ProviderSet` carries the headers in JSON, which
would put other bytes in its place for the next start. The headers an
answer or the host set are at most 8 together, as the set the host keeps
carries them: an answer or an initial set that would make more is refused,
and a call of the host that adds past 8 fails, while one that replaces or
removes headers never does. What came later wins, and the host wins
over an answer to a request that was sent before the host changed the
headers: `provider_apply` tells that with `is_headers_stale`. Only an applied
answer changes a header, so a refused or a lost answer leaves the token in
use; a backend keeps the previous token valid until a request brings the new
one, and some time after that, in case the process dies before the host
keeps it. Values show nowhere but in the request: not in events, Status,
the report, logs or errors, only the names.

The backend refuses the headers with a 401 or a 403 and an error body with
the code `unauthorized` or `forbidden`. Every trigger but a host request
then waits for `authRetryDelay`, spread by `backoff.jitterFraction`, or until the headers
change; after the pause one refresh tries them again, and again once per
pause until one gets through. The refusal grows no backoff, and the leases
it asked about are asked about again after the pause. `"0s"` waits for new
headers without a pause. `authRetryDelay` is at least 5 minutes and at most 24
hours. A 401 or a 403 without such a body is a failure like any other, and
the next `via` is tried: portals and firewalls on mobile networks and in
hotels answer so often. A refusal of a request that was sent before the host
changed the headers is no refusal of the new ones: the refresh is tried
again with them as soon as the limits allow.

**Initial set.** `initialAnswer` is an answer of the backend that the core
applies as it starts, before the first connection and without a request:
the answer of a connect, or the set the host kept from `ProviderSet` with
`receivedAt`, the time it arrived by the clock of the device. It is read as
an answer, with the same checks, except that its `headers` may have any
name the host may set and that it may have `receivedAt`. Its outbounds are
registered by the time the first connection comes; connections that came
earlier stay on the outbounds of the config. A set the host kept counts as
old as it is: its leases count from its `serverTime` plus the time since
`receivedAt`, by the clock `saved`, and `maxAge` and `nextRefreshIn` from
when it arrived; its renewal comes at `renewAtFraction` and `urgentAtFraction` of the
length a lease had then, as in the run that got it, so a start does not put
it off; a lease that ended meanwhile ends at the start with renewal
`saved`, and one urgent refresh asks about it, also under a start pin. A
fresh one counts from its `serverTime`, as an answer that just arrived. Its
headers lie over those of the config and under those the host set, also
before the start, name by name: a header of another name that the host set,
such as one that names the device, leaves the token of the set in place. An
initial set may be larger than `maxBytes` by what `ProviderSet` adds to an
answer, the headers and the clock, some 200 KiB at most. An initial set
that is not an object, is larger than that, breaks the protocol or does not
build is reported in `provider_apply` with trigger `initial` and `is_ok`
false, and the core starts on the outbounds of the config: a bad saved set
never keeps the core from starting. Once applied, the set lives on only as
the last applied answer of `ProviderSet`. A host passes the answer of a
connect when it has one, and the kept set only without it: the headers of a
kept set would lie over a fresh token in the config, which then needs
`SetProviderHeaders` after the start.

**Base config.** A backend that generates the config of the core names it
with `baseVersion`, at most 256 bytes; without one there, the `baseVersion`
of a fresh `initialAnswer`, the answer of the connect that brought the config,
names it, also when its outbounds do not build. A set the host kept named
the config of an earlier run, so its `baseVersion` never becomes the base.
The body of every request tells the base, and an answer may name the one
the backend serves now: an applied answer that names another one, the same
set and a kept initial set at the start as well, gives
`provider_base_changed` once per value, so a host learns also of a change
it put off. The host then fetches the config anew and restarts the core
when that is cheap, after a random delay, as a fleet that restarts at once
loads the backend like a storm. An answer that names the base of the core
again ends the change, and a later one is told again; an answer without
`baseVersion` changes nothing. The set applies whatever base it names: the
backend answers with outbounds that work with the base of the request.
Without a base nothing is told, and Status shows what the answers name.

**Triggers.** Any trigger may start a refresh:

| Trigger | Fires |
| --- | --- |
| `interval` | `interval` +- `jitterFraction` after the previous successful refresh. |
| `minHealthyOutbounds` | The balancer has fewer candidates that are up or not judged yet; once, again only after it got back above. |
| `shouldRefreshWhenExhausted` | The balancer has no usable candidate and the network is up; again after `exhaustedMinInterval` while it lasts. After a refresh that added outbounds, only once a connection brought them to the balancer. |
| `expiry` | A lease reached `renewAtFraction` of its length, spread by `jitterFraction` per lease, or ended, and no refresh that got an answer asked about this value of it: once per value, so a backend that keeps a lease short is not asked again. From `urgentAtFraction` of the lease on, and once it ended, the refresh is urgent: it waits only for `exhaustedMinInterval` and runs while memory is low. Before that it waits for `minInterval`, and while memory is low for `urgentAtFraction`. `renewAtFraction` 0 turns renewal off; a lease that ended is still asked about once. `renewAtFraction` is at most 0.95, `urgentAtFraction` between it and 0.95, `jitterFraction` at most 0.5. |
| `whenCostAbove` | The balancer has kept outbounds of at least `cost` for `after`; the time counts again from every successful refresh. `after: "0s"` asks in the turn that chooses such an outbound, using `exhaustedMinInterval`. A positive `after` uses `minInterval`. |
| `maxAge` | The set was fetched longer ago than this. |
| `networkChangeMode` | `networkSettleDelay` after a network change: `unknownNotCheapest` in a network without a remembered working outbound when the balancer is not on its cheapest candidate, `unknown` in such a network, `any`, or `never`. A network where the balancer was only pinned has none remembered, since the host chose the outbound there. Under a pin the trigger waits for the release, and `unknownNotCheapest` judges the outbound the balancer is on after it; while a failover is under way, the one the failover moves to. |
| `nextRefreshIn` | The time the last answer asked for. |
| host | `RequestRefresh`, see below. |

Every trigger but a host request waits while the network is down, while the
balancer of the provider is pinned, by the host or by `startPinnedOutbound`, while
the backend refuses the headers (see Token), and while there is no user
traffic, except the first request for a lease: an outbound would end under
a sleeping device. The host decides about a pinned outbound; one that
verifies the outbound of a start pin stops the core when the check fails,
and a refresh then would only load the backend, once for every attempt of a
host that starts the core again. The triggers go on at the release, by the
host or by the timer of the start pin. The first request about a lease that
reached `urgentAtFraction` or ended goes under the pin all the same, once per value
of the lease: a long pin of the host would let the lease of the pinned
outbound end, and a kept set whose leases ended would never get live ones
under a start pin. Its retry waits for the release. A refresh asks about
the leases due by then, to be renewed or ended, whatever its trigger; when
it fails, they are asked about again after the backoff, and then only with
user traffic, so a dead backend does not wake the device.

**Limits.** Two refreshes start at least `minInterval` apart, or
`exhaustedMinInterval` for an exhausted balancer, an urgent renewal or a
lease that ended, host requests, and `whenCostAbove` with `after: "0s"`;
at most `maxRefreshesPerHour` start within an
hour. After a failure the next attempt waits for `backoff`, counted from
that attempt, so a failing refresh never runs in a loop. A refresh for an
exhausted balancer that brings the same set counts as a failure for the
backoff. A failed host request and a failed network
change refresh are retried after the backoff, or after the pause a busy
backend asked for.

When several triggers are due, one held by its interval does not hide a
later trigger whose interval permits a request. The shorter interval for
immediate cost feedback keeps all other gates, including low memory,
idle, pin, network, refused credentials, backoff and `Retry-After`.

## Probes on request

The host may know more than the observatory, for example that the tunnel
carried nothing after the device woke up, or that the user asked to
reconnect. `RequestProbe` asks for probes of a `leastCost` balancer now and
returns a ticket with the id of the request at once; the results appear in
`Status` under `probe_requests` and in `ProbeResult`. Ids start from 1 in
every run, so the host matches the `run` of the ticket with the one in
Status. The limits below are those of `schedule.hostProbes`.

- Without outbounds a request covers the outbound the balancer uses, then
  its candidates that are not lazy: the usable ones from the cheapest, then
  the ones in quarantine. Listed outbounds have to be candidates of the
  balancer and are taken as they are, lazy ones too. A request covers at most
  `maxOutbounds`. Before the first connection through the balancer it has no
  candidates, and a request fails.
- A request checks one representative of each explicit group, including
  when several listed tags belong to it. Before group quarantine ends,
  its representative is answered `down` with source `group_quarantine`.
  The default plan excludes lazy and expired members, and prefers a member
  outside individual quarantine. At equal price it starts at the hash position;
  resting representatives follow their price and each group's first appearance
  in the declared outbounds.
  An explicit list uses its first listed member of each group.
- An outbound in quarantine is answered `down` without a probe: the request
  does not shorten its rest. An outbound that works and proved it within
  `reuseWindow`, by a probe or by a connection that received `successBytes`, is
  answered `ok` without a probe. A probe of the observer that runs or is due
  answers the request first, and the probe of the host is then dropped.
- A balancer has one open request at a time: a request for a balancer with
  one open joins it and gets the same id.
- Probes of the host come last. They wait while a probe of the observer waits
  for the budget, start at most `probesPerMinute` times a minute and take at most
  half of the probe starts of a server name within a minute, so the
  observer always has budget left for finding failures. Otherwise the usual
  limits hold. They run without user traffic, but not without a network.
- The result is evidence like that of any other probe. A failed probe rules
  the outbound out and the balancer moves away from it (a pinned one reports
  `pinned_down` and stays); it ends a return under way to that outbound. A
  passed probe clears a suspicion as a false alarm and brings an exhausted
  balancer back with `recovered`. A passed probe of a cheaper outbound does
  not move the balancer and does not count toward a return.
- A request ends when every outbound has an answer, after `timeout` (probes
  that have not started are dropped, running ones finish), after a network
  change, or when its balancer is gone.

Results of an outbound:

| `result` | `source` | Meaning |
| --- | --- | --- |
| `pending` | | Waits for a probe. |
| `ok`, `failed` | `probe` | A probe answered, with `rtt_ms` and `error`; `probe_reason` names the probe of the observer that answered instead of one of the host. |
| `ok` | `recent` | It proved to work `age_ms` ago. |
| `down` | `quarantine` | It rests in quarantine for `quarantine_ms` more. |
| `skipped` | `memory` | Memory is low, see Memory. |
| `expired` | `timeout`, `network_change` | The request ended first. |
| `gone` | | The balancer no longer offers the outbound, or the balancer is gone. |

A host that calls in a loop costs at most `probesPerMinute` probes a minute and a
few kilobytes: the observer keeps the open requests and the latest four
closed ones.

## Sleep

An outbound that can release what it holds while idle, such as WireGuard
with its tunnel, network stack and their goroutines, creates them with its
first connection or probe and releases them when the observatory puts it to
sleep; its next connection creates them again, at the cost of a handshake.
The observatory decides when, since it knows what uses an outbound. An
outbound sleeps `sleep.afterLastUse` after its last use, and at once while it rests
in quarantine or memory is low, when all of this holds:

- it was used since it last slept: an outbound never used is never asked;
- no connection goes through it;
- no balancer chooses it, pinned or not: a chosen outbound never sleeps,
  however long it idles;
- it is not the target of a return, but while memory is low, and no
  failover under way probes it or tried it;
- no probe of it runs or is about to start, and no probe request of the
  host waits for it; a probe held back while the network is down, the
  device idles or memory is low does not count, as it wakes the outbound
  once it starts;
- no provider request goes through it, and it is neither the outbound of the
  network check nor a witness;
- it is not leaving its provider set, which the drain closes, nor having its
  handler replaced.

An outbound found down sleeps at once because its tunnel leads nowhere.
Kept up, a WireGuard device repeats its handshake to the dead address every
5 s for 90 s, and starts over with every packet its stack still sends, such
as the FIN that a connection which ended repeats for minutes: the battery
pays for it, and the address shows, until `sleep.afterLastUse`. Its connections
that received nothing were ended when it went down, and so are those that
receive nothing for `stallTimeout` after that, whether its traffic or a
failed probe ruled it out, see How it decides; those that still receive
keep it awake until they end.
The probe at the end of its quarantine wakes it, and one that fails puts it
to sleep again at once. One ruled out while the network check was failing
waits for `sleep.afterLastUse` as usual: it comes back as soon as the network does.

A use is a switch to or away from it, a probe, a connection the observatory
follows, the start and the end of a provider request through it as a `via`,
and the start and the end of every connection its handler gives it. A
connection that reaches it other than by a balancer, a rule straight at its
tag, a `fallbackTag` or a `dialerProxy` chain, creates its tunnel again all
the same, before it reports anything about the tunnel, and the observatory
follows the connections only of outbounds it judges: the handler of an
outbound that can sleep tells the observatory of each connection, so such
a use wakes it in Status too and it sleeps again after the last one. The outbound has the last word: it
refuses while a connection uses it. A refused sleep, or one an open
connection the observatory follows holds back, is asked for again after
`sleep.afterLastUse`, at least a minute, or after 5 s for an outbound found down,
doubling with every refusal in a row up to 30 minutes. A use starts the
count again, and the end of a connection the observatory ended has it ask
at once: an outbound found down sleeps as soon as the connections ended for
it are gone. Whether an outbound can sleep at all is asked at its first
use, and again only once a provider replaced its handler; one that cannot
is never asked to sleep. A sleeping outbound shows `is_asleep` in Status, and
every sleep writes `outbound_sleep`.
Without user traffic the observatory still wakes for this, once per used
outbound `sleep.afterLastUse` after its last use and for the retries; once
everything sleeps no timer is left. An outbound the observatory does not
observe never sleeps, even when a provider request goes through it.

The flows a phone opens through an outbound while a balancer chooses it,
such as push connections to ports 5228 and 5223, iCloud over QUIC or TLS,
outlive the choice and stay silent most of the time. On a path that works
nothing ends them, and they would keep the tunnel awake for good, with its
keepalives and their battery cost. With `closeIdleAfter`, an outbound that
can sleep, that nothing but its connections holds by the list above and
that no balancer has chosen for `sleep.afterLastUse` has its connections that
received nothing for `closeIdleAfter` ended, TCP and UDP alike, one
`idle_closed` per check. Their applications reconnect through the outbound
chosen now, and it sleeps with reason `idle` 10 s after the rest are gone,
the shortest `sleep.afterLastUse`, rather than `sleep.afterLastUse` again: a connection
that just ended may still be closing inside the tunnel, and its server
would repeat its end of it to a tunnel gone. A
connection that received anything within `closeIdleAfter` is kept, and so
is everything while a balancer chooses the outbound or something else above
holds it, while the network is down or in doubt, and until it has worked
for `closeIdleAfter` after a network change or an outage: silence then may
be the network's. A spliced connection is judged by what its socket
received; on a platform that does not tell, it is kept. A new connection
through the outbound is a use like any other, after which it waits
`sleep.afterLastUse` again. The observer notes what the connections have received
at the first turn after the outbound was left or used, and looks again
once, when the first of them may have been silent long enough: at the later
of `sleep.afterLastUse` after the last use and `closeIdleAfter` after its silence
began, a connection past `successBytes` counting from the note, as its
reads are no longer timed. Nothing is planned while there is nothing to
wait for, and each look reads the counters of the open connections once.

## Memory

What the observatory keeps does not grow with the length of a session or the
number of networks, outbounds and connections: what it knows about an
outbound is forgotten once no balancer offers it (one that can sleep once it
sleeps or its handler goes), failures per outbound are capped, probes end by
a deadline, and the config values that size anything have ceilings. For an
open connection of a watched outbound it keeps under 1 KB and no goroutine
of its own. The report keeps fixed counters, the latest 10 switches, and
window counters for up to 64 outbounds and their configured probe steps. A provider keeps its last applied answer for
`ProviderSet`, at most `maxBytes`, a kept initial set some 200 KiB more,
and replaced by the next one; the config drops its initial set once
applied.

A host that watches the memory of the process, such as an iOS network
extension close to its limit, reports pressure with `SetMemoryLow(true)` and
its end with `SetMemoryLow(false)`. While memory is low:

- What only improves the choice waits: returns to a cheaper outbound, also
  one under way between its probes, probes after a network change and of
  the outbound remembered for the network, and timed provider refreshes
  (`provider_skip` with reason `memory`), the renewal of a lease too until
  `urgentAtFraction` of it. A refresh the host asked for, or one for an exhausted
  balancer or an urgent or ended lease, still runs, through the first
  usable `via` only. A network check in doubt repeats at most every 2
  minutes.
- What can go goes at once: drains of outbounds without connections that no
  balancer chooses, the tunnels of outbounds no balancer chooses, which
  sleep without waiting for `sleep.afterLastUse` (see Sleep), and what the
  observatory knows about outbounds no balancer offers.
- Finding failures goes on as usual: confirming probes, failovers, rechecks
  of an exhausted balancer and the first network check. No user connection
  is ended for memory.
- What can be observed stays as it is: the event log keeps its limits, every
  event is written and the report counts as usual. Pressure is when a host
  most needs to see what happened, and the log costs a few kilobytes. The
  last answer of a provider stays too: it is state, not work, and without
  it a process killed under pressure loses a rotated token.
- Probes the host asks for go on only for outbounds a balancer uses. The
  others are answered `skipped` unless a probe of the observer answers them,
  their probes that have not started are dropped, and one closed request is
  kept.

Once memory is back, what waited goes on from where it stopped. A host that
never reports pressure, such as a desktop, gets the usual behavior.

## Host API

The `HealthService` gRPC service (enable it in `api.services`) and
`xray api health` expose the same calls; embedders can call the `Observer`
methods directly: `health.FromInstance(instance)`, or `health.Current()`
when the core runs through a library that keeps the instance to itself:

| Call | Purpose |
| --- | --- |
| `NetworkChanged(id)` / `xray api health network <id>` | The device moved to another network. `id` lets the observatory remember working outbounds per network; empty when the host cannot tell networks apart. |
| `UserPresent()` / `xray api health present` | The user unlocked the device. Queue checks of stale unpinned selected paths and return immediately; see User presence. The host supplies this signal through `HealthService.UserPresent` or the observer API. |
| `NetworkLost()` / `lost` | No network; decisions pause until the next change. |
| `SetMemoryLow(low)` | The process is short of memory, or no longer; see Memory. Safe from any goroutine, a repeated value does nothing. Only for embedders. |
| `Pin(balancer, outbound, run)` / `pin` | Keep the balancer on an outbound. |
| `Release(balancer, run)` / `release` | Let the balancer choose again. |
| `RequestRefresh(provider)` / `refresh [provider]` | Fetch the set of a provider, or of every provider when empty, as soon as the limits allow. False when there is no such provider. |
| `SetProviderHeaders(provider, headers)` / `headers <provider>` | Put headers over those of the config for the requests of a provider, see Token; an empty value removes one. The command reads them as a JSON object from stdin, never from the arguments, which the process list shows, and prints `{"ok":true}`. Ends the pause after refused headers, not a pause the backend asked for nor the backoff, and starts no refresh. An error names the header, never its value: no such provider, a name that is not a token or belongs to the transport, a bad value, more than 8 headers in the call or in all. |
| `ProviderSet(provider, after)` / `set <provider> [after]` | The last set the provider applied as JSON valid as its `initialAnswer`, including its original `outbounds[].path` and `groups`, with `seq`, a number that grows when the set, the end of a lease, the headers an answer or the host set or the base the answer names change. `seq` starts every run from the wall clock in milliseconds, so it grows across runs as well: a host may keep it with the set and ask with it after a restart. A clock set back between runs starts lower; a host that cannot rule that out asks with 0 once per run. The answer keeps its keys, with those headers in place of its own (the headers of the config stay out), `serverTime` the time its leases count from and `receivedAt`. Nil (`null`) when nothing was applied or nothing changed since `after`; then the call allocates nothing, so a host may ask next to `TakeReport`. The host keeps the set in safe storage, as it holds the token and the keys of the outbounds, and passes it as `initialAnswer` when it starts the core without a fresh answer. Over gRPC it goes to any client of the API, as outbounds do through `HandlerService`, so the API listens only where the host reaches it. |
| `RequestProbe(request)` / `probe <balancer> [outbound...]` | Probe outbounds of a balancer now, see Probes on request. Returns the ticket at once or an error: no such balancer, no candidates yet, not a candidate, too many outbounds. The command waits for the result up to its timeout (`-t`) and prints it; over gRPC `wait_ms` does the same. |
| `ProbeResult(id)` | One request as in Status and whether it ended; nil when it is no longer kept. |
| `Status()` / `status` | Current state as JSON, see below. |
| `DrainEvents()` / `events` | Events since the previous drain as a JSON array, and how many were dropped because the buffer was full. |
| `TakeReport(final)` / `report take`, `report final` | The report when one is due, and a new window; nil (`null`) when none is due. See Report. |
| `Report()` / `report` | The report a stop would bring now, as a snapshot that closes nothing; nil when it would bring none. |

`run` names the run of the core the request is meant for, as `Status()`
reports it; a request for another run is ignored, so a release sent to a
core that was started again in the meantime does not lift its new pin.
An empty `run` skips the check.

## Events

Every event is `{"seq": n, "ts": unix_ms, "type": "...", "fields": {...}}`.
`seq` grows by one per event, so gaps show dropped events.

| Type | Fields | When |
| --- | --- | --- |
| `switch` | `balancer`, `from`, `to`, `reason`, `held_ms`, `cost` | New connections go to another outbound. `reason`: `start`, `start_pinned`, `start_remembered`, `start_with`, `failover`, `return`, `recovered`, `remembered`, `network_change`, `network_up` (the network check passed again and brought back outbounds ruled out while it failed, to a balancer that had nothing usable), `candidates_changed`, `pin`, `retired` (the previous one left its provider set), `expired` (the lease of the previous one ended; one that failed as well is left with `failover`). `held_ms` is the time on the previous one. |
| `path_state` | `outbound`, `state`, `reason`; for `suspect` also `failures`, `targets`, `kinds`, `is_strong`; for `down` also `quarantine_ms`, `quarantine_level` and `is_provisional` when it was ruled out while the network check was failing; for `rescued` also `by`: `confirm` or `audit` | An outbound became `up`, `degraded`, `suspect`, `down` or `unknown`. For `degraded`, `reason` is the first failed optional step. Other reasons: `traffic`, `probe`, `return_probe`, `host_probe`, `network_change`, `network_down`, `network_up`, `replaced` (a provider
changed its settings), `probe_changed` (a provider changed the profile of an unchanged outbound), `rescued` (a confirmation already running or an audit assigned to the down proved it false, see How it decides). `kinds` counts failures by kind: `dial_failed`, `dial_timeout`, `server_unverified`, `no_response`, `closed`, `stall`, `partial_stall`. For an outbound with a tunnel, `dial_failed` and `dial_timeout` tell that the tunnel could not be set up or that its server did not answer a connection, see How it decides. |
| `probe_start` | `outbound`, `reason`, `balancer`, `running`, `request` | A probe started. `reason`: `confirm`, `failover`, `network_change`, `wake`, `remembered`, `recheck`, `lone`, `audit`, `return`, `group_recheck`, `host`. `running` counts the outbound probes running now, this one included. `request` is the id of the request a `host` probe is for. |
| `probe_result` | `outbound`, `reason`, `balancer`, `is_ok`, `rtt_ms`, `bytes`, `error`, `is_stale`, `request`, `profile`, `state`, `steps` | A probe finished. `steps` lists only attempted steps with `name`, `is_passed`, `connect_ms`, `ms`, `bytes`, a `destination` index on success or an `error` class on failure. `bytes` counts both directions above TLS. `is_stale` means the network, handler or probe profile changed while it ran, so its result did not change the path. Its attempted steps still count in the report. |
| `group_state` | `balancer`, `group`, `state`, `members`, `failed`, `quarantine_ms` | An explicit group became `down` after consecutive failures or `up` after a successful recovery check. Unvisited members of a down group are skipped. |
| `probe_request` | `request`, `balancer`, `outbounds`, `end`, `ms`, `probed`, `reused`, `ok`, `failed`, `down`, `skipped`, `expired`, `gone` | A request of the host that waited for a probe ended; one answered at once writes none. `end`: `done`, `timeout`, `network_change`, `gone`. The counts are by result; `probed` and `reused` count the answers of a probe and of a recent proof. |
| `exhausted` | `balancer`, `from`, `candidates` | No usable candidate is left; traffic goes to `fallbackTag`. |
| `pending_closed` | `outbound`, `connections`, `reason`, `is_silent` | Connections of an outbound found down were ended: at the decision and at the switch away from it those that had received nothing, or, with `is_silent` true, `stallTimeout` after the decision those that received nothing since it, see How it decides. `reason` is the one the outbound went down for. |
| `return_start` | `balancer`, `from`, `to` | Probing a cheaper outbound to return to it. |
| `return_failed` | `balancer`, `outbound`, `successes` | The return was abandoned after a failed probe. |
| `return_delayed` | `balancer`, `outbound`, `delay_ms` | The return waits longer because the previous one failed. |
| `return_penalty` | `balancer`, `outbound`, `multiplier` | A return failed within `penaltyWindow`. |
| `pin`, `release` | `balancer`, `outbound`; for `release` also `by`: `host` or `timer` | The host pinned or released a balancer, or the start pin ran out. |
| `stale_request` | `request`, `balancer`, `run` | A pin or release meant for another run of the core was ignored. |
| `pinned_down` | `balancer`, `outbound`, `is_start_pinned` | The pinned outbound failed; the balancer stays on it. `is_start_pinned` is `true` when the pin is `startPinnedOutbound` rather than one of the host. |
| `connectivity` | `is_ok`, `rtt_ms`, `error` | Result of a network check. |
| `network_down` | `error` | The network check failed; decisions paused. `error` is `host` when the host reported the loss. |
| `network_up` | `by` | Decisions resumed: `connectivity` (a check passed), `traffic` or `probe` (an outbound worked). |
| `connectivity_doubt` | `failed_checks`, `paused_ms` | The check kept failing for `maxPause`; decisions resumed provisionally. |
| `connectivity_ignored` | `failed_checks` | The check failed while an outbound worked; it is not used until the next network change. |
| `network_change` | `network`, `is_known`, `generation`, `is_ignored` | The host reported a network change. `is_ignored` when no balancer forgets on it, as each has `networkChangeAction` `ignore`: what was learned stays and `generation` does not grow. |
| `memory_low` | `is_low` | The host reported that memory is low (`is_low` true) or no longer. |
| `provider_fetch` | `provider`, `trigger`, `retry`, `attempt`, `via`, `outbound`, `method`, `refresh`, `is_ok`, `status`, `ms`, `bytes`, `code`, `retry_after_ms`, `error` | One request of a refresh through one `via` entry; `outbound` is what the entry resolved to, `refresh` the number of the refresh as the body tells it, `bytes` the size of the answer. `code` is the code of an error body of the protocol, `retry_after_ms` the pause `Retry-After` asked for, after the ceiling and before the spread. A skipped entry has `is_ok` false and only `error`: `balancer exhausted`, `outbound down` or `no such outbound`. `error` never holds the URL. `trigger`: `host`, `exhausted`, `expiry`, `min_healthy`, `cost_above`, `network_change`, `next_refresh`, `max_age`, `interval`; `retry` is the level of the backoff, as the body of the request tells it: refreshes in a row since the last success that failed, brought an answer the core refused, or brought an exhausted balancer the same set; a `Retry-After` pause and refused headers leave it as it is. An answer that breaks the protocol has `is_ok` true: the backend answered, and the refusal is the `provider_apply`'s. |
| `provider_apply` | `provider`, `trigger`, `is_ok`, `is_changed`, `added`, `replaced`, `removed`, `unchanged`, `version`, `was_exhausted`, `rules`, `routing_error`, `is_routing_ignored`, `leases`, `renewed`, `expired`, `skew_ms`, `clock`, `headers`, `is_headers_stale`, `base_version`, `error` | The answer was applied (`is_changed`), was the applied set already, or was rejected (`is_ok` false with `error`); `trigger` `initial` is the initial set at the start. `was_exhausted` tells whether the balancer had nothing usable at that moment. `rules` counts the routing rules applied with the set; `is_routing_ignored` tells that the answer had routing while `apply.isRoutingAccepted` is off. When the answer names leases: `leases` counts them, `renewed` the ones it gave a later end that has not passed, ended ones too, `expired` the leases that ended after it, where one that had ended and got a later end that passed as well gives no second `provider_expired`; `skew_ms` is how far the clock they count from is ahead of the device, and `clock` which one that is: `server`, `date` (the `Date` of the answer plus `Age`), `saved` (the `serverTime` of a kept initial set plus its age) or `device`. `headers` names the headers of the answer; `is_headers_stale` tells that the host changed the headers while the request ran, and the answer did not set them, or that the host had set some names of the initial set before the start, which stay its own. `base_version` is the base config the answer named. |
| `provider_expired` | `provider`, `outbound`, `was_active`, `renewal` | The lease of an outbound ended, or an answer brought it ended. `was_active` tells whether a balancer chose the outbound. `renewal` tells why no later end came: `refused` (the backend answered without one), `failed` (the last refresh that asked failed), `off` (`renewAtFraction` is 0), `saved` (it ended in a set the host kept, before the start) or `pending` (no refresh that asked about it has ended: no network, the limits, or the one that asks is still under way). |
| `provider_auth_failed` | `provider`, `trigger`, `status`, `via`, `code` | The backend refused the headers of a request with `status` 401 or 403 and the error `code` `unauthorized` or `forbidden`; every trigger but the host waits, see Token. |
| `provider_headers` | `provider`, `by`, `names` | Headers of the requests changed: `by` an `answer`, the `initial` set or the `host`. `names` are the headers whose values changed; the values are never told. |
| `provider_base_changed` | `provider`, `from`, `to` | An applied answer named the base config `to` while the core runs with `from`: the host is to fetch the config anew, see Base config. Once per value, until an answer names `from` again. |
| `provider_restart_needed` | `provider`, `version`, `sections` | Reserved for an answer with sections of the config the core reads only at the start. Protocol 1 refuses such an answer, see [PROVIDER.md](PROVIDER.md#answer), so this version never writes it. |
| `provider_skip` | `provider`, `trigger`, `reason`, `wait_ms` | A trigger wants a refresh that cannot start yet. `reason`: `min_interval`, `backoff`, `retry_after`, `max_per_hour`, `idle`, `network_down`, `pinned` (the balancer of the provider is pinned), `memory`, `auth` (the backend refused the headers). Reported once per trigger and reason until a refresh starts. |
| `provider_drained` | `provider`, `outbound`, `is_replaced` | An outbound that left the set, or the old handler of a replaced one, was closed: after `drainTimeout`, or at the release of a pin that held it past that, at once without connections, or early to keep at most `maxOutbounds` draining. |
| `outbound_sleep` | `outbound`, `idle_ms`, `reason` | An outbound released what it held, see Sleep. `idle_ms` is the time since its last use; `reason` is `idle` (after `sleep.afterLastUse`, or 10 s after the last connection ended once `idle_closed` ended its idle ones), `down` (sooner, while it rests in quarantine) or `memory` (sooner, while memory is low). |
| `idle_closed` | `outbound`, `connections` | Connections of an outbound that can sleep, which no balancer has chosen for `sleep.afterLastUse`, were ended after they received nothing for `sleep.closeIdleAfter`, see Sleep. The path works: unlike `pending_closed`, no failure is behind it. |

## Report

The observatory adds up its events into a report for the host to send, so
the host needs neither the event log nor counters of its own. Every event is
counted as it is written: a full log, or a host that never drains it,
changes nothing in the report.

A decision is a `switch` for any reason but `start`, `start_pinned`,
`start_remembered` and `start_with`, `exhausted`, `return_failed`,
`pinned_down` without `is_start_pinned` (the host that verifies the outbound
of the start pin reports a failure itself), a `provider_apply` that was
rejected or changed the set, `provider_restart_needed`, `provider_expired`,
`provider_auth_failed` and `provider_base_changed`. The initial set is no
decision, as a start switch is not, nor a lease that ended in a set the
host kept before the start, the usual morning after a device was off;
otherwise every session would have one. A lease that came ended in the
answer of a connect is. A refused initial set is. Events the log dropped
are no decision.

`TakeReport(final)` returns the report when one is due and starts a new
window:

- `final`, before the host stops the core: due when the run had a decision or
  was selected by `report.quietShare`, with reason `stop`. Nothing is due
  after it, final or not.
- Otherwise due once a window with a decision lasts `report.interval` by the
  wall clock, which goes on while the device sleeps; reason `interval`. A
  decision after a window that long without one goes with the next call.

The core keeps no timer for it: the host asks, for example every 30 seconds,
and a call with nothing due allocates nothing. Low memory does not make a
report due. A host whose process may be killed without a stop keeps the
snapshot of `Report()`, reason `snapshot`, replaces it after every report it
takes, and sends the kept one when it starts again; the reason it sends is
its own choice.

`report.quietShare` selects a fraction of core runs once at startup (0 by
default). A selected run can produce a final report and a snapshot without
any decision; `is_quiet_sample` identifies it. Quiet runs do not create
interval reports merely because the interval elapsed.

Counters cover the window: from the start of the run or the previous report
to this one. Reports of one run add up.

A controlled 64-path P3 fixture produces a 35,998-byte report; removing
only the new UDP, down-strength and quiet-sample fields from that same JSON
leaves 12,358 bytes, a 23,640-byte increase. The stripped shape is a
comparison fixture, not a separate P2 binary measurement.
On an Apple M4 Pro with Go 1.26.4, five serial steady-state samples of
`BenchmarkObservedUDPLinkRealHealthUplink` have a median of 14.42 ns/write,
versus 1.076 ns/write without observation, with zero allocations in both.
The benchmark uses the real tracker and checks its final public report;
it excludes network I/O, initialization and concurrent flows. UDP accounting
uses a short per-flow mutex and lifecycle coordination in addition to
atomic counters, so its cost is measured rather than described as one
atomic operation. The functional live matrix covered REALITY, relay, local WS,
local XHTTP and AWG, with focused controls for unanswered flows and mux reconnect.
Long duration and performance acceptance were excluded by the project decision.

| Field | Meaning |
| --- | --- |
| `health_run` | The run of the core, as `run` in Status. |
| `report_reason` | `stop`, `interval` or `snapshot`. |
| `report_index` | Number of the report in the run, from 1. |
| `is_quiet_sample` | This run was selected by `report.quietShare`; selection does not change between report windows. |
| `window_ms`, `session_ms` | Length of the window and of the run in milliseconds of the wall clock. |
| `events` | Events written in the window. |
| `events_dropped` | Events the log dropped in the window because it was full: they never reached `DrainEvents`. |
| `switches`, `switch_reasons`, `switch_path` | `switch` events, their count by reason, and the latest 10 as `from>to:reason`. |
| `exhausted_count`, `pinned_down_count` | Events of those types. |
| `path_down_count`, `path_suspect_count`, `path_degraded_count` | `path_state` events with those states. |
| `group_down_count` | `group_state` events with state `down`. |
| `path_rescued_count` | `path_state` events with reason `rescued`: downs a confirming probe or audit proved false. A rescue changes no choice by itself and is no decision; a balancer it brings back switches with `recovered`. |
| `pending_closed_count` | Connections of outbounds found down ended by `pending_closed` events: those that had received nothing and, with `is_silent`, those that went silent after the decision. |
| `idle_closed_count` | Connections of outbounds no balancer chooses ended by `idle_closed` events, so that the outbounds can sleep. Ending them changes no choice and is no decision. |
| `probes`, `probes_failed`, `probe_bytes` | `probe_result` events, the failed ones, and their `bytes`. |
| `host_probe_requests`, `host_probes` | `probe_request` events, and `probe_result` events with reason `host`. |
| `return_attempts`, `return_failures` | `return_start` and `return_failed` events. |
| `network_changes`, `network_known`, `network_down_count` | `network_change` events, the ones with `is_known`, and `network_down` events. |
| `provider_fetches`, `provider_fetch_failures`, `provider_triggers` | `provider_fetch` events, the ones without `is_ok`, and their count by `trigger`. |
| `provider_retry_after` | `provider_fetch` events with `retry_after_ms`: a busy backend asked to wait. |
| `provider_applied`, `provider_rejected`, `provider_restart_needed` | `provider_apply` events that changed the set, but for the initial set, the rejected ones, and `provider_restart_needed` events, which this version never writes: always 0. |
| `provider_expired` | `provider_expired` events: leases that ended. |
| `provider_renewals` | `provider_apply` events with `is_ok` and `renewed` above zero, but for the initial set: answers that renewed leases. A renewal keeps what the balancer has and is no decision. |
| `provider_auth_failed` | `provider_auth_failed` events: the backend refused the headers. |
| `provider_base_changed` | `provider_base_changed` events: the backend serves another base config. |
| `pins`, `releases` | `pin` and `release` events. |
| `memory_low_count` | `memory_low` events with `is_low`. |
| `outbound_sleeps` | `outbound_sleep` events: outbounds that released what they held. A sleep changes no choice and is no decision. |
| `active`, `is_balancer_exhausted`, `is_network_up`, `connectivity` | At the time of the report: the chosen outbound by balancer, whether a balancer is exhausted, and the network as in Status. |
| `paths` | The first 64 outbounds by tag. `state` and `ttfb_p50_ms` (over the whole life of the outbound, 0 without samples) as they are now; `active_ms`, `degraded_ms`, `bytes_up`, `bytes_down`, `conns_ok`, `conns_failed`, `probes` and `probe_bytes` cover this report window. `probe_steps` gives `name`, `passed` and `failed` for steps actually attempted in the window, including stale probes. Steps follow the current profile order; names from an earlier profile in the same window follow in their first observed order. An outbound forgotten and seen again counts from zero. |
| `wake_probe_count`, `wake_probe_failed_count`, `wake_skipped_count` | Started wake probes, completed failed wake probes (including stale results), and eligible unique selected paths skipped per host call because proof is fresh or the wake interval/job limit applies. A queued cancellation has no start count; a canceled probe has no result count. Other probe work, pins, exhausted balancers, or unavailable networks add no skip count. |
| `lone_probe_count`, `lone_probe_failed_count` | Started lone probes and completed failed lone probes, including stale results; canceled probes have no result count. |
| `lone_probes`, `lone_probes_failed` | The same lone counters for this outbound in the report window. A failed current lone probe contributes one `downs_strong`; a stale completion contributes no down. |
| `downs_strong`, `downs_weak` | New downs backed by strong or weak passive evidence for that outbound in this window. They include failed current lone probes paired with a dial failure, but exclude pure wake and network-change probe failures. Use `wake_probe_count` and `wake_probe_failed_count` for the wake failure rate; passive down counters are not its denominator. |
| `udp_flows`, `udp_flows_unanswered` | Routed UDP link trackers that ended in this window; unanswered means at least one uplink buffer and no downlink bytes. A SOCKS UDP association can create more than one routed link after mux reconnect. An open tracker belongs to the window where it ends; final stop ends all open trackers. |
| `udp_bytes_up`, `udp_bytes_down`, `udp_writes`, `udp_reads` | Payload bytes and nonempty buffers at the logical outbound link. Each buffer is approximately one UDP packet. Downlink is counted before the underlying writer, so bytes received by the core do not imply that the application read them. These fields do not include proxy framing or AWG handshakes; the existing mixed `bytes_up/down` and `conns_ok/failed` keep their transport semantics. |
| `udp_first_reply_p50_ms` | Median time from the first uplink buffer to the first downlink buffer for routed links ending in this window, at the histogram bucket upper bound; 0 without samples. |
| `udp_ports` | Nonzero `dns` (53), `quic` (443), `stun` (3478-3479 and 19302), `rtc` (50000-65535), and `other` buckets. Each contains `flows` and `unanswered`. The first routed destination port fixes the bucket for the whole routed link, even if later packets name other destinations. |
| `groups` | Current explicit-group summaries as described in Outbound groups. |
| `quality` | Measured network segments of this report window, each with top-four admitted paths and optional witness; see Quality measurements. |
| `quality_segments_dropped` | Nonzero count of older nonempty quality segments discarded by the four-segment limit, reset when a report is taken. |

Per-outbound fields `audits`, `audits_passed`, `lone_probes`, `lone_probes_failed`,
`downs_strong`, `downs_weak` and all `udp_*` fields belong to `paths.<tag>`.

## Status

`Status()` returns the run of the core (`run`), the network (`network`, `is_network_up`, `connectivity`:
`ok`, `paused`, `doubt` or `ignored`, `generation`, `is_idle`), whether memory is low (`is_memory_low`), probe queue sizes, per outbound the state and its age, failures in
the window, quarantine, last probe RTT and error, connections, bytes, time to
first byte (p50, p90), failures by kind, probes and their bytes, time as
the chosen outbound, what is left of its provider lease
(`expires_in_ms`, below zero with `is_expired` once it ended) and whether it
sleeps (`is_asleep`); per balancer the chosen and pinned outbound, whether it
is exhausted (`is_exhausted`), the time on the chosen outbound and the next return attempt
(`return_in_ms`, `return_target`); per provider whether a fetch runs (`is_fetching`), the method, the version
of the set, its size, outbounds still draining, refreshes and failures, the
age of the set, the next timed refresh, the backoff, what is left of a pause
a busy backend asked for (`retry_after_ms`), the next renewal of a lease
(`renew_in_ms`, below zero while it waits for a limit), the leases that
ended (`expired`), the `skew_ms` and `clock` of the last answer as in
`provider_apply`, none when it named no lease, the names of the headers an
answer or the host set (`headers`), the number of the set `ProviderSet`
returns (`answer_seq`), the base config the core runs with (`base_version`),
the one the last answer named (`answer_base_version`) and whether another
base was told that no answer took back (`is_base_changed`), and whether the backend refused the
headers (`is_auth_refused`) with what is left of the pause (`auth_retry_in_ms`,
below zero once a refresh may try them again, none while it waits for new
headers); the probe requests of the host (`probe_requests`), each with `id`,
`balancer`, `age_ms`, `is_done`, `end` and the result per outbound, see Probes
on request.

Each path also has `probe`, the last finished result in the current network:
`profile`, `reason` (the probe job's reason), `state`, `ago_ms` and attempted `steps`. Step keys are snake_case:
`name`, `is_passed`, `connect_ms`, `ms`, `bytes`, `destination` only for a
successful address (zero is valid), and `error` only as a failure class.
`connect_ms` measures connection establishment and `ms` the complete step.
The result disappears on a network change. The event keeps stale results for
traffic diagnostics, without replacing this Status snapshot.
## Quality measurements

User TCP connections produce quality summaries for paths and the aggregate of
configured `connectivity.witnesses`. These measurements never change path state,
balancer choice, probes, quarantine, or provider triggers.
`passive.shouldSkipQuality` defaults to `false`; `true` disables these connection
measurements. UDP trackers and the observatory's own probes add no quality samples.
The legacy TTFB fields keep their lifetime scope and existing semantics.

The five signals are TCP TTFB, successful own-socket dial time, TCP socket
statistics, counted bulk-download speed, and the same measurements for witness.
TTFB runs from the first request write, or from the dial when there was no write,
to the first reply. Dial time is recorded only when the successful dial returns
an individual raw socket; it includes that transport's handshake and any DNS.
S1 is read from the open server socket at the first reply, before the observer
lock. S2 is read when counted download bytes cross 1, 4, 16, or 64 MiB. One read
crossing several marks uses one clock read and one S2; quality adds no clock or
socket reads between marks. The latest successful S2 survives a later failed read.

Bulk speed uses the observed byte and time difference between the first reply
and the last counted mark, in decimal Kbit/s. The tail and the time until
completion do not change it. It needs a positive byte/time interval. Spliced
downloads have no speed sample; completed spliced downloads of at least 1 MiB
increment `bulk_spliced_count`. After splice starts, an eligible path can retain
TTFB and S1 when its existing passive socket sweep observes real receive
progress on the open socket. Aggregate bytes arriving at completion cannot
supply a first-reply timestamp. Witness has no such socket sweep, so splice
before its first counted reply can leave its TTFB and S1 unavailable. Quality
adds no polling.

Mux quality describes the observed physical TCP connection to the server:
counts, first reply, byte marks, and socket statistics belong to the channel
shared by its user streams. Datagrams carried inside it add no separate UDP
quality sample.

Unsupported or unmeasured fields are absent; measured zeros remain present.
Linux checks the length returned by `getsockopt` before reading each field:
RTT and `Min_rtt` are microseconds, `Min_rtt = ~uint32(0)` is unavailable, and
zero `Rcv_rtt` is unavailable. Darwin `Srtt` is milliseconds. Its reported
minimum is the minimum sampled S1 `Srtt` in the path or witness scope, not a
kernel lifetime minimum; S2 `Srtt` is a coarse load indicator. Linux RX ratios
use packets, Darwin RX ratios use bytes. RX out-of-order is an indicator, not
a packet-loss percentage. Windows and transports without an individual raw
socket supply no TCP_INFO fields; counted TTFB and speed can still be available.
CDN sockets measure the path to the CDN edge. Witness can reach different
destinations, so absolute witness and path RTTs need not be comparable.

Reports carry optional `quality` segments and nonzero
`quality_segments_dropped` when older segments were discarded. A segment has
`is_network_known`, `duration_ms`, `paths`, optional `witness`, and nonzero
`paths_dropped` or `samples_dropped`. `is_network_known` records whether the
network was remembered in this core run when the segment began. Quality does
not parse the opaque `NetworkChanged(id)` string or export network type,
operator, or fingerprint. Network/path/session labels belong to the separate
measurement-context feature and are not implemented here. The existing local
Status/event network ID and provider-specific `network.id` hash keep their
contracts; quality adds no field to `network_change`.

Current-network and open-window collections each admit the first 64 measured
tags with at most 16 KiB of copied tag bytes, retaining their complete history
even after retirement. Witness is separate and consumes neither tag budget.
The report retains the latest four nonempty segments, each with the four
busiest admitted paths by completed `bytes_down`; exact tag order breaks ties.
`paths_dropped` counts admitted tags outside that projection. `samples_dropped`
counts rejected meaningful phases, not distinct rejected tags. Rejected tags
may be busier than retained tags, so ranking is exact among admitted tags;
`bytes_down_overflow` makes the true ordering of saturated byte totals unknown.

Every `NetworkChanged`, including the same or empty ID and IGNORE routing
policy, advances an independent measurement epoch and resets current quality.
Routing `generation` can stay unchanged under IGNORE. Later phases of old-epoch
connections are discarded; already accepted phases stay in their old segment.
`NetworkLost` alone does not advance the epoch. A successful nonnull report take
starts a fresh window without resetting current-network Status/provider history.
Snapshots and nil takes close nothing. Initial timing and completion data may
therefore belong to different report windows; this is completion accounting,
not a byte-stream split at the window boundary.

Completion never waits for an in-flight quality socket sample. The collector
under the observer lock fixes the byte cutoff and closes sampling admission.
The last admitted sample publishes the quality end once. If a report was taken
meanwhile, the delayed phase enters the then-available window; an intervening
network change discards it. Returned snapshots stay unchanged. Final reports
remain terminal: data published later cannot enter that run's report. A host
gathering completion evidence waits with a fixed timeout before its final take
and records incomplete coverage if publication has not completed.

Status attaches `paths[].quality` to existing policy paths by exact full tag,
with optional `witness_quality` and nonzero `quality_samples_dropped` for the
current network. Reading Status creates no policy path for a retired tag.
Providers get `outbounds[].quality` only for their own registered set, plus
optional `witnessQuality` and nonzero `qualitySamplesDropped`; see
[PROVIDER.md](PROVIDER.md#quality-measurements).

| Quality field | Meaning |
| --- | --- |
| `bytes_down` | Bytes received by completed connections, including measured zero. |
| `bytes_down_overflow` | Present and true when cumulative bytes saturated at int64 maximum. |
| `ttfb_count`, `ttfb_p50_ms`, `ttfb_p90_ms` | TCP first-reply count and quantiles. |
| `dial_count`, `dial_p50_ms`, `dial_p90_ms` | Successful own-socket dial count and quantiles. |
| `tcp_count`, `tcp_rtt_p50_ms`, `tcp_rtt_p90_ms`, `tcp_rtt_min_ms` | S1 RTT count and quantiles; minimum measured S1 minimum RTT, in output milliseconds. |
| `bulk_count`, `bulk_kbps_p50`, `bulk_kbps_p90`, `bulk_kbps_max` | Valid counted-download speed count and quantiles; maximum in output Kbit/s. |
| `bulk_rtt_p50_ms` | S2 load RTT quantile, independently of speed availability. |
| `bulk_spliced_count` | Spliced bulk completions; known zero remains present for counted bulk connections. |
| `rx_out_of_order_fraction` | Sum of valid S1-to-S2 out-of-order deltas divided by received deltas. Decreasing counters and zero denominators are omitted. |
| `tx_retransmit_fraction` | Retransmitted/sent totals from each completed connection's last successful snapshot, included once. |

TX uses a saved S2 snapshot if present, otherwise S1; an absent TX pair in
that selected S2 is not replaced by the S1 pair. Fractions are ratios of sums,
not averages of connection ratios. Quantiles use inclusive bucket bounds and
rounded sample ranks, at least one. The unbounded bucket returns the last
finite bound: 8000 ms or 500000 Kbit/s. Counts are needed to assess sparse
samples. User-traffic speed is a lower bound affected by reader pauses, slow
start, and the application's bitrate; compare its p90 and maximum with witness.
