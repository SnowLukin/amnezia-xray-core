# Provider protocol 1

This is the contract between the core and a backend that serves the set of
a provider: what the core sends, what it accepts and how it reacts. The
client side, the config of a provider and what the core does with a set,
is in the [README](README.md#providers). The words must, should and may
are meant as in RFC 2119.

A backend serves one protocol for every client. There are no adapters per
client: what a client does not understand, it refuses, and it says so in
its next request.

## Exchange

1. A trigger of the provider fires: an exhausted balancer, a lease due for
   renewal, a timer, the host and so on, see
   [When the core asks](#when-the-core-asks).
2. The core sends one request through the first `via` entry of the
   provider, an outbound or the current choice of a balancer, and on to
   the next entry while the answer may have broken on the way. Every
   attempt of one refresh sends the same body.
3. The backend answers with a set of outbounds and what goes with them, or
   with an error.
4. The core checks the whole answer before anything changes. An answer it
   accepts replaces the set of the provider in the balancer without a
   restart; one it refuses changes nothing, and the reason goes with the
   next request.

Keys are camelCase in the request and the answer. The request carries
`"protocol": 1`. An answer carries `"protocol": 1` or no `protocol`, never a
protocol above the one of the request, and only keys of its protocol: a new
key of the answer is a new protocol. A backend must ignore fields of the
request it does not know, since protocol 1 may gain fields in the request.

## When the core asks

The core asks only when it has a reason, and never more often than its
limits allow. The `trigger` of the request names the reason:

| `trigger` | The core asks because |
| --- | --- |
| `host` | The host asked, for example after the user pressed reconnect. |
| `exhausted` | The balancer has no usable outbound left. |
| `expiry` | A lease reached its renewal point or ended, see [Leases](#leases). |
| `min_healthy` | Fewer outbounds of the balancer work than the config wants. |
| `cost_above` | The chosen outbound costs at least `whenCostAbove.cost`. With `after: "0s"`, the request starts in the same turn that chooses it, subject to the limits and gates; a positive `after` waits that long. |
| `network_change` | The device moved to a network where the set does not work as well. |
| `next_refresh` | The time the last answer asked for with `nextRefreshIn` came. |
| `max_age` | The set is older than the config allows. |
| `interval` | The regular refresh, `interval` after the previous one. |

By default two refreshes start at least 5 minutes apart, or 30 seconds for
an exhausted balancer, an urgent renewal, the host and cost feedback with
`whenCostAbove.after: "0s"`; at most 6 start
within an hour; after a failure the next waits for a backoff that starts at
30 seconds and doubles up to 30 minutes, spread by 30%. Without user
traffic only the host and the first renewal of a lease ask. While the
network is down only the host asks. While the balancer of the provider is
pinned, by the host or by the start pin of the config, only the host and
the first request about a lease that reached its urgent point or ended ask,
once per value of the lease: a host that verifies the pinned outbound stops
the core when the check fails, and a refresh would only load the backend,
but a lease must not end under a long pin. A retry of that request waits
for the release. While memory is low only the host, an exhausted balancer
and an urgent renewal ask, through the first usable `via` only. These
limits live in the memory of the core: a host that restarts the core in a
loop resets them, so a backend must limit requests by token on its side as
well.

## Request

### Transport

- `POST` of the URL of the provider with the body below and
  `Content-Type: application/json`, the default. A provider that serves a
  static file on a web server is configured with `GET`, which sends no
  body.
- `Accept: application/json`, unless the config sets another.
- The headers of the config, over them those of the initial set, and over
  both the headers an answer or the host set since, see [Token](#token). A
  header the host set before the start lies over the one of the same name
  in the initial set; the other headers of the set apply. Credentials
  travel in headers, never in the URL.
- `https`; plain `http` only where the client allows it explicitly.
- Each attempt has its own timeout, 10 seconds by default. Redirects are
  not followed: a 3xx is a failure of that attempt.
- An answer larger than the limit of the provider is a failure: by default
  4 KiB per outbound the set may hold, 256 KiB for 64 outbounds, at most
  1 MiB.

### Body

```json
{
  "protocol": 1,
  "provider": "fresh",
  "run": "9f2c6a1e5b7d3c40",
  "refreshNumber": 7,
  "trigger": "expiry",
  "retryLevel": 1,
  "lastFailure": { "status": 503, "error": "status", "agoMs": 61000 },
  "version": "slots-7781",
  "baseVersion": "cfg-12",
  "clientTime": "2026-10-08T12:00:03Z",
  "network": { "id": "5d0c1f2a9b3e4c71", "isKnown": false, "isUp": true },
  "isMemoryLow": false,
  "balancer": { "tag": "auto", "current": "x2", "isOwn": true, "cost": 3, "isExhausted": false, "isPinned": false,
                "heldMs": 840000 },
  "outbounds": [
    { "tag": "x1", "state": "down", "reason": "traffic", "stateMs": 81000, "seenMs": 3600000,
      "quarantineLevel": 2, "quarantineMs": 81000, "failures": { "dial_timeout": 4 },
      "probe": { "profile": "full", "state": "down", "steps": [
        { "name": "transport", "isPassed": false, "connectMs": 800, "ms": 1800, "bytes": 0, "error": "timeout" }
      ], "isPassed": false, "error": "timeout", "agoMs": 30000 },
      "connsOk": 12, "connsFailed": 4, "bytesUp": 90112, "bytesDown": 1048576,
      "activeMs": 600000, "ttfbP50Ms": 300, "expiresInMs": 61200000 },
    { "tag": "x2", "state": "degraded", "probe": { "profile": "full", "state": "degraded", "steps": [
        { "name": "transport", "isPassed": true, "connectMs": 80, "ms": 180, "bytes": 32768, "destination": 0 },
        { "name": "egress", "isPassed": false, "connectMs": 70, "ms": 120, "bytes": 0, "error": "closed" }
      ], "isPassed": true, "rttMs": 180, "agoMs": 840000 },
      "connsOk": 412, "bytesDown": 52428800, "activeMs": 840000, "expiresInMs": 61200000 },
    { "tag": "x3", "state": "unknown", "expiresInMs": -5000 }
  ]
}
```

Legacy fields with zero or empty values are left out, except those marked always.
Inside quality, measured zeros remain present; unavailable measurements are omitted.

| Field | Meaning |
| --- | --- |
| `protocol` | Always 1. |
| `provider` | Always. The tag of the provider: one backend may serve several providers of one client, for example one per kind of outbound. |
| `run` | Always. Names the run of the core, hashed for this provider: the first 16 hex digits of SHA-256 of the URL, a zero byte and the run. Stable within a run; two backends cannot match theirs. |
| `refreshNumber` | Always. Grows with every refresh of the run and is the same in every attempt of one refresh: `run` and `refreshNumber` together tell a request sent again through another `via` from a new one. |
| `trigger` | Always. Why the core asks, see [When the core asks](#when-the-core-asks). |
| `retryLevel` | Always. The level of the backoff this refresh waited for: refreshes in a row since the last success that failed, brought an answer the core refused, or, for an exhausted balancer, brought the same set again. A pause the backend asked for with `Retry-After` and refused headers leave it as it is; any other applied answer sets it back to 0. |
| `lastFailure` | Only when the previous refresh failed: `status`, the HTTP status or 0; `agoMs`, how long ago; `error`, the reason the core refused a well-formed answer (at most 200 bytes, see [Answer](#answer)) or the class of the failure: `timeout`, `network`, `status`, `too_large` or `broken_json`. It is what the last `via` that was asked met. |
| `version` | The set the core applies now, as the answer named it; none before the first set. |
| `baseVersion` | The base config the core runs with, see [Base config](#base-config); none without one. |
| `clientTime` | Always. The clock of the device in RFC 3339, UTC: the backend sees how far off it is. |
| `network` | Always. `id` names the network the host reported, hashed for this provider like `run` and left out when the host cannot tell networks apart; `isKnown`, always, whether the core remembers an outbound that worked in this network; `isUp`, always, whether the network works. |
| `isMemoryLow` | Always. The host reported that memory is short: the core asks only when it must. |
| `balancer` | Always. The balancer the provider feeds: `tag`, always; `current`, the outbound it chose without the prefix of the provider and only when it is one of this provider, which `isOwn`, always, tells; `cost`, the cost of the chosen outbound; `isExhausted` and `isPinned`, always; `heldMs`, how long it has held the chosen outbound. |
| `outbounds` | Always, maybe empty. The set of the provider in the order of the answer, by tag without the prefix; before the first answer the outbounds of the config with the prefix of the provider. Outbounds that left the set are not listed, ones whose lease ended are. |
| `groups` | Present for explicit groups with members owned by this provider. Each summary has `group`, `state` (`up` or `down`), `members`, `healthy`, `quarantined`, `clientPosition`, `failed`, `quarantineMs` and optional `lastFailure`. Counts cover only this provider's members; `healthy` counts proven working members with live leases and is zero while the group is down. `clientPosition` is the zero-based position in the full group. A down group contributes no usable members to `minHealthyOutbounds`; it introduces no new provider trigger. |

Per outbound:

| Field | Meaning |
| --- | --- |
| `tag` | Always. |
| `state` | Always. `up`, `degraded`, `suspect`, `down`, or `unknown` for an outbound the core knows nothing about yet. `degraded` means required probe steps passed but an optional step failed; an `up` candidate is preferred before cost is considered. |
| `reason`, `stateMs` | What set the state (`traffic`, `probe`, `return_probe`, `host_probe`, `network_change`, `network_down`, `network_up`, `replaced`, `rescued` for an outbound whose down a confirmation already running or an assigned audit proved false, or `chosen` for an outbound a balancer chose again after its rest, judged afresh) and how long ago. The `path_state` event distinguishes rescues with `by: confirm` or `by: audit`. |
| `seenMs` | How long the core has kept the counters of the outbound. |
| `quarantineLevel`, `quarantineMs` | While the outbound rests after a failure: the level of its rest, which grows with every failure in a row, and how much longer it lasts. |
| `failures` | Failures of user connections by kind over the life of the outbound: `dial_failed`, `dial_timeout`, `server_unverified` (REALITY did not verify the server), `no_response`, `closed`, `stall`, `partial_stall`. |
| `probe` | The last current probe that finished: an actual snapshot includes `profile` (including an empty built-in name), `reason` (the probe job's reason), `state`, and attempted `steps` with `name`, `isPassed`, `connectMs` (connection establishment), `ms` (whole step), `bytes`, a `destination` index on success (including zero), or an `error` class on failure. If only legacy result fields exist, `profile`, `reason`, `state`, and `steps` are absent. The compatibility fields remain: `isPassed` means state other than `down`, `rttMs` when it passed, `error` as a class when it failed, and `agoMs` since its result. Failure classes include `dial_failed`, `dial_timeout`, `server_unverified`, `closed`, `timeout`, `no_progress`, `status_<N>`, `short_body` and `other`; addresses and error text are never sent. Stale or canceled probes do not replace the snapshot. A network change clears it unless the configured action is IGNORE. |
| `connsOk`, `connsFailed`, `bytesUp`, `bytesDown` | User connections and their bytes. |
| `activeMs` | How long a balancer has chosen the outbound, the current stretch included. |
| `ttfbP50Ms` | The median time to the first byte. |
| `expiresInMs` | What is left of the lease, below zero once it ended; none without a lease. |

The body never holds destinations, domains, addresses or server names,
the targets of failures, the text of an error that may name a URL, header
values, or the raw run and network id. A usual set takes a few kilobytes.
The legacy fixture with 128 outbounds and busy counters stays below 64 KiB,
excluding backend-chosen tags and version. The fixture with four raw probe
steps per outbound has a separate ceiling of 150 KiB, including the longest
probe-job reason. Snapshot metadata takes 70 bytes with empty profile and
steps; each step's fixed metadata takes up to 160 bytes. JSON-escaped profile and step-name contents are excluded
as caller-chosen text. Actual probe measurements are retained in full.
These are regression-fixture ceilings, not runtime truncation limits for
arbitrary values.

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


### Quality measurements

Protocol 1 requests may include `outbounds[].quality` for measured members
of this provider's registered set, optional `witnessQuality`, and nonzero
`qualitySamplesDropped`. Lookup uses the exact internal tag before removing
its provider prefix for the body. Another provider's outbound measurements
are not disclosed. The witness aggregates configured witness outbounds and
is separate from the provider set.

Quality covers the current network since the last `NetworkChanged`,
independently of report windows. Every such call, including the same/empty ID
and IGNORE routing policy, advances a separate measurement epoch; subsequent
old-epoch phases are discarded. Quality does not parse the opaque network ID
or export `networkType` or `networkOperator`. The existing provider-specific
`network.id` hash still covers the complete host ID. Network/path/session
labels belong to the separate measurement-context feature and are not
implemented here. These measurements create no refresh trigger or client
policy. They are request additions to protocol 1; unknown request fields must
be ignored. Answer keys are unchanged.

`qualitySamplesDropped` counts meaningful phases rejected across the current
collection when its 64-tag or 16 KiB copied-tag admission budget is full,
not distinct rejected tags. Histories of admitted tags remain until the next
network change, including retired tags. The backend must consider this partial
coverage alongside the histogram counts.

Missing quality fields were not measured; measured zeros remain present,
including `bytesDown`, `tcpRttMinMs`, `bulkKbpsMax`, `bulkSplicedCount`,
`rxOutOfOrderFraction`, and `txRetransmitFraction`. Durations are integer
milliseconds and speeds are decimal Kbit/s. A counted speed sample needs a
positive interval between the first reply and the last observed 1/4/16/64 MiB
mark. Completion publishes bytes, speed, S2, and fractions after admitted
samples finish; a connection's Finished callback does not wait for them.

Spliced bulk completions have no speed sample. An eligible path may retain
TTFB and S1 when its existing passive socket sweep observes real receive
progress; the completion byte counter cannot create a first-reply timestamp.
Witness has no such sweep and can omit TTFB/S1 if splice began before its first
counted reply. Quality adds no polling. Mux measurements describe the shared
physical TCP connection to the server, not each user stream.

| Quality field | Meaning |
| --- | --- |
| `bytesDown`, `bytesDownOverflow` | Completed-connection bytes; the optional true flag means saturation at int64 maximum. |
| `ttfbCount`, `ttfbP50Ms`, `ttfbP90Ms` | TCP first-reply count and millisecond quantiles. |
| `dialCount`, `dialP50Ms`, `dialP90Ms` | Successful individual raw-socket dial count and quantiles, including its handshake and any DNS. |
| `tcpCount`, `tcpRttP50Ms`, `tcpRttP90Ms`, `tcpRttMinMs` | S1 RTT count, quantiles, and minimum measured S1 minimum RTT in output milliseconds. |
| `bulkCount`, `bulkKbpsP50`, `bulkKbpsP90`, `bulkKbpsMax` | Valid counted bulk speeds and quantiles; maximum in output Kbit/s. |
| `bulkRttP50Ms` | S2 load RTT, even when speed is unavailable. |
| `bulkSplicedCount` | Completed spliced downloads of at least 1 MiB, with no speed sample. |
| `rxOutOfOrderFraction` | Ratio of summed valid S1-to-S2 out-of-order/received deltas; decreasing pairs and zero denominators are omitted. |
| `txRetransmitFraction` | Ratio of summed retransmitted/sent totals from each completed connection's last successful snapshot, included once. |

Unsupported TCP fields are omitted, including fields beyond the returned
socket-structure length. Linux minimum comes from valid S1 `Min_rtt`; Darwin
minimum is the minimum sampled S1 `Srtt` in this scope. Darwin `Srtt` uses
milliseconds and its S2 load RTT is a coarse sender estimate. Linux RX counters
are packets; Darwin RX counters are bytes. Out-of-order is not a packet-loss
percentage. TX uses saved S2 if present, otherwise S1; unavailable TX in that
selected S2 does not fall back to the S1 pair. Witness and path can reach
different destinations; compare relative load growth, retransmissions, and
bulk p90/max with sufficient counts. Traffic-derived speed is a lower bound.

Quality adds at most 64 measured path records and one witness record to the
request. The existing legacy and raw probe-step fixture ceilings documented
above remain separate. The earlier combined quality fixture used a 128 KiB
core budget; it is not a whole-body bound with all Stage 1 probe steps or
arbitrary backend-chosen tags/version. Record the actual combined serialized
size after integration on the supported runtime.

## Answer

```json
{
  "protocol": 1,
  "version": "slots-7781",
  "outbounds": [
    { "tag": "x1", "protocol": "vless", "settings": {}, "streamSettings": {},
      "path": { "balancer": "auto", "group": "g" } },
    { "tag": "x2", "protocol": "vless", "settings": {}, "streamSettings": {},
      "path": { "balancer": "auto", "group": "g" } },
    { "tag": "b", "protocol": "vless", "settings": {}, "streamSettings": {},
      "path": { "balancer": "auto", "cost": 3, "isLazy": true, "returnInterval": "10m" } }
  ],
  "groups": [{ "name": "g", "cost": 1, "probe": "large", "failedInRowToLeave": 3 }],
  "expiresAt": { "x1": "2026-10-09T12:00:00Z", "x2": "2026-10-09T12:00:00Z", "b": "2026-10-09T12:00:00Z" },
  "serverTime": "2026-10-08T12:00:00Z",
  "nextRefreshIn": "2h",
  "headers": { "Authorization": "Bearer <new token>" },
  "baseVersion": "cfg-12",
  "routing": { "rules": [], "balancers": [], "domainStrategy": "IPIfNonMatch" }
}
```

A 200 with a JSON object. These are all the keys of protocol 1:

The example's `large` probe must be declared by the client's
`healthObservatory.probes` with `name: "large"`. A path without `probe` uses
the built-in `https://www.gstatic.com/generate_204` probe. There is no
special named `default` profile.

| Key | Rule | What the core does |
| --- | --- | --- |
| `protocol` | The number 1, or no key. | Anything else refuses the answer as `protocol N is not supported`, whatever its other keys. |
| `outbounds` | Required, at least one. Each outbound needs a unique non-empty `tag`, an allowed protocol and `path.balancer` exactly equal to `apply.balancer`. Its `path` has either a positive `cost` with optional `isLazy`, `returnInterval` and `probe`, or only `group` naming a group of this answer. The default allowed protocols are `vless`, `vmess`, `trojan`, `shadowsocks`, `wireguard`, `hysteria`, `socks` and `http`; direct `freedom` is not allowed. At most 64 by default. | Every outbound gets `tagPrefix`. One whose settings did not change keeps its connections; a replaced or removed one is drained. Their array order determines group order and breaks ties between equal-cost paths. |
| `groups` | Optional list with unique `name`, positive `cost`, optional `isLazy`, `returnInterval`, `probe` and `failedInRowToLeave` (default 3). A member cannot override these properties. A declared group may have no members in this answer. | Names receive `tagPrefix` and stay within this answer. A member cannot join a base or another provider's group. A collision with a foreign group rejects the whole answer; refreshing this provider's own group is allowed. |
| `version` | A string. | Names the set. Without it the set is named by a hash of `outbounds`, `groups` and `routing`. |
| `nextRefreshIn` | A Go duration string such as `"90m"`; numbers are refused. | Replaces the regular interval until the next refresh. `"0s"` asks immediately, subject to the provider limits; omission uses the regular interval, `null` is refused. |
| `expiresAt` | Tag to a time in RFC 3339 with a zone. | Leases, see [Leases](#leases). Tags outside the set are ignored. |
| `serverTime` | A time in RFC 3339 with a zone. | The clock the leases count from. |
| `headers` | Header name to value, see [Token](#token). | Headers of the next requests. |
| `baseVersion` | A string of at most 256 bytes. | See [Base config](#base-config). |
| `routing` | Only `rules`, `balancers` and `domainStrategy` (`AsIs`, `IPIfNonMatch` or `IPOnDemand`), in the format of the config file. Rules and balancers refer to final tags with `tagPrefix`. | Replaces allowed routing settings when the client accepts routing; otherwise it is ignored. Strategy settings may change, but leastCost membership and order remain derived from assembled outbound paths. |

Any other key refuses the whole answer: a section of the config such as
`dns`, `inbounds` or `policy` as `unknown key "dns"`, and
`healthObservatory` as `healthObservatory is not accepted in an answer`.
The retired answer key `costs` is also refused. The base config likewise
rejects `leastCost.selector`, `costs`, `defaultCost`, `lazyOutbounds`,
`healthObservatory.subjectSelector` and `probes[].selector`; stock Xray
strategies continue using their own selectors.
The core never takes a section it would have to restart for, and a backend
never takes a key for applied that the core threw away. `receivedAt` is a
key of the kept set only, see [Initial and kept sets](#initial-and-kept-sets).

The core checks, and changes nothing until every check passed:

1. the body is a JSON object: otherwise it may have broken on the way, and
   the next `via` is tried;
2. the keys and `protocol`;
3. the values of `expiresAt`, `serverTime`, `headers`, `baseVersion` and
   `nextRefreshIn`;
4. the outbounds: their number, tags, protocols and paths, the groups and
   named probes, and that each
   builds;
5. `routing`: a rule or a fallback that goes to an outbound or a balancer
   that does not exist, a webhook, dropping the balancer of the provider or
   an invalid path definition refuse it.

A refused answer changes nothing: neither the set, nor a lease, nor the
token. The reason, cut to 200 bytes, goes with the next request in
`lastFailure.error`, for example `invalid answer: unknown key "dns"` or
`build x3: ...`. The next attempt waits for the backoff.

**The same set.** An answer whose `version` is that of the applied set
changes no outbound, while its leases, headers and base apply as in any
other answer. This is the usual answer to a renewal: the same set with
later ends. The core keeps such an answer for the next start as well, so
its outbounds still go through the checks that need no build, their
number, tags, protocols, paths and groups, and one that fails them is refused. The keys other than `outbounds`, `groups` and `routing` do not
name the set, so a fresh `serverTime` does not make a new one.

## Errors and Retry-After

An answer other than 200 may carry an error body of the protocol:

```json
{ "protocol": 1, "code": "rate_limited", "message": "..." }
```

The codes are `unauthorized`, `forbidden`, `rate_limited`, `unavailable`,
`bad_request` and `internal`. The core reads the first 4 KiB of the body,
waiting at most 2 seconds after the status, and keeps only the code;
`message` is kept nowhere, since it may echo the request.

The attempts end when the backend itself answered, readably or against the
schema; an answer that a firewall, a portal or a server on the way may have
sent goes on to the next `via`:

| Answer | Next `via` | Then |
| --- | --- | --- |
| 200 with a valid answer | no | Applied; the backoff starts over, unless an exhausted balancer got the same set again. |
| 200 that is no JSON object, is cut short or too large | yes | A failure; the backoff grows. |
| 200 with a JSON object against the protocol, or a set that does not build | no | Refused, see [Answer](#answer); the backoff grows. |
| 401 or 403 with the code `unauthorized` or `forbidden` | no | The headers are refused, see [Token](#token). |
| 429 or 503 with `Retry-After` | no | Every trigger waits as asked; no failure, the backoff does not grow. |
| Any other status with an error body | no | A failure; the backoff grows. |
| 401 or 403 without an error body; 429 or 503 without `Retry-After` and body; 3xx; any other status; a timeout or a broken connection | yes | A failure; the backoff grows. |

**Retry-After** counts only with 429 and 503: in seconds, or as an HTTP
date counted from the `Date` of the same answer, so a wrong clock of the
device neither stretches nor cuts the pause, or from the clock of the
device without `Date`. Zero, a date gone by and anything unreadable count
as none. The core waits at most its `backoff.maxDelay`, 30 minutes by default,
then spreads the pause by up to its `backoff.jitterFraction`, 30%, upward only: no
client comes back sooner than asked, and clients told the same pause come
back apart. The pause holds every trigger, the host and an exhausted
balancer too.

## Leases

`expiresAt` tells by tag when the backend stops serving an outbound, and
`serverTime` is the clock of the backend when it answered. The clock of the
device may be off by any amount, so the core keeps a lease as what was left
of it when the answer arrived:

- it counts from the later of `serverTime` and the `Date` of the answer
  plus its `Age`, which a cache sets; without either from the clock of the
  device;
- it is at most 30 days;
- it counts down by the longer of the monotonic clock, which stands while
  the device sleeps, and the wall clock: it ends early rather than late.

Every answer is the whole truth about the outbounds in it: a later end
renews a lease, the same end only brings a fresher clock, an earlier end
shortens it, an outbound of the set without an end in the answer has no
lease from then on, and an outbound that leaves the set takes its lease
along. A time the core cannot read refuses the answer.

**Renewal.** The core asks with trigger `expiry` once a lease reached half
of its length, spread by 10% per lease, or ended; from 80% of it on and
once it ended the request is urgent and waits only for the shorter limit.
Every request asks about the leases due by then, whatever its trigger. The
core asks once per value of a lease: a backend that answers with the same
end is not asked again for it, and the lease then ends. A request that
fails is repeated after the backoff, and while the device is idle only
with user traffic. The client may change these shares or turn renewal off.

**The end.** An outbound whose lease ended stays a candidate of the
balancer, the last resort: new connections go to it only while no other
usable outbound is left, so when every lease ended traffic still goes
through those outbounds. Old connections are not ended. Such an outbound is
not probed in the background, and while no balancer chooses it, failures of
its old connections are not held against it.

## Token

`headers` of an answer replace headers of the next requests, to rotate the
token of the provider:

- only names the client allows, by default `Authorization`, in any case:
  `authorization` replaces the `Authorization` of the config;
- at most 8, also together with the headers the host and earlier answers
  set, each name at most 256 bytes, each value non-empty, in UTF-8, at
  most 4 KiB, without line breaks or other control characters;
- names of the transport (`Host`, `Content-Length`, `Content-Type`,
  `Transfer-Encoding`, `Connection`, `Keep-Alive`, `TE`, `Trailer`,
  `Upgrade`, `Accept`, `Proxy-*`) never.

A header that breaks these rules refuses the answer. Only an applied
answer changes a header: a refused or lost answer leaves the token in use.
The host may set headers as well; when it changed them while the request
ran, the host wins, and the headers of the answer are not taken. The core
never shows header values anywhere but in the requests of the provider.

The backend refuses the headers with 401 or 403 and an error body with the
code `unauthorized` or `forbidden`. Every trigger but the host then waits
for an hour by default, spread, or until the headers change; after the
pause one refresh tries them again, once per pause, and the leases it
asked about are asked about again. The host is told and fetches a new
token when it can. A 401 or 403 without such a body is a failure like any
other, and the next `via` is tried: portals and firewalls on mobile
networks and in hotels answer so. A refusal of headers that the host
replaced while the request ran is no refusal of the new ones.

## Base config

`baseVersion` names the config the core runs with. The backend that
generated the config names it in `baseVersion` of the provider in the
config, or in the answer of the connect that came with the config; a set
the host kept named the config of an earlier run and never becomes the
base. The body of every request tells the base. An answer may name the base
config the backend serves now: when that is another one, the host is told
once per value, fetches the config anew and restarts the core when that is
cheap. An answer that names the base of the core again ends the change. The
set of such an answer applies all the same: the backend answers with
outbounds that work with the base of the request.

## Initial and kept sets

**Initial set.** The host may start the core with an answer of the
backend, the answer of a connect: the core applies it as it starts, with
the same checks and without a request, so it runs on the set from the
first connection. The answer of a connect therefore has to be a valid
answer of protocol 1, with `serverTime` and the leases. Its headers may
carry any name the host may set. The first request of such a core comes
from a trigger, not from the start, except for one urgent request when a
lease had already ended.

**Kept set.** The host keeps the last answer the core applied, with the
headers an answer or the host set in place of its own, `serverTime`
replaced by the time its leases count from and `receivedAt` added, the
clock of the device when it arrived, and starts the next core with it when
there is no fresh answer of a connect. The leases of a kept set count from
its `serverTime` plus the time since `receivedAt` and are renewed when they
would have been without the restart; a lease that ended meanwhile is asked
about once at the start, also under a start pin. So after a restart the
backend sees the token it rotated last, the `version` of the kept set and,
after a long pause, an `expiry` request.

## What a backend must do

- **Protocol.** Answer with `protocol` no higher than in the request and
  only with keys of that protocol. Ignore fields of the request it does not
  know.
- **Clock.** Put `serverTime` into every answer.
- **Outbounds.** Keep the tags and the settings of an outbound stable
  between answers, so that it keeps its connections. Put IP addresses into
  the outbounds and the server name apart: an answer cannot bring the `dns`
  a new name may need. The core builds outbounds with the config loader
  of Xray, so an outbound it would refuse at start is refused in an answer
  too: for example, VLESS or Trojan without TLS or REALITY to a public
  address.
- **Paths and groups.** Put an explicit `path` on every returned outbound.
  Keep the member order in the `outbounds` array and declare each group by
  raw `name` in `groups`. Put cost, laziness, return interval and probe on
  the group, never on its members. Validate generated base configs with
  `xray run -test` before delivery; the core rejects an unused base group.
- **Leases.** Give leases much longer than the refresh interval, a day for
  a refresh every 2 hours, and at least ten times the shortest pause
  between refreshes. Put an end for every leased outbound into every
  answer: an answer renews every lease it names and drops those it leaves
  out.
- **Token.** Keep the previous token valid until a request brings the new
  one and for 24 hours after it: the answer that rotated it may be lost, or
  the process killed before the host kept the token. A token lives longer
  than a lease.
- **401 and 403.** Send them only for a token that is really invalid, and
  always with an error body of the protocol.
- **Load.** Answer 429 or 503 with `Retry-After`, different for different
  clients, and limit requests per token: a host that restarts the core
  resets its limits.
- **Repeats.** Answer a request with the `run` and `refresh` of an earlier
  one with the same outbounds.
- **Base config.** Change `baseVersion` with every change of the config
  outside the set, and roll the change out in cohorts: hosts that restart
  at once load the backend like a storm.
- **Errors.** Read `lastFailure.error`: an answer the core refused names
  what it refused, and the backend should answer the next request without
  it.
