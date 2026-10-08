# Health observatory and the leastCost balancer

The health observatory judges outbounds by the connections users already
make and chooses outbounds for balancers with the `leastCost` strategy. It
probes only when there is a reason. The core knows nothing about what an
outbound means to the product: everything is set by tags, costs and rules in
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
2. **Suspicion.** `failures` path failures, or `failures` failures over at
   least `distinctTargets` destinations, since the last proof that the path
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
4. **Failover.** The chosen outbound goes down and rests in quarantine with a
   growing, jittered backoff. The balancer moves new connections to the
   cheapest usable candidate, after probing it if `switchAfterProbe`. Up to
   `concurrent` candidates are probed at once, cheapest first and one per
   server name, so dead candidates do not cost a probe timeout each in
   turn. The balancer takes the cheapest candidate that passed once every
   cheaper one has failed: a dearer one that answers first waits for them.
   Probes that answer after the move only record the state of their
   outbound. When nothing is left, the balancer returns no tag and traffic
   goes to `fallbackTag`; quarantined candidates are rechecked while users
   keep trying.
5. **Return to cheaper.** Not before `minHold`, on `upgrade.interval` (with a
   random phase per device) or after a network change. The cheaper candidate
   must pass `upgrade.successes` probes in a row, `upgrade.spacing` apart. A
   return that fails within `penaltyWindow` multiplies the wait before the
   next one; a return that holds clears it.
6. **Networks.** On `NetworkChanged(id)` results of the previous network are
   forgotten. If the network was seen before, the outbound that last worked
   there is tried first, with one probe and no round over all candidates.

At most `concurrent` probes run at a time and at most `concurrent` start
within one `spacing`: with the default of one, probes run one by one at
least `spacing` apart. They stay within `perMinute` in total and
`perServerNamePerMinute` per TLS or REALITY server name, and two probes of
one server name never run at once: several near-simultaneous handshakes to
one server name are a known censor signal. Without user traffic for
`pauseWhenIdle` there are no probes and no timers, except the renewal and
the end of a provider lease, see Providers. Closing the core ends the
probes still running, and their outcome is not held against the outbound.

Existing connections stay where they are; only new connections move. The
exception is an outbound found down: its connections that have received
nothing yet, still dialing or waiting for an answer, are ended at the
decision and at the switch away from it. Applications see the failure at once
and retry on the outbound chosen now instead of waiting for their own
timeout, and the stuck connections stop holding memory. Connections that
received something keep going, since an address that refuses new
connections may still serve old ones. `keepPendingOnDown` turns this off. A
decision made while the network check was failing ends nothing.

## Config

```json
{
  "healthObservatory": {
    "subjectSelector": ["a", "b", "c"],
    "passive": {
      "window": "60s", "dialTimeout": "6s", "firstByteTimeout": "5s", "stallTimeout": "8s",
      "failures": 3, "distinctTargets": 2, "successBytes": 24576, "sweepInterval": "500ms",
      "partialStallMaxAge": "30s", "ignorePartialStalls": false, "dialOnly": false,
      "keepPendingOnDown": false
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
      "perServerNamePerMinute": 3, "perMinute": 12, "suspectCooldown": "1m",
      "hostProbes": { "perMinute": 4, "reuse": "15s", "maxOutbounds": 8, "timeout": "2m" }
    },
    "events": { "maxEvents": 1000, "maxBytes": 65536 },
    "network": { "remember": true, "maxNetworks": 16 },
    "report": { "interval": "24h" }
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
        "startPinned": "", "startPinnedFor": "0s",
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

Values that size what the observatory keeps have ceilings, and a larger
value is taken as the ceiling: probe and network check timeouts 30 s,
`concurrent` 8, `maxEvents` 4096, events `maxBytes` 256 KiB, `maxNetworks`
64; for a provider `maxBytes` 1 MiB, `maxOutbounds` 128 and `drain` 30 min;
for `hostProbes` `perMinute` half of `schedule.perMinute`, `reuse` 5 min,
`maxOutbounds` 16 and `timeout` 10 min.

- `costs` uses the format of `leastLoad`: a substring or a `regexp` of the
  tag and a value above zero. Lower is preferred. Equal costs prefer
  outbounds known to work, then tag order: the outbound manager hands the
  candidates over sorted by tag.
- Every `selector` of a `leastCost` balancer must be covered by
  `subjectSelector` or by the `tagPrefix` of a provider: an outbound nobody
  observes is never left.
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
  `startPinnedFor` releases it by itself after that long: a host that
  verifies within a known time and stops the core when the check fails
  then needs no release call.
- Set `fallbackTag` to a blackhole: without it, an empty choice sends traffic
  to the default outbound.
- `report.interval`: a window with a decision is reported once it lasts
  this long, see Report. `"0s"` leaves only the report at the stop.
- `hostProbes` limits the probes the host asks for, see Probes on request.

## Providers

A provider fetches a fresh set of outbounds over HTTP and puts it into a
balancer without a restart, for example when every address of the current
set is blocked. It is configured inside `healthObservatory`, because its
triggers are judged on what the observatory knows about the balancer.

```json
{
  "healthObservatory": {
    "subjectSelector": ["a", "b"],
    "providers": [{
      "tag": "fresh",
      "url": "https://api.example.com/set",
      "method": "POST",
      "headers": { "Authorization": "Bearer <token>" },
      "via": ["auto", "backup", "direct"],
      "timeout": "10s", "maxBytes": 262144,
      "triggers": {
        "interval": "2h", "jitter": 0.2, "minHealthy": 2, "whenExhausted": true,
        "whenCostAbove": { "cost": 10, "for": "15m" }, "maxAge": "6h",
        "onNetworkChange": "unknownNotCheapest", "networkSettle": "30s",
        "expiry": { "renewAt": 0.5, "urgentAt": 0.8, "jitter": 0.1 }
      },
      "limits": {
        "minInterval": "5m", "exhaustedMinInterval": "30s", "maxPerHour": 6,
        "backoff": { "initial": "30s", "multiplier": 2, "max": "30m", "jitter": 0.3 }
      },
      "apply": { "balancer": "auto", "tagPrefix": "p-", "drain": "5m", "maxOutbounds": 64, "routing": false }
    }]
  }
}
```

The values above are the defaults, except `whenCostAbove`, which is off
unless set, and `maxBytes`, which is 4 KiB per outbound of `maxOutbounds`. `tag`, `url`, `apply.balancer` and `apply.tagPrefix` are
required. A trigger or a limit set to an explicit zero (`"0s"`, `0`) is off.

**Request.** A provider that serves a static file on a web server needs
`"method": "GET"`: by default the request is a POST, which a static host
answers with 405 or 403. A POST of `url` with `headers` carries the state of
the core as JSON, see below; a GET has no body. Credentials belong in a header,
the URL must not carry them. The URL must be `https`; `allowPlainHttp: true`
permits `http`, and the headers then cross the network unencrypted. The request goes through the `via` entries in
order until one brings a well-formed answer: an outbound tag, or the tag of a
`leastCost` balancer, which means its current choice. An exhausted balancer,
an outbound in quarantine and a tag with no outbound are skipped. Without `via` the request is
routed like any other connection. Each attempt has its own `timeout`;
redirects are not followed. A body that is not a JSON object, is cut short
or is larger than `maxBytes` may have broken on the way, and the next entry
is tried. An answer that is a JSON object but breaks the protocol ends the
attempts: the backend itself sent it, and another entry leads to the same
backend.

An answer other than 200 may carry an error body of the protocol,
`{"protocol": 1, "error": "<code>", "message": "..."}`, sent along with the
status. The core reads its first 4 KiB, waiting at most 2 seconds after the
status, and takes only the code, into `provider_fetch`; the message may
echo the request and is kept nowhere. A 429 or 503 with `Retry-After`, in
seconds or as an HTTP date counted from the `Date` of the answer, holds
every trigger, the host and an exhausted balancer too, for that long: at
most `backoff.max`, then spread by up to `backoff.jitter` upward only, so no
client comes back sooner than asked and clients told the same pause come
back apart. Such a pause is no failure, and the backoff does not grow. An
error body and `Retry-After` end the attempts, as the backend itself
answered. A bare status may come from a firewall, a portal or a proxy on
one way, and the next entry is tried.

The body of a POST tells the backend why it is asked and what broke, so that
it can answer with the right outbounds at once:

```json
{
  "protocol": 1, "provider": "fresh", "run": "9f2c6a1e5b7d3c40", "refresh": 7,
  "trigger": "exhausted", "retry": 1,
  "lastFailure": { "status": 503, "error": "status", "agoMs": 61000 },
  "version": "2026-10-08.3", "clientTime": "2026-10-08T12:00:03Z",
  "network": { "id": "5d0c1f2a9b3e4c71", "known": false, "up": true },
  "memoryLow": false,
  "balancer": { "tag": "auto", "current": "x2", "own": true, "cost": 3, "exhausted": false, "pinned": false,
                "heldMs": 840000 },
  "outbounds": [
    { "tag": "x1", "state": "down", "reason": "traffic", "stateMs": 81000, "seenMs": 3600000,
      "quarantineLevel": 2, "quarantineMs": 81000, "failures": { "dial_timeout": 4 },
      "probe": { "ok": false, "error": "timeout", "agoMs": 30000 },
      "connsOk": 12, "connsFailed": 4, "bytesUp": 90112, "bytesDown": 1048576, "activeMs": 600000,
      "ttfbP50Ms": 300, "expiresInMs": 61200000 },
    { "tag": "x2", "state": "up", "probe": { "ok": true, "rttMs": 180, "agoMs": 840000 },
      "connsOk": 412, "bytesDown": 52428800, "activeMs": 840000, "expiresInMs": 61200000 }
  ]
}
```

- `run` and `network.id` go out hashed for the provider: the first 16 hex
  digits of SHA-256 of `url`, a zero byte and the value. One backend sees
  them stable across runs, two backends cannot match theirs, and the raw
  run, which the report carries as `health_run`, never leaves the device.
  An empty network id is left out.
- `refresh` grows with every refresh and is the same through every `via` of
  it, which all send the same bytes: `run` and `refresh` together tell a
  request sent again from a new one. `trigger` and `retry` are those of
  `provider_fetch`.
- `lastFailure` comes when the previous refresh failed: the HTTP status or
  0, how long ago, and as `error` the reason a well-formed answer was
  refused (at most 200 bytes) or the class of the failure: `timeout`,
  `network`, `status`, `too_large` or `broken_json`. It is what the last
  `via` that was asked met; entries skipped after it do not hide it.
- `version` is the set applied now; `clientTime` the clock of the device, so
  the backend sees how far off it is.
- `balancer` is the one the provider feeds. `current` and `own` tell whether
  it uses an outbound of this provider, without the prefix; the path of
  another provider or of the config is not named. `cost` is the cost of the
  chosen outbound, `heldMs` how long it has been chosen.
- `outbounds` are the set of the provider in the order of the answer; before
  the first answer the outbounds of the config with its prefix. Tags go
  without `tagPrefix`, and outbounds that left the set are not listed.
  `state`, `reason` and `stateMs` are as in `path_state`, `unknown` for an
  outbound the observatory knows nothing about yet; `seenMs` is how long its
  counters have been kept. Quarantine shows while it lasts. `failures` counts
  by kind over the life of the outbound, `probe` is the last probe that
  finished, `agoMs` after its result, with its error as a class: `timeout`,
  `no_progress`, `status_<N>`, `short_body`, `reset`, `refused`, `eof`,
  `tls` or `other`. A probe still running is not in it. The traffic counters
  are those of Status. `expiresInMs` is what is left of the lease of the
  outbound, below zero once it ended; there is none without a lease.

Zero and empty values are left out, except `protocol`, `provider`, `run`,
`refresh`, `trigger`, `retry`, `clientTime`, `network.known`, `network.up`,
`memoryLow` and the flags of `balancer`. The body never holds destinations,
domains, addresses or server names, the targets of failures, the text of an
error that may name the probe URL, or header values. Its size is an
estimate, not a limit: up to about 500 bytes per outbound besides its tag,
so a usual set takes a few kilobytes and the largest one with every counter
busy some 60 KB, plus the tags and the version the backend chose itself. A
backend ignores fields it does not know: protocol 1 may gain fields in the
body.

**Answer.**

```json
{
  "protocol": 1,
  "outbounds": [ { "tag": "x1", "protocol": "vless", "settings": {}, "streamSettings": {} } ],
  "costs": { "x1": 1 },
  "routing": { "rules": [], "balancers": [], "domainStrategy": "IPIfNonMatch" },
  "expiresAt": { "x1": "2026-10-09T12:00:00Z" },
  "serverTime": "2026-10-08T12:00:00Z",
  "nextRefreshIn": "90m",
  "version": "2026-10-08.3"
}
```

The answer is read strictly. `protocol` is 1 or left out; an answer of
another protocol is refused for that, whatever keys it has. A key not shown
here, a section of the config such as `dns`, `inbounds` or `policy`, and
`healthObservatory` refuse the whole answer, so a backend never takes a key
for applied that the core threw away; a new key is a new protocol. A refused
answer changes nothing and is reported in `provider_apply` with `ok` false
and the reason, cut to 200 bytes like every reason that quotes the answer.

`outbounds` use the format of the config file and need unique tags. Their
protocols must be in `apply.protocols`; by default `vless`, `vmess`,
`trojan`, `shadowsocks`, `wireguard`, `hysteria`, `socks` and `http`, so an
answer cannot send traffic straight out with `freedom`.
`costs` are by those tags, above zero, and override the costs of the
balancer for them.
`nextRefreshIn` (a duration or seconds) replaces `interval` until the next
refresh. `version` (or `hash`) names the set; without it the set is named by
a hash of `outbounds`, `costs` and `routing`, so an answer that differs only
in `protocol`, `nextRefreshIn`, `expiresAt` or `serverTime` is the same set.
A set with the same name as the applied one changes no outbound; its leases
still apply.

`routing` takes `rules`, `balancers` and `domainStrategy` in the format of
the config file and replaces all rules and balancers at once, and the domain
strategy when the answer has one, without a restart, when `apply.routing` is
on; any other key in it refuses the answer. Rules refer to the final tags, `tagPrefix` included.
The answer is refused when a rule or a fallback goes to an outbound or a
balancer that does not exist (Xray would take its default outbound for it,
which may be outside the tunnel), when the leastCost balancer of the
provider is gone, when a leastCost balancer chooses outbounds nothing
observes, or when a rule has a webhook, which would post every matching
connection with its destination. Balancers the new routing no longer has are forgotten, with the
outbounds they remembered per network. While `apply.routing` is off the
routing of an answer is ignored, which `provider_apply` tells with
`routing_ignored`: taking routing from a provider is the choice of the
client.

**Apply.** Every outbound gets `tagPrefix` in front of its tag.
`apply.balancer` has to be a `leastCost` balancer with a selector that
matches that prefix, and the observatory observes such outbounds without
listing them in `subjectSelector`. The provider owns every outbound whose
tag starts with the prefix, also ones from the config, so a host can start
the core with the set it saved last time; such an outbound should not be the
first one, which the core takes as the default. All outbounds are
validated and built before anything changes; a broken answer or a set that
does not build changes nothing. Outbounds whose settings did not change are
not touched and keep their connections. An outbound whose settings changed
is replaced under the same tag. An outbound that left the set is no longer
a candidate, keeps its connections for `drain` and is then removed and
closed, and what the observatory knew about it and its traffic counters are
forgotten; if the balancer uses it, the balancer moves to another candidate
the usual way and stays where it is while there is nothing else. The old
handler of a replaced outbound drains the same way. A drain ends at once
when the outbound has no open connections and no balancer chooses it, and
at most `maxOutbounds` outbounds drain at a time: beyond that the oldest
drains end early.

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
taken as the outbound remembered for a network or as `startWith`, and not in
the plan of a probe request of the host. A failover tries it after every
other candidate. Its state, quarantine and connections stay as they are, and
failures of old connections are ignored while no balancer chooses it: a
revoked outbound is no block. One a balancer still uses is judged as usual.
`minHealthy` counts it as gone. A network change keeps the leases.

**Triggers.** Any trigger may start a refresh:

| Trigger | Fires |
| --- | --- |
| `interval` | `interval` +- `jitter` after the previous successful refresh. |
| `minHealthy` | The balancer has fewer candidates that are up or not judged yet; once, again only after it got back above. |
| `whenExhausted` | The balancer has no usable candidate and the network is up; again after `exhaustedMinInterval` while it lasts. After a refresh that added outbounds, only once a connection brought them to the balancer. |
| `expiry` | A lease reached `renewAt` of its length, spread by `jitter` per lease, or ended, and no refresh that got an answer asked about this value of it: once per value, so a backend that keeps a lease short is not asked again. From `urgentAt` of the lease on, and once it ended, the refresh is urgent: it waits only for `exhaustedMinInterval` and runs while memory is low. Before that it waits for `minInterval`, and while memory is low for `urgentAt`. `renewAt` 0 turns renewal off; a lease that ended is still asked about once. `renewAt` is at most 0.95, `urgentAt` between it and 0.95, `jitter` at most 0.5. |
| `whenCostAbove` | The balancer has kept outbounds of at least `cost` for `for`; the time counts again from every successful refresh. |
| `maxAge` | The set was fetched longer ago than this. |
| `onNetworkChange` | `networkSettle` after a network change: `unknownNotCheapest` in a network without a remembered working outbound when the balancer is not on its cheapest candidate, `unknown` in such a network, `any`, or `never`. |
| `nextRefreshIn` | The time the last answer asked for. |
| host | `RequestRefresh`, see below. |

Every trigger but a host request waits while the network is down, and while
there is no user traffic, except the first request for a lease: an outbound
would end under a sleeping device. A refresh asks about the leases due by
then, to be renewed or ended, whatever its trigger; when it fails, they are
asked about again after the backoff, and then only with user traffic, so a
dead backend does not wake the device.

**Limits.** Two refreshes start at least `minInterval` apart, or
`exhaustedMinInterval` for an exhausted balancer, an urgent renewal or a
lease that ended, and host requests; at most `maxPerHour` start within an
hour. After a failure the next attempt waits for `backoff`, counted from
that attempt, so a failing refresh never runs in a loop. A refresh for an
exhausted balancer that brings the same set counts as a failure for the
backoff. A failed host request and a failed network
change refresh are retried after the backoff, or after the pause a busy
backend asked for.

## Probes on request

The host may know more than the observatory, for example that the tunnel
carried nothing after the device woke up, or that the user asked to
reconnect. `RequestProbe` asks for probes of a `leastCost` balancer now and
returns a ticket with the id of the request at once; the results appear in
`Status` under `probe_requests` and in `ProbeResult`. Ids start from 1 in
every run, so the host matches the `run` of the ticket with the one in
Status. The limits below are those of `schedule.hostProbes`.

- Without outbounds a request covers the outbound the balancer uses, then
  its candidates that are not `lazy`: the usable ones from the cheapest, then
  the ones in quarantine. Listed outbounds have to be candidates of the
  balancer and are taken as they are, lazy ones too. A request covers at most
  `maxOutbounds`. Before the first connection through the balancer it has no
  candidates, and a request fails.
- An outbound in quarantine is answered `down` without a probe: the request
  does not shorten its rest. An outbound that works and proved it within
  `reuse`, by a probe or by a connection that received `successBytes`, is
  answered `ok` without a probe. A probe of the observer that runs or is due
  answers the request first, and the probe of the host is then dropped.
- A balancer has one open request at a time: a request for a balancer with
  one open joins it and gets the same id.
- Probes of the host come last. They wait while a probe of the observer waits
  for the budget, start at most `perMinute` times a minute and take at most
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

A host that calls in a loop costs at most `perMinute` probes a minute and a
few kilobytes: the observer keeps the open requests and the latest four
closed ones.

## Memory

What the observatory keeps does not grow with the length of a session or the
number of networks, outbounds and connections: what it knows about an
outbound is forgotten once no balancer offers it, failures per outbound are
capped, probes end by a deadline, and the config values that size anything
have ceilings. For an open connection of a watched outbound it keeps under
1 KB and no goroutine of its own. The report keeps fixed counters, the
latest 10 switches and seven numbers per outbound.

A host that watches the memory of the process, such as an iOS network
extension close to its limit, reports pressure with `SetMemoryLow(true)` and
its end with `SetMemoryLow(false)`. While memory is low:

- What only improves the choice waits: returns to a cheaper outbound, also
  one under way between its probes, probes after a network change and of
  the outbound remembered for the network, and timed provider refreshes
  (`provider_skip` with reason `memory`), the renewal of a lease too until
  `urgentAt` of it. A refresh the host asked for, or one for an exhausted
  balancer or an urgent or ended lease, still runs, through the first
  usable `via` only. A network check in doubt repeats at most every 2
  minutes.
- What can go goes at once: drains of outbounds without connections that no
  balancer chooses, and what the observatory knows about outbounds no
  balancer offers.
- Finding failures goes on as usual: confirming probes, failovers, rechecks
  of an exhausted balancer and the first network check. No user connection
  is ended for memory.
- What can be observed stays as it is: the event log keeps its limits, every
  event is written and the report counts as usual. Pressure is when a host
  most needs to see what happened, and the log costs a few kilobytes.
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
| `NetworkLost()` / `lost` | No network; decisions pause until the next change. |
| `SetMemoryLow(low)` | The process is short of memory, or no longer; see Memory. Safe from any goroutine, a repeated value does nothing. Only for embedders. |
| `Pin(balancer, outbound, run)` / `pin` | Keep the balancer on an outbound. |
| `Release(balancer, run)` / `release` | Let the balancer choose again. |
| `RequestRefresh(provider)` / `refresh [provider]` | Fetch the set of a provider, or of every provider when empty, as soon as the limits allow. False when there is no such provider. |
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
| `switch` | `balancer`, `from`, `to`, `reason`, `held_ms`, `cost` | New connections go to another outbound. `reason`: `start`, `start_pinned`, `start_remembered`, `failover`, `upgrade`, `recovered`, `remembered`, `network_change`, `candidates_changed`, `pin`, `retired` (the previous one left its provider set), `expired` (the lease of the previous one ended). `held_ms` is the time on the previous one. |
| `path_state` | `outbound`, `state`, `reason`; for `suspect` also `failures`, `targets`, `kinds`, `strong`; for `down` also `quarantine_ms`, `quarantine_level` and `provisional` when it was ruled out while the network check was failing | An outbound became `up`, `suspect`, `down` or `unknown`. `reason`: `traffic`, `probe`, `upgrade_probe`, `host_probe`, `network_change`, `network_down`, `network_up`, `replaced` (a provider
changed its settings). `kinds` counts failures by kind: `dial_failed`, `dial_timeout`, `server_unverified`, `no_response`, `closed`, `stall`, `partial_stall`. |
| `probe_start` | `outbound`, `reason`, `balancer`, `running`, `request` | A probe started. `reason`: `confirm`, `failover`, `network_change`, `remembered`, `recheck`, `upgrade`, `host`. `running` counts the outbound probes running now, this one included. `request` is the id of the request a `host` probe is for. |
| `probe_result` | `outbound`, `reason`, `balancer`, `ok`, `rtt_ms`, `bytes`, `error`, `stale`, `request` | A probe finished. `bytes` counts both directions above TLS. `stale` means it was made in the previous network and was ignored. |
| `probe_request` | `request`, `balancer`, `outbounds`, `end`, `ms`, `probed`, `reused`, `ok`, `failed`, `down`, `skipped`, `expired`, `gone` | A request of the host that waited for a probe ended; one answered at once writes none. `end`: `done`, `timeout`, `network_change`, `gone`. The counts are by result; `probed` and `reused` count the answers of a probe and of a recent proof. |
| `exhausted` | `balancer`, `from`, `candidates` | No usable candidate is left; traffic goes to `fallbackTag`. |
| `pending_closed` | `outbound`, `connections`, `reason` | Connections of an outbound found down that had received nothing were ended. `reason` is the one the outbound went down for. |
| `upgrade_start` | `balancer`, `from`, `to` | Probing a cheaper outbound to return to it. |
| `upgrade_failed` | `balancer`, `outbound`, `successes` | The return was abandoned after a failed probe. |
| `upgrade_delayed` | `balancer`, `outbound`, `delay_ms` | The return waits longer because the previous one failed. |
| `upgrade_penalty` | `balancer`, `outbound`, `multiplier` | A return failed within `penaltyWindow`. |
| `pin`, `release` | `balancer`, `outbound`; for `release` also `by`: `host` or `timer` | The host pinned or released a balancer, or the start pin ran out. |
| `stale_request` | `request`, `balancer`, `run` | A pin or release meant for another run of the core was ignored. |
| `pinned_down` | `balancer`, `outbound` | The pinned outbound failed; the balancer stays on it. |
| `connectivity` | `ok`, `rtt_ms`, `error` | Result of a network check. |
| `network_down` | `error` | The network check failed; decisions paused. `error` is `host` when the host reported the loss. |
| `network_up` | `by` | Decisions resumed: `connectivity` (a check passed), `traffic` or `probe` (an outbound worked). |
| `connectivity_doubt` | `failed_checks`, `paused_ms` | The check kept failing for `maxPause`; decisions resumed provisionally. |
| `connectivity_ignored` | `failed_checks` | The check failed while an outbound worked; it is not used until the next network change. |
| `network_change` | `network`, `known`, `generation` | The host reported a network change. |
| `memory_low` | `on` | The host reported that memory is low (`on` true) or no longer. |
| `provider_fetch` | `provider`, `trigger`, `retry`, `attempt`, `via`, `outbound`, `method`, `refresh`, `ok`, `status`, `ms`, `bytes`, `code`, `retry_after_ms`, `error` | One request of a refresh through one `via` entry; `outbound` is what the entry resolved to, `refresh` the number of the refresh as the body tells it, `bytes` the size of the answer. `code` is the code of an error body of the protocol, `retry_after_ms` the pause `Retry-After` asked for, after the ceiling and before the spread. A skipped entry has `ok` false and only `error`: `balancer exhausted`, `outbound down` or `no such outbound`. `error` never holds the URL. `trigger`: `host`, `exhausted`, `expiry`, `min_healthy`, `cost_above`, `network_change`, `next_refresh`, `max_age`, `interval`; `retry` counts failed refreshes before this one. An answer that breaks the protocol has `ok` true: the backend answered, and the refusal is the `provider_apply`'s. |
| `provider_apply` | `provider`, `trigger`, `ok`, `changed`, `added`, `replaced`, `removed`, `unchanged`, `version`, `exhausted`, `rules`, `routing_error`, `routing_ignored`, `leases`, `renewed`, `expired`, `skew_ms`, `clock`, `error` | The answer was applied (`changed`), was the applied set already, or was rejected (`ok` false with `error`). `exhausted` tells whether the balancer had nothing usable at that moment. `rules` counts the routing rules applied with the set; `routing_ignored` tells that the answer had routing while `apply.routing` is off. When the answer names leases: `leases` counts them, `renewed` the ones it gave a later end that has not passed, ended ones too, `expired` the leases that ended after it, where one that had ended and got a later end that passed as well gives no second `provider_expired`; `skew_ms` is how far the clock they count from is ahead of the device, and `clock` which one that is: `server`, `date` (the `Date` of the answer plus `Age`) or `device`. |
| `provider_expired` | `provider`, `outbound`, `active`, `renewal` | The lease of an outbound ended, or an answer brought it ended. `active` tells whether a balancer chose the outbound. `renewal` tells why no later end came: `refused` (the backend answered without one), `failed` (the last refresh that asked failed), `off` (`renewAt` is 0) or `pending` (no refresh asked yet: no network, the limits). |
| `provider_restart_needed` | `provider`, `version`, `sections` | Kept for sections of the config the core reads only at start. Protocol 1 refuses an answer with such a section, so this version does not write it. |
| `provider_skip` | `provider`, `trigger`, `reason`, `wait_ms` | A trigger wants a refresh that cannot start yet. `reason`: `min_interval`, `backoff`, `retry_after`, `max_per_hour`, `idle`, `network_down`, `memory`. Reported once per trigger and reason until a refresh starts. |
| `provider_drained` | `provider`, `outbound`, `replaced` | An outbound that left the set, or the old handler of a replaced one, was closed: after `drain`, at once without connections, or early to keep at most `maxOutbounds` draining. |

## Report

The observatory adds up its events into a report for the host to send, so
the host needs neither the event log nor counters of its own. Every event is
counted as it is written: a full log, or a host that never drains it,
changes nothing in the report.

A decision is a `switch` for any reason but `start`, `start_pinned` and
`start_remembered`, `exhausted`, `upgrade_failed`, `pinned_down`, a
`provider_apply` that was rejected or changed the set,
`provider_restart_needed` and `provider_expired`. Events the log dropped are
no decision.

`TakeReport(final)` returns the report when one is due and starts a new
window:

- `final`, before the host stops the core: due when the run had a decision,
  with reason `stop`. Nothing is due after it, final or not.
- Otherwise due once a window with a decision lasts `report.interval` by the
  wall clock, which goes on while the device sleeps; reason `interval`. A
  decision after a window that long without one goes with the next call.

The core keeps no timer for it: the host asks, for example every 30 seconds,
and a call with nothing due allocates nothing. Low memory does not make a
report due. A host whose process may be killed without a stop keeps the
snapshot of `Report()`, reason `snapshot`, replaces it after every report it
takes, and sends the kept one when it starts again; the reason it sends is
its own choice.

Counters cover the window: from the start of the run or the previous report
to this one. Reports of one run add up.

| Field | Meaning |
| --- | --- |
| `health_run` | The run of the core, as `run` in Status. |
| `report_reason` | `stop`, `interval` or `snapshot`. |
| `report_index` | Number of the report in the run, from 1. |
| `window_sec`, `session_sec` | Length of the window and of the run in seconds of the wall clock. |
| `events` | Events written in the window. |
| `events_dropped` | Events the log dropped in the window because it was full: they never reached `DrainEvents`. |
| `switches`, `switch_reasons`, `switch_path` | `switch` events, their count by reason, and the latest 10 as `from>to:reason`. |
| `exhausted`, `pinned_down` | Events of those types. |
| `path_down`, `path_suspect` | `path_state` events with those states. |
| `pending_closed` | Connections ended by `pending_closed` events. |
| `probes`, `probes_failed`, `probe_bytes` | `probe_result` events, the failed ones, and their `bytes`. |
| `host_probe_requests`, `host_probes` | `probe_request` events, and `probe_result` events with reason `host`. |
| `upgrade_attempts`, `upgrade_failures` | `upgrade_start` and `upgrade_failed` events. |
| `network_changes`, `network_known`, `network_down` | `network_change` events, the ones with `known`, and `network_down` events. |
| `provider_fetches`, `provider_fetch_failures`, `provider_triggers` | `provider_fetch` events, the ones without `ok`, and their count by `trigger`. |
| `provider_retry_after` | `provider_fetch` events with `retry_after_ms`: a busy backend asked to wait. |
| `provider_applied`, `provider_rejected`, `provider_restart_needed` | `provider_apply` events that changed the set, the rejected ones, and `provider_restart_needed` events. |
| `provider_expired` | `provider_expired` events: leases that ended. |
| `provider_renewals` | `provider_apply` events with `ok` and `renewed` above zero: answers that renewed leases. A renewal keeps what the balancer has and is no decision. |
| `pins`, `releases` | `pin` and `release` events. |
| `memory_low` | `memory_low` events with `on`. |
| `active`, `balancer_exhausted`, `network_up`, `connectivity` | At the time of the report: the chosen outbound by balancer, whether a balancer is exhausted, and the network as in Status. |
| `paths` | The first 16 outbounds by tag. `state` and `ttfb_p50_ms` (over the whole life of the outbound, 0 without samples) as they are now; `active_ms`, `bytes_up`, `bytes_down`, `conns_ok`, `conns_failed`, `probes` and `probe_bytes` grown since the previous report. An outbound forgotten and seen again counts from zero. |

## Status

`Status()` returns the run of the core (`run`), the network (`network`, `network_up`, `connectivity`:
`ok`, `paused`, `doubt` or `ignored`, `generation`, `idle`), whether memory is low (`memory_low`), probe queue sizes, per outbound the state and its age, failures in
the window, quarantine, last probe RTT and error, connections, bytes, time to
first byte (p50, p90), failures by kind, probes and their bytes, time as
the chosen outbound, and what is left of its provider lease
(`expires_in_ms`, below zero with `expired` once it ended); per balancer the
chosen and pinned outbound, whether it is exhausted, the time on the chosen
outbound and the next return attempt; per provider the method, the version
of the set, its size, outbounds still draining, refreshes and failures, the
age of the set, the next timed refresh, the backoff, what is left of a pause
a busy backend asked for (`retry_after_ms`), the next renewal of a lease
(`renew_in_ms`, below zero while it waits for a limit), the leases that
ended (`expired`), and the `skew_ms` and `clock` of the last answer as in
`provider_apply`; the probe requests of the host (`probe_requests`), each with `id`,
`balancer`, `age_ms`, `done`, `end` and the result per outbound, see Probes
on request.
