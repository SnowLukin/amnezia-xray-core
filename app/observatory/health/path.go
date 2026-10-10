package health

import (
	"time"
)

type pathStatus int

const (
	statusUnknown pathStatus = iota
	statusUp
	statusSuspect
	statusDown
)

func (s pathStatus) String() string {
	switch s {
	case statusUp:
		return "up"
	case statusSuspect:
		return "suspect"
	case statusDown:
		return "down"
	default:
		return "unknown"
	}
}

type failureKind int

const (
	// The path itself failed, whatever the destination.
	failDial failureKind = iota
	failDialTimeout
	failUnverified
	// The failure may belong to one destination.
	failNoResponse
	failStall
	failPartialStall
	// The connection sent a request and ended before any answer, as a
	// transport that connects lazily does when its server resets it.
	failClosed
)

func (k failureKind) String() string {
	switch k {
	case failDial:
		return "dial_failed"
	case failDialTimeout:
		return "dial_timeout"
	case failUnverified:
		return "server_unverified"
	case failNoResponse:
		return "no_response"
	case failStall:
		return "stall"
	case failPartialStall:
		return "partial_stall"
	case failClosed:
		return "closed"
	default:
		return "unknown"
	}
}

func (k failureKind) pathLevel() bool {
	return k == failDial || k == failDialTimeout || k == failUnverified
}

type failure struct {
	at     time.Time
	kind   failureKind
	target string
	// Session of the user connection; its dial retries count once.
	conn uint32
}

const maxFailures = 64

// path is everything known about one outbound.
type path struct {
	tag string
	// When the outbound first showed up as a candidate or with a connection:
	// the counters below count from here.
	created     time.Time
	status      pathStatus
	statusSince time.Time
	reason      string

	// Failures since the last proof that the path works.
	failures    []failure
	lastSuccess time.Time
	// Start of the newest connection that received bytes. Dial failures
	// from older connections may arrive after that reply.
	lastAnsweredStart int64

	// After a probe cleared a suspicion, new suspicions from weak evidence wait
	// until this time. The wait grows with every false alarm in a row.
	cooldownUntil time.Time
	falseAlarms   int
	// The suspicion a confirming probe is about rests on evidence about the
	// path itself, which confirm always probes as well: a probe that fails
	// confirms strong evidence then, not weak.
	strongSuspicion bool

	quarantineLevel int
	quarantineUntil time.Time
	// Ruled out while the connectivity check was failing.
	provisional bool
	// The outbound went down while a confirming probe of it ran: what a
	// pass of that probe takes back, see rescue.go.
	rescue *rescueMark

	probing bool
	// When the last probe started, and when the last one finished: lastRTT,
	// lastError and lastClass are those of the probe that finished.
	lastProbe    time.Time
	lastProbeEnd time.Time
	lastRTT      time.Duration
	lastError    string
	// The class of lastError, which a provider may see: the error itself may
	// carry the URL of the probe.
	lastClass string

	serverName string
	stats      pathStats
	// The counters at the previous report.
	reported pathTotals

	// Sleep, see sleep.go. The outbound was used since it last slept, last
	// at usedAt; uses counts the uses, so a result of Sleep that a later use
	// overtook is told apart. asleep: Sleep released what it held.
	awake  bool
	asleep bool
	usedAt time.Time
	uses   uint64
	// Whether the proxy can sleep, once the observer asked its handler.
	sleeper sleeperKind
	// A batch asks the handler now.
	sleeping bool
	// Refusals in a row, and when to ask again after the last one.
	sleepTries   int
	nextSleepTry time.Time
	// Connections its proxy processes now, as its handler tells: it does
	// not sleep under one.
	processes int
	// Ending idle connections, see idle.go: the uses of the path at the
	// last check, and when a connection it kept may first have received
	// nothing for closeIdleAfter, zero when none may. idleClosed: a check
	// ended connections, and the outbound sleeps soon after the rest are
	// gone.
	idleUses   uint64
	idleNext   time.Time
	idleClosed bool
}

type pathStats struct {
	connsOK        int64
	connsFailed    int64
	bytesUp        int64
	bytesDown      int64
	failuresByKind [failClosed + 1]int64
	ttfb           histogram
	probes         int64
	probesFailed   int64
	probeBytes     int64
	activeTime     time.Duration
}

func (p *path) setStatus(s pathStatus, reason string, now time.Time) bool {
	if p.status == s && p.reason == reason {
		return false
	}
	changed := p.status != s
	p.status = s
	p.reason = reason
	if changed {
		p.statusSince = now
	}
	return changed
}

// addFailure records f and reports whether it is new evidence. AllowedProtocols retry
// a failed dial several times for one user connection; only the first
// failure of a connection counts.
func (p *path) addFailure(f failure, window time.Duration) bool {
	p.pruneFailures(f.at, window)
	if f.conn != 0 {
		for _, old := range p.failures {
			if old.conn == f.conn && old.kind.pathLevel() == f.kind.pathLevel() {
				return false
			}
		}
	}
	if len(p.failures) == maxFailures {
		copy(p.failures, p.failures[1:])
		p.failures = p.failures[:maxFailures-1]
	}
	p.failures = append(p.failures, f)
	p.stats.failuresByKind[f.kind]++
	return true
}

func (p *path) pruneFailures(now time.Time, window time.Duration) {
	if window <= 0 {
		return
	}
	cut := 0
	for cut < len(p.failures) && now.Sub(p.failures[cut].at) > window {
		cut++
	}
	if cut > 0 {
		p.failures = append(p.failures[:0], p.failures[cut:]...)
	}
}

// verdict tells whether the recent failures make the path suspect.
type verdict struct {
	suspect bool
	// All failures are about the path, not about destinations. Such evidence
	// needs no confirming probe.
	strong   bool
	failures int
	targets  int
	kinds    map[string]int
}

func (p *path) evaluate(cfg *passiveSettings) verdict {
	v := verdict{failures: len(p.failures), kinds: map[string]int{}}
	pathLevel := 0
	targets := map[string]struct{}{}
	for _, f := range p.failures {
		v.kinds[f.kind.String()]++
		if f.kind.pathLevel() {
			pathLevel++
			continue
		}
		targets[f.target] = struct{}{}
	}
	v.targets = len(targets)
	if pathLevel >= cfg.failures {
		v.suspect = true
		v.strong = true
		return v
	}
	if v.failures >= cfg.failures && v.targets >= cfg.minDistinctTargets {
		v.suspect = true
	}
	return v
}

// success forgets the failures: they happened before the path was proven to work.
func (p *path) success(now time.Time) {
	p.failures = p.failures[:0]
	p.lastSuccess = now
}

func (p *path) quarantined(now time.Time) bool {
	return p.status == statusDown && now.Before(p.quarantineUntil)
}

// usable means the path may be chosen without waiting.
func (p *path) usable(now time.Time) bool {
	return !p.quarantined(now)
}
