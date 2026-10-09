package health

import (
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/xtls/xray-core/app/router"
)

// Defaults apply to every zero value of the protobuf config. Boolean fields
// are phrased so that false is the default, therefore a config built in
// protobuf directly behaves like the same config in JSON.
const (
	defaultWindow           = 60 * time.Second
	defaultDialTimeout      = 6 * time.Second
	defaultFirstByteTimeout = 5 * time.Second
	defaultStallTimeout     = 8 * time.Second
	defaultFailures         = 3
	defaultDistinctTargets  = 2
	defaultSuccessBytes     = 24 * 1024
	defaultSweepInterval    = 500 * time.Millisecond
	defaultPartialStallAge  = 30 * time.Second
	defaultStallMinWrite    = 128
	defaultPartialMinBytes  = 8 * 1024

	defaultProbeDestination = "https://www.gstatic.com/generate_204"
	defaultProbeTimeout     = 8 * time.Second
	defaultProbeProgress    = 4 * time.Second
	// A probe without minBytes still stops reading here.
	probeReadLimit = 64 * 1024

	defaultConnectivityTimeout = 4 * time.Second
	defaultMaxPause            = time.Minute

	defaultPauseWhenIdle     = 2 * time.Minute
	defaultConcurrent        = 1
	defaultSpacing           = 2 * time.Second
	defaultSpacingJitter     = 0.3
	defaultPerServerName     = 3
	defaultPerMinute         = 12
	defaultSuspectCooldown   = time.Minute
	defaultHostPerMinute     = 4
	defaultHostReuse         = 15 * time.Second
	defaultHostMaxOutbounds  = 8
	defaultHostTimeout       = 2 * time.Minute
	maxSuspectCooldown       = time.Hour
	defaultMaxEvents         = 1000
	defaultReportInterval    = 24 * time.Hour
	defaultMaxEventBytes     = 64 * 1024
	defaultMaxNetworks       = 16
	defaultMinHold           = 2 * time.Minute
	defaultUpgradeInterval   = 30 * time.Minute
	defaultUpgradeSuccesses  = 3
	defaultUpgradeSpacing    = 20 * time.Second
	defaultPenaltyWindow     = 5 * time.Minute
	defaultPenaltyMultiplier = 2
	defaultMaxPenalty        = 2 * time.Hour
	maxJitter                = 0.9

	// The version of the provider protocol the core speaks.
	providerProtocol = 1
	// How much of an answer that is not 200 is read for its error code, and
	// how long the read may take after the status: a backend sends its error
	// body along with the status.
	maxErrorBody  = 4 << 10
	errorBodyWait = 2 * time.Second
	// The reason an answer was refused, as events, Status and the next
	// request tell it.
	maxFailureText = 200
	// A lease of an outbound counts at most this long: a far end or a clock
	// gone wild does not keep an outbound for good.
	maxLease = 30 * 24 * time.Hour
	// Headers an answer or the host put over those of the config: at most
	// this many, each value at most this long.
	maxRotatedHeaders = 8
	maxHeaderValue    = 4096

	defaultProviderTimeout      = 10 * time.Second
	defaultRefreshInterval      = 2 * time.Hour
	defaultRefreshJitter        = 0.2
	defaultMinHealthy           = 2
	defaultCostAboveAfter       = 15 * time.Minute
	defaultMaxSetAge            = 6 * time.Hour
	defaultNetworkSettle        = 30 * time.Second
	defaultRefreshMinInterval   = 5 * time.Minute
	defaultExhaustedMinInterval = 30 * time.Second
	defaultRefreshesPerHour     = 6
	defaultDrain                = 5 * time.Minute
	defaultMaxOutbounds         = 64
	// After the backend refused the headers, one refresh an hour tries
	// them again: a fleet refused by a mistake of the backend heals by
	// itself, and a dead token costs little.
	defaultAuthRetry = time.Hour
	// A lease is renewed at this share of it, spread by renewJitter, and
	// the renewal is urgent from urgentAt on. Clients that got their leases
	// at once renew them apart: 24 h leases spread over about 2.4 hours.
	defaultRenewAt     = 0.5
	defaultUrgentAt    = 0.8
	defaultRenewJitter = 0.1
	// The renewal comes before the end, and its spread stays within the lease.
	maxRenewAt     = 0.95
	maxRenewJitter = 0.5
	// Without maxBytes an answer may take this much per outbound it may
	// carry. An outbound in the format of the config file takes 1-2 KiB.
	answerBytesPerOutbound = 4 << 10

	// Ceilings of config values: the memory the observer may take stays
	// bounded even when a config asks for more.
	maxProbeTimeout      = 30 * time.Second
	maxConcurrent        = 8
	maxEvents            = 4096
	maxEventBytes        = 256 << 10
	maxNetworks          = 64
	maxAnswerBytes       = 1 << 20
	maxProviderOutbounds = 128
	maxDrain             = 30 * time.Minute
	maxHostReuse         = 5 * time.Minute
	maxHostOutbounds     = 16
	maxHostTimeout       = 10 * time.Minute
)

var (
	defaultProviderProtocols = []string{"vless", "vmess", "trojan", "shadowsocks", "wireguard", "hysteria", "socks", "http"}
	defaultRotatedHeaders    = []string{"Authorization"}
	// A censor that froze a server keeps it frozen for about two minutes and
	// longer if it sees new handshakes, so a failed outbound rests at least
	// that long.
	defaultQuarantine   = backoff{initial: 2 * time.Minute, multiplier: 2, max: 30 * time.Minute, jitter: 0.3}
	defaultConnRecheck  = backoff{initial: 2 * time.Second, multiplier: 2, max: 30 * time.Second, jitter: 0.2}
	defaultExpectStatus = []int32{}
	defaultRefreshRetry = backoff{initial: 30 * time.Second, multiplier: 2, max: 30 * time.Minute, jitter: 0.3}
)

type backoff struct {
	initial    time.Duration
	multiplier float64
	max        time.Duration
	jitter     float64
}

// delay returns the pause for the given attempt (0-based) without jitter.
func (b backoff) delay(attempt int) time.Duration {
	d := float64(b.initial)
	for i := 0; i < attempt; i++ {
		d *= b.multiplier
		if time.Duration(d) >= b.max {
			return b.max
		}
	}
	if time.Duration(d) > b.max {
		return b.max
	}
	return time.Duration(d)
}

type passiveSettings struct {
	window           time.Duration
	dialTimeout      time.Duration
	firstByteTimeout time.Duration
	stallTimeout     time.Duration
	failures         int
	distinctTargets  int
	successBytes     int64
	sweepInterval    time.Duration
	dialOnly         bool
	partialStallAge  time.Duration
	stallMinWrite    int64
	partialMinBytes  int64
	keepPending      bool
}

type probeSettings struct {
	selector        []string
	destination     string
	method          string
	expectStatus    []int32
	minBytes        int64
	timeout         time.Duration
	progressTimeout time.Duration
}

func (p *probeSettings) statusOK(code int) bool {
	if len(p.expectStatus) == 0 {
		return code >= 200 && code < 300
	}
	for _, s := range p.expectStatus {
		if int(s) == code {
			return true
		}
	}
	return false
}

type connectivitySettings struct {
	outbound    string
	destination string
	timeout     time.Duration
	recheck     backoff
	maxPause    time.Duration
	witnesses   []string
}

type scheduleSettings struct {
	pauseWhenIdle   time.Duration
	quarantine      backoff
	concurrent      int
	spacing         time.Duration
	spacingJitter   float64
	perServerName   int
	perMinute       int
	suspectCooldown time.Duration
	hostProbes      hostProbeSettings
}

type hostProbeSettings struct {
	perMinute    int
	reuse        time.Duration
	maxOutbounds int
	timeout      time.Duration
}

type settings struct {
	subjects     []string
	passive      passiveSettings
	probes       []probeSettings
	connectivity *connectivitySettings
	schedule     scheduleSettings
	maxEvents    int
	maxBytes     int
	remember     bool
	maxNetworks  int
	providers    []*providerSettings
	// Zero turns the periodic report off.
	reportInterval time.Duration
}

type providerSettings struct {
	tag string
	url string
	// http.MethodPost or http.MethodGet.
	method   string
	headers  map[string]string
	via      []string
	timeout  time.Duration
	maxBytes int64
	// An answer to apply at the start; dropped once applied.
	initial []byte

	// Zero durations and counts below mean the trigger or limit is off.
	interval       time.Duration
	jitter         float64
	minHealthy     int
	whenExhausted  bool
	costAbove      bool
	costThreshold  float64
	costAboveAfter time.Duration
	maxAge         time.Duration
	onNetwork      RefreshOnNetwork
	networkSettle  time.Duration
	// Shares of a lease at which it is renewed and the renewal is urgent;
	// renewAt zero turns renewal off, the lease still ends.
	renewAt     float64
	urgentAt    float64
	renewJitter float64

	minInterval          time.Duration
	exhaustedMinInterval time.Duration
	maxPerHour           int
	backoff              backoff
	// How long every trigger but the host waits after the backend refused
	// the headers; zero waits until they change.
	authRetry time.Duration

	balancer     string
	prefix       string
	drain        time.Duration
	maxOutbounds int
	protocols    []string
	routing      bool
	// Canonical header names an answer may set; none turns that off.
	rotate []string
}

// optional maps a config value where zero means the default and a negative
// value turns the feature off.
func optional(v int64, def time.Duration) time.Duration {
	if v < 0 {
		return 0
	}
	return dur(v, def)
}

func optionalNum(v int32, def int) int {
	if v < 0 {
		return 0
	}
	return num(v, def)
}

func newProviderSettings(c *ProviderConfig) *providerSettings {
	t := c.GetTriggers()
	l := c.GetLimits()
	a := c.GetApply()
	maxOutbounds := min(num(a.GetMaxOutbounds(), defaultMaxOutbounds), maxProviderOutbounds)
	p := &providerSettings{
		tag:      c.GetTag(),
		url:      c.GetUrl(),
		method:   http.MethodPost,
		headers:  c.GetHeaders(),
		via:      c.GetVia(),
		timeout:  dur(c.GetTimeout(), defaultProviderTimeout),
		maxBytes: min(size(c.GetMaxBytes(), int64(maxOutbounds)*answerBytesPerOutbound), maxAnswerBytes),
		initial:  c.GetInitial(),

		interval:      optional(t.GetInterval(), defaultRefreshInterval),
		minHealthy:    optionalNum(t.GetMinHealthy(), defaultMinHealthy),
		whenExhausted: !t.GetSkipWhenExhausted(),
		maxAge:        optional(t.GetMaxAge(), defaultMaxSetAge),
		onNetwork:     t.GetOnNetworkChange(),
		networkSettle: dur(t.GetNetworkSettle(), defaultNetworkSettle),

		minInterval:          optional(l.GetMinInterval(), defaultRefreshMinInterval),
		exhaustedMinInterval: optional(l.GetExhaustedMinInterval(), defaultExhaustedMinInterval),
		maxPerHour:           optionalNum(l.GetMaxPerHour(), defaultRefreshesPerHour),
		backoff:              toBackoff(l.GetBackoff(), defaultRefreshRetry),
		authRetry:            optional(l.GetAuthRetry(), defaultAuthRetry),

		balancer:     a.GetBalancer(),
		prefix:       a.GetTagPrefix(),
		drain:        min(dur(a.GetDrain(), defaultDrain), maxDrain),
		maxOutbounds: maxOutbounds,
		protocols:    stringsOr(a.GetProtocols(), defaultProviderProtocols),
		routing:      a.GetRouting(),
	}
	if strings.EqualFold(c.GetMethod(), http.MethodGet) {
		p.method = http.MethodGet
	}
	if !a.GetFixedHeaders() {
		// A name the request may not take from outside is never rotated,
		// even when a config built without the JSON loader lists it.
		for _, name := range stringsOr(a.GetHeaders(), defaultRotatedHeaders) {
			if canon := http.CanonicalHeaderKey(name); CheckHeaderName(name) == nil && !slices.Contains(p.rotate, canon) {
				p.rotate = append(p.rotate, canon)
			}
		}
	}
	if t.GetJitter() >= 0 {
		p.jitter = jitter(t.GetJitter(), defaultRefreshJitter)
	}
	if ca := t.GetCostAbove(); ca != nil {
		p.costAbove = true
		p.costThreshold = float64(ca.GetCost())
		p.costAboveAfter = dur(ca.GetAfter(), defaultCostAboveAfter)
	}
	if ex := t.GetExpiry(); ex.GetRenewAt() >= 0 {
		p.renewAt = min(fnum(ex.GetRenewAt(), defaultRenewAt), maxRenewAt)
		p.urgentAt = min(max(fnum(ex.GetUrgentAt(), defaultUrgentAt), p.renewAt), maxRenewAt)
		if ex.GetJitter() >= 0 {
			p.renewJitter = min(fnum(ex.GetJitter(), defaultRenewJitter), maxRenewJitter)
		}
	}
	return p
}

func dur(v int64, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return time.Duration(v)
}

func num(v int32, def int) int {
	if v <= 0 {
		return def
	}
	return int(v)
}

func fnum(v float64, def float64) float64 {
	if v <= 0 {
		return def
	}
	return v
}

func size(v int64, def int64) int64 {
	if v <= 0 {
		return def
	}
	return v
}

// jitter keeps a spread within [0, maxJitter]: a spread of 1 or more would
// make pauses zero or negative.
func jitter(v float64, def float64) float64 {
	v = fnum(v, def)
	if v > maxJitter {
		return maxJitter
	}
	return v
}

func toBackoff(b *Backoff, def backoff) backoff {
	if b == nil {
		return def
	}
	return backoff{
		initial:    dur(b.Initial, def.initial),
		multiplier: fnum(b.Multiplier, def.multiplier),
		max:        dur(b.Max, def.max),
		jitter:     jitter(b.Jitter, def.jitter),
	}
}

func newSettings(c *Config) *settings {
	s := &settings{subjects: append([]string(nil), c.GetSubjectSelector()...)}
	p := c.GetPassive()
	s.passive = passiveSettings{
		window:           dur(p.GetWindow(), defaultWindow),
		dialTimeout:      dur(p.GetDialTimeout(), defaultDialTimeout),
		firstByteTimeout: dur(p.GetFirstByteTimeout(), defaultFirstByteTimeout),
		stallTimeout:     dur(p.GetStallTimeout(), defaultStallTimeout),
		failures:         num(p.GetFailures(), defaultFailures),
		distinctTargets:  num(p.GetDistinctTargets(), defaultDistinctTargets),
		successBytes:     size(p.GetSuccessBytes(), defaultSuccessBytes),
		sweepInterval:    dur(p.GetSweepInterval(), defaultSweepInterval),
		dialOnly:         p.GetDialOnly(),
		partialStallAge:  dur(p.GetPartialStallMaxAge(), defaultPartialStallAge),
		stallMinWrite:    size(p.GetStallMinWriteBytes(), defaultStallMinWrite),
		partialMinBytes:  size(p.GetPartialStallMinBytes(), defaultPartialMinBytes),
		keepPending:      p.GetKeepPendingOnDown(),
	}
	if p.GetIgnorePartialStalls() {
		s.passive.partialStallAge = 0
	}
	for _, pp := range c.GetProbes() {
		s.probes = append(s.probes, probeSettings{
			selector:        pp.GetSelector(),
			destination:     stringOr(pp.GetDestination(), defaultProbeDestination),
			method:          strings.ToUpper(stringOr(pp.GetMethod(), "GET")),
			expectStatus:    pp.GetExpectStatus(),
			minBytes:        pp.GetMinBytes(),
			timeout:         min(dur(pp.GetTimeout(), defaultProbeTimeout), maxProbeTimeout),
			progressTimeout: min(dur(pp.GetProgressTimeout(), defaultProbeProgress), maxProbeTimeout),
		})
	}
	if cc := c.GetConnectivity(); cc != nil && cc.GetOutbound() != "" && cc.GetDestination() != "" {
		s.connectivity = &connectivitySettings{
			outbound:    cc.GetOutbound(),
			destination: cc.GetDestination(),
			timeout:     min(dur(cc.GetTimeout(), defaultConnectivityTimeout), maxProbeTimeout),
			recheck:     toBackoff(cc.GetRecheck(), defaultConnRecheck),
			maxPause:    dur(cc.GetMaxPause(), defaultMaxPause),
			witnesses:   cc.GetWitnesses(),
		}
	}
	sc := c.GetSchedule()
	s.schedule = scheduleSettings{
		pauseWhenIdle:   dur(sc.GetPauseWhenIdle(), defaultPauseWhenIdle),
		quarantine:      toBackoff(sc.GetQuarantine(), defaultQuarantine),
		concurrent:      min(num(sc.GetConcurrent(), defaultConcurrent), maxConcurrent),
		spacing:         dur(sc.GetSpacing(), defaultSpacing),
		spacingJitter:   jitter(sc.GetSpacingJitter(), defaultSpacingJitter),
		perServerName:   num(sc.GetPerServerNamePerMinute(), defaultPerServerName),
		perMinute:       num(sc.GetPerMinute(), defaultPerMinute),
		suspectCooldown: dur(sc.GetSuspectCooldown(), defaultSuspectCooldown),
	}
	hp := sc.GetHostProbes()
	s.schedule.hostProbes = hostProbeSettings{
		// The observer keeps at least half of the budget for finding failures.
		perMinute:    min(num(hp.GetPerMinute(), defaultHostPerMinute), max(s.schedule.perMinute/2, 1)),
		reuse:        min(dur(hp.GetReuse(), defaultHostReuse), maxHostReuse),
		maxOutbounds: min(num(hp.GetMaxOutbounds(), defaultHostMaxOutbounds), maxHostOutbounds),
		timeout:      min(dur(hp.GetTimeout(), defaultHostTimeout), maxHostTimeout),
	}
	s.maxEvents = min(num(c.GetEvents().GetMaxEvents(), defaultMaxEvents), maxEvents)
	s.maxBytes = int(min(size(c.GetEvents().GetMaxBytes(), defaultMaxEventBytes), maxEventBytes))
	s.reportInterval = optional(c.GetReport().GetInterval(), defaultReportInterval)
	s.remember = !c.GetNetwork().GetNoMemory()
	s.maxNetworks = min(num(c.GetNetwork().GetMaxNetworks(), defaultMaxNetworks), maxNetworks)
	for _, pc := range c.GetProviders() {
		ps := newProviderSettings(pc)
		s.providers = append(s.providers, ps)
		// Outbounds a provider brings are observed like the configured ones.
		if ps.prefix != "" && !matchesAny(ps.prefix, s.subjects) {
			s.subjects = append(s.subjects, ps.prefix)
		}
	}
	return s
}

func (s *settings) observes(tag string) bool {
	return matchesAny(tag, s.subjects)
}

func (s *settings) witness(tag string) bool {
	return s.connectivity != nil && len(s.connectivity.witnesses) > 0 && matchesAny(tag, s.connectivity.witnesses)
}

func (s *settings) probeFor(tag string) *probeSettings {
	for i := range s.probes {
		if len(s.probes[i].selector) == 0 || matchesAny(tag, s.probes[i].selector) {
			return &s.probes[i]
		}
	}
	return &probeSettings{
		destination:     defaultProbeDestination,
		method:          "GET",
		expectStatus:    defaultExpectStatus,
		timeout:         defaultProbeTimeout,
		progressTimeout: defaultProbeProgress,
	}
}

type selectorSettings struct {
	weights          *router.WeightManager
	lazy             []string
	switchAfterProbe bool
	confirm          ConfirmMode
	minHold          time.Duration
	upgrade          upgradeSettings
	startPinned      string
	startPinnedFor   time.Duration
	startWith        string
	onNetworkChange  NetworkChangeMode
}

type upgradeSettings struct {
	interval            time.Duration
	randomPhase         bool
	successes           int
	spacing             time.Duration
	penaltyWindow       time.Duration
	penaltyMultiplier   float64
	maxPenalty          time.Duration
	onNetworkChange     bool
	unknownNetworksOnly bool
}

func newSelectorSettings(c *SelectorConfig) *selectorSettings {
	u := c.GetUpgrade()
	defaultCost := float64(c.GetDefaultCost())
	if defaultCost == 0 {
		for _, w := range c.GetCosts() {
			if float64(w.GetValue()) > defaultCost {
				defaultCost = float64(w.GetValue())
			}
		}
	}
	return &selectorSettings{
		weights:          router.NewWeightManager(c.GetCosts(), defaultCost, func(v, w float64) float64 { return v * w }),
		lazy:             c.GetLazy(),
		switchAfterProbe: !c.GetSwitchWithoutProbe(),
		confirm:          c.GetConfirm(),
		minHold:          dur(c.GetMinHold(), defaultMinHold),
		upgrade: upgradeSettings{
			interval:            dur(u.GetInterval(), defaultUpgradeInterval),
			randomPhase:         !u.GetFixedPhase(),
			successes:           num(u.GetSuccesses(), defaultUpgradeSuccesses),
			spacing:             dur(u.GetSpacing(), defaultUpgradeSpacing),
			penaltyWindow:       dur(u.GetPenaltyWindow(), defaultPenaltyWindow),
			penaltyMultiplier:   fnum(u.GetPenaltyMultiplier(), defaultPenaltyMultiplier),
			maxPenalty:          dur(u.GetMaxPenalty(), defaultMaxPenalty),
			onNetworkChange:     !u.GetSkipOnNetworkChange(),
			unknownNetworksOnly: !u.GetAllNetworks(),
		},
		startPinned:     c.GetStartPinned(),
		startPinnedFor:  time.Duration(max(c.GetStartPinnedFor(), 0)),
		startWith:       c.GetStartWith(),
		onNetworkChange: c.GetOnNetworkChange(),
	}
}

func (s *selectorSettings) cost(tag string) float64 {
	return s.weights.Get(tag)
}

func (s *selectorSettings) isLazy(tag string) bool {
	return matchesAny(tag, s.lazy)
}

func matchesAny(tag string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(tag, p) {
			return true
		}
	}
	return false
}

func stringsOr(v, def []string) []string {
	if len(v) == 0 {
		return def
	}
	return v
}

func stringOr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
