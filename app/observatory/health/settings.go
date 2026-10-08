package health

import (
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
	maxSuspectCooldown       = time.Hour
	defaultMaxEvents         = 1000
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

	defaultProviderTimeout      = 10 * time.Second
	defaultProviderMaxBytes     = 1 << 20
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
)

var (
	defaultProviderProtocols = []string{"vless", "vmess", "trojan", "shadowsocks", "wireguard", "hysteria", "socks", "http"}
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
}

type providerSettings struct {
	tag      string
	url      string
	headers  map[string]string
	via      []string
	timeout  time.Duration
	maxBytes int64

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

	minInterval          time.Duration
	exhaustedMinInterval time.Duration
	maxPerHour           int
	backoff              backoff

	balancer     string
	prefix       string
	drain        time.Duration
	maxOutbounds int
	protocols    []string
	routing      bool
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
	p := &providerSettings{
		tag:      c.GetTag(),
		url:      c.GetUrl(),
		headers:  c.GetHeaders(),
		via:      c.GetVia(),
		timeout:  dur(c.GetTimeout(), defaultProviderTimeout),
		maxBytes: size(c.GetMaxBytes(), defaultProviderMaxBytes),

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

		balancer:     a.GetBalancer(),
		prefix:       a.GetTagPrefix(),
		drain:        dur(a.GetDrain(), defaultDrain),
		maxOutbounds: num(a.GetMaxOutbounds(), defaultMaxOutbounds),
		protocols:    stringsOr(a.GetProtocols(), defaultProviderProtocols),
		routing:      a.GetRouting(),
	}
	if t.GetJitter() >= 0 {
		p.jitter = jitter(t.GetJitter(), defaultRefreshJitter)
	}
	if ca := t.GetCostAbove(); ca != nil {
		p.costAbove = true
		p.costThreshold = float64(ca.GetCost())
		p.costAboveAfter = dur(ca.GetAfter(), defaultCostAboveAfter)
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
			timeout:         dur(pp.GetTimeout(), defaultProbeTimeout),
			progressTimeout: dur(pp.GetProgressTimeout(), defaultProbeProgress),
		})
	}
	if cc := c.GetConnectivity(); cc != nil && cc.GetOutbound() != "" && cc.GetDestination() != "" {
		s.connectivity = &connectivitySettings{
			outbound:    cc.GetOutbound(),
			destination: cc.GetDestination(),
			timeout:     dur(cc.GetTimeout(), defaultConnectivityTimeout),
			recheck:     toBackoff(cc.GetRecheck(), defaultConnRecheck),
			maxPause:    dur(cc.GetMaxPause(), defaultMaxPause),
			witnesses:   cc.GetWitnesses(),
		}
	}
	sc := c.GetSchedule()
	s.schedule = scheduleSettings{
		pauseWhenIdle:   dur(sc.GetPauseWhenIdle(), defaultPauseWhenIdle),
		quarantine:      toBackoff(sc.GetQuarantine(), defaultQuarantine),
		concurrent:      num(sc.GetConcurrent(), defaultConcurrent),
		spacing:         dur(sc.GetSpacing(), defaultSpacing),
		spacingJitter:   jitter(sc.GetSpacingJitter(), defaultSpacingJitter),
		perServerName:   num(sc.GetPerServerNamePerMinute(), defaultPerServerName),
		perMinute:       num(sc.GetPerMinute(), defaultPerMinute),
		suspectCooldown: dur(sc.GetSuspectCooldown(), defaultSuspectCooldown),
	}
	s.maxEvents = num(c.GetEvents().GetMaxEvents(), defaultMaxEvents)
	s.maxBytes = int(size(c.GetEvents().GetMaxBytes(), defaultMaxEventBytes))
	s.remember = !c.GetNetwork().GetNoMemory()
	s.maxNetworks = num(c.GetNetwork().GetMaxNetworks(), defaultMaxNetworks)
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
