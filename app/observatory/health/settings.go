package health

import (
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/net"
)

// Defaults apply to every zero value of the protobuf config. Boolean fields
// are phrased so that false is the default, therefore a config built in
// protobuf directly behaves like the same config in JSON.
const (
	defaultWakeFreshness      = time.Minute
	defaultWakeMinInterval    = 3 * time.Hour
	defaultWindow             = 60 * time.Second
	defaultDialTimeout        = 6 * time.Second
	defaultFirstByteTimeout   = 5 * time.Second
	defaultStallTimeout       = 8 * time.Second
	defaultFailures           = 3
	defaultMinDistinctTargets = 2
	defaultSuccessBytes       = 24 * 1024
	defaultSweepInterval      = 500 * time.Millisecond
	defaultPartialStallAge    = 30 * time.Second
	defaultStallMinWrite      = 128
	defaultPartialMinBytes    = 8 * 1024

	defaultProbeTimeout  = 8 * time.Second
	defaultProbeProgress = 4 * time.Second
	// A probe without minBytes still stops reading here.
	probeReadLimit = 64 * 1024

	defaultConnectivityTimeout = 4 * time.Second
	defaultMaxPause            = time.Minute

	defaultIdleAfter                    = 2 * time.Minute
	defaultConcurrent                   = 1
	defaultSpacing                      = 2 * time.Second
	defaultSpacingJitter                = 0.3
	defaultProbesPerServerNamePerMinute = 3
	defaultPerMinute                    = 12
	defaultSuspectCooldown              = time.Minute
	defaultHostPerMinute                = 4
	defaultHostReuse                    = 15 * time.Second
	defaultHostMaxOutbounds             = 8
	defaultHostTimeout                  = 2 * time.Minute
	defaultProbeOnlyKeepVerified        = 1
	defaultProbeOnlyRecheckInterval     = 15 * time.Minute
	maxProbeOnlyKeepVerified            = 4
	maxSuspectCooldown                  = time.Hour
	defaultMaxEvents                    = 1000
	defaultReportInterval               = 24 * time.Hour
	defaultCensusStartAfter             = 3 * time.Minute
	defaultMaxEventBytes                = 64 * 1024
	defaultMaxNetworks                  = 16
	defaultMinHoldTime                  = 2 * time.Minute
	defaultReturnToCheaperInterval      = 30 * time.Minute
	defaultReturnToCheaperSuccesses     = 3
	defaultReturnToCheaperSpacing       = 20 * time.Second
	defaultPenaltyWindow                = 5 * time.Minute
	defaultPenaltyMultiplier            = 2
	defaultMaxPenalty                   = 2 * time.Hour
	maxJitter                           = 0.9

	// An outbound no balancer chooses sleeps this long after its last use,
	// as long as penaltyWindow: a return to a cheaper outbound that fails
	// within it finds the previous one still awake. Shorter, a connection
	// that just ended may still be closing inside the tunnel.
	defaultSleepAfter = 5 * time.Minute
	minSleepAfter     = 10 * time.Second
	// An outbound that refused to sleep is asked again after the longer of
	// sleep.afterLastUse and minSleepRetry, doubling with every refusal up to
	// maxSleepRetry: the observer may not follow all of its connections,
	// such as one idle for long, and they end by themselves.
	minSleepRetry = time.Minute
	maxSleepRetry = 30 * time.Minute
	// An outbound found down sleeps at once, and one that refused is asked
	// again after minDownSleepRetry, doubling the same way.
	minDownSleepRetry = 5 * time.Second
	// sleep.closeIdleAfter, when on, is at least the shortest sleep.afterLastUse:
	// a connection between two requests is not idle. It is at most
	// trackerAbandoned, after which the observer may stop following a
	// connection without traffic and could no longer end it.
	minCloseIdleAfter = minSleepAfter
	maxCloseIdleAfter = trackerAbandoned

	// The version of the provider protocol the core speaks.
	providerProtocol = 1
	// How much of an answer that is not 200 is read for its error code, and
	// how long the read may take after the status: a backend sends its error
	// body along with the status.
	maxErrorBody  = 4 << 10
	errorBodyWait = 2 * time.Second
	// One refresh ends after this many answers out of the protocol, a
	// server that took the request and did not answer among them, or after
	// this many timeouts of any kind.
	maxNonProtocolAnswers = 2
	maxRefreshTimeouts    = 4
	// The reason an answer was refused, as events, Status and the next
	// request tell it.
	maxFailureText = 200
	// A lease of an outbound counts at most this long: a far end or a clock
	// gone wild does not keep an outbound for good.
	maxLease = 30 * 24 * time.Hour
	// Headers an answer or the host put over those of the config: at most
	// this many, each name and value at most this long.
	maxRotatedHeaders = 8
	maxHeaderName     = 256
	maxHeaderValue    = 4096
	// What ProviderSet may add to an answer of at most maxBytes: the
	// headers an answer or the host set, a value at most six times as long
	// once escaped for JSON, and serverTime and receivedAt. An initial set,
	// which may be such a set, may be this much larger than an answer.
	keptSetRoom = maxRotatedHeaders*(maxHeaderName+6*maxHeaderValue+6) + 16 + 2*64
	// The name of a base config, in the config and in an answer.
	maxBaseVersion = 256

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
	udpReplyTimeout    time.Duration
	loneFailureAfter   time.Duration
	window             time.Duration
	dialTimeout        time.Duration
	firstByteTimeout   time.Duration
	stallTimeout       time.Duration
	failures           int
	minDistinctTargets int
	successBytes       int64
	sweepInterval      time.Duration
	dialOnly           bool
	partialStallAge    time.Duration
	stallMinWrite      int64
	partialMinBytes    int64
	keepPending        bool
	keepSilent         bool
	skipQuality        bool
}

type probeSettings struct {
	isProbeOnly     bool
	name            string
	destination     string
	method          string
	expectStatus    []int32
	minBytes        int64
	timeout         time.Duration
	progressTimeout time.Duration
	steps           []probeStepSettings
}

type probeStepSettings struct {
	name            string
	destinations    []string
	isOptional      bool
	method          string
	expectStatus    []int32
	minBytes        int64
	timeout         time.Duration
	progressTimeout time.Duration
}

func (p *probeSettings) statusOK(code int) bool {
	return probeStatusOK(p.expectStatus, code)
}

func (p *probeStepSettings) statusOK(code int) bool {
	return probeStatusOK(p.expectStatus, code)
}

func probeStatusOK(expected []int32, code int) bool {
	if len(expected) == 0 {
		return code >= 200 && code < 300
	}
	for _, s := range expected {
		if int(s) == code {
			return true
		}
	}
	return false
}

type connectivitySettings struct {
	destinations []string
	outbound     string
	destination  string
	timeout      time.Duration
	recheck      backoff
	maxPause     time.Duration
	witnesses    []string
}

type scheduleSettings struct {
	idleAfter                    time.Duration
	quarantine                   backoff
	concurrent                   int
	spacing                      time.Duration
	spacingJitter                float64
	probesPerServerNamePerMinute int
	perMinute                    int
	suspectCooldown              time.Duration
	hostProbes                   hostProbeSettings
	probeOnly                    probeOnlySettings
}

type probeOnlySettings struct {
	keepVerified    int
	recheckInterval time.Duration
}

type pathSettings struct {
	labels      map[string]string
	probe       string
	isProbeOnly bool
}

type hostProbeSettings struct {
	perMinute    int
	reuse        time.Duration
	maxOutbounds int
	timeout      time.Duration
}

type wakeSettings struct {
	freshness   time.Duration
	minInterval time.Duration
}

type censusSettings struct {
	sampleFraction float64
	startAfter     time.Duration
	onDemand       map[string]bool
}

type settings struct {
	census            *censusSettings
	labelRequest      *labelRequestSettings
	reportLabels      map[string]string
	disabledProviders map[string]bool
	features          featureSettings
	trace             *traceSettings
	witnesses         []string
	udpProbe          *udpSettings
	wake              wakeSettings
	paths             sync.Map
	baseOrder         map[string][]string
	passive           passiveSettings
	probes            []probeSettings
	connectivity      *connectivitySettings
	schedule          scheduleSettings
	maxEvents         int
	maxBytes          int
	remember          bool
	maxNetworks       int
	providers         []*providerSettings
	// Zero turns the periodic report off.
	reportInterval   time.Duration
	reportQuietShare float64
	// How long after its last use an outbound no balancer chooses is put
	// to sleep; zero turns sleep off.
	sleepAfter time.Duration
	// Connections that received nothing for this long are ended on an
	// outbound that can sleep and that no balancer has chosen for
	// sleepAfter, see idle.go; zero turns it off.
	closeIdleAfter time.Duration
}

type providerSettings struct {
	probeOnlyRoads  bool
	plainHTTP       bool
	leaseRenewal    bool
	enabled         bool
	backendSchedule bool
	expiredFallback bool
	tag             string
	url             string
	urls            []string
	// http.MethodPost or http.MethodGet.
	method   string
	headers  map[string]string
	via      []string
	timeout  time.Duration
	maxBytes int64
	// An answer to apply at the start; dropped once applied.
	initial []byte
	// The base config the core started with as the backend named it; empty
	// takes the one of a fresh initial.
	baseVersion string

	// Zero durations and counts below mean the trigger or limit is off.
	periodic          bool
	intervalMax       time.Duration
	networkSettleMax  time.Duration
	costAboveAfterMax time.Duration
	exhaustedDelay    durationRange
	minHealthyDelay   durationRange
	interval          time.Duration
	jitter            float64
	minHealthy        int
	whenExhausted     bool
	costAbove         bool
	costThreshold     float64
	costAboveAfter    time.Duration
	maxAge            time.Duration
	onNetwork         RefreshOnNetwork
	networkSettle     time.Duration
	// Shares of a lease at which it is renewed and the renewal is urgent;
	// renewAt zero turns renewal off, the lease still ends.
	renewAt     float64
	urgentAt    float64
	renewJitter float64

	minInterval          time.Duration
	exhaustedMinInterval time.Duration
	maxPerHour           int
	backoff              backoff
	// How long every trigger, the host's too, waits after the backend
	// refused the headers; zero waits until they change.
	authRetry time.Duration

	balancers    []string
	balancer     string
	prefix       string
	drain        time.Duration
	maxOutbounds int
	protocols    []string
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

// Singular protobuf carriers are retained for in-process callers; JSON only
// exposes lists. Copy lists so normalization cannot alias caller-owned slices.
func configuredAddresses(values []string, legacy string) []string {
	if len(values) > 0 {
		return slices.Clone(values)
	}
	if legacy != "" {
		return []string{legacy}
	}
	return nil
}

func newProviderSettings(c *ProviderConfig) *providerSettings {
	f := c.GetFeatures()
	if f == nil {
		f = &ProviderFeatureConfig{}
	}
	t := c.GetTriggers()
	if t == nil {
		t = &ProviderTriggers{}
	}
	l := c.GetLimits()
	a := c.GetApply()
	maxOutbounds := min(num(a.GetMaxOutbounds(), defaultMaxOutbounds), maxProviderOutbounds)
	p := &providerSettings{
		probeOnlyRoads:  c.GetProbeOnlyRoads(),
		plainHTTP:       c.GetIsPlainHttpAllowed(),
		enabled:         featureValue(c.IsEnabled, false),
		periodic:        featureValue(f.Periodic, false),
		backendSchedule: featureValue(f.BackendSchedule, true),
		leaseRenewal:    featureValue(f.LeaseRenewal, true),
		expiredFallback: featureValue(f.ExpiredLeaseFallback, true),
		tag:             c.GetTag(),
		url:             c.GetUrl(),
		urls:            slices.Clone(c.GetUrls()),
		method:          http.MethodPost,
		headers:         c.GetHeaders(),
		via:             c.GetVia(),
		timeout:         dur(c.GetTimeout(), defaultProviderTimeout),
		maxBytes:        min(int64(maxOutbounds)*answerBytesPerOutbound, maxAnswerBytes),
		initial:         c.GetInitialAnswer(),
		// The JSON loader refuses a longer one; an answer cannot name it.
		baseVersion: cutText(c.GetBaseVersion(), maxBaseVersion),

		interval:      optional(t.GetInterval(), defaultRefreshInterval),
		minHealthy:    optionalNum(t.GetMinHealthyOutbounds(), defaultMinHealthy),
		whenExhausted: featureValue(f.Exhausted, true),
		maxAge:        optional(t.GetMaxAge(), defaultMaxSetAge),
		onNetwork:     t.GetNetworkChangeMode(),
		networkSettle: presentDelay(t.NetworkSettleDelay, defaultNetworkSettle),

		minInterval:          optional(l.GetMinInterval(), defaultRefreshMinInterval),
		exhaustedMinInterval: optional(l.GetExhaustedMinInterval(), defaultExhaustedMinInterval),
		maxPerHour:           optionalNum(l.GetMaxRefreshesPerHour(), defaultRefreshesPerHour),
		backoff:              defaultRefreshRetry,
		authRetry:            defaultAuthRetry,

		balancers:    slices.Clone(a.GetBalancers()),
		prefix:       a.GetTagPrefix(),
		drain:        min(dur(a.GetDrainTimeout(), defaultDrain), maxDrain),
		maxOutbounds: maxOutbounds,
		protocols:    stringsOr(a.GetAllowedProtocols(), defaultProviderProtocols),
	}
	if len(p.balancers) > 0 {
		p.balancer = p.balancers[0]
	}
	p.interval, p.intervalMax = configuredRange(t.GetInterval(), t.IntervalMax, durationRange{96 * time.Minute, 144 * time.Minute})
	p.networkSettle, p.networkSettleMax = configuredRange(t.GetNetworkSettleDelay(), t.NetworkSettleMax, durationRange{30 * time.Second, 90 * time.Second})
	if t.NetworkSettleDelay != nil && t.NetworkSettleMax == nil {
		p.networkSettle = time.Duration(*t.NetworkSettleDelay)
		p.networkSettleMax = p.networkSettle
	}
	p.exhaustedDelay = protoRange(t.GetExhaustedDelay(), durationRange{0, 10 * time.Second})
	p.minHealthyDelay = protoRange(t.GetMinHealthyDelay(), durationRange{0, 30 * time.Second})
	if len(p.urls) == 0 && p.url != "" {
		p.urls = []string{p.url}
	}
	if len(p.urls) > 0 {
		p.url = p.urls[0]
	}
	if strings.EqualFold(c.GetMethod(), http.MethodGet) {
		p.method = http.MethodGet
	}
	if !a.GetFixedHeaders() {
		// A name the request may not take from outside is never rotated,
		// even when a config built without the JSON loader lists it.
		for _, name := range stringsOr(a.GetAcceptedHeaders(), defaultRotatedHeaders) {
			if canon := http.CanonicalHeaderKey(name); CheckHeaderName(name) == nil && !slices.Contains(p.rotate, canon) {
				p.rotate = append(p.rotate, canon)
			}
		}
	}
	p.jitter = defaultRefreshJitter
	if ca := t.GetWhenCostAbove(); ca != nil {
		p.costAbove = true
		p.costThreshold = float64(ca.GetCost())
		p.costAboveAfter, p.costAboveAfterMax = configuredRange(ca.GetAfter(), ca.AfterMax, durationRange{15 * time.Minute, 25 * time.Minute})
		if ca.After != nil {
			p.costAboveAfter = time.Duration(max(ca.GetAfter(), 0))
			if ca.AfterMax == nil {
				p.costAboveAfterMax = p.costAboveAfter
			}
		}
	}
	if ex := t.GetExpiry(); ex.GetRenewAtFraction() >= 0 {
		p.renewAt = min(fnum(ex.GetRenewAtFraction(), defaultRenewAt), maxRenewAt)
		p.urgentAt = min(max(fnum(ex.GetUrgentAtFraction(), defaultUrgentAt), p.renewAt), maxRenewAt)
		p.renewJitter = defaultRenewJitter
	}
	if !featureValue(f.Periodic, false) || (t.GetInterval() < 0 && t.IntervalMax == nil) {
		p.periodic = false
		p.interval, p.intervalMax = 0, 0
	}
	if !featureValue(f.MinHealthy, false) {
		p.minHealthy = 0
	}
	if !featureValue(f.Exhausted, true) {
		p.whenExhausted = false
	}
	if !featureValue(f.CostAbove, false) {
		p.costAbove = false
	}
	if !featureValue(f.MaxAge, false) {
		p.maxAge = 0
	}
	if !featureValue(f.NetworkChange, false) {
		p.onNetwork = RefreshOnNetwork_REFRESH_NEVER
	}
	if !featureValue(f.LeaseRenewal, true) {
		p.renewAt = 0
	}
	return p
}

func dur(v int64, def time.Duration) time.Duration {
	if v < 0 {
		return 0
	}
	if v == 0 {
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
		initial:    dur(b.InitialDelay, def.initial),
		multiplier: fnum(b.Multiplier, def.multiplier),
		max:        dur(b.MaxDelay, def.max),
		jitter:     jitter(b.JitterFraction, def.jitter),
	}
}

func newSettings(c *Config) *settings {
	s := &settings{baseOrder: map[string][]string{}, features: newFeatureSettings(c.GetFeatures())}
	if tc := c.GetTrace(); tc != nil {
		s.features.Trace = true
		size := int(tc.GetMaxBytes())
		if size <= 0 {
			size = defaultTraceBytes
		}
		s.trace = &traceSettings{maxBytes: min(max(size, minTraceBytes), maxTraceBytes), connections: featureValue(tc.ConnectionsEnabled, true)}
	}
	if cc := c.GetCensus(); cc != nil {
		s.features.Census = true
		s.census = &censusSettings{sampleFraction: cc.SampleFraction, startAfter: dur(cc.StartAfter, defaultCensusStartAfter), onDemand: make(map[string]bool, len(cc.OnDemand))}
		for _, name := range cc.OnDemand {
			s.census.onDemand[name] = true
		}
	}
	if s.features.SessionLabels {
		s.reportLabels = filterLabels(c.GetSessionLabels(), 8)
	}
	if lr := c.GetLabelRequest(); s.features.LabelRequest && lr != nil {
		s.labelRequest = &labelRequestSettings{outbound: lr.Outbound, destinations: configuredAddresses(lr.GetDestinations(), lr.GetDestination()), labels: maps.Clone(lr.Labels), timeout: dur(lr.Timeout, 4*time.Second), requestsPerHour: num(lr.RequestsPerHour, 6)}
	}
	if u := c.GetUdpProbe(); s.features.UdpProbes && u != nil {
		s.udpProbe = &udpSettings{requests: num(u.Requests, 5), interval: dur(u.Interval, 200*time.Millisecond), timeout: dur(u.Timeout, 2*time.Second), perHour: num(u.PerHour, 6)}
		for _, address := range u.Destinations {
			destination, _ := net.ParseDestination("udp:" + address)
			s.udpProbe.destinations = append(s.udpProbe.destinations, destination)
		}
	}
	s.wake = wakeSettings{freshness: dur(c.GetWake().GetFreshness(), defaultWakeFreshness), minInterval: dur(c.GetWake().GetMinInterval(), defaultWakeMinInterval)}
	for _, p := range c.GetPaths() {
		s.paths.Store(p.Tag, pathSettings{probe: p.Probe, isProbeOnly: p.IsProbeOnly, labels: filterLabels(p.Labels, 6)})
		s.baseOrder[p.Balancer] = append(s.baseOrder[p.Balancer], p.Tag)
	}
	p := c.GetPassive()
	s.passive = passiveSettings{
		udpReplyTimeout:    dur(p.GetUdpReplyTimeout(), 5*time.Second),
		loneFailureAfter:   dur(p.GetLoneFailureAfter(), 2*time.Minute),
		window:             defaultWindow,
		dialTimeout:        dur(p.GetDialTimeout(), defaultDialTimeout),
		firstByteTimeout:   dur(p.GetFirstByteTimeout(), defaultFirstByteTimeout),
		stallTimeout:       dur(p.GetStallTimeout(), defaultStallTimeout),
		failures:           defaultFailures,
		minDistinctTargets: defaultMinDistinctTargets,
		successBytes:       defaultSuccessBytes,
		sweepInterval:      defaultSweepInterval,
		dialOnly:           false,
		partialStallAge:    dur(p.GetPartialStallMaxAge(), defaultPartialStallAge),
		stallMinWrite:      defaultStallMinWrite,
		partialMinBytes:    defaultPartialMinBytes,
		keepPending:        false,
		keepSilent:         false,
		skipQuality:        !s.features.Quality,
	}
	if !s.features.PartialStalls {
		s.passive.partialStallAge = 0
	}
	if !s.features.UdpReplies {
		s.passive.udpReplyTimeout = 0
	}
	if !s.features.LoneFailureProbe {
		s.passive.loneFailureAfter = 0
	}
	for _, pp := range c.GetProbes() {
		profile := probeSettings{
			name:            pp.GetName(),
			destination:     pp.GetDestination(),
			method:          strings.ToUpper(stringOr(pp.GetMethod(), "GET")),
			expectStatus:    pp.GetExpectedStatuses(),
			minBytes:        pp.GetMinBytes(),
			timeout:         min(dur(pp.GetTimeout(), defaultProbeTimeout), maxProbeTimeout),
			progressTimeout: min(dur(pp.GetProgressTimeout(), defaultProbeProgress), maxProbeTimeout),
		}
		if len(pp.GetSteps()) == 0 {
			profile.steps = []probeStepSettings{{
				name: pp.GetName(), destinations: []string{profile.destination},
				method: profile.method, expectStatus: profile.expectStatus, minBytes: profile.minBytes,
				timeout: profile.timeout, progressTimeout: profile.progressTimeout,
			}}
		} else {
			for _, step := range pp.GetSteps() {
				profile.steps = append(profile.steps, probeStepSettings{
					name: step.GetName(), destinations: step.GetDestinations(), isOptional: step.GetIsOptional(),
					method: strings.ToUpper(stringOr(step.GetMethod(), "GET")), expectStatus: step.GetExpectedStatuses(),
					minBytes: step.GetMinBytes(), timeout: min(dur(step.GetTimeout(), defaultProbeTimeout), maxProbeTimeout),
					progressTimeout: min(dur(step.GetProgressTimeout(), defaultProbeProgress), maxProbeTimeout),
				})
			}
			first := profile.steps[0]
			profile.destination = ""
			if len(first.destinations) > 0 {
				profile.destination = first.destinations[0]
			}
			profile.method = first.method
			profile.expectStatus = first.expectStatus
			profile.minBytes = first.minBytes
			profile.timeout = first.timeout
			profile.progressTimeout = first.progressTimeout
		}
		s.probes = append(s.probes, profile)
	}
	if cc := c.GetConnectivity(); s.features.Connectivity && cc != nil && cc.GetOutbound() != "" && (len(cc.GetDestinations()) > 0 || cc.GetDestination() != "") {
		s.connectivity = &connectivitySettings{
			outbound:     cc.GetOutbound(),
			destination:  cc.GetDestination(),
			destinations: configuredAddresses(cc.GetDestinations(), cc.GetDestination()),
			timeout:      min(dur(cc.GetTimeout(), defaultConnectivityTimeout), maxProbeTimeout),
			recheck:      defaultConnRecheck,
			maxPause:     defaultMaxPause,
			witnesses:    cc.GetWitnesses(),
		}
	}
	if s.features.Witnesses {
		s.witnesses = c.GetConnectivity().GetWitnesses()
	}
	sc := c.GetSchedule()
	s.schedule = scheduleSettings{
		idleAfter:                    defaultIdleAfter,
		quarantine:                   defaultQuarantine,
		concurrent:                   min(num(sc.GetMaxConcurrentProbes(), defaultConcurrent), maxConcurrent),
		spacing:                      defaultSpacing,
		spacingJitter:                defaultSpacingJitter,
		probesPerServerNamePerMinute: num(sc.GetProbesPerServerNamePerMinute(), defaultProbesPerServerNamePerMinute),
		perMinute:                    num(sc.GetProbesPerMinute(), defaultPerMinute),
		suspectCooldown:              defaultSuspectCooldown,
	}
	po := sc.GetProbeOnly()
	s.schedule.probeOnly = probeOnlySettings{
		keepVerified:    min(num(po.GetKeepVerified(), defaultProbeOnlyKeepVerified), maxProbeOnlyKeepVerified),
		recheckInterval: dur(max(po.GetRecheckInterval(), 0), defaultProbeOnlyRecheckInterval),
	}
	hp := sc.GetHostProbes()
	s.schedule.hostProbes = hostProbeSettings{
		// The observer keeps at least half of the budget for finding failures.
		perMinute:    min(num(hp.GetProbesPerMinute(), defaultHostPerMinute), max(s.schedule.perMinute/2, 1)),
		reuse:        defaultHostReuse,
		maxOutbounds: min(num(hp.GetMaxOutbounds(), defaultHostMaxOutbounds), maxHostOutbounds),
		timeout:      min(dur(hp.GetTimeout(), defaultHostTimeout), maxHostTimeout),
	}
	s.maxEvents = min(num(c.GetEvents().GetMaxEvents(), defaultMaxEvents), maxEvents)
	s.maxBytes = int(min(size(c.GetEvents().GetMaxBytes(), defaultMaxEventBytes), maxEventBytes))
	if s.features.Report {
		s.reportInterval = dur(c.GetReport().GetInterval(), defaultReportInterval)
	}
	if s.features.Report {
		s.reportQuietShare = c.GetReport().GetQuietShare()
	}
	if !s.features.Events {
		s.maxEvents, s.maxBytes = 0, 0
	}
	if after := dur(c.GetSleep().GetAfterLastUse(), defaultSleepAfter); s.features.TunnelSleep && after > 0 {
		// The JSON loader refuses a shorter one; a config built without it
		// must not cut connections that are still closing either.
		s.sleepAfter = max(after, minSleepAfter)
	}
	if s.features.CloseUnchosenIdle {
		n := dur(c.GetSleep().GetCloseIdleAfter(), 15*time.Second)
		s.closeIdleAfter = min(max(n, minCloseIdleAfter), maxCloseIdleAfter)
	}
	s.remember = s.features.NetworkMemory
	s.maxNetworks = min(num(c.GetNetwork().GetMaxNetworks(), defaultMaxNetworks), maxNetworks)
	for _, pc := range c.GetProviders() {
		if !featureValue(pc.IsEnabled, false) {
			if s.disabledProviders == nil {
				s.disabledProviders = make(map[string]bool)
			}
			s.disabledProviders[pc.GetTag()] = true
			continue
		}
		ps := newProviderSettings(pc)
		s.providers = append(s.providers, ps)
	}
	return s
}

func (s *settings) observes(tag string) bool {
	_, ok := s.paths.Load(tag)
	return ok
}

func (s *settings) witness(tag string) bool {
	return len(s.witnesses) > 0 && matchesAny(tag, s.witnesses)
}

func (s *settings) isProbeOnly(tag string) bool {
	value, _ := s.paths.Load(tag)
	path, _ := value.(pathSettings)
	return path.isProbeOnly
}

func (s *settings) probeFor(tag string) *probeSettings {
	value, _ := s.paths.Load(tag)
	path, _ := value.(pathSettings)
	if path.probe != "" {
		for i := range s.probes {
			if s.probes[i].name == path.probe {
				if !path.isProbeOnly {
					return &s.probes[i]
				}
				profile := s.probes[i]
				profile.isProbeOnly = true
				profile.steps = nil
				for _, step := range s.probes[i].steps {
					if !step.isOptional {
						profile.steps = append(profile.steps, step)
					}
				}
				return &profile
			}
		}
	}
	// No implicit third-party destination: JSON selections require a profile.
	// Direct protobuf callers without one receive an inert profile.
	return &probeSettings{isProbeOnly: path.isProbeOnly, method: "GET",
		timeout: defaultProbeTimeout, progressTimeout: defaultProbeProgress}
}

type selectorSettings struct {
	fallbackBalancer string
	closePending     bool
	closeSilent      bool
	closeSilentAfter time.Duration
	raw              *SelectorConfig
	paths            map[string]*costEntrySettings
	baseOrder        []string
	switchAfterProbe bool
	confirm          ConfirmMode
	skipDownAudit    bool
	minHoldTime      time.Duration
	returnToCheaper  returnToCheaperSettings
	startPinned      string
	startPinTimeout  time.Duration
	startOutbound    string
	onNetworkChange  NetworkChangeMode
}

type costEntrySettings struct {
	labels         map[string]string
	groupName      string
	groupLimit     int
	cost           float64
	lazy           bool
	returnInterval int64
	returnEnabled  *bool
	probe          string
	isProbeOnly    bool
}

type returnToCheaperSettings struct {
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
	u := c.GetReturnToCheaper()
	f := c.GetFeatures()
	if f == nil {
		f = &SelectorFeatureConfig{}
	}
	groups := map[string]*PathGroup{}
	for _, g := range c.GetGroups() {
		groups[g.Name] = g
	}
	paths := map[string]*costEntrySettings{}
	order := make([]string, 0, len(c.GetPaths()))
	for _, p := range c.GetPaths() {
		entry := &costEntrySettings{labels: filterLabels(p.Labels, 6), cost: float64(p.Cost), lazy: p.IsLazy, returnInterval: p.ReturnInterval, returnEnabled: p.ReturnIsEnabled, probe: p.Probe, isProbeOnly: p.IsProbeOnly}
		if g := groups[p.Group]; g != nil && p.Group != "" {
			entry.labels = mergeLabels(g.Labels, p.Labels)
			entry.groupName = g.Name
			entry.groupLimit = min(max(num(g.FailedInRowToLeave, 3), 1), 16)
			entry.cost = float64(g.Cost)
			entry.lazy = g.IsLazy
			entry.returnInterval = g.ReturnInterval
			entry.returnEnabled = g.ReturnIsEnabled
			entry.probe = g.Probe
		}
		paths[p.Tag] = entry
		order = append(order, p.Tag)
	}
	result := &selectorSettings{
		closePending:     featureValue(f.ClosePending, true),
		closeSilent:      featureValue(f.CloseSilent, true),
		closeSilentAfter: dur(c.GetCloseSilentAfter(), defaultStallTimeout),
		raw:              c,
		fallbackBalancer: c.GetFallbackBalancer(),
		paths:            paths,
		baseOrder:        order,
		switchAfterProbe: featureValue(f.ProbeBeforeSwitch, true),
		confirm:          c.GetConfirmWithProbe(),
		skipDownAudit:    !featureValue(f.DownAudit, false),
		minHoldTime:      presentDelay(c.MinHoldTime, defaultMinHoldTime),
		returnToCheaper: returnToCheaperSettings{
			interval:            dur(u.GetInterval(), defaultReturnToCheaperInterval),
			randomPhase:         true,
			successes:           defaultReturnToCheaperSuccesses,
			spacing:             defaultReturnToCheaperSpacing,
			penaltyWindow:       defaultPenaltyWindow,
			penaltyMultiplier:   defaultPenaltyMultiplier,
			maxPenalty:          defaultMaxPenalty,
			onNetworkChange:     featureValue(f.ReturnOnNetworkChange, false),
			unknownNetworksOnly: !u.GetTryOnAllNetworks(),
		},
		startPinned:     c.GetStartPinnedOutbound(),
		startPinTimeout: time.Duration(max(c.GetStartPinTimeout(), 0)),
		startOutbound:   c.GetStartOutbound(),
		onNetworkChange: c.GetNetworkChangeAction(),
	}
	if !featureValue(f.ReturnToCheaper, false) {
		result.returnToCheaper.interval = 0
	}
	if !featureValue(f.NetworkChanges, false) {
		result.onNetworkChange = NetworkChangeMode_IGNORE
	}
	if !featureValue(f.StartupPin, false) {
		result.startPinned = ""
	}
	return result
}

func (s *selectorSettings) cost(tag string) float64 {
	if p := s.paths[tag]; p != nil {
		return p.cost
	}
	return 0
}

func (s *selectorSettings) returnInterval(tag string) time.Duration {
	if s.returnToCheaper.interval == 0 {
		return 0
	}
	if p := s.paths[tag]; p != nil {
		if !featureValue(p.returnEnabled, true) {
			return 0
		}
		return dur(p.returnInterval, s.returnToCheaper.interval)
	}
	return s.returnToCheaper.interval
}

// The next selected outbound is unknown when a return fails, so use the
// shortest possible base to keep its multiplier finite without losing delay.
func (s *selectorSettings) penaltyLimit() float64 {
	base := maxDuration(s.returnToCheaper.interval, s.minHoldTime)
	for _, entry := range s.paths {
		interval := optional(entry.returnInterval, s.returnToCheaper.interval)
		base = min(base, maxDuration(interval, s.minHoldTime))
	}
	if base == 0 {
		base = time.Nanosecond
	}
	return max(1, float64(s.returnToCheaper.maxPenalty)/float64(base))
}

func (s *selectorSettings) isProbeOnly(tag string) bool {
	return s.paths[tag] != nil && s.paths[tag].isProbeOnly
}

func (s *selectorSettings) isLazy(tag string) bool {
	return s.paths[tag] != nil && s.paths[tag].lazy
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

func presentDelay(value *int64, fallback time.Duration) time.Duration {
	if value == nil {
		return fallback
	}
	return time.Duration(max(*value, 0))
}
