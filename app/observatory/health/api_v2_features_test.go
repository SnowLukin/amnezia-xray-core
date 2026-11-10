package health

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
)

func v2Bool(v bool) *bool { return &v }
func v2NoEvidence() *FeatureConfig {
	return &FeatureConfig{DialFailures: v2Bool(false), DialTimeout: v2Bool(false), ServerUnverified: v2Bool(false), FirstReply: v2Bool(false), EarlyClose: v2Bool(false), StalledRequests: v2Bool(false)}
}
func v2Observer(f *FeatureConfig, sf *SelectorFeatureConfig) (*Observer, *selector, *time.Time) {
	cfg := &Config{Features: f, Paths: []*PathDefinition{{Tag: "a", Balancer: "auto", Cost: 1}, {Tag: "b", Balancer: "auto", Cost: 2}}}
	o := newObserver(context.Background(), cfg)
	now := time.Unix(1800000000, 0)
	o.clock = func() time.Time { return now }
	o.base = now.Add(-time.Second)
	sc := &SelectorConfig{Features: sf, Paths: cfg.Paths}
	s := o.e.addSelector("auto", newSelectorSettings(sc))
	o.e.updateCandidates(s, []string{"a", "b"}, now)
	o.e.noteDemand(now)
	return o, s, &now
}
func v2NoClosures() *SelectorFeatureConfig {
	return &SelectorFeatureConfig{ClosePending: v2Bool(false), CloseSilent: v2Bool(false)}
}

func TestV2PresentAwareDefaults(t *testing.T) {
	a := newSettings(&Config{})
	if !a.features.DialFailures || !a.features.DialTimeout || !a.features.FirstReply || !a.features.EarlyClose || !a.features.StalledRequests || !a.features.ServerUnverified {
		t.Fatal("essential TCP defaults missing")
	}
	if a.features.Quality || a.features.Report || a.features.Events || a.remember || a.sleepAfter != 0 || a.udpProbe != nil || a.passive.udpReplyTimeout != 0 || a.passive.loneFailureAfter != 0 || a.passive.partialStallAge != 0 {
		t.Fatal("optional feature started from absence")
	}
	b := newSettings(&Config{Features: v2NoEvidence()})
	if b.features.passiveTCP() {
		t.Fatal("explicit false was lost")
	}
	s := newSelectorSettings(&SelectorConfig{})
	if !s.switchAfterProbe || !s.closePending || !s.closeSilent || !s.skipDownAudit || s.returnToCheaper.interval != 0 || s.onNetworkChange != NetworkChangeMode_IGNORE {
		t.Fatalf("selector defaults %+v", s)
	}
	disabled := newSettings(&Config{Providers: []*ProviderConfig{{Tag: "prepared", InitialAnswer: []byte("broken")}}})
	if len(disabled.providers) != 0 {
		t.Fatal("disabled provider allocated runtime")
	}
}

func TestV2AllEvidenceOffHasNoTrackersOrSweep(t *testing.T) {
	o, s, now := v2Observer(v2NoEvidence(), v2NoClosures())
	defer o.Close()
	if s.current() != "a" || len(o.e.queue) != 0 {
		t.Fatal("minimal initial choice changed")
	}
	for kind := failDial; kind <= failClosed; kind++ {
		for i := range 4 {
			o.e.onFailure("a", kind, "target", uint32(i), *now)
		}
	}
	if o.e.path("a").status != statusUnknown || len(o.e.queue) != 0 {
		t.Fatal("disabled evidence changed choice")
	}
	if o.StartDial(context.Background(), "a", net.Destination{}, nil) != nil {
		t.Fatal("disabled features made user tracker")
	}
	if o.StartUDPFlow(context.Background(), "a", 53) != nil {
		t.Fatal("disabled UDP made flow tracker")
	}
	if o.trackers != nil || o.closures != nil || o.watching != nil || o.sweeping || o.e.quality != nil || o.e.events != nil || o.e.report.stepCounts != nil {
		t.Fatal("disabled feature infrastructure allocated")
	}
	if n := testing.AllocsPerRun(100, func() {
		if o.StartDial(context.Background(), "a", net.Destination{}, nil) != nil {
			panic("unexpected tracker")
		}
	}); n != 0 {
		t.Fatalf("disabled StartDial allocations: %g", n)
	}
	*now = now.Add(time.Hour)
	jobs, next := o.e.tick(*now)
	if len(jobs) != 0 || !next.IsZero() {
		t.Fatalf("disabled optional work started: jobs=%v next=%v", jobs, next)
	}
}

func TestV2ClosureRegistryWorksWithoutDetectors(t *testing.T) {
	o, _, now := v2Observer(v2NoEvidence(), nil)
	defer o.Close()
	var pending, replied atomic.Int32
	p := o.StartDial(context.Background(), "a", net.Destination{}, func() { pending.Add(1) }).(*closureTracker)
	r := o.StartDial(context.Background(), "a", net.Destination{}, func() { replied.Add(1) }).(*closureTracker)
	p.Dialed(nil, nil, nil, nil)
	read, _ := r.Dialed(nil, nil, nil, nil)
	read.Add(7)
	if unsafe.Sizeof(*p) >= unsafe.Sizeof(connTracker{}) {
		t.Fatal("closure tracker is not smaller")
	}
	if o.e.quality != nil || o.sweeping || len(o.watching) != 0 || len(o.trackers) != 0 {
		t.Fatal("closure enabled detector/quality work")
	}
	mark := downMark{tag: "a", at: *now, pending: true, watch: true}
	o.closePending(mark)
	if pending.Load() != 1 || replied.Load() != 0 {
		t.Fatal("pending closure confused answered connection")
	}
	*now = now.Add(8 * time.Second)
	o.closePending(downMark{tag: "a", at: mark.at, silent: true})
	if replied.Load() != 1 {
		t.Fatal("silent closure needs passive detector")
	}
	p.Finished(nil)
	r.Finished(nil)
	if len(o.closures) != 0 {
		t.Fatal("closure registry retained finished connection")
	}
}

func TestV2ClosureUploadIsNotSilence(t *testing.T) {
	o, _, now := v2Observer(v2NoEvidence(), nil)
	defer o.Close()
	tr := o.StartDial(context.Background(), "a", net.Destination{}, nil).(*closureTracker)
	read, write := tr.Dialed(nil, nil, nil, nil)
	read.Add(7)
	*now = now.Add(time.Minute)
	if write == nil {
		t.Fatal("writes of a registered connection are not timed")
	}
	write.Add(64 << 10)
	if since, ok := tr.idleSince(o.nowNano()); !ok || since != o.nowNano() {
		t.Fatalf("idle since %v (%v), want the last write", time.Duration(o.nowNano()-since), ok)
	}
	tr.Finished(nil)
}

// A connection whose proxy never returns is let go like a full tracker.
func TestV2AbandonedClosureLeavesTheRegistry(t *testing.T) {
	o, _, now := v2Observer(v2NoEvidence(), nil)
	defer o.Close()
	stuck := o.StartDial(context.Background(), "a", net.Destination{}, nil).(*closureTracker)
	stuck.Dialed(nil, nil, nil, nil)
	*now = now.Add(trackerAbandoned + time.Second)
	var live []*closureTracker
	for range 256 {
		tr := o.StartDial(context.Background(), "a", net.Destination{}, nil).(*closureTracker)
		tr.Dialed(nil, nil, nil, nil)
		live = append(live, tr)
	}
	o.trackMu.Lock()
	_, kept := o.closures[stuck]
	n := len(o.closures)
	o.trackMu.Unlock()
	if kept || n != len(live) {
		t.Fatalf("abandoned kept %v, %d registered, want %d", kept, n, len(live))
	}
	for _, tr := range live {
		tr.Finished(nil)
	}
}

func TestV2UDPOnlyUsesMinimalTCPProof(t *testing.T) {
	f := v2NoEvidence()
	f.UdpReplies = v2Bool(true)
	o, _, now := v2Observer(f, v2NoClosures())
	defer o.Close()
	tr := o.StartDial(context.Background(), "a", net.Destination{}, nil).(*closureTracker)
	if !tr.tcpProof {
		t.Fatal("UDP lost independent TCP proof")
	}
	read, _ := tr.Dialed(nil, nil, nil, nil)
	read.Add(o.s.passive.successBytes)
	if !o.e.path("a").lastTCPSuccess.Equal(*now) {
		t.Fatal("UDP did not receive required TCP evidence")
	}
	if o.sweeping || o.e.quality != nil || o.e.events != nil || o.e.report.stepCounts != nil || o.closures != nil {
		t.Fatal("TCP proof started unrelated features")
	}
	tr.Finished(nil)
}

func TestV2CollectorsAreIndependent(t *testing.T) {
	t.Run("report without events", func(t *testing.T) {
		f := v2NoEvidence()
		f.Report = v2Bool(true)
		o, s, now := v2Observer(f, v2NoClosures())
		defer o.Close()
		o.e.switchTo(s, "b", "traffic", now.Add(time.Second))
		if o.e.events != nil || o.e.report.counts.Switches != 2 || o.e.snapshotReport(*now) == nil {
			t.Fatal("report requires event log")
		}
	})
	t.Run("quality without report", func(t *testing.T) {
		f := v2NoEvidence()
		f.Quality = v2Bool(true)
		o, _, now := v2Observer(f, v2NoClosures())
		defer o.Close()
		tr := o.StartDial(context.Background(), "a", net.Destination{}, nil).(*connTracker)
		read, write := tr.Dialed(nil, nil, nil, nil)
		write.Add(50)
		*now = now.Add(20 * time.Millisecond)
		read.Add(100)
		if !o.e.quality.current.nonempty() || o.e.quality.open.nonempty() || o.e.quality.reporting || len(o.e.quality.closed) != 0 || o.e.snapshotReport(*now) != nil {
			t.Fatal("quality kept report histogram")
		}
		o.NetworkChanged("other")
		if o.e.quality.open.nonempty() || len(o.e.quality.closed) != 0 {
			t.Fatal("network change retained report segment")
		}
		tr.Finished(nil)
	})
}

func TestV2WitnessWorksWithoutConnectivity(t *testing.T) {
	f := v2NoEvidence()
	f.Witnesses = v2Bool(true)
	o := newObserver(context.Background(), &Config{Features: f, Connectivity: &ConnectivityConfig{Witnesses: []string{"direct"}}})
	defer o.Close()
	o.NetworkLost()
	tr := o.StartDial(context.Background(), "direct", net.Destination{}, nil).(*witnessTracker)
	read, _ := tr.Dialed(nil, nil, nil, nil)
	read.Add(o.s.passive.successBytes)
	if o.e.networkDown || o.s.connectivity != nil || len(o.e.queue) != 0 || tr.quality != nil {
		t.Fatal("witness recovery requires active check")
	}
}

type v2PlainProxy struct{}

func (v2PlainProxy) Process(context.Context, *transport.Link, internet.Dialer) error { return nil }

type v2SleepProxy struct {
	v2PlainProxy
	sleeps int
}

func (p *v2SleepProxy) Sleep() bool { p.sleeps++; return true }

type v2Handler struct {
	outbound.Handler
	p proxy.Outbound
}

func (h *v2Handler) GetOutbound() proxy.Outbound { return h.p }

type v2Manager struct {
	outbound.Manager
	h map[string]outbound.Handler
}

func (m *v2Manager) GetHandler(tag string) outbound.Handler { return m.h[tag] }

func TestV2CloseUnchosenIdleWithoutTunnelSleep(t *testing.T) {
	f := v2NoEvidence()
	f.CloseUnchosenIdle = v2Bool(true)
	o, s, now := v2Observer(f, v2NoClosures())
	defer o.Close()
	sleepy := &v2SleepProxy{}
	o.ohm = &v2Manager{h: map[string]outbound.Handler{"a": &v2Handler{p: sleepy}, "b": &v2Handler{p: v2PlainProxy{}}}}
	if o.StartDial(context.Background(), "b", net.Destination{}, nil) != nil {
		t.Fatal("unsupported outbound created idle tracker")
	}
	var closed atomic.Int32
	tr := o.StartDial(context.Background(), "a", net.Destination{}, func() { closed.Add(1) }).(*closureTracker)
	tr.Dialed(nil, nil, nil, nil)
	o.e.processStarted("a", *now)
	o.e.switchTo(s, "b", "host", *now)
	o.e.planSleeps(*now)
	batch := o.e.takeSleeps()
	if len(batch) != 2 {
		t.Fatalf("capability discovery jobs %v", batch)
	}
	o.sleepOutbounds(batch)
	o.e.planSleeps(*now)
	batch = o.e.takeSleeps()
	if len(batch) != 1 || !batch[0].idleCheck {
		t.Fatalf("independent close jobs %v", batch)
	}
	o.sleepOutbounds(batch)
	*now = now.Add(o.s.closeIdleAfter)
	o.e.planSleeps(*now)
	o.sleepOutbounds(o.e.takeSleeps())
	if closed.Load() != 1 || sleepy.sleeps != 0 || o.s.sleepAfter != 0 {
		t.Fatalf("close=%d sleep=%d", closed.Load(), sleepy.sleeps)
	}
	tr.Finished(nil)
}

func TestV2ClosureReadFinishRace(t *testing.T) {
	o, _, _ := v2Observer(v2NoEvidence(), nil)
	defer o.Close()
	tr := o.StartDial(context.Background(), "a", net.Destination{}, nil).(*closureTracker)
	read, _ := tr.Dialed(nil, nil, nil, nil)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 100 {
			read.Add(1)
		}
	}()
	go func() { defer wg.Done(); tr.Finished(nil) }()
	wg.Wait()
	if len(o.closures) != 0 {
		t.Fatal("finished closure retained")
	}
}

func TestV2DirectFeatureDependencies(t *testing.T) {
	f := v2NoEvidence()
	f.LoneFailureProbe = v2Bool(true)
	if (&Config{Features: f}).ValidateFeatures() == nil {
		t.Fatal("lone accepted without path-level detector")
	}
	f.LoneFailureProbe = v2Bool(false)
	f.SessionLabels = v2Bool(true)
	if (&Config{Features: f}).ValidateFeatures() == nil {
		t.Fatal("session labels accepted without report")
	}
	if validateSelectorFeatures(&SelectorConfig{Features: &SelectorFeatureConfig{DownAudit: v2Bool(true)}, ConfirmWithProbe: ConfirmMode_ALWAYS}, newFeatureSettings(nil)) == nil {
		t.Fatal("audit accepted without auto")
	}
	provider := &ProviderConfig{IsEnabled: v2Bool(true), Tag: "p", Urls: []string{"https://x.test/set"}, Apply: &ProviderApply{Balancers: []string{"auto"}}}
	if err := (&Config{Providers: []*ProviderConfig{provider}}).ValidateFeatures(); err == nil || !strings.Contains(err.Error(), "tagPrefix") {
		t.Fatalf("a provider without tagPrefix would own every outbound: %v", err)
	}
}

func v2Int64(v int64) *int64 { return &v }

func TestV2GenuineZeroDelaysPreservePresence(t *testing.T) {
	absent := newSelectorSettings(&SelectorConfig{})
	immediate := newSelectorSettings(&SelectorConfig{MinHoldTime: v2Int64(0)})
	if absent.minHoldTime != defaultMinHoldTime || immediate.minHoldTime != 0 {
		t.Fatal("minHoldTime absence/zero collapsed")
	}
	provider := func(delay *int64, after *int64) *providerSettings {
		return newProviderSettings(&ProviderConfig{Features: &ProviderFeatureConfig{NetworkChange: v2Bool(true), CostAbove: v2Bool(true)}, Triggers: &ProviderTriggers{NetworkSettleDelay: delay, WhenCostAbove: &CostTrigger{Cost: 10, After: after}}})
	}
	a, b := provider(nil, nil), provider(v2Int64(0), v2Int64(0))
	if a.networkSettle != defaultNetworkSettle || a.costAboveAfter != defaultCostAboveAfter || b.networkSettle != 0 || b.costAboveAfter != 0 {
		t.Fatal("provider delays absence/zero collapsed")
	}
}

func TestV2RetiredControlsDoNotOverridePositiveFlags(t *testing.T) {
	s := newSettings(&Config{Features: &FeatureConfig{Quality: v2Bool(true), Report: v2Bool(true), Events: v2Bool(true)}, Passive: &PassiveConfig{IsDialOnly: true, ShouldSkipQuality: true, Window: 1, FailuresToSuspect: 1, SuccessBytes: 1, SweepInterval: 1, ShouldKeepPendingOnDown: true, ShouldKeepSilentOnDown: true}, Schedule: &ScheduleConfig{IdleAfter: 1, MinProbeInterval: 1, SuspectCooldown: 1}})
	if s.passive.skipQuality || s.passive.dialOnly || s.passive.window != defaultWindow || s.passive.failures != defaultFailures || s.passive.successBytes != defaultSuccessBytes || s.passive.sweepInterval != defaultSweepInterval || s.schedule.spacing != defaultSpacing || s.schedule.idleAfter != defaultIdleAfter {
		t.Fatal("retired control still overrides runtime")
	}
	sc := newSelectorSettings(&SelectorConfig{Features: &SelectorFeatureConfig{ProbeBeforeSwitch: v2Bool(true), DownAudit: v2Bool(true)}, SkipProbeBeforeSwitch: true, ShouldSkipDownAudit: true})
	if !sc.switchAfterProbe || sc.skipDownAudit {
		t.Fatal("negative alias overrides positive selector flag")
	}
	p := newProviderSettings(&ProviderConfig{Features: &ProviderFeatureConfig{Exhausted: v2Bool(true)}, Triggers: &ProviderTriggers{SkipRefreshWhenExhausted: true}})
	if !p.whenExhausted {
		t.Fatal("negative alias overrides exhausted flag")
	}
}

func TestV2FirstReplyHasItsOwnDeadlineWithoutDialTimeout(t *testing.T) {
	f := v2NoEvidence()
	f.FirstReply = v2Bool(true)
	o, _, _ := v2Observer(f, v2NoClosures())
	defer o.Close()
	tr := o.StartDial(context.Background(), "a", net.Destination{}, nil).(*connTracker)
	if tr.watched.Load() || o.sweeping {
		t.Fatal("disabled dial timeout admitted a dialing deadline")
	}
	tr.Dialed(nil, nil, nil, nil)
	if !tr.watched.Load() || !o.sweeping {
		t.Fatal("firstReply lost its independent deadline after dial")
	}
	tr.Finished(nil)
}

func TestV2EachPassiveEvidenceHasAnIndependentGate(t *testing.T) {
	for kind := failDial; kind <= failClosed; kind++ {
		t.Run(kind.String(), func(t *testing.T) {
			f := v2NoEvidence()
			switch kind {
			case failDial:
				f.DialFailures = v2Bool(true)
			case failDialTimeout:
				f.DialTimeout = v2Bool(true)
			case failUnverified:
				f.ServerUnverified = v2Bool(true)
			case failNoResponse:
				f.FirstReply = v2Bool(true)
			case failStall:
				f.StalledRequests = v2Bool(true)
			case failPartialStall:
				f.PartialStalls = v2Bool(true)
			case failClosed:
				f.EarlyClose = v2Bool(true)
			}
			o, _, now := v2Observer(f, v2NoClosures())
			defer o.Close()
			for other := failDial; other <= failClosed; other++ {
				if other == kind {
					continue
				}
				o.e.onFailure("a", other, "target", 1, *now)
			}
			if len(o.e.path("a").failures) != 0 {
				t.Fatal("disabled neighboring evidence leaked")
			}
			o.e.onFailure("a", kind, "target", 2, *now)
			if len(o.e.path("a").failures) != 1 || o.e.path("a").failures[0].kind != kind {
				t.Fatal("own positive evidence flag ignored")
			}
		})
	}
}

func TestV2ContextGateKeepsOwnProbesWithoutUserHooks(t *testing.T) {
	f := v2NoEvidence()
	f.Events = v2Bool(true)
	f.ProbeMeasurements = v2Bool(true)
	o, _, _ := v2Observer(f, v2NoClosures())
	defer o.Close()
	if o.NeedsObservation(context.Background(), "a") {
		t.Fatal("events/probe measurements enabled user hooks")
	}
	ctx := withProbe(context.Background(), &atomic.Int64{})
	if !o.NeedsObservation(ctx, "a") {
		t.Fatal("core probe context lost tracker")
	}
	if _, ok := o.StartDial(ctx, "a", net.Destination{}, nil).(*probeTracker); !ok {
		t.Fatal("core probe failed to acquire tracker")
	}
}

func TestV2OptionalControllersUseMinimalActivityForLongFlow(t *testing.T) {
	f := v2NoEvidence()
	sf := v2NoClosures()
	sf.ReturnToCheaper = v2Bool(true)
	o, _, now := v2Observer(f, sf)
	defer o.Close()
	if !o.NeedsObservation(context.Background(), "a") {
		t.Fatal("return controller lost activity hooks")
	}
	tr := o.StartDial(context.Background(), "a", net.Destination{}, nil).(*closureTracker)
	if !tr.activity || tr.proof || tr.tcpProof {
		t.Fatal("return controller enabled unrelated proof")
	}
	read, write := tr.Dialed(nil, nil, nil, nil)
	*now = now.Add(3 * time.Minute)
	if !o.e.idle(*now) {
		t.Fatal("fixture was not idle")
	}
	write.Add(1)
	if o.e.idle(*now) {
		t.Fatal("ongoing upload still considered idle")
	}
	*now = now.Add(3 * time.Minute)
	read.Add(1)
	if o.e.idle(*now) {
		t.Fatal("ongoing download still considered idle")
	}
	if o.sweeping || o.e.quality != nil || o.e.events != nil || o.closures != nil {
		t.Fatal("activity created unrelated collector/deadline/registry")
	}
	tr.Finished(nil)
}

func TestV2NetworkMemoryUsesMinimalSuccessProof(t *testing.T) {
	f := v2NoEvidence()
	f.NetworkMemory = v2Bool(true)
	o, _, _ := v2Observer(f, v2NoClosures())
	defer o.Close()
	o.NetworkChanged("network")
	tr := o.StartDial(context.Background(), "a", net.Destination{}, nil).(*closureTracker)
	if !tr.proof {
		t.Fatal("network memory lacks minimal proof")
	}
	read, _ := tr.Dialed(nil, nil, nil, nil)
	read.Add(o.s.passive.successBytes)
	if o.e.networks["network"]["auto"] != "a" || o.e.path("a").lastSuccess.IsZero() {
		t.Fatal("network memory did not record success")
	}
	if o.sweeping || o.e.quality != nil || o.e.events != nil || o.e.report.stepCounts != nil {
		t.Fatal("proof started unrelated telemetry")
	}
	tr.Finished(nil)
}

func TestV2CollectorsNeverGrantControllerProof(t *testing.T) {
	type snapshot struct {
		state                   pathStatus
		networkDown             bool
		choice                  string
		answered                int64
		lastSuccess, tcpSuccess time.Time
	}
	for _, consumer := range []string{"none", "udp", "memory", "present"} {
		for _, collector := range []string{"quality", "report"} {
			t.Run(consumer+"/"+collector, func(t *testing.T) {
				capture := func(enabled bool) snapshot {
					f := v2NoEvidence()
					switch consumer {
					case "udp":
						f.UdpReplies = v2Bool(true)
					case "memory":
						f.NetworkMemory = v2Bool(true)
					case "present":
						f.UserPresent = v2Bool(true)
					}
					if collector == "quality" {
						f.Quality = v2Bool(enabled)
					} else {
						f.Report = v2Bool(enabled)
					}
					o, s, now := v2Observer(f, v2NoClosures())
					defer o.Close()
					o.NetworkLost()
					tr := o.StartDial(context.Background(), "a", net.Destination{}, nil)
					if tr != nil {
						read, _ := tr.Dialed(nil, nil, nil, nil)
						*now = now.Add(time.Millisecond)
						read.Add(o.s.passive.successBytes)
						tr.Finished(nil)
					}
					p := o.e.path("a")
					return snapshot{p.status, o.e.networkDown, s.current(), p.lastAnsweredStart, p.lastSuccess, p.lastTCPSuccess}
				}
				off, on := capture(false), capture(true)
				if off != on {
					t.Fatalf("collector grants proof: off=%+v on=%+v", off, on)
				}
				if consumer == "none" || consumer == "udp" {
					if on.state != statusUnknown || !on.networkDown {
						t.Fatal("no controller consumer was enabled")
					}
				} else if on.state != statusUp || on.networkDown {
					t.Fatal("explicit controller consumer lost proof")
				}
				if consumer == "udp" && on.tcpSuccess.IsZero() {
					t.Fatal("UDP detector lost independent TCP proof")
				}
			})
		}
	}
}

func v2NetworkScopeObserver(t *testing.T, shared bool) (*Observer, *selector, *time.Time) {
	t.Helper()
	f := v2NoEvidence()
	f.ProbeRequests = v2Bool(true)
	sf := v2NoClosures()
	sf.NetworkChanges = v2Bool(true)
	o, _, now := v2Observer(f, sf)
	paths := []*PathDefinition{{Tag: "c", Balancer: "other", Cost: 1}, {Tag: "d", Balancer: "other", Cost: 2}}
	if shared {
		paths = append(paths, &PathDefinition{Tag: "a", Balancer: "other", Cost: 3})
	}
	for _, p := range paths {
		if p.Tag != "a" {
			o.s.paths.Store(p.Tag, pathSettings{})
		}
	}
	other := o.e.addSelector("other", newSelectorSettings(&SelectorConfig{Paths: paths, Features: v2NoClosures()}))
	tags := []string{"c", "d"}
	if shared {
		tags = append(tags, "a")
	}
	o.e.updateCandidates(other, tags, *now)
	t.Cleanup(func() { o.Close() })
	return o, other, now
}

func TestV2NetworkChangesPreserveIgnoredPrivatePolicy(t *testing.T) {
	o, other, now := v2NetworkScopeObserver(t, false)
	p := o.e.path("d")
	p.setStatus(statusDown, "probe", *now)
	p.quarantineLevel = 2
	p.quarantineUntil = now.Add(2 * time.Minute)
	other.penalty["d"] = 2
	other.returnedTo = "d"
	other.failoverTried = map[string]bool{"d": true}
	beforeUntil := p.quarantineUntil
	o.NetworkChanged("new")
	if p.status != statusDown || p.quarantineLevel != 2 || !p.quarantineUntil.Equal(beforeUntil) || other.penalty["d"] != 2 || other.returnedTo != "d" || !other.failoverTried["d"] {
		t.Fatal("reacting sibling reset ignored private policy")
	}
	if p.controllerEpoch != 0 || o.e.path("a").controllerEpoch != o.e.generation {
		t.Fatal("path controller epochs were not scoped")
	}
}

func TestV2NetworkChangesPreserveIgnoredLiveProof(t *testing.T) {
	o, _, _ := v2NetworkScopeObserver(t, false)
	o.s.features.NetworkMemory = true
	o.s.remember = true
	o.e.networks = map[string]map[string]string{}
	tr := o.StartDial(context.Background(), "c", net.Destination{}, nil).(*closureTracker)
	read, _ := tr.Dialed(nil, nil, nil, nil)
	o.NetworkChanged("new")
	read.Add(o.s.passive.successBytes)
	if o.e.path("c").status != statusUp || o.e.networks["new"]["other"] != "c" {
		t.Fatal("enabled sibling invalidated ignored live proof")
	}
	tr.Finished(nil)
}

func TestV2NetworkChangesPreserveIgnoredRunningRequest(t *testing.T) {
	o, _, now := v2NetworkScopeObserver(t, false)
	id, _, err := o.e.requestProbeKind("other", []string{"c"}, false, *now)
	if err != nil {
		t.Fatal(err)
	}
	var job *probeJob
	for _, j := range o.e.queue {
		if j.tag == "c" {
			job = j
			break
		}
	}
	if job == nil {
		t.Fatal("request did not queue a probe")
	}
	o.e.queue = nil
	o.e.begin(job, *now)
	o.NetworkChanged("new")
	var request *probeRequest
	for _, r := range o.e.requests {
		if r.id == id {
			request = r
		}
	}
	if request == nil || !request.open() || job.invalidated || job.generation != o.e.generation || request.generation != job.generation {
		t.Fatal("ignored request/probe lost its generation claim")
	}
	o.e.onProbeResult(job, probeResult{ok: true}, now.Add(time.Second))
	if request.open() || request.items[0].result != probeOK {
		t.Fatal("retained ignored probe could not complete its request")
	}
}

func TestV2NetworkChangesScopeMixedSharedRequest(t *testing.T) {
	o, _, now := v2NetworkScopeObserver(t, true)
	_, _, err := o.e.requestProbeKind("other", []string{"a", "c"}, false, *now)
	if err != nil {
		t.Fatal(err)
	}
	request := o.e.requests[0]
	o.NetworkChanged("new")
	if !request.open() || request.items[0].result != probeExpired || request.items[1].result != probePending {
		t.Fatalf("mixed request items: %+v", request.items)
	}
	var private *probeJob
	for _, j := range o.e.queue {
		if j.tag == "c" {
			private = j
			break
		}
	}
	if private == nil || private.generation != o.e.generation {
		t.Fatal("shared reset removed private pending work")
	}
	o.e.begin(private, *now)
	o.e.onProbeResult(private, probeResult{ok: true}, now.Add(time.Second))
	if request.open() || request.items[1].result != probeOK {
		t.Fatal("private item did not finish after shared item expired")
	}
}

func TestV2NetworkChangesRefreshSharedPhysicalClaims(t *testing.T) {
	for _, reason := range []probeReason{reasonFailover, reasonReturnToCheaper} {
		t.Run(reason.String(), func(t *testing.T) {
			o, other, now := v2NetworkScopeObserver(t, true)
			other.cfg.returnToCheaper.interval = time.Minute
			if reason == reasonFailover {
				other.failoverProbing = map[string]bool{"a": true}
				other.failoverTried = map[string]bool{"c": true, "a": true}
			} else {
				other.returnToCheaperTarget = "a"
			}
			old := o.e.enqueue("a", reason, "other", *now)
			if old == nil {
				t.Fatal("shared claim probe missing")
			}
			o.e.queue = nil
			o.e.begin(old, *now)
			o.NetworkChanged("new")
			if !old.invalidated {
				t.Fatal("shared old callback can still mutate reset physical state")
			}
			var replacement *probeJob
			for _, j := range o.e.queue {
				if j.tag == "a" && j.selector == "other" && j.reason == reason {
					replacement = j
					break
				}
			}
			if replacement == nil || replacement.generation != o.e.generation {
				t.Fatal("shared IGNORE claim was stranded without replacement work")
			}
		})
	}
}

func TestV2NetworkChangesPreserveIgnoredGroupAndSilence(t *testing.T) {
	o, other, now := v2NetworkScopeObserver(t, false)
	other.cfg.paths["c"].groupName = "set"
	other.cfg.paths["c"].groupLimit = 3
	other.cfg.paths["d"].groupName = "set"
	other.cfg.paths["d"].groupLimit = 3
	o.e.syncGroups(other)
	for range 3 {
		o.e.groupFailure("c", *now)
	}
	group := other.groups["set"]
	if group == nil || !group.down {
		t.Fatal("fixture did not quarantine its group")
	}
	until, failed, level := group.until, group.failed, group.level
	p := o.e.path("d")
	p.setStatus(statusDown, "probe", *now)
	p.quarantineUntil = now.Add(time.Minute)
	mark := downMark{tag: "d", at: *now, watch: true, silentAfter: 8 * time.Second}
	o.e.watchSilence(mark)
	o.NetworkChanged("new")
	if !group.down || !group.until.Equal(until) || group.failed != failed || group.level != level {
		t.Fatal("enabled sibling reset ignored group")
	}
	if len(o.e.silences) != 1 || o.e.silences[0].generation != o.e.generation || o.e.silenceVerdict(o.e.silences[0]) != silenceEnd {
		t.Fatal("ignored silent-close claim was lost through global generation")
	}
}

func TestV2NetworkChangesNeverRevivePreviouslyStaleJob(t *testing.T) {
	o, _, now := v2NetworkScopeObserver(t, false)
	old := o.e.enqueue("c", reasonRecheck, "other", *now)
	o.NetworkChanged("one")
	// A delayed callback already belonging to an obsolete generation stays stale.
	old.generation = 0
	o.NetworkChanged("two")
	if old.generation == o.e.generation {
		t.Fatal("network change revived a previously stale job")
	}
	o.e.onProbeResult(old, probeResult{ok: true}, now.Add(time.Second))
	if o.e.path("c").status != statusUnknown {
		t.Fatal("old callback changed current ignored path")
	}
}

// Mirrors a directly routed AWG stream: capability discovery and first idle
// sample run before the response, then one small response leaves it silent.
func TestV2CloseUnchosenIdleCountsFromLastRead(t *testing.T) {
	f := v2NoEvidence()
	f.CloseUnchosenIdle = v2Bool(true)
	o, s, now := v2Observer(f, v2NoClosures())
	defer o.Close()
	sleepy := &v2SleepProxy{}
	o.ohm = &v2Manager{h: map[string]outbound.Handler{"a": &v2Handler{p: v2PlainProxy{}}, "b": &v2Handler{p: sleepy}}}
	if s.current() != "a" {
		t.Fatal("unexpected chosen path")
	}
	var closed atomic.Int32
	o.ProcessStarted("b")
	tr := o.StartDial(context.Background(), "b", net.Destination{}, func() { closed.Add(1) }).(*closureTracker)
	defer tr.Finished(nil)
	read, _ := tr.Dialed(nil, nil, nil, nil)
	turn := func() time.Time {
		wake := o.e.planSleeps(*now)
		if batch := o.e.takeSleeps(); len(batch) > 0 {
			o.sleepOutbounds(batch)
		}
		return wake
	}
	turn() // Discover resource capability.
	turn() // Sample before first response.
	*now = now.Add(100 * time.Millisecond)
	read.Add(1024)
	lastRead := *now
	*now = now.Add(o.s.closeIdleAfter - time.Millisecond - 100*time.Millisecond)
	turn()
	if closed.Load() != 0 {
		t.Fatal("closed before idle deadline")
	}
	*now = lastRead.Add(o.s.closeIdleAfter - time.Millisecond)
	turn()
	if closed.Load() != 0 {
		t.Fatal("closed before last-read idle deadline")
	}
	*now = lastRead.Add(o.s.closeIdleAfter)
	turn()
	if closed.Load() != 1 {
		t.Fatalf("silent lightweight stream was not closed at lastRead+after: closes=%d planned=%v after=%v", closed.Load(), o.e.path("b").idleNext.Sub(lastRead), o.s.closeIdleAfter)
	}
	if sleepy.sleeps != 0 || o.s.sleepAfter != 0 || o.sweeping || o.trackers != nil {
		t.Fatal("idle closure activated sleep or passive detector infrastructure")
	}
	if s.current() != "a" {
		t.Fatal("resource closure changed choice")
	}
}
