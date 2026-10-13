package health

import (
	"context"
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
