package health

import (
	"context"
	"fmt"
	gonet "net"
	"runtime"
	"strings"
	"testing"
	"time"
	"weak"

	"github.com/xtls/xray-core/common/net"
)

type heldConn struct {
	gonet.Conn
	payload []byte
}

func collected[T any](ptrs ...weak.Pointer[T]) int {
	n := 0
	for _, p := range ptrs {
		if p.Value() == nil {
			n++
		}
	}
	return n
}

func gcUntil(done func() bool) {
	deadline := time.Now().Add(2 * time.Second)
	for !done() && time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
}

// Connections that ended while others still wait for their deadline must not
// stay reachable through the sweep.
func TestSweepLetsEndedConnectionsGo(t *testing.T) {
	o := newObserver(context.Background(), &Config{Paths: observed("a1", "a2"),
		Passive: &PassiveConfig{SweepInterval: int64(5 * time.Millisecond)}})
	defer o.Close()
	dest := net.TCPDestination(net.LocalHostIP, 443)
	// Few enough for the sweep to keep its batch between passes.
	const n = 100
	var ended []weak.Pointer[connTracker]
	trackers := make([]*connTracker, 0, n)
	for i := 0; i < n; i++ {
		tr := o.StartDial(context.Background(), "a1", dest, nil).(*connTracker)
		tr.Dialed(nil, nil, nil, nil)
		trackers = append(trackers, tr)
		ended = append(ended, weak.Make(tr))
	}
	// Still dialing, so the sweep keeps running.
	o.StartDial(context.Background(), "a1", dest, nil)
	time.Sleep(50 * time.Millisecond)
	for _, tr := range trackers {
		tr.Finished(nil)
	}
	trackers = nil
	time.Sleep(50 * time.Millisecond)
	gcUntil(func() bool { return collected(ended...) == n })
	if got := collected(ended...); got != n {
		t.Fatalf("%d of %d ended connections are still held while the sweep runs", n-got, n)
	}
}

func keepAlive(p *[4096]byte) func() { return func() { runtime.KeepAlive(p) } }

// A tracker someone forgot to let go of does not keep the connection, or the
// user session behind its abort, alive.
func TestEndedTrackerHoldsNeitherConnectionNorAbort(t *testing.T) {
	o := newObserver(context.Background(), &Config{Paths: observed("a1", "a2")})
	defer o.Close()
	conn := &heldConn{payload: make([]byte, 32*1024)}
	session := new([4096]byte)
	tr := o.StartDial(context.Background(), "a1", net.TCPDestination(net.LocalHostIP, 443), keepAlive(session)).(*connTracker)
	tr.Dialed(conn, nil, nil, nil)
	wc, ws := weak.Make(conn), weak.Make(session)
	conn, session = nil, nil
	tr.Finished(nil)
	gcUntil(func() bool { return wc.Value() == nil && ws.Value() == nil })
	if wc.Value() != nil || ws.Value() != nil {
		t.Fatalf("an ended tracker holds its connection %v, its session %v", wc.Value() != nil, ws.Value() != nil)
	}
	runtime.KeepAlive(tr)
}

// Outbounds no balancer offers lose their path, their server address and
// the penalty of a failed return.
func TestGCForgetsEverythingAboutAnOutbound(t *testing.T) {
	o := newObserver(context.Background(), &Config{Paths: observed("a1", "a2")})
	defer o.Close()
	m := newFakeManager()
	m.handlers["a2"] = &fakeHandler{tag: "a2"}
	o.ohm = m
	s := o.e.addSelector("auto", newSelectorSettings(&SelectorConfig{Paths: pathSpecs("a1", 1, "a2", 1)}))
	o.e.noteDemand(t0)
	o.e.updateCandidates(s, []string{"a1", "a2"}, t0)
	o.StartDial(context.Background(), "a2", net.TCPDestination(net.ParseAddress("192.0.2.7"), 443), nil)
	if got := o.serverName("a2"); got != "192.0.2.7" {
		t.Fatalf("setup: server of a2 %q", got)
	}
	s.penalty["a2"] = 4
	o.e.updateCandidates(s, []string{"a1"}, t0.Add(time.Second))
	o.e.tick(t0.Add(time.Hour))
	if _, ok := o.e.paths["a2"]; ok {
		t.Fatal("a2 is no longer offered but its path is kept")
	}
	if got := o.serverName("a2"); got != "a2" {
		t.Fatalf("a2 is no longer offered but its server %q is kept", got)
	}
	if _, ok := s.penalty["a2"]; ok {
		t.Fatal("a2 is no longer offered but its penalty is kept")
	}
}

// Outbounds a provider adds later are hooked like the configured ones.
func TestObservesJudgedOutboundsAndTheNetworkCheck(t *testing.T) {
	o := newObserver(context.Background(), &Config{
		Features:     &FeatureConfig{Connectivity: v2Bool(true), Witnesses: v2Bool(true)},
		Paths:        observed("a1", "a2"),
		Connectivity: &ConnectivityConfig{Outbound: "check", Destination: "https://check.test/", Witnesses: []string{"direct"}},
		Providers:    []*ProviderConfig{quietProvider(nil)},
	})
	defer o.Close()
	for tag, want := range map[string]bool{"a1": true, "p-x9": false, "direct": true, "check": true, "block": false, "dns": false} {
		if got := o.Observes(tag); got != want {
			t.Errorf("observes %s: %v, want %v", tag, got, want)
		}
	}
}

func TestConfigValuesHaveCeilings(t *testing.T) {
	huge := int64(24 * time.Hour)
	s := newSettings(&Config{
		Features:     &FeatureConfig{Connectivity: v2Bool(true), Events: v2Bool(true)},
		Probes:       []*ProbeProfile{{Timeout: huge, ProgressTimeout: huge}},
		Connectivity: &ConnectivityConfig{Outbound: "direct", Destination: "https://check.test/", Timeout: huge},
		Schedule: &ScheduleConfig{MaxConcurrentProbes: 1 << 20, HostProbes: &HostProbesConfig{
			ProbesPerMinute: 1 << 20, ReuseWindow: huge, MaxOutbounds: 1 << 20, Timeout: huge,
		}},
		Events:  &EventsConfig{MaxEvents: 1 << 30, MaxBytes: 1 << 40},
		Network: &NetworkConfig{MaxNetworks: 1 << 30},
		Providers: []*ProviderConfig{quietProvider(func(p *ProviderConfig) {
			p.MaxBytes = 1 << 40
			p.Apply.MaxOutbounds = 1 << 30
			p.Apply.DrainTimeout = huge
			p.BaseVersion = strings.Repeat("b", 1<<20)
		})},
	})
	p := s.providers[0]
	for name, v := range map[string][2]int64{
		"probe timeout":        {int64(s.probes[0].timeout), int64(maxProbeTimeout)},
		"probe progress":       {int64(s.probes[0].progressTimeout), int64(maxProbeTimeout)},
		"connectivity timeout": {int64(s.connectivity.timeout), int64(maxProbeTimeout)},
		"maxConcurrentProbes":  {int64(s.schedule.concurrent), maxConcurrent},
		"events":               {int64(s.maxEvents), maxEvents},
		"event bytes":          {int64(s.maxBytes), maxEventBytes},
		"networks":             {int64(s.maxNetworks), maxNetworks},
		"answer bytes":         {p.maxBytes, min(maxProviderOutbounds*answerBytesPerOutbound, maxAnswerBytes)},
		"outbounds":            {int64(p.maxOutbounds), maxProviderOutbounds},
		"drainTimeout":         {int64(p.drain), int64(maxDrain)},
		"base version":         {int64(len(p.baseVersion)), maxBaseVersion},
		"host probes a minute": {int64(s.schedule.hostProbes.perMinute), defaultPerMinute / 2},
		"host reuse":           {int64(s.schedule.hostProbes.reuse), int64(defaultHostReuse)},
		"host outbounds":       {int64(s.schedule.hostProbes.maxOutbounds), maxHostOutbounds},
		"host timeout":         {int64(s.schedule.hostProbes.timeout), int64(maxHostTimeout)},
	} {
		if v[0] != v[1] {
			t.Errorf("%s %d, want the ceiling %d", name, v[0], v[1])
		}
	}
}

// Provider requests through a balancer go through whatever it chose, and a
// provider rotates its tags: the counts of requests under way by outbound
// keep nothing once the requests ended.
func TestFetchViaKeepsNothingAfterTheRequests(t *testing.T) {
	te := newTestEngine(t, &Config{Paths: observed("p-0")}, &SelectorConfig{SkipProbeBeforeSwitch: true}, "p-0")
	for i := 1; i <= 100; i++ {
		tag := fmt.Sprintf("p-%d", i)
		te.updateCandidates(te.sel, []string{tag}, at(time.Duration(i)*time.Second))
		te.sel.cfg.paths[tag] = &costEntrySettings{cost: 1}
		te.sel.cfg.baseOrder = append(te.sel.cfg.baseOrder, tag)
		te.updateCandidates(te.sel, []string{tag}, at(time.Duration(i)*time.Second))
		via, skip := te.resolveVia("auto", at(time.Duration(i)*time.Second))
		if via != tag || skip != "" {
			t.Fatalf("setup: via auto resolves to %q (%s), want %s", via, skip, tag)
		}
		te.fetchingVia(via, 1, at(time.Duration(i)*time.Second))
		te.fetchingVia(via, -1, at(time.Duration(i)*time.Second+time.Millisecond))
	}
	if len(te.fetchVia) != 0 {
		t.Fatalf("%d outbounds counted after every request ended", len(te.fetchVia))
	}
}
