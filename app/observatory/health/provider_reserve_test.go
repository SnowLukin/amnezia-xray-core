package health

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func reserveProof(e *engine, tag string, at time.Time) {
	p := e.pathAt(tag, at)
	p.candidateVerifiedAt, p.candidateEpoch = at, e.candidateEpoch
	p.candidateChecked = true
	p.lastProbeResult = probeResult{ok: true, reason: "candidate"}
	p.status = statusUp
}

func TestProbeOnlyRoadsOrderBoundAndUserIsolation(t *testing.T) {
	paths := []*PathDefinition{{Tag: "p-user", Cost: 1}}
	for _, tag := range []string{"p-a", "p-b", "p-c", "p-d"} {
		paths = append(paths, &PathDefinition{Tag: tag, Cost: 2, IsProbeOnly: true})
	}
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Paths: paths}, "p-user", "p-a", "p-b", "p-c", "p-d")
	cfg := te.prov().cfg
	for i, tag := range []string{"p-a", "p-b", "p-c", "p-d"} {
		reserveProof(te.engine, tag, at(time.Duration(i+1)*time.Second))
	}
	te.sel.setActive("")
	te.sel.exhausted = true
	if _, skip := te.resolveProviderVia(cfg, "auto", at(5*time.Second)); skip != "balancer_exhausted" {
		t.Fatal(skip)
	}
	cfg.probeOnlyRoads = true
	roads, skip := te.resolveProviderVia(cfg, "auto", at(5*time.Second))
	if skip != "" || !slices.Equal(roads, []string{"p-d", "p-c"}) {
		t.Fatal(roads, skip)
	}
	te.paths["p-d"].candidateEpoch++
	te.retiring["p-c"] = true
	roads, skip = te.resolveProviderVia(cfg, "auto", at(5*time.Second))
	if skip != "" || !slices.Equal(roads, []string{"p-b", "p-a"}) {
		t.Fatal(roads, skip)
	}
	te.sel.setActive("p-user")
	roads, skip = te.resolveProviderVia(cfg, "auto", at(5*time.Second))
	if skip != "" || !slices.Equal(roads, []string{"p-user"}) {
		t.Fatal(roads, skip)
	}
	if _, skip = te.resolveProviderVia(cfg, "p-b", at(5*time.Second)); skip != "probe_only_outbound" {
		t.Fatal(skip)
	}
	te.paths["p-user"].status = statusDown
	te.paths["p-user"].quarantineUntil = at(time.Hour)
	te.sel.setActive("")
	te.updateCandidates(te.sel, []string{"p-user", "p-a", "p-b", "p-c", "p-d"}, at(5*time.Second))
	if got := te.sel.current(); got != "" {
		t.Fatalf("user selected probe-only %s", got)
	}
}

// The balancer still names the outbound it just found dead while it fails
// over: a refresh fired at that moment must not wait out its timeout there.
// Found on the warm-reserve stand: the minHealthy refresh went through the
// dead slot and the next one waited for minInterval.
func TestProviderViaSkipsCurrentOutboundDown(t *testing.T) {
	paths := []*PathDefinition{{Tag: "p-user", Cost: 1}, {Tag: "p-warm", Cost: 1}, {Tag: "p-probe", Cost: 2, IsProbeOnly: true}}
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Paths: paths}, "p-user", "p-warm", "p-probe")
	cfg := te.prov().cfg
	reserveProof(te.engine, "p-probe", at(time.Second))
	te.sel.setActive("p-user")
	if roads, skip := te.resolveProviderVia(cfg, "auto", at(2*time.Second)); skip != "" || !slices.Equal(roads, []string{"p-user"}) {
		t.Fatal(roads, skip)
	}
	te.paths["p-user"].status = statusDown
	if _, skip := te.resolveProviderVia(cfg, "auto", at(2*time.Second)); skip != "current_outbound_down" {
		t.Fatal(skip)
	}
	cfg.probeOnlyRoads = true
	if roads, skip := te.resolveProviderVia(cfg, "auto", at(2*time.Second)); skip != "" || !slices.Equal(roads, []string{"p-probe"}) {
		t.Fatal(roads, skip)
	}
}

// A provider request through a probe-only road passes the guard of that
// outbound alone; user traffic and a road marked for another outbound do not.
// Found on the stand: without it every road request ended as a closed pipe.
func TestProviderRoadPassesProbeOnlyGuard(t *testing.T) {
	o := newObserver(context.Background(), &Config{Paths: observed("road", "other")})
	o.s.paths.Store("road", pathSettings{isProbeOnly: true})
	o.s.paths.Store("other", pathSettings{isProbeOnly: true})
	var wire atomic.Int64
	road := withProviderRoad(context.Background(), &wire, "road")
	if !o.AllowDispatch(road, "road", true) {
		t.Fatal("provider road rejected")
	}
	if o.AllowDispatch(road, "other", true) {
		t.Fatal("road marker admitted another probe-only outbound")
	}
	if o.AllowDispatch(context.Background(), "road", true) {
		t.Fatal("user traffic admitted on probe-only")
	}
	o.s.paths.Store("road", pathSettings{})
	if o.AllowDispatch(road, "road", true) {
		t.Fatal("road marker admitted a handler that is no longer probe-only")
	}
}

// A minHealthy refresh waits while the balancer fails over from the outbound
// it found in doubt, so it goes through the outbound the balancer settles on.
func TestMinHealthyWaitsForFailover(t *testing.T) {
	paths := []*PathDefinition{{Tag: "p-user", Cost: 1}, {Tag: "p-warm", Cost: 1}, {Tag: "p-probe", Cost: 2, IsProbeOnly: true}}
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Paths: paths}, "p-user", "p-warm", "p-probe")
	p := te.prov()
	p.cfg.minHealthy = 2
	te.sel.started = true
	reserveProof(te.engine, "p-probe", at(time.Second))
	te.paths["p-warm"].status = statusUp
	te.sel.setActive("p-user")
	te.paths["p-user"].status = statusSuspect
	if want, _, _ := te.wantedRefresh(p, at(2*time.Second)); want[triggerMinHealthy] {
		t.Fatal("minHealthy refreshed through the outbound in doubt")
	}
	te.paths["p-user"].status = statusDown
	te.sel.setActive("p-warm")
	if want, _, _ := te.wantedRefresh(p, at(3*time.Second)); !want[triggerMinHealthy] {
		t.Fatal("minHealthy did not refresh after the failover")
	}
}

// Plain http needs the consent of the config; new mirrors end the backoff of
// failures on the old ones, but not a pause the backend asked for.
func TestSetProviderURLsPlainHTTPAndPauses(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Paths: []*PathDefinition{{Tag: "p-a", Cost: 1}}}, "p-a")
	p := te.prov()
	p.cfg.plainHTTP = false
	if err := te.setProviderURLs(p.cfg.tag, []string{"http://a.test/set"}, at(0)); err == nil {
		t.Fatal("plain http accepted without consent")
	}
	p.failures, p.backoffUntil, p.lastClass = 3, at(time.Hour), "timeout"
	p.retryAfterUntil = at(time.Minute)
	if err := te.setProviderURLs(p.cfg.tag, []string{"https://b.test/set"}, at(time.Second)); err != nil {
		t.Fatal(err)
	}
	if p.failures != 0 || !p.backoffUntil.IsZero() || !p.retryAfterUntil.Equal(at(time.Minute)) || p.urlsGen != 1 {
		t.Fatalf("failures %d backoff %v retryAfter %v gen %d", p.failures, p.backoffUntil, p.retryAfterUntil, p.urlsGen)
	}
	p.cfg.plainHTTP = true
	if err := te.setProviderURLs(p.cfg.tag, []string{"http://a.test/set"}, at(2*time.Second)); err != nil || p.urlsGen != 2 {
		t.Fatal(err, p.urlsGen)
	}
}
