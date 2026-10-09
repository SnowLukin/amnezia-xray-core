package health

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
)

// leasedAnswer is a set of x1 and x2 by the clock of a backend at server,
// with the lease of x1 ending at end, and more keys of the answer.
func leasedAnswer(t *testing.T, version string, server, end time.Time, more string) *providerAnswer {
	t.Helper()
	a, err := parseAnswer([]byte(fmt.Sprintf(`{"version": %q, "serverTime": %q, "expiresAt": {"x1": %q},
		"outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}, {"tag": "x2", "protocol": "vless", "v": 1}]%s}`,
		version, stamp(server), stamp(end), more)))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func (f *applyFixture) providerSet(t *testing.T, after uint64) (map[string]json.RawMessage, uint64) {
	t.Helper()
	raw, seq, err := f.o.ProviderSet("fresh", after)
	if err != nil {
		t.Fatal(err)
	}
	if raw == nil {
		return nil, seq
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("%s: %v", raw, err)
	}
	return doc, seq
}

func TestProviderSetNilUntilApplied(t *testing.T) {
	f, _ := initialFixture(t, "")
	if raw, seq, err := f.o.ProviderSet("other", 0); raw != nil || seq != 0 || err == nil || err.Error() != "no provider other" {
		t.Fatalf("another provider: %s, %d, %v", raw, seq, err)
	}
	// Headers alone are no set to start with.
	if err := f.o.SetProviderHeaders("fresh", map[string]string{"Authorization": "Bearer host"}); err != nil {
		t.Fatal(err)
	}
	if doc, seq := f.providerSet(t, 0); doc != nil || seq != 1 {
		t.Fatalf("before an apply: %v, %d", doc, seq)
	}
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", Bad: true})); err == nil {
		t.Fatal("a set that does not build applied")
	}
	if doc, _ := f.providerSet(t, 0); doc != nil {
		t.Fatalf("after a refused answer: %v", doc)
	}
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); err != nil {
		t.Fatal(err)
	}
	if doc, seq := f.providerSet(t, 0); doc == nil || seq != 2 {
		t.Fatalf("after an apply: %v, %d", doc, seq)
	}
}

// The set the host keeps starts the next core as the last one ran: the same
// version, the token rotated last, also a header the host set under a name
// the answer may not set, and the leases less the time the core was down.
func TestProviderSetRoundTripsAsInitial(t *testing.T) {
	f, now := initialFixture(t, "")
	wall := now.Round(0)
	// The backend runs an hour ahead of the device.
	a := leasedAnswer(t, "v1", wall.Add(time.Hour), wall.Add(6*time.Hour), `, "headers": {"Authorization": "Bearer t2"}`)
	if _, err := f.apply(t, a); err != nil {
		t.Fatal(err)
	}
	if err := f.o.SetProviderHeaders("fresh", map[string]string{"X-Session": "s1"}); err != nil {
		t.Fatal(err)
	}
	raw, seq, err := f.o.ProviderSet("fresh", 0)
	if err != nil || seq != 2 {
		t.Fatalf("set %s, seq %d, %v", raw, seq, err)
	}
	var doc struct {
		Version    string            `json:"version"`
		Headers    map[string]string `json:"headers"`
		ServerTime time.Time         `json:"serverTime"`
		ReceivedAt time.Time         `json:"receivedAt"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil || doc.Version != "v1" || fmt.Sprint(doc.Headers) != "map[Authorization:Bearer t2 X-Session:s1]" ||
		!doc.ServerTime.Equal(wall.Add(time.Hour)) || !doc.ReceivedAt.Equal(wall) {
		t.Fatalf("set %s: %v", raw, err)
	}

	// The next core starts two hours later with a token of its own config.
	g, _ := initialFixture(t, string(raw))
	later := now.Add(2 * time.Hour)
	g.o.clock = func() time.Time { return later }
	g.rt.cfg.headers = map[string]string{"Authorization": "Bearer config"}
	g.o.applyInitial()
	if ev := g.lastApply(t); ev["ok"] != true || ev["trigger"] != "initial" || ev["version"] != "v1" || ev["clock"] != "saved" {
		t.Fatalf("initial %v", ev)
	}
	g.o.mu.Lock()
	p := g.rt.p
	left, skew, headers := p.leases["p-x1"].left(later), p.skew, fmt.Sprint(p.requestHeaders())
	g.o.mu.Unlock()
	if left != 3*time.Hour || skew != time.Hour || headers != "map[Authorization:Bearer t2 X-Session:s1]" {
		t.Fatalf("lease left %v, skew %v, headers %s", left, skew, headers)
	}
	// And the same answer again changes no outbound.
	if changed, err := g.apply(t, leasedAnswer(t, "v1", wall.Add(3*time.Hour), wall.Add(6*time.Hour), "")); changed || err != nil {
		t.Fatalf("the same set: %v, %v", changed, err)
	}
	// What the second core keeps is what the first one kept.
	again, seq, err := g.o.ProviderSet("fresh", 0)
	if err != nil || seq != 1 {
		t.Fatalf("set of the second core %s, seq %d, %v", again, seq, err)
	}
}

// The number grows with what the host keeps: the set, the end of a lease,
// the headers. An answer with only a fresher clock leaves it, so a host
// that asks with the number it has gets nothing.
func TestProviderSetSeqGrowsOnSetLeaseAndHeaders(t *testing.T) {
	f, now := initialFixture(t, "")
	wall := now.Round(0)
	step := func(name string, want uint64, apply func()) {
		t.Helper()
		_, before := f.providerSet(t, 0)
		apply()
		doc, seq := f.providerSet(t, before)
		if seq != want || (doc == nil) != (seq == before) {
			t.Fatalf("%s: seq %d after %d, set %v; want %d", name, seq, before, doc != nil, want)
		}
	}
	apply := func(a *providerAnswer) func() {
		return func() {
			if _, err := f.apply(t, a); err != nil {
				t.Fatal(err)
			}
		}
	}
	step("the first set", 1, apply(leasedAnswer(t, "v1", wall, wall.Add(time.Hour), "")))
	step("the same answer with a later clock", 1, apply(leasedAnswer(t, "v1", wall.Add(time.Minute), wall.Add(time.Hour), "")))
	step("a later end", 2, apply(leasedAnswer(t, "v1", wall, wall.Add(2*time.Hour), "")))
	step("an earlier end", 3, apply(leasedAnswer(t, "v1", wall, wall.Add(90*time.Minute), "")))
	step("a token", 4, apply(leasedAnswer(t, "v1", wall, wall.Add(90*time.Minute), `, "headers": {"Authorization": "Bearer t2"}`)))
	step("the same token", 4, apply(leasedAnswer(t, "v1", wall, wall.Add(90*time.Minute), `, "headers": {"Authorization": "Bearer t2"}`)))
	step("the host", 5, func() { _ = f.o.SetProviderHeaders("fresh", map[string]string{"Authorization": "Bearer t3"}) })
	step("the host again", 5, func() { _ = f.o.SetProviderHeaders("fresh", map[string]string{"Authorization": "Bearer t3"}) })
	noLease := answer(t, fakeOutbound{Tag: "x1", V: 1}, fakeOutbound{Tag: "x2", V: 1})
	noLease.version = "v1"
	step("no lease", 6, apply(noLease))
	step("another set", 7, apply(answer(t, fakeOutbound{Tag: "x1", V: 1})))
	f.o.mu.Lock()
	defer f.o.mu.Unlock()
	if st := f.o.e.status(now).Providers[0]; st.AnswerSeq != 7 {
		t.Fatalf("status answer_seq %d", st.AnswerSeq)
	}
}

// The host asks while a set is being built, which may take a while: it gets
// the last applied set at once.
func TestProviderSetDoesNotWaitForAnApply(t *testing.T) {
	f, _ := initialFixture(t, "")
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); err != nil {
		t.Fatal(err)
	}
	build, release := make(chan struct{}), make(chan struct{})
	newHandler := f.o.newHandler
	f.o.newHandler = func(c *core.OutboundHandlerConfig) (outbound.Handler, error) {
		close(build)
		<-release
		return newHandler(c)
	}
	done := make(chan error)
	go func() {
		_, err := f.apply(t, answer(t, fakeOutbound{Tag: "x2", V: 1}))
		done <- err
	}()
	<-build
	start := time.Now()
	doc, seq := f.providerSet(t, 0)
	if took := time.Since(start); doc == nil || seq != 1 || took > 100*time.Millisecond {
		t.Fatalf("set %v, seq %d after %v", doc != nil, seq, took)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, seq := f.providerSet(t, 0); seq != 2 {
		t.Fatalf("seq %d after the apply", seq)
	}
}

func TestProviderSetUnchangedAllocatesNothing(t *testing.T) {
	f, _ := initialFixture(t, "")
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); err != nil {
		t.Fatal(err)
	}
	_, seq := f.providerSet(t, 0)
	if allocs := testing.AllocsPerRun(100, func() {
		if raw, _, err := f.o.ProviderSet("fresh", seq); raw != nil || err != nil {
			t.Fatal("a set newer than the one the host has")
		}
	}); allocs != 0 {
		t.Fatalf("%v allocations per call with nothing new", allocs)
	}
}

// Headers of the config are the config of the next core: only the ones an
// answer or the host set go with the set.
func TestProviderSetLeavesConfigHeadersOut(t *testing.T) {
	f, _ := initialFixture(t, "")
	f.rt.cfg.headers = map[string]string{"Authorization": "Bearer config", "X-Client": "core"}
	a, err := parseAnswer([]byte(`{"outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}], "headers": {}}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.apply(t, a); err != nil {
		t.Fatal(err)
	}
	if doc, _ := f.providerSet(t, 0); doc == nil || doc["headers"] != nil {
		t.Fatalf("set %v", doc)
	}
	if err := f.o.SetProviderHeaders("fresh", map[string]string{"X-Session": "s1"}); err != nil {
		t.Fatal(err)
	}
	if doc, _ := f.providerSet(t, 0); string(doc["headers"]) != `{"X-Session":"s1"}` {
		t.Fatalf("headers %s", doc["headers"])
	}
}

// One answer is kept per provider, without whitespace, and a new one takes
// its place.
func TestKeptAnswerIsReplacedNotAdded(t *testing.T) {
	f, _ := initialFixture(t, "")
	for i := range 200 {
		body := fmt.Sprintf("{ \"version\": \"v%d\",\n  \"outbounds\": [ {\"tag\": \"x1\", \"protocol\": \"vless\", \"v\": %d,\n \"pad\": %q} ] }",
			i, i, strings.Repeat("p", i*100))
		a, err := parseAnswer([]byte(body))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.apply(t, a); err != nil {
			t.Fatal(err)
		}
		f.o.mu.Lock()
		kept := f.rt.p.last.raw
		f.o.mu.Unlock()
		if int64(len(kept)) > f.rt.cfg.maxBytes || len(kept) >= len(body) || strings.ContainsAny(string(kept[:20]), " \n") {
			t.Fatalf("answer %d kept as %d bytes of %d: %.40s", i, len(kept), len(body), kept)
		}
	}
	if doc, seq := f.providerSet(t, 0); string(doc["version"]) != `"v199"` || seq != 200 {
		t.Fatalf("version %s, seq %d", doc["version"], seq)
	}
}
