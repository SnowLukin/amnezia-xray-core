package health

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/transport"
)

type fakeHandler struct {
	tag    string
	proxy  *serial.TypedMessage
	mu     sync.Mutex
	closed int
}

func (h *fakeHandler) Start() error { return nil }
func (h *fakeHandler) Close() error {
	h.mu.Lock()
	h.closed++
	h.mu.Unlock()
	return nil
}
func (h *fakeHandler) closes() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}
func (h *fakeHandler) Tag() string                                   { return h.tag }
func (h *fakeHandler) Dispatch(context.Context, *transport.Link)     {}
func (h *fakeHandler) SenderSettings() *serial.TypedMessage          { return serial.ToTypedMessage(&Backoff{}) }
func (h *fakeHandler) ProxySettings() *serial.TypedMessage           { return h.proxy }
func (h *fakeHandler) String() string                                { return h.tag }
func (h *fakeHandler) withProxy(m *serial.TypedMessage) *fakeHandler { h.proxy = m; return h }

type fakeManager struct {
	mu       sync.Mutex
	handlers map[string]outbound.Handler
	failAdd  string
}

func newFakeManager() *fakeManager { return &fakeManager{handlers: map[string]outbound.Handler{}} }

func (m *fakeManager) Type() interface{}                   { return outbound.ManagerType() }
func (m *fakeManager) Start() error                        { return nil }
func (m *fakeManager) Close() error                        { return nil }
func (m *fakeManager) GetDefaultHandler() outbound.Handler { return nil }
func (m *fakeManager) GetHandler(tag string) outbound.Handler {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.handlers[tag]
}
func (m *fakeManager) AddHandler(_ context.Context, h outbound.Handler) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h.Tag() == m.failAdd {
		return errors.New("refused ", h.Tag())
	}
	if _, ok := m.handlers[h.Tag()]; ok {
		return errors.New("existing tag found: ", h.Tag())
	}
	m.handlers[h.Tag()] = h
	return nil
}
func (m *fakeManager) RemoveHandler(_ context.Context, tag string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.handlers, tag)
	return nil
}
func (m *fakeManager) ListHandlers(context.Context) []outbound.Handler {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []outbound.Handler{}
	for _, h := range m.handlers {
		out = append(out, h)
	}
	return out
}
func (m *fakeManager) tags() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for t := range m.handlers {
		out = append(out, t)
	}
	return strings.Join(sortedCopy(out), ",")
}

func sortedCopy(s []string) []string {
	out := append([]string(nil), s...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// fakeOutbound is {"tag": "x1", "v": 1}; v stands for the settings.
type fakeOutbound struct {
	Tag      string `json:"tag"`
	Protocol string `json:"protocol"`
	V        int64  `json:"v"`
	Bad      bool   `json:"bad"`
}

func fakeSettings(v int64) *serial.TypedMessage { return serial.ToTypedMessage(&Backoff{Initial: v}) }

type applyFixture struct {
	o       *Observer
	rt      *providerRuntime
	m       *fakeManager
	created []*fakeHandler
}

func newApplyFixture(t *testing.T, drain time.Duration) *applyFixture {
	t.Helper()
	pc := quietProvider(func(p *ProviderConfig) { p.Apply.Drain = int64(drain) })
	o := newObserver(context.Background(), &Config{Providers: []*ProviderConfig{pc}})
	t.Cleanup(func() { o.Close() })
	f := &applyFixture{o: o, rt: o.providers[0], m: newFakeManager()}
	o.ohm = f.m
	o.parseOutbounds = func(raw json.RawMessage) ([]*core.OutboundHandlerConfig, error) {
		var set []fakeOutbound
		if err := json.Unmarshal(raw, &set); err != nil {
			return nil, err
		}
		var out []*core.OutboundHandlerConfig
		for _, fo := range set {
			if fo.Tag == "unparsable" {
				return nil, errors.New("unknown protocol")
			}
			cfg := &core.OutboundHandlerConfig{Tag: fo.Tag, ProxySettings: fakeSettings(fo.V), SenderSettings: serial.ToTypedMessage(&Backoff{})}
			if fo.Bad {
				cfg.ProxySettings = serial.ToTypedMessage(&Backoff{Initial: -1})
			}
			out = append(out, cfg)
		}
		return out, nil
	}
	o.newHandler = func(c *core.OutboundHandlerConfig) (outbound.Handler, error) {
		if inst, _ := c.ProxySettings.GetInstance(); inst.(*Backoff).Initial < 0 {
			return nil, errors.New("cannot build")
		}
		h := &fakeHandler{tag: c.Tag, proxy: c.ProxySettings}
		f.created = append(f.created, h)
		return h, nil
	}
	s := o.e.addSelector("auto", newSelectorSettings(&SelectorConfig{Costs: costs("p-", 1)}))
	o.e.noteDemand(time.Now())
	o.e.updateCandidates(s, []string{"p-x1"}, time.Now())
	return f
}

func answer(t *testing.T, outbounds ...fakeOutbound) *providerAnswer {
	t.Helper()
	for i := range outbounds {
		if outbounds[i].Protocol == "" {
			outbounds[i].Protocol = "vless"
		}
	}
	body, _ := json.Marshal(map[string]any{"outbounds": outbounds})
	a, err := parseAnswer(body)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func (f *applyFixture) apply(t *testing.T, a *providerAnswer) (bool, error) {
	t.Helper()
	return f.o.applySet(f.rt, a, &refreshJob{p: f.rt.p, trigger: triggerHost})
}

func (f *applyFixture) lastApply(t *testing.T) map[string]any {
	t.Helper()
	raw, _ := f.o.DrainEvents()
	var evs []Event
	_ = json.Unmarshal(raw, &evs)
	for i := len(evs) - 1; i >= 0; i-- {
		if evs[i].Type == "provider_apply" {
			return evs[i].Fields
		}
	}
	t.Fatal("no provider_apply event")
	return nil
}

func TestApplyKeepsUnchangedAddsNewAndDrainsRemoved(t *testing.T) {
	f := newApplyFixture(t, 50*time.Millisecond)
	// An outbound with the provider prefix from the config is adopted.
	adopted := (&fakeHandler{tag: "p-x1"}).withProxy(fakeSettings(1))
	f.m.handlers["p-x1"] = adopted

	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1}, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	if ev := f.lastApply(t); ev["added"] != 1.0 || ev["unchanged"] != 1.0 || ev["removed"] != 0.0 {
		t.Fatalf("first apply %v", ev)
	}
	if f.m.GetHandler("p-x1") != adopted || f.m.tags() != "p-x1,p-x2" {
		t.Fatalf("manager %s, adopted handler replaced: %v", f.m.tags(), f.m.GetHandler("p-x1") != adopted)
	}

	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x2", V: 1}, fakeOutbound{Tag: "x3", V: 1})); err != nil {
		t.Fatal(err)
	}
	if ev := f.lastApply(t); ev["added"] != 1.0 || ev["unchanged"] != 1.0 || ev["removed"] != 1.0 {
		t.Fatalf("second apply %v", ev)
	}
	// The removed outbound still serves its connections and is only out of
	// the balancer.
	f.o.mu.Lock()
	retiring := f.o.e.retiring["p-x1"]
	f.o.mu.Unlock()
	if f.m.GetHandler("p-x1") == nil || !retiring || adopted.closes() != 0 {
		t.Fatalf("p-x1 registered %v retiring %v closed %d right after the apply", f.m.GetHandler("p-x1") != nil, retiring, adopted.closes())
	}
	time.Sleep(200 * time.Millisecond)
	if f.m.tags() != "p-x2,p-x3" || adopted.closes() != 1 {
		t.Fatalf("after drain: manager %s, p-x1 closed %d times", f.m.tags(), adopted.closes())
	}
	f.o.mu.Lock()
	retiring = f.o.e.retiring["p-x1"]
	f.o.mu.Unlock()
	if retiring {
		t.Fatal("p-x1 is still retiring after removal")
	}
}

func TestApplyReplacesChangedOutboundUnderSameTag(t *testing.T) {
	f := newApplyFixture(t, 50*time.Millisecond)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); err != nil {
		t.Fatal(err)
	}
	old := f.m.GetHandler("p-x1").(*fakeHandler)
	f.open("p-x1")
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 2})); err != nil {
		t.Fatal(err)
	}
	if ev := f.lastApply(t); ev["replaced"] != 1.0 || ev["added"] != 0.0 {
		t.Fatalf("apply %v", ev)
	}
	now := f.m.GetHandler("p-x1").(*fakeHandler)
	if now == old || old.closes() != 0 {
		t.Fatalf("not replaced, or the old handler closed before its drain")
	}
	time.Sleep(200 * time.Millisecond)
	if old.closes() != 1 || now.closes() != 0 || f.m.GetHandler("p-x1") != now {
		t.Fatalf("after drain: old closed %d, new closed %d", old.closes(), now.closes())
	}
}

func TestApplyBrokenSetChangesNothing(t *testing.T) {
	cases := map[string][]fakeOutbound{
		"has no tag":    {{Tag: "x2", V: 1}, {V: 1}},
		"duplicate tag": {{Tag: "x2", V: 1}, {Tag: "x2", V: 2}},
		"build:":        {{Tag: "x2", V: 1}, {Tag: "unparsable"}},
		"build p-x3":    {{Tag: "x2", V: 1}, {Tag: "x3", Bad: true}},
		"not allowed":   {{Tag: "x2", V: 1}, {Tag: "x3", Protocol: "freedom"}},
	}
	for name, set := range cases {
		t.Run(name, func(t *testing.T) {
			f := newApplyFixture(t, time.Minute)
			if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); err != nil {
				t.Fatal(err)
			}
			before := f.m.GetHandler("p-x1")
			f.created = nil
			if _, err := f.apply(t, answer(t, set...)); err == nil {
				t.Fatal("broken set applied")
			}
			if ev := f.lastApply(t); ev["ok"] != false || !strings.Contains(ev["error"].(string), name) {
				t.Fatalf("event %v, want an error about %q", ev, name)
			}
			if f.m.tags() != "p-x1" || f.m.GetHandler("p-x1") != before {
				t.Fatalf("manager changed: %s", f.m.tags())
			}
			for _, h := range f.created {
				if h.closes() != 1 {
					t.Fatalf("handler %s built for the broken set was not closed", h.tag)
				}
			}
			// The same answer fixed later still applies: a failure does not
			// remember its version.
			if changed, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1}, fakeOutbound{Tag: "x2", V: 1})); err != nil || !changed {
				t.Fatalf("good set after a broken one: changed %v, %v", changed, err)
			}
		})
	}
}

func TestApplyRejectsCostsNotAboveZero(t *testing.T) {
	for _, cost := range []string{"0", "-1"} {
		f := newApplyFixture(t, time.Minute)
		a, err := parseAnswer([]byte(`{"outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}], "costs": {"x1": ` + cost + `}}`))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.apply(t, a); err == nil || f.m.tags() != "" {
			t.Fatalf("cost %s: applied, manager %q", cost, f.m.tags())
		}
	}
}

func TestApplyRollsBackWhenRegistrationFails(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1}, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	x1 := f.m.GetHandler("p-x1")
	f.m.failAdd = "p-x3"
	f.created = nil
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 2}, fakeOutbound{Tag: "x3", V: 1})); err == nil {
		t.Fatal("apply succeeded although registration failed")
	}
	if f.m.tags() != "p-x1,p-x2" || f.m.GetHandler("p-x1") != x1 {
		t.Fatalf("manager after rollback: %s, p-x1 restored %v", f.m.tags(), f.m.GetHandler("p-x1") == x1)
	}
	for _, h := range f.created {
		if h.closes() != 1 {
			t.Fatalf("new handler %s not closed after rollback", h.tag)
		}
	}
	f.o.mu.Lock()
	n := len(f.o.e.retiring)
	f.o.mu.Unlock()
	if n != 0 || len(f.rt.draining) != 0 {
		t.Fatalf("rollback left %d retiring and %d draining outbounds", n, len(f.rt.draining))
	}
}

func TestApplySameVersionDoesNothing(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	set := answer(t, fakeOutbound{Tag: "x1", V: 1})
	if changed, _ := f.apply(t, set); !changed {
		t.Fatal("first apply did not change anything")
	}
	f.created = nil
	if changed, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); changed || err != nil {
		t.Fatalf("same set: changed %v, %v", changed, err)
	}
	if ev := f.lastApply(t); ev["changed"] != false || len(f.created) != 0 {
		t.Fatalf("same set built %d handlers, event %v", len(f.created), ev)
	}
}

func TestApplyRevivesRetiringOutbound(t *testing.T) {
	f := newApplyFixture(t, 100*time.Millisecond)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1}, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	x1 := f.m.GetHandler("p-x1").(*fakeHandler)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1}, fakeOutbound{Tag: "x2", V: 1}, fakeOutbound{Tag: "x4", V: 1})); err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	f.o.mu.Lock()
	retiring := f.o.e.retiring["p-x1"]
	f.o.mu.Unlock()
	if f.m.GetHandler("p-x1") != x1 || x1.closes() != 0 || retiring {
		t.Fatalf("revived p-x1: same handler %v, closed %d, retiring %v", f.m.GetHandler("p-x1") == x1, x1.closes(), retiring)
	}
}

func TestCloseStopsDrainsAndClosesReplacedHandlers(t *testing.T) {
	f := newApplyFixture(t, time.Hour)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); err != nil {
		t.Fatal(err)
	}
	old := f.m.GetHandler("p-x1").(*fakeHandler)
	f.open("p-x1")
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 2})); err != nil {
		t.Fatal(err)
	}
	f.o.closeProviders()
	if old.closes() != 1 || len(f.rt.draining) != 0 {
		t.Fatalf("replaced handler closed %d times, %d drains left", old.closes(), len(f.rt.draining))
	}
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x9", V: 1})); err == nil {
		t.Fatal("applied after close")
	}
}

func TestParseAnswer(t *testing.T) {
	if _, err := parseAnswer([]byte(`{"outbounds": [`)); err == nil {
		t.Fatal("broken JSON parsed")
	}
	if _, err := parseAnswer([]byte(`{"outbounds": []}`)); err == nil {
		t.Fatal("empty set accepted")
	}
	a, err := parseAnswer([]byte(`{"outbounds": [{"tag": "x"}], "nextRefreshIn": "90m", "costs": {"x": 3}}`))
	if err != nil || a.nextRefreshIn != 90*time.Minute || a.costs["x"] != 3 {
		t.Fatalf("parsed %+v, %v", a, err)
	}
	if a, _ := parseAnswer([]byte(`{"outbounds": [{"tag": "x"}], "nextRefreshIn": 600}`)); a.nextRefreshIn != 10*time.Minute {
		t.Fatalf("seconds parsed as %v", a.nextRefreshIn)
	}
	if _, err := parseAnswer([]byte(`{"outbounds": [{"tag": "x"}], "nextRefreshIn": "soon"}`)); err == nil {
		t.Fatal("bad nextRefreshIn accepted")
	}
	if a, _ := parseAnswer([]byte(`{"outbounds": [{"tag": "x"}], "version": "v7", "hash": "h"}`)); a.version != "v7" {
		t.Fatalf("version %q, want the one the server sent", a.version)
	}
	a1, _ := parseAnswer([]byte(`{"outbounds": [{"tag": "x", "v": 1}]}`))
	a2, _ := parseAnswer([]byte("{\"outbounds\": [ {\"tag\" : \"x\",\n \"v\": 1} ], \"nextRefreshIn\": 5}"))
	a3, _ := parseAnswer([]byte(`{"outbounds": [{"tag": "x", "v": 2}]}`))
	if a1.version != a2.version || a1.version == a3.version {
		t.Fatalf("hashes: %s %s %s; want equal for the same set and different for another", a1.version, a2.version, a3.version)
	}
}

// An outbound replaced under its tag may stand for another server: the
// address learned from the old one is forgotten.
func TestReplacedOutboundForgetsItsServer(t *testing.T) {
	f := newApplyFixture(t, 50*time.Millisecond)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); err != nil {
		t.Fatal(err)
	}
	f.o.StartDial(context.Background(), "p-x1", net.TCPDestination(net.ParseAddress("192.0.2.7"), 443), nil)
	if got := f.o.serverName("p-x1"); got != "192.0.2.7" {
		t.Fatalf("setup: server of p-x1 %q", got)
	}
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 2})); err != nil {
		t.Fatal(err)
	}
	if got := f.o.serverName("p-x1"); got != "p-x1" {
		t.Fatalf("p-x1 was replaced, yet probes still count it as server %q", got)
	}
}

// An outbound gone from the set leaves nothing behind once its drain ends.
func TestDrainedOutboundLeavesNothingBehind(t *testing.T) {
	f := newApplyFixture(t, 50*time.Millisecond)
	f.o.e.addSelector("auto", newSelectorSettings(&SelectorConfig{Costs: costs("p-", 1), SwitchWithoutProbe: true}))
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1}, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	f.o.mu.Lock()
	s := f.o.e.selectors["auto"]
	f.o.e.updateCandidates(s, []string{"p-x1", "p-x2"}, time.Now())
	s.penalty["p-x1"] = 2
	f.o.mu.Unlock()
	tr := f.o.StartDial(context.Background(), "p-x1", net.TCPDestination(net.ParseAddress("192.0.2.7"), 443), nil)
	tr.Dialed(nil, nil, nil, nil)
	tr.Finished(nil)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	// A later outbound under the same tag.
	f.m.mu.Lock()
	f.m.handlers["p-x1"] = &fakeHandler{tag: "p-x1"}
	f.m.mu.Unlock()
	f.o.mu.Lock()
	_, path := f.o.e.paths["p-x1"]
	_, penalty := s.penalty["p-x1"]
	f.o.mu.Unlock()
	if path || penalty || f.o.serverName("p-x1") != "p-x1" {
		t.Fatalf("after the drain of p-x1: path kept %v, penalty kept %v, server %q", path, penalty, f.o.serverName("p-x1"))
	}
}

// follow makes the balancer see the outbounds of the provider as the router
// would and leave a retired one without probing.
func (f *applyFixture) follow(tags ...string) {
	f.o.mu.Lock()
	s := f.o.e.addSelector("auto", newSelectorSettings(&SelectorConfig{Costs: costs("p-", 1), SwitchWithoutProbe: true}))
	f.o.e.updateCandidates(s, tags, time.Now())
	f.o.mu.Unlock()
}

// open starts a user connection through the outbound and leaves it open.
func (f *applyFixture) open(tag string) {
	tr := f.o.StartDial(context.Background(), tag, net.TCPDestination(net.ParseAddress("192.0.2.7"), 443), nil)
	tr.Dialed(nil, nil, nil, nil)
}

func TestDrainWithoutConnectionsEndsAtOnce(t *testing.T) {
	f := newApplyFixture(t, time.Hour)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1}, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	f.follow("p-x1", "p-x2")
	x1 := f.m.GetHandler("p-x1").(*fakeHandler)
	x2 := f.m.GetHandler("p-x2").(*fakeHandler)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x2", V: 2})); err != nil {
		t.Fatal(err)
	}
	if x1.closes() != 1 || x2.closes() != 1 || f.m.tags() != "p-x2" || len(f.rt.draining) != 0 {
		t.Fatalf("without connections: removed closed %d, replaced closed %d, manager %s, %d draining",
			x1.closes(), x2.closes(), f.m.tags(), len(f.rt.draining))
	}
}

func TestDrainWaitsForOpenConnections(t *testing.T) {
	f := newApplyFixture(t, 100*time.Millisecond)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1}, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	f.follow("p-x1", "p-x2")
	x1 := f.m.GetHandler("p-x1").(*fakeHandler)
	x2 := f.m.GetHandler("p-x2").(*fakeHandler)
	f.open("p-x1")
	f.open("p-x2")
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x2", V: 2})); err != nil {
		t.Fatal(err)
	}
	if x1.closes() != 0 || x2.closes() != 0 {
		t.Fatalf("closed with open connections: removed %d, replaced %d", x1.closes(), x2.closes())
	}
	time.Sleep(300 * time.Millisecond)
	if x1.closes() != 1 || x2.closes() != 1 {
		t.Fatalf("after the drain: removed closed %d, replaced closed %d", x1.closes(), x2.closes())
	}
}

// Sets that keep being replaced, as during a wave of blocks, do not pile up
// handlers: past maxOutbounds the oldest drains end early.
func TestDrainsAreCappedAtMaxOutbounds(t *testing.T) {
	f := newApplyFixture(t, time.Hour)
	f.rt.cfg.maxOutbounds = 2
	var handlers []*fakeHandler
	for gen := 0; gen < 3; gen++ {
		a, b := fmt.Sprintf("x%d", 2*gen+1), fmt.Sprintf("x%d", 2*gen+2)
		if _, err := f.apply(t, answer(t, fakeOutbound{Tag: a, V: 1}, fakeOutbound{Tag: b, V: 1})); err != nil {
			t.Fatal(err)
		}
		f.follow("p-"+a, "p-"+b)
		f.open("p-" + a)
		f.open("p-" + b)
		handlers = append(handlers, f.m.GetHandler("p-"+a).(*fakeHandler), f.m.GetHandler("p-"+b).(*fakeHandler))
	}
	var closed []int
	for _, h := range handlers {
		closed = append(closed, h.closes())
	}
	if fmt.Sprint(closed) != "[1 1 0 0 0 0]" || len(f.rt.draining) != 2 {
		t.Fatalf("closes by age %v, %d draining; want the two oldest closed and two draining", closed, len(f.rt.draining))
	}
}

type fakeStats struct {
	stats.NoopManager
	mu      sync.Mutex
	removed []string
}

func (s *fakeStats) UnregisterCounter(name string) error {
	s.mu.Lock()
	s.removed = append(s.removed, name)
	s.mu.Unlock()
	return nil
}

// Traffic counters of an outbound go with its tag, not with a handler that
// was replaced under it.
func TestDrainedOutboundLeavesNoCounters(t *testing.T) {
	f := newApplyFixture(t, 50*time.Millisecond)
	sm := &fakeStats{}
	f.o.stats = sm
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1}, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	f.follow("p-x1", "p-x2")
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x2", V: 2})); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if strings.Join(sm.removed, ",") != "outbound>>>p-x1>>>traffic>>>uplink,outbound>>>p-x1>>>traffic>>>downlink" {
		t.Fatalf("counters removed: %v", sm.removed)
	}
}

func TestAnswerLimitFollowsTheSetSize(t *testing.T) {
	for _, tc := range []struct {
		outbounds int32
		maxBytes  int64
		want      int64
	}{
		{0, 0, 256 << 10},
		{16, 0, 64 << 10},
		{128, 0, 512 << 10},
		{0, 300_000, 300_000},
	} {
		p := newProviderSettings(quietProvider(func(p *ProviderConfig) {
			p.Apply.MaxOutbounds = tc.outbounds
			p.MaxBytes = tc.maxBytes
		}))
		if p.maxBytes != tc.want {
			t.Errorf("maxOutbounds %d, maxBytes %d: limit %d, want %d", tc.outbounds, tc.maxBytes, p.maxBytes, tc.want)
		}
	}
}

// The answer is read once: what it holds after parsing is about one copy of
// the body, not one per step.
func TestAnswerIsParsedInOnePass(t *testing.T) {
	pad := strings.Repeat("x", 4000)
	var outs []string
	for i := 0; i < 100; i++ {
		outs = append(outs, fmt.Sprintf(`{"tag": "x%d", "protocol": "vless", "settings": {"pad": %q}}`, i, pad))
	}
	body := []byte(`{"outbounds": [` + strings.Join(outs, ",") + `], "costs": {"x1": 2},
		"routing": {"rules": [], "balancers": []}, "dns": {"servers": ["1.1.1.1"]}}`)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	a, err := parseAnswer(body)
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	got := after.TotalAlloc - before.TotalAlloc
	t.Logf("parsing a %d byte answer allocated %d bytes", len(body), got)
	if got > uint64(len(body))*3/2 {
		t.Fatalf("parsing a %d byte answer allocated %d bytes", len(body), got)
	}
	runtime.KeepAlive(a)
}

func TestTooManyOutboundsAreRefusedBeforeBuilding(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	f.rt.cfg.maxOutbounds = 2
	parse := f.o.parseOutbounds
	parsed := false
	f.o.parseOutbounds = func(raw json.RawMessage) ([]*core.OutboundHandlerConfig, error) {
		parsed = true
		return parse(raw)
	}
	_, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1"}, fakeOutbound{Tag: "x2"}, fakeOutbound{Tag: "x3"}))
	if err == nil || !strings.Contains(err.Error(), "at most 2") || parsed {
		t.Fatalf("three outbounds with a limit of two: %v, built %v", err, parsed)
	}
}
