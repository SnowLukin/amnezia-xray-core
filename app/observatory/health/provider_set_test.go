package health

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
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
	Tag string `json:"tag"`
	V   int64  `json:"v"`
	Bad bool   `json:"bad"`
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
	f := &applyFixture{o: o, rt: o.providers[0], m: newFakeManager()}
	o.ohm = f.m
	o.parseOutbounds = func(raw []json.RawMessage) ([]*core.OutboundHandlerConfig, error) {
		var out []*core.OutboundHandlerConfig
		for _, r := range raw {
			var fo fakeOutbound
			if err := json.Unmarshal(r, &fo); err != nil {
				return nil, err
			}
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
		a, err := parseAnswer([]byte(`{"outbounds": [{"tag": "x1", "v": 1}], "costs": {"x1": ` + cost + `}}`))
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
