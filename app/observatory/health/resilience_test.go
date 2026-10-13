package health

import (
	"context"
	"encoding/json"
	"github.com/xtls/xray-core/common/safe"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"strings"
	"testing"
	"time"
)

func TestObserverPanicFreezesChoiceAndExports(t *testing.T) {
	safe.Reset()
	defer safe.Reset()
	o, s, _ := v2Observer(&FeatureConfig{Report: v2Bool(true), Events: v2Bool(true)}, nil)
	defer o.Close()
	chosen := s.current()
	if chosen == "" {
		t.Fatal("no initial selection")
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		o.guard("test.go:Observer.loop", func() { func() { o.mu.Lock(); defer o.mu.Unlock(); panic("private domain") }() })
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("recovery blocked")
	}
	h := selectorHandle{o: o, s: s}
	if got := h.Select([]string{"b"}); got != chosen {
		t.Fatalf("choice %q -> %q", chosen, got)
	}
	status := string(o.Status())
	if !strings.Contains(status, "app/observatory/health") {
		t.Fatal(status)
	}
	report := o.TakeReport(true)
	var v map[string]any
	if err := json.Unmarshal(report, &v); err != nil {
		t.Fatal(err, string(report))
	}
	if v["panics_recovered"] != float64(1) {
		t.Fatal(v)
	}
	events, _ := o.DrainEvents()
	if !strings.Contains(string(events), "panic_recovered") {
		t.Fatal("missing event")
	}
	if o.mu.TryLock() {
		o.mu.Unlock()
	} else {
		t.Fatal("lock leaked")
	}
}

func TestFailedObserverKeepsFallbackTargetWithoutNewDecision(t *testing.T) {
	safe.Reset()
	defer safe.Reset()
	o, s, _ := v2Observer(&FeatureConfig{Report: v2Bool(true), Events: v2Bool(true)}, nil)
	defer o.Close()
	s.cfg.fallbackBalancer = "main"
	s.setActive("")
	s.exhausted = true
	o.guard("test.go:Observer.loop", func() { panic("observer") })
	h := selectorHandle{o: o, s: s}
	if h.FallbackBalancer() != "main" {
		t.Fatal("existing fallback target lost")
	}
	h.FallbackSelected("main")
	if o.e.report.counts.FallbackBalancerCount != 0 {
		t.Fatal("failed observer started a new fallback decision")
	}
}

type panicSetManager struct {
	*fakeManager
	bad outbound.Handler
}

func (m *panicSetManager) AddHandler(ctx context.Context, h outbound.Handler) error {
	if err := m.fakeManager.AddHandler(ctx, h); err != nil {
		return err
	}
	if h == m.bad {
		panic("handler start")
	}
	return nil
}
func TestProviderPanicRollsBackLastSet(t *testing.T) {
	safe.Reset()
	defer safe.Reset()
	o := newObserver(context.Background(), &Config{})
	defer o.Close()
	old := &fakeHandler{tag: "p-a"}
	next := &fakeHandler{tag: "p-a"}
	m := &panicSetManager{fakeManager: newFakeManager(), bad: next}
	m.handlers[old.tag] = old
	o.ohm = m
	o.guard("provider_set.go:Observer.runRefresh", func() { o.registerSet(nil, []*outboundChange{{tag: old.tag, old: old, handler: next}}) })
	if m.GetHandler(old.tag) != old {
		t.Fatal("last handler not restored")
	}
	if next.closes() != 1 {
		t.Fatal("new handler not closed", next.closes())
	}
	if !o.mu.TryLock() {
		t.Fatal("mutex leaked")
	}
	o.mu.Unlock()
}
func TestProviderBuildPanicClosesPreparedHandlers(t *testing.T) {
	safe.Reset()
	defer safe.Reset()
	f := newApplyFixture(t, time.Hour)
	first := &fakeHandler{tag: "p-a"}
	calls := 0
	f.o.newHandler = func(cfg *core.OutboundHandlerConfig) (outbound.Handler, error) {
		calls++
		if calls == 2 {
			panic("build")
		}
		return first, nil
	}
	f.o.guard("provider_set.go:Observer.runRefresh", func() { f.o.buildSet(f.rt, answer(t, fakeOutbound{Tag: "a", V: 1}, fakeOutbound{Tag: "b", V: 1})) })
	if first.closes() != 1 {
		t.Fatalf("prepared handler retained: %d", first.closes())
	}
}
func TestProviderApplyPanicRestoresModeAndChoice(t *testing.T) {
	safe.Reset()
	defer safe.Reset()
	f := newApplyFixture(t, time.Hour)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); err != nil {
		t.Fatal(err)
	}
	old := f.m.GetHandler("p-x1")
	next := &fakeHandler{tag: "p-x1"}
	f.o.newHandler = func(*core.OutboundHandlerConfig) (outbound.Handler, error) { return next, nil }
	f.o.ohm = &panicSetManager{fakeManager: f.m, bad: next}
	sel := &selectorHandle{o: f.o, s: f.o.e.selectors["auto"]}
	chosen := sel.Select([]string{"p-x1"})
	savedActiveSince := sel.s.activeSince
	sel.s.pinned = "p-x1"
	sel.s.startPin = true
	sel.s.pinnedUntil = f.o.now().Add(time.Hour)
	savedPinnedUntil := sel.s.pinnedUntil

	f.o.guard("provider_set.go:Observer.runRefresh", func() {
		f.o.applySet(f.rt, answer(t, fakeOutbound{Tag: "x1", V: 2, Path: json.RawMessage(`{"balancer":"auto","cost":1,"isProbeOnly":true}`)}), &refreshJob{p: f.rt.p, trigger: triggerHost})
	})
	if f.m.GetHandler("p-x1") != old || f.o.s.isProbeOnly("p-x1") {
		t.Fatal("last handler/mode lost")
	}
	if got := sel.Select([]string{"p-x1"}); got != chosen {
		t.Fatalf("choice %s -> %s", chosen, got)
	}
	if sel.s.exhausted || !sel.s.activeSince.Equal(savedActiveSince) || sel.s.pinned != "p-x1" || !sel.s.startPin || !sel.s.pinnedUntil.Equal(savedPinnedUntil) {
		t.Fatal("selector metadata not restored")
	}

	if len(f.o.e.replacing) != 0 {
		t.Fatal("replacement marker leaked")
	}
}
