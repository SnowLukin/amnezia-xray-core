package health

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/safe"
)

// panics runs fn the way the inbound worker runs a connection: a panic is
// recovered and the caller goes on.
func panics(fn func()) (panicked bool) {
	defer func() { panicked = recover() != nil }()
	fn()
	return false
}

// within fails the test when fn does not return in time. The tests that use
// it do not defer Close: with the lock leaked, Close would hang the test.
func within(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s blocked after a panic on a connection goroutine", what)
	}
}

// frozenBy tells that fn froze the observer on a panic of the engine and
// returned to its caller, which goes on.
func frozenBy(t *testing.T, o *Observer, what string, fn func()) {
	t.Helper()
	if panics(fn) {
		t.Fatalf("a panic in %s reached the connection", what)
	}
	if !o.failed.Load() || !safe.Broken("app/observatory/health") {
		t.Fatalf("a panic in %s left the observer running", what)
	}
}

func TestSelectPanicReleasesObserverLock(t *testing.T) {
	safe.Reset()
	defer safe.Reset()
	o, s, _ := v2Observer(&FeatureConfig{}, nil)
	o.e.providers = append(o.e.providers, nil)
	h := &selectorHandle{o: o, s: s}
	frozenBy(t, o, "updateCandidates", func() { h.Select([]string{"b"}) })
	within(t, "observer lock", func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.e.providers = o.e.providers[:len(o.e.providers)-1]
	})
	within(t, "Select", func() { h.Select([]string{"a", "b"}) })
	within(t, "StartDial", func() {
		if tr := o.StartDial(context.Background(), "a", net.Destination{}, nil); tr != nil {
			tr.Dialed(nil, context.Canceled, nil, nil)
		}
	})
	within(t, "Close", func() { o.Close() })
}

func TestConnectionSuccessPanicReleasesObserverLock(t *testing.T) {
	safe.Reset()
	defer safe.Reset()
	o, _, _ := v2Observer(&FeatureConfig{}, nil)
	p := o.e.path("a")
	o.e.selectors["broken"] = nil
	frozenBy(t, o, "onSuccess", func() {
		o.connectionSuccess("a", o.nowNano(), p.controllerEpoch, nil, o.e.udpEpoch, p.udpIncarnation, false, true)
	})
	within(t, "observer lock", func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		delete(o.e.selectors, "broken")
	})
	within(t, "StartDial", func() {
		if tr := o.StartDial(context.Background(), "a", net.Destination{}, nil); tr != nil {
			tr.Dialed(nil, context.Canceled, nil, nil)
		}
	})
	within(t, "Close", func() { o.Close() })
}

// A frozen observer keeps the choice it had, and connections no longer
// reach the engine, whose state the panic may have left half changed.
func TestFrozenObserverLeavesEngineAlone(t *testing.T) {
	safe.Reset()
	defer safe.Reset()
	o, s, now := v2Observer(&FeatureConfig{Report: v2Bool(true)}, nil)
	defer o.Close()
	ok := o.StartDial(context.Background(), "a", net.Destination{}, nil).(*connTracker)
	refused := o.StartDial(context.Background(), "b", net.Destination{}, nil).(*connTracker)
	o.guard("test.go:Observer.loop", func() { panic("engine") })
	if !o.failed.Load() {
		t.Fatal("observer not frozen")
	}
	*now = now.Add(time.Minute)
	a, b := *o.e.path("a"), *o.e.path("b")
	if tr := o.StartDial(context.Background(), "a", net.Destination{}, nil); tr != nil {
		t.Fatal("frozen observer tracks a new connection")
	}
	read, _ := ok.Dialed(nil, nil, nil, nil)
	read.Add(o.s.passive.successBytes)
	ok.Finished(nil)
	refused.Dialed(nil, errors.New("connection refused"), nil, nil)
	if got := o.e.path("a"); got.lastSuccess != a.lastSuccess || got.stats.connsOK != a.stats.connsOK {
		t.Fatal("frozen observer counted a success")
	}
	if got := o.e.path("b"); got.stats.failuresByKind != b.stats.failuresByKind || got.status != b.status {
		t.Fatal("frozen observer counted a failure")
	}
	if got := (&selectorHandle{o: o, s: s}).Select([]string{"b"}); got != "a" {
		t.Fatalf("frozen choice %q, want a", got)
	}
}
