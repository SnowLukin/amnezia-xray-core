package health

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
)

func TestAPIRemovedProviderDrainForgetsItsState(t *testing.T) {
	f := newApplyFixture(t, time.Hour)
	for i := 0; i < 3; i++ {
		if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1}, fakeOutbound{Tag: "x2", V: int64(i + 1)})); err != nil {
			t.Fatal(err)
		}
		f.follow("p-x1", "p-x2")
		x1 := f.m.GetHandler("p-x1").(*fakeHandler)
		tr := f.o.StartDial(context.Background(), "p-x1", net.TCPDestination(net.ParseAddress("192.0.2.7"), 443), nil)
		tr.Dialed(nil, nil, nil, nil)
		if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x2", V: int64(i + 1)})); err != nil {
			t.Fatal(err)
		}
		// HandlerService removes the handler and does not close it.
		_ = f.m.RemoveHandler(context.Background(), "p-x1")
		tr.Finished(nil)
		f.o.settleAllDrains()
		if len(f.rt.draining) != 0 || x1.closes() != 1 {
			t.Fatalf("after the drain: %d draining, closed %d times", len(f.rt.draining), x1.closes())
		}
		if f.rt.built["p-x1"] != nil {
			t.Fatal("the removed handler's config remains cached")
		}
		f.o.mu.Lock()
		_, path := f.o.e.paths["p-x1"]
		retiring := f.o.e.retiring["p-x1"]
		_, metadata := f.o.s.paths.Load("p-x1")
		f.o.mu.Unlock()
		if path || retiring || metadata {
			t.Fatalf("removed handler remains in the observer: path=%v retiring=%v metadata=%v", path, retiring, metadata)
		}
	}
}

func TestAPIReplacedProviderDrainPreservesTheNewHandler(t *testing.T) {
	f := newApplyFixture(t, time.Hour)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1}, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	f.follow("p-x1", "p-x2")
	x1 := f.m.GetHandler("p-x1").(*fakeHandler)
	tr := f.o.StartDial(context.Background(), "p-x1", net.TCPDestination(net.ParseAddress("192.0.2.7"), 443), nil)
	tr.Dialed(nil, nil, nil, nil)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	_ = f.m.RemoveHandler(context.Background(), "p-x1")
	replacement := (&fakeHandler{tag: "p-x1"}).withProxy(fakeSettings(2))
	if err := f.m.AddHandler(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	tr.Finished(nil)
	f.o.settleAllDrains()
	if f.m.GetHandler("p-x1") != replacement || replacement.closes() != 0 || x1.closes() != 1 {
		t.Fatal("finishing an old drain removed or closed the API replacement")
	}
	if f.rt.built["p-x1"] != nil {
		t.Fatal("an old config remains cached for the API replacement")
	}
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1}, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	if f.m.GetHandler("p-x1") == replacement {
		t.Fatal("a later provider answer reused the API replacement based on a stale cached config")
	}
	f.o.mu.Lock()
	retiring := f.o.e.retiring["p-x1"]
	f.o.mu.Unlock()
	if retiring {
		t.Fatal("a later provider answer did not revive the API-replaced path")
	}
}

// HandlerService takes a draining outbound out of the outbound manager, which
// then does not close it on shutdown: the observer does.
func TestShutdownClosesADrainTheAPIRemoved(t *testing.T) {
	f := newApplyFixture(t, time.Hour)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1}, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	f.follow("p-x1", "p-x2")
	x1 := f.m.GetHandler("p-x1").(*fakeHandler)
	x2 := f.m.GetHandler("p-x2").(*fakeHandler)
	f.open("p-x1")
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	_ = f.m.RemoveHandler(context.Background(), "p-x1")
	f.o.closeProviders()
	if x1.closes() != 1 || x2.closes() != 0 {
		t.Fatalf("on shutdown: removed drain closed %d times, registered outbound %d", x1.closes(), x2.closes())
	}
}
