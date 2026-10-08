package outbound

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
)

type plainProxy struct{}

func (plainProxy) Process(context.Context, *transport.Link, internet.Dialer) error { return nil }

type sleepyProxy struct {
	plainProxy
	asleep bool
	calls  int
}

func (p *sleepyProxy) Sleep() bool {
	p.calls++
	return p.asleep
}

func TestHandlerSleepAsksProxy(t *testing.T) {
	for _, asleep := range []bool{true, false} {
		p := &sleepyProxy{asleep: asleep}
		h := &Handler{proxy: p}
		if got := h.Sleep(); got != asleep || p.calls != 1 {
			t.Errorf("Sleep() = %v after %d calls, want %v after 1", got, p.calls, asleep)
		}
	}
	if (&Handler{proxy: plainProxy{}}).Sleep() {
		t.Error("a handler whose proxy cannot sleep reported sleeping")
	}
}
