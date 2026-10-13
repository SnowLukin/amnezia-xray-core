package task

import (
	"context"
	"errors"
	"github.com/xtls/xray-core/common/safe"
	"testing"
	"time"
)

func TestRunPanicReturnsInsteadOfWaiting(t *testing.T) {
	safe.Reset()
	defer safe.Reset()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := Run(ctx, func() error { panic("connection") }, func() error { return nil }); !errors.Is(err, safe.ErrPanic) {
		t.Fatal(err)
	}
}
func TestParallelPanicReturnsError(t *testing.T) {
	safe.Reset()
	defer safe.Reset()
	if err := ParallelForN(8, func(i int) error {
		if i == 2 {
			panic("worker")
		}
		return nil
	}); !errors.Is(err, safe.ErrPanic) {
		t.Fatal(err)
	}
}
func TestPeriodicPanicStopsOwner(t *testing.T) {
	safe.Reset()
	defer safe.Reset()
	p := &Periodic{Interval: time.Millisecond, Execute: func() error { panic("subsystem") }}
	if err := p.Start(); !errors.Is(err, safe.ErrPanic) {
		t.Fatal(err)
	}
	if !p.hasClosed() {
		t.Fatal("periodic worker still live")
	}
}
