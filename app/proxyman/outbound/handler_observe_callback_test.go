package outbound

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/stats"
)

type lifecycleCallbackTracker struct {
	finished atomic.Int32
	err      error
	onFinish func()
}

func (*lifecycleCallbackTracker) Dialed(net.Conn, error, stats.Counter, stats.Counter) (stats.Counter, stats.Counter) {
	return nil, nil
}

func (t *lifecycleCallbackTracker) Finished(err error) {
	t.err = err
	t.finished.Add(1)
	if t.onFinish != nil {
		t.onFinish()
	}
}

// A physical dial can return after the proxy ended. Its terminal callback
// must preserve the process error and run outside the dialer's mutex, like
// the callbacks of already registered dials do.
func TestObservedDialerLateTrackerCallbackRunsOutsideLock(t *testing.T) {
	d := &observedDialer{}
	want := errors.New("proxy processing ended")
	d.finish(want)
	tracker := &lifecycleCallbackTracker{onFinish: func() { d.connection(context.Background()) }}
	done := make(chan struct{})
	go func() {
		d.keep(tracker)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("late completion called the tracker while holding the dialer lock")
	}
	if tracker.finished.Load() != 1 || tracker.err != want {
		t.Fatalf("late tracker terminal callback: count=%d error=%v", tracker.finished.Load(), tracker.err)
	}
}
