//go:build linux || darwin

package health

import (
	"context"
	gonet "net"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
)

type pausedSpliceConn struct {
	gonet.Conn
	entered chan struct{}
	release chan struct{}
}

func (c *pausedSpliceConn) NetConn() gonet.Conn {
	close(c.entered)
	<-c.release
	return c.Conn
}

// A copy task can enter notifySpliced after its sibling failed and Process
// began finishing. Pausing the unwrap makes this order deterministic: a late
// splice setup must not republish its socket after the tracker was cleaned up.
func TestTrackerSpliceSetupOvertakenByFinishReleasesSocket(t *testing.T) {
	client, peer, raw := qualitySpliceFirstSocket(t)
	defer client.Close()
	defer peer.Close()
	if rx, ok := kernelReceived(raw); !ok || rx == 0 {
		t.Skip("socket has no positive receive baseline")
	}
	o := newObserver(context.Background(), &Config{Paths: observed("a1")})
	defer o.Close()
	c := &pausedSpliceConn{Conn: client, entered: make(chan struct{}), release: make(chan struct{})}
	tr := o.StartDial(context.Background(), "a1", net.Destination{}, nil).(*connTracker)
	read, _ := tr.Dialed(c, nil, nil, nil)
	done := make(chan struct{})
	go func() {
		read.(*trackerReadCounter).Spliced()
		close(done)
	}()
	<-c.entered
	tr.Finished(nil)
	close(c.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("finishing the tracker blocked its splice setup")
	}
	if tr.dialedConn != nil || tr.socket.Load() != nil {
		t.Fatal("late splice setup retained the socket after tracker finish")
	}
	if len(o.trackers) != 0 || len(o.watching) != 0 {
		t.Fatal("late splice setup restarted tracking after finish")
	}
}
