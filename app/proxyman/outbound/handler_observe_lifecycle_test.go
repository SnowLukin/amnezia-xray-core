package outbound

import (
	"context"
	"errors"
	"fmt"
	gonet "net"
	"sync/atomic"
	"testing"

	"github.com/xtls/xray-core/common/net"
	obs "github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/stats"
	_ "github.com/xtls/xray-core/transport/internet/tcp"
)

type delayedObserver struct {
	started chan struct{}
	resume  chan struct{}
	tracker lifecycleTracker
}

func (*delayedObserver) Observes(string) bool { return true }
func (o *delayedObserver) StartDial(context.Context, string, net.Destination, func()) obs.DialTracker {
	close(o.started)
	<-o.resume
	return &o.tracker
}

type lifecycleTracker struct{ finished atomic.Int32 }

func (*lifecycleTracker) Dialed(gonet.Conn, error, stats.Counter, stats.Counter) (stats.Counter, stats.Counter) {
	return nil, nil
}
func (t *lifecycleTracker) Finished(error) { t.finished.Add(1) }

func TestObserverDialCompletingAfterProcessIsFinished(t *testing.T) {
	for _, tunnel := range []bool{false, true} {
		name := "tcp"
		if tunnel {
			name = "tunnel"
		}
		t.Run(name, func(t *testing.T) {
			o := &delayedObserver{started: make(chan struct{}), resume: make(chan struct{})}
			d := &observedDialer{Handler: &Handler{tag: "path", observer: o}}
			ln, err := gonet.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			dest := net.TCPDestination(net.LocalHostIP, net.Port(ln.Addr().(*gonet.TCPAddr).Port))
			completed := make(chan error, 1)
			go func() {
				if tunnel {
					tracker := d.StartTunnelDial(context.Background(), dest)
					if tracker != nil {
						completed <- fmt.Errorf("completed process returned a tunnel tracker")
					} else {
						completed <- nil
					}
					return
				}
				conn, err := d.Dial(context.Background(), dest)
				if conn != nil {
					conn.Close()
				}
				completed <- err
			}()
			<-o.started
			d.finish(errors.New("processing ended while a background dial was in progress"))
			close(o.resume)
			if err := <-completed; err != nil {
				t.Fatal(err)
			}
			if got := o.tracker.finished.Load(); got != 1 {
				t.Fatalf("tracker finished %d times, want once", got)
			}
			if len(d.trackers) != 0 {
				t.Fatal("completed process retained a late tracker")
			}
		})
	}
}
