package health

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
)

func TestObserverNewReplyMakesOlderDialFailuresStale(t *testing.T) {
	for _, kind := range []failureKind{failDial, failDialTimeout, failUnverified} {
		for _, bytes := range []int64{1, defaultSuccessBytes} {
			t.Run(kind.String()+"/"+strconv.FormatInt(bytes, 10)+"bytes", func(t *testing.T) {
				o, clk := newTestObserver()
				var old []*connTracker
				for range o.s.passive.failures {
					old = append(old, o.StartDial(context.Background(), "a1", net.Destination{}, nil).(*connTracker))
				}
				clk.advance(time.Second)
				newer := o.StartDial(context.Background(), "a1", net.Destination{}, nil)
				r, _ := newer.Dialed(nil, nil, nil, nil)
				r.Add(bytes)
				clk.advance(o.s.passive.dialTimeout + time.Second)
				for _, tr := range old {
					switch kind {
					case failDial:
						tr.Dialed(nil, errors.New("connection refused"), nil, nil)
					case failUnverified:
						tr.Dialed(nil, errors.New("REALITY: processed invalid connection"), nil, nil)
					case failDialTimeout:
						tr.sweep(o.nowNano())
					}
				}
				if got := failures(o, kind); got != 0 {
					t.Fatalf("%d older %s failures survived a newer reply", got, kind)
				}
				if bytes < o.s.passive.successBytes && o.e.path("a1").status != statusUnknown {
					t.Fatal("a small reply marked the path up without reaching successBytes")
				}
			})
		}
	}
}

func TestObserverOldReplyDoesNotHideNewDialFailure(t *testing.T) {
	o, clk := newTestObserver()
	old := o.StartDial(context.Background(), "a1", net.Destination{}, nil)
	r, _ := old.Dialed(nil, nil, nil, nil)
	clk.advance(time.Second)
	fresh := o.StartDial(context.Background(), "a1", net.Destination{}, nil)
	clk.advance(time.Second)
	r.Add(o.s.passive.successBytes)
	fresh.Dialed(nil, errors.New("new TCP connections are blocked"), nil, nil)
	if got := failures(o, failDial); got != 1 {
		t.Fatalf("old working connection hid the new dial failure: got %d, want 1", got)
	}
}

func TestObserverUnansweredNewDialDoesNotHideOlderFailure(t *testing.T) {
	o, clk := newTestObserver()
	old := o.StartDial(context.Background(), "a1", net.Destination{}, nil)
	clk.advance(time.Second)
	newer := o.StartDial(context.Background(), "a1", net.Destination{}, nil)
	newer.Dialed(nil, nil, nil, nil)
	old.Dialed(nil, errors.New("connection refused"), nil, nil)
	if got := failures(o, failDial); got != 1 {
		t.Fatalf("an unanswered dial hid the older failure: got %d, want 1", got)
	}
}

func TestObserverReplyKeepsWeakFailuresOfOlderConnections(t *testing.T) {
	for _, kind := range []failureKind{failNoResponse, failStall, failPartialStall, failClosed} {
		t.Run(kind.String(), func(t *testing.T) {
			o, clk := newTestObserver()
			old := o.StartDial(context.Background(), "a1", net.Destination{}, nil).(*connTracker)
			r, w := old.Dialed(nil, nil, nil, nil)
			w.Add(o.s.passive.stallMinWrite)
			switch kind {
			case failStall:
				r.Add(1)
				w.Add(o.s.passive.stallMinWrite)
			case failPartialStall:
				r.Add(o.s.passive.partialMinBytes)
			}
			clk.advance(time.Second)
			newer := o.StartDial(context.Background(), "a1", net.Destination{}, nil)
			newRead, _ := newer.Dialed(nil, nil, nil, nil)
			newRead.Add(1)
			clk.advance(max(o.s.passive.firstByteTimeout, o.s.passive.stallTimeout) + time.Second)
			if kind == failClosed {
				old.Finished(errors.New("connection reset before response"))
			} else {
				old.sweep(o.nowNano())
			}
			if got := failures(o, kind); got != 1 {
				t.Fatalf("newer reply hid %s of an older connection: got %d, want 1", kind, got)
			}
		})
	}
}

func TestObserverLateOldReplyDoesNotReplaceNewerReply(t *testing.T) {
	o, clk := newTestObserver()
	old := o.StartDial(context.Background(), "a1", net.Destination{}, nil)
	oldRead, _ := old.Dialed(nil, nil, nil, nil)
	clk.advance(time.Second)
	middle := o.StartDial(context.Background(), "a1", net.Destination{}, nil)
	clk.advance(time.Second)
	newer := o.StartDial(context.Background(), "a1", net.Destination{}, nil)
	newRead, _ := newer.Dialed(nil, nil, nil, nil)
	newRead.Add(1)
	clk.advance(time.Second)
	oldRead.Add(1)
	middle.Dialed(nil, errors.New("connection refused"), nil, nil)
	if got := failures(o, failDial); got != 0 {
		t.Fatalf("late old reply replaced newer evidence: got %d failures, want 0", got)
	}
}

func TestObserverReplyDoesNotHideSameStartOrOtherPathFailure(t *testing.T) {
	for _, tag := range []string{"a1", "a2"} {
		t.Run(tag, func(t *testing.T) {
			o, clk := newTestObserver()
			failed := o.StartDial(context.Background(), "a1", net.Destination{}, nil)
			if tag != "a1" {
				clk.advance(time.Second)
			}
			answered := o.StartDial(context.Background(), tag, net.Destination{}, nil)
			r, _ := answered.Dialed(nil, nil, nil, nil)
			r.Add(1)
			failed.Dialed(nil, errors.New("connection refused"), nil, nil)
			if got := failures(o, failDial); got != 1 {
				t.Fatalf("same-start or different-path reply hid the failure: got %d, want 1", got)
			}
		})
	}
}
