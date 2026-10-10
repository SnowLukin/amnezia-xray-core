/* SPDX-License-Identifier: MIT */

package device_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/amnezia-vpn/amneziawg-go/v3/device"
)

func TestTimerExpirationConcurrentIsPending(t *testing.T) {
	expired := make(chan time.Duration, 1)
	peer := &device.Peer{}
	timer := peer.NewTimer(func(_ *device.Peer, duration time.Duration) {
		expired <- duration
	})
	defer timer.DelSync()

	stop := make(chan struct{})
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
				timer.IsPending()
				runtime.Gosched()
			}
		}
	}()
	defer func() {
		close(stop)
		<-done
	}()
	<-started

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for i := 0; i < 1000; i++ {
		timer.Mod(time.Nanosecond)
		select {
		case duration := <-expired:
			if duration != time.Nanosecond {
				t.Fatalf("expiration duration = %v, want %v", duration, time.Nanosecond)
			}
		case <-deadline.C:
			t.Fatal("timer did not expire")
		}
		if timer.IsPending() {
			t.Fatal("expired timer is still pending")
		}
	}
}

func TestTimerModDuringExpiration(t *testing.T) {
	expired := make(chan time.Duration, 2)
	release := make(chan struct{})
	peer := &device.Peer{}
	timer := peer.NewTimer(func(_ *device.Peer, duration time.Duration) {
		expired <- duration
		<-release
	})
	defer timer.DelSync()
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	timer.Mod(time.Nanosecond)
	select {
	case <-expired:
	case <-deadline.C:
		t.Fatal("first timer did not expire")
	}

	timer.Mod(time.Millisecond)
	if !timer.IsPending() {
		t.Fatal("rearmed timer is not pending")
	}
	close(release)
	released = true
	select {
	case duration := <-expired:
		if duration != time.Millisecond {
			t.Fatalf("rearmed duration = %v, want %v", duration, time.Millisecond)
		}
	case <-deadline.C:
		t.Fatal("rearmed timer did not expire")
	}
}
