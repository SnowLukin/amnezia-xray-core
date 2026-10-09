package wireguard

import (
	"sync"

	"github.com/amnezia-vpn/amneziawg-go/v3/tun"
)

// readyTun holds the first read of a device until the device is configured.
//
// The device reads from its TUN as soon as it exists, and it places each
// packet behind room for the S4 padding it has at the time it starts waiting.
// The packet that ends the first wait, after IpcSet set S4, would go out with
// the padding of an unconfigured device, and the peer would drop it. That
// packet is the first SYN or DNS query of a new device, and a client creates
// a new device whenever it wakes.
//
// It also closes its TUN only once. A client whose init failed closes the TUN
// under the device init left behind, and that device closes it again as the
// failed read ends it.
type readyTun struct {
	tun.Device
	ready   chan struct{}
	opening sync.Once
	closing sync.Once
	waited  bool // only the reading goroutine of the device uses it
}

func newReadyTun(t tun.Device) *readyTun {
	return &readyTun{Device: t, ready: make(chan struct{})}
}

// open lets the device read; call it once the device is configured.
func (t *readyTun) open() {
	t.opening.Do(func() { close(t.ready) })
}

func (t *readyTun) Read(bufs [][]byte, sizes []int, offset int) (int, error) {
	if !t.waited {
		<-t.ready
		t.waited = true
		// Read nothing, so that the device computes the offset again.
		return 0, nil
	}
	return t.Device.Read(bufs, sizes, offset)
}

// Close also releases a device waiting to read, which closing the device
// waits for.
func (t *readyTun) Close() (err error) {
	t.open()
	t.closing.Do(func() { err = t.Device.Close() })
	return err
}
