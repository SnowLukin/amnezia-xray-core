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
// a new device whenever it wakes. On an inbound, it is the first answer to the
// first client.
//
// It also closes its TUN only once. A client whose init failed, or an inbound
// whose Start failed, closes the TUN under the device left behind, and that
// device closes it again as the failed read ends it.
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

// open lets the device read; a client calls it once the device is configured.
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

// Write opens the TUN for an inbound, which therefore need not call open:
// the stack of an inbound sends nothing but answers, and the first packet a
// peer sends through the device comes after IpcSet configured the peers.
func (t *readyTun) Write(bufs [][]byte, offset int) (int, error) {
	t.open()
	return t.Device.Write(bufs, offset)
}

// Close also releases a device waiting to read, which closing the device
// waits for.
func (t *readyTun) Close() (err error) {
	t.open()
	t.closing.Do(func() { err = t.Device.Close() })
	return err
}
