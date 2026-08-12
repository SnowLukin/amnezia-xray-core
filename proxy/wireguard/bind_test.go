package wireguard

import (
	"bytes"
	"net"
	"testing"

	"github.com/amnezia-vpn/amneziawg-go/v3/conn"
)

// receiveOnce pushes one packet through the bind receive path and returns what
// the device would see.
func receiveOnce(t *testing.T, reserved []byte, packet []byte) []byte {
	t.Helper()

	local, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen local: %v", err)
	}
	defer local.Close()

	remote, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen remote: %v", err)
	}
	defer remote.Close()

	b := &bind{
		reserved:   reserved,
		listenFunc: func() (net.PacketConn, error) { return local, nil },
	}
	fns, _, err := b.Open(0)
	if err != nil {
		t.Fatalf("open bind: %v", err)
	}
	if len(fns) != 1 {
		t.Fatalf("got %d receive funcs, want 1", len(fns))
	}

	if _, err := remote.WriteTo(packet, local.LocalAddr()); err != nil {
		t.Fatalf("send packet: %v", err)
	}

	bufs := [][]byte{make([]byte, 1500)}
	sizes := make([]int, 1)
	eps := make([]conn.Endpoint, 1)
	if _, err := fns[0](bufs, sizes, eps); err != nil {
		t.Fatalf("receive: %v", err)
	}

	return bufs[0][:sizes[0]]
}

// With zero S padding the AmneziaWG magic header sits in the first four bytes,
// exactly where the WARP reserved field lives, so it must survive untouched.
func TestBindReceiveKeepsMagicHeader(t *testing.T) {
	packet := []byte{0x11, 0x22, 0x33, 0x44, 0xAA, 0xBB}

	got := receiveOnce(t, nil, append([]byte(nil), packet...))

	if !bytes.Equal(got, packet) {
		t.Errorf("packet altered: got % x, want % x", got, packet)
	}
}

func TestBindReceiveStripsReservedWhenSet(t *testing.T) {
	packet := []byte{0x11, 0x22, 0x33, 0x44, 0xAA, 0xBB}
	want := []byte{0x11, 0x00, 0x00, 0x00, 0xAA, 0xBB}

	got := receiveOnce(t, []byte{1, 2, 3}, append([]byte(nil), packet...))

	if !bytes.Equal(got, want) {
		t.Errorf("reserved not stripped: got % x, want % x", got, want)
	}
}
