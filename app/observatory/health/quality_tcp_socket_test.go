//go:build linux || darwin

package health

import (
	"errors"
	"io"
	"net"
	"syscall"
	"testing"
	"time"
)

type qualityFailingRawConn struct {
	syscall.RawConn
	err error
}

func (c qualityFailingRawConn) Control(f func(uintptr)) error {
	if c.RawConn != nil {
		if err := c.RawConn.Control(f); err != nil {
			return err
		}
	}
	return c.err
}

type qualityInvalidRawConn struct{ syscall.RawConn }

func (qualityInvalidRawConn) Control(f func(uintptr)) error {
	f(^uintptr(0))
	return nil
}

func TestKernelTCPQualityUnavailable(t *testing.T) {
	for name, rc := range map[string]syscall.RawConn{
		"nil":           nil,
		"control_error": qualityFailingRawConn{err: errors.New("closed")},
		"syscall_error": qualityInvalidRawConn{},
	} {
		t.Run(name, func(t *testing.T) {
			if got := kernelTCPQuality(rc); got != (tcpQualitySnapshot{}) {
				t.Fatalf("unavailable socket yielded quality: %+v", got)
			}
		})
	}
}

func TestKernelTCPQualityLiveSocket(t *testing.T) {
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := listener.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	client, err := net.DialTCP("tcp4", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	for _, c := range []*net.TCPConn{client, server} {
		if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	var request [1]byte
	if _, err := io.ReadFull(server, request[:]); err != nil {
		t.Fatal(err)
	}
	var reply [128]byte
	if _, err := server.Write(reply[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(client, reply[:]); err != nil {
		t.Fatal(err)
	}
	rc, err := client.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	got := kernelTCPQuality(rc)
	if got.present&qualityHasRTT == 0 {
		t.Fatalf("TCP socket has no RTT field: %+v", got)
	}
	// Older Linux kernels legitimately lack one or both counter pairs.
	if got.present&qualityHasRX != 0 && got.rxTotal == 0 {
		t.Fatalf("received data without a receive counter: %+v", got)
	}
	if got.present&qualityHasTX != 0 && got.txTotal == 0 {
		t.Fatalf("sent data without a transmit counter: %+v", got)
	}
	// Even a successful callback must be discarded when RawConn reports failure.
	failing := qualityFailingRawConn{RawConn: rc, err: errors.New("control failed")}
	if got := kernelTCPQuality(failing); got != (tcpQualitySnapshot{}) {
		t.Fatalf("Control failure retained the callback's data: %+v", got)
	}
}
