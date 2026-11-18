package socks

import (
	"context"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/signal"
	"github.com/xtls/xray-core/features/stats"
)

func NewTempUDPConn(udpConn net.PacketConn, tcpConn net.Conn, expectedRemote *net.UDPAddr) *TempUDPConn {
	t := &TempUDPConn{
		PacketConn:        udpConn,
		AssociatedTCPConn: tcpConn,
		rawConn:           buf.ReadyRawConn(udpConn),
	}
	t.Timer = signal.CancelAfterInactivity(context.Background(), func() {
		t.Close()
	}, net.ConnIdleTimeout)
	t.ExpectedRemote.Store(expectedRemote)
	return t
}

// TempUDPConn wait for the first packet to determine the remote address
// SetTimeout MUST be called before any read/write operation
type TempUDPConn struct {
	net.PacketConn
	AssociatedTCPConn net.Conn
	ExpectedRemote    atomic.Pointer[net.UDPAddr]
	Timer             *signal.ActivityTimer
	// ReadCounter counts what ReadMultiBuffer reads, if it is set
	ReadCounter stats.Counter
	// rawConn is the socket of PacketConn when a packet can be read into a buffer that is taken only once it came
	rawConn syscall.RawConn
}

// accept tells whether a packet from remote is of the client, and takes the port of the client from its first packet
func (c *TempUDPConn) accept(remote *net.UDPAddr) bool {
	expected := c.ExpectedRemote.Load()
	if remote.IP.Equal(expected.IP) {
		if remote.Port == expected.Port {
			c.Timer.Update()
			return true
		}
		if expected.Port == 0 {
			c.ExpectedRemote.Store(remote)
			c.Timer.Update()
			return true
		}
	}
	return false
}

func (c *TempUDPConn) Read(b []byte) (n int, err error) {
	var remote net.Addr
	for {
		n, remote, err = c.PacketConn.ReadFrom(b)
		if err != nil {
			return
		}
		if c.accept(remote.(*net.UDPAddr)) {
			return
		}
	}
}

// ReadMultiBuffer implements buf.Reader. The association waits for its client most of the time, so where the system
// lets it a packet is read into a buffer that is taken only once the packet came, see buf.ReadPacketReady
func (c *TempUDPConn) ReadMultiBuffer() (buf.MultiBuffer, error) {
	if c.rawConn == nil {
		mb, err := (&buf.PacketReader{Reader: c}).ReadMultiBuffer()
		if c.ReadCounter != nil && mb != nil {
			c.ReadCounter.Add(int64(mb.Len()))
		}
		return mb, err
	}
	for {
		b, remote, err := buf.ReadPacketReady(c.rawConn)
		if err != nil {
			return nil, err
		}
		if b.IsEmpty() || !c.accept(remote) {
			b.Release()
			continue
		}
		if c.ReadCounter != nil {
			c.ReadCounter.Add(int64(b.Len()))
		}
		return buf.MultiBuffer{b}, nil
	}
}

func (c *TempUDPConn) Write(b []byte) (n int, err error) {
	c.Timer.Update()
	return c.PacketConn.WriteTo(b, c.ExpectedRemote.Load())
}

func (c *TempUDPConn) RemoteAddr() net.Addr {
	return c.ExpectedRemote.Load()
}

func (c *TempUDPConn) SetTimeout(d time.Duration) {
	c.Timer.SetTimeout(d)
}

func (c *TempUDPConn) Close() error {
	c.Timer.SetTimeout(0)
	c.AssociatedTCPConn.Close()
	return c.PacketConn.Close()
}
