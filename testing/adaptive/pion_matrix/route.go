package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/pion/transport/v4"
	"github.com/pion/transport/v4/stdnet"
)

var virtualIP = net.IPv4(198, 18, 0, 1)

type routeEvent struct {
	AtUnixNano      int64  `json:"at_unix_ns"`
	Direction       string `json:"direction"`
	SourcePort      int    `json:"source_port"`
	Destination     string `json:"destination"`
	Bytes           int    `json:"bytes"`
	PayloadSHA256   string `json:"payload_sha256"`
	ApplicationData bool   `json:"dtls_application_data"`
}

type routeLedger struct {
	mu          sync.Mutex
	packets     int
	sourcePorts []int
	events      []routeEvent
}

func (l *routeLedger) add(direction string, sourcePort int, destination string, payload []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if direction == "send" {
		l.packets++
	}
	digest := sha256.Sum256(payload)
	l.events = append(l.events, routeEvent{AtUnixNano: time.Now().UnixNano(), Direction: direction,
		SourcePort: sourcePort, Destination: destination, Bytes: len(payload),
		PayloadSHA256: hex.EncodeToString(digest[:]), ApplicationData: carriesApplicationData(payload)})
}

func (l *routeLedger) source(port int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sourcePorts = append(l.sourcePorts, port)
}

func (l *routeLedger) result() map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	return map[string]any{"routed_packets": l.packets, "source_ports": l.sourcePorts, "events": l.events}
}

func carriesApplicationData(payload []byte) bool {
	if len(payload) >= 13 && payload[0] == 23 && payload[1] == 0xfe &&
		(payload[2] == 0xfd || payload[2] == 0xff) {
		return true
	}
	if len(payload) >= 4 && payload[0]&0xc0 == 0x40 {
		size := int(binary.BigEndian.Uint16(payload[2:4]))
		return size <= len(payload)-4 && carriesApplicationData(payload[4:4+size])
	}
	if len(payload) >= 20 && binary.BigEndian.Uint32(payload[4:8]) == 0x2112a442 {
		end := 20 + int(binary.BigEndian.Uint16(payload[2:4]))
		if end > len(payload) {
			return false
		}
		for offset := 20; offset+4 <= end; {
			kind := binary.BigEndian.Uint16(payload[offset : offset+2])
			size := int(binary.BigEndian.Uint16(payload[offset+2 : offset+4]))
			if offset+4+size > end {
				return false
			}
			if kind == 0x0013 && carriesApplicationData(payload[offset+4:offset+4+size]) {
				return true
			}
			offset += 4 + (size+3)/4*4
		}
	}
	return false
}

type routedNet struct {
	*stdnet.Net
	socks    string
	hostPort int
	mode     string
	ledger   *routeLedger
}

func newRoutedNet(socks string, hostPort int, mode string, ledger *routeLedger) (*routedNet, error) {
	std, err := stdnet.NewNet()
	if err != nil {
		return nil, err
	}
	return &routedNet{Net: std, socks: socks, hostPort: hostPort, mode: mode, ledger: ledger}, nil
}

func (n *routedNet) fakeInterface() *transport.Interface {
	iface := transport.NewInterface(net.Interface{Name: "xray-socks", Index: 420, Flags: net.FlagUp})
	iface.AddAddress(&net.IPNet{IP: virtualIP, Mask: net.CIDRMask(32, 32)})
	return iface
}

func (n *routedNet) Interfaces() ([]*transport.Interface, error) {
	return []*transport.Interface{n.fakeInterface()}, nil
}
func (n *routedNet) InterfaceByIndex(index int) (*transport.Interface, error) {
	if index != 420 {
		return nil, transport.ErrInterfaceNotFound
	}
	return n.fakeInterface(), nil
}
func (n *routedNet) InterfaceByName(name string) (*transport.Interface, error) {
	if name != "xray-socks" {
		return nil, transport.ErrInterfaceNotFound
	}
	return n.fakeInterface(), nil
}
func (n *routedNet) ListenPacket(network, _ string) (net.PacketConn, error) {
	if network != "udp" && network != "udp4" {
		return nil, fmt.Errorf("routed network only supports udp4: %s", network)
	}
	return newSocksPacketConn(n.socks, n.hostPort, nil, n.mode, n.ledger)
}
func (n *routedNet) ListenUDP(network string, _ *net.UDPAddr) (transport.UDPConn, error) {
	if network != "udp" && network != "udp4" {
		return nil, fmt.Errorf("routed network only supports udp4: %s", network)
	}
	return newSocksPacketConn(n.socks, n.hostPort, nil, n.mode, n.ledger)
}
func (n *routedNet) DialUDP(network string, _, remote *net.UDPAddr) (transport.UDPConn, error) {
	if network != "udp" && network != "udp4" {
		return nil, fmt.Errorf("routed network only supports udp4: %s", network)
	}
	return newSocksPacketConn(n.socks, n.hostPort, remote, n.mode, n.ledger)
}
func (n *routedNet) Dial(network, address string) (net.Conn, error) {
	return nil, fmt.Errorf("direct dial forbidden in Pion matrix: %s %s", network, address)
}
func (n *routedNet) DialTCP(network string, _, remote *net.TCPAddr) (transport.TCPConn, error) {
	return nil, fmt.Errorf("direct TCP forbidden in Pion matrix: %s %s", network, remote)
}
func (n *routedNet) ListenTCP(network string, _ *net.TCPAddr) (transport.TCPListener, error) {
	return nil, fmt.Errorf("direct TCP listen forbidden in Pion matrix: %s", network)
}
func (n *routedNet) CreateDialer(*net.Dialer) transport.Dialer { return deniedDialer{} }
func (n *routedNet) CreateListenConfig(*net.ListenConfig) transport.ListenConfig {
	return routedListenConfig{n}
}

type deniedDialer struct{}

func (deniedDialer) Dial(network, address string) (net.Conn, error) {
	return nil, fmt.Errorf("direct dial forbidden: %s %s", network, address)
}

type routedListenConfig struct{ n *routedNet }

func (r routedListenConfig) Listen(context.Context, string, string) (net.Listener, error) {
	return nil, errors.New("TCP listen forbidden in Pion matrix")
}
func (r routedListenConfig) ListenPacket(_ context.Context, network, address string) (net.PacketConn, error) {
	return r.n.ListenPacket(network, address)
}

type socksPacketConn struct {
	tcp    net.Conn
	udp    *net.UDPConn
	relay  *net.UDPAddr
	local  *net.UDPAddr
	remote *net.UDPAddr
	ledger *routeLedger
}

func readFull(conn net.Conn, length int) ([]byte, error) {
	data := make([]byte, length)
	_, err := io.ReadFull(conn, data)
	return data, err
}

func newSocksPacketConn(socks string, hostPort int, remote *net.UDPAddr, mode string, ledger *routeLedger) (*socksPacketConn, error) {
	control, err := net.DialTimeout("tcp", socks, 5*time.Second)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*socksPacketConn, error) { control.Close(); return nil, err }
	if _, err = control.Write([]byte{5, 1, 0}); err != nil {
		return fail(err)
	}
	hello, err := readFull(control, 2)
	if err != nil {
		return fail(err)
	}
	if hello[0] != 5 || hello[1] != 0 {
		return fail(errors.New("SOCKS greeting rejected"))
	}
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return fail(err)
	}
	sourcePort := udp.LocalAddr().(*net.UDPAddr).Port
	request := []byte{5, 3, 0, 1, 0, 0, 0, 0, byte(sourcePort >> 8), byte(sourcePort)}
	if _, err = control.Write(request); err != nil {
		udp.Close()
		return fail(err)
	}
	reply, err := readFull(control, 4)
	if err != nil {
		udp.Close()
		return fail(err)
	}
	if reply[0] != 5 || reply[1] != 0 {
		udp.Close()
		return fail(fmt.Errorf("SOCKS UDP associate rejected: %v", reply))
	}
	bound, err := readSOCKSAddress(control, reply[3])
	if err != nil {
		udp.Close()
		return fail(err)
	}
	if bound.IP.IsUnspecified() {
		bound.IP = net.IPv4(127, 0, 0, 1)
	}
	conn := &socksPacketConn{tcp: control, udp: udp, relay: bound,
		local: &net.UDPAddr{IP: virtualIP, Port: hostPort}, remote: remote, ledger: ledger}
	ledger.source(sourcePort)
	if mode == "host" {
		registration := []byte("MATRIX-REGISTER:" + strconv.Itoa(hostPort))
		if _, err = conn.WriteTo(registration, conn.local); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

func readSOCKSAddress(reader io.Reader, atyp byte) (*net.UDPAddr, error) {
	var raw []byte
	var err error
	switch atyp {
	case 1:
		raw, err = readBytes(reader, 4)
	case 4:
		raw, err = readBytes(reader, 16)
	case 3:
		length, e := readBytes(reader, 1)
		if e != nil {
			return nil, e
		}
		raw, err = readBytes(reader, int(length[0]))
	default:
		return nil, fmt.Errorf("unsupported SOCKS address type %d", atyp)
	}
	if err != nil {
		return nil, err
	}
	port, err := readBytes(reader, 2)
	if err != nil {
		return nil, err
	}
	if atyp == 3 {
		return net.ResolveUDPAddr("udp4", net.JoinHostPort(string(raw), strconv.Itoa(int(port[0])<<8|int(port[1]))))
	}
	return &net.UDPAddr{IP: net.IP(raw), Port: int(port[0])<<8 | int(port[1])}, nil
}

func readBytes(reader io.Reader, count int) ([]byte, error) {
	data := make([]byte, count)
	_, err := io.ReadFull(reader, data)
	return data, err
}

func (c *socksPacketConn) ReadFrom(data []byte) (int, net.Addr, error) {
	frame := make([]byte, len(data)+64)
	n, _, err := c.udp.ReadFromUDP(frame)
	if err != nil {
		return 0, nil, err
	}
	if n < 10 || frame[0] != 0 || frame[1] != 0 || frame[2] != 0 {
		return 0, nil, errors.New("invalid SOCKS UDP reply")
	}
	reader := &byteReader{data: frame[4:n]}
	remote, err := readSOCKSAddress(reader, frame[3])
	if err != nil {
		return 0, nil, err
	}
	if len(reader.data) >= 6 && string(reader.data[:4]) == "MTRX" {
		remote = &net.UDPAddr{IP: virtualIP, Port: int(reader.data[4])<<8 | int(reader.data[5])}
		reader.data = reader.data[6:]
	}
	copied := copy(data, reader.data)
	c.ledger.add("recv", c.udp.LocalAddr().(*net.UDPAddr).Port, remote.String(), data[:copied])
	return copied, remote, nil
}

type byteReader struct{ data []byte }

func (r *byteReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func (c *socksPacketConn) WriteTo(data []byte, address net.Addr) (int, error) {
	dest, ok := address.(*net.UDPAddr)
	if !ok || dest.IP.To4() == nil {
		return 0, fmt.Errorf("SOCKS UDP requires IPv4 destination: %v", address)
	}
	frame := make([]byte, 10+len(data))
	frame[3] = 1
	copy(frame[4:8], dest.IP.To4())
	frame[8], frame[9] = byte(dest.Port>>8), byte(dest.Port)
	copy(frame[10:], data)
	_, err := c.udp.WriteToUDP(frame, c.relay)
	if err != nil {
		return 0, err
	}
	c.ledger.add("send", c.udp.LocalAddr().(*net.UDPAddr).Port, dest.String(), data)
	return len(data), nil
}
func (c *socksPacketConn) Close() error                       { c.tcp.Close(); return c.udp.Close() }
func (c *socksPacketConn) LocalAddr() net.Addr                { return c.local }
func (c *socksPacketConn) RemoteAddr() net.Addr               { return c.remote }
func (c *socksPacketConn) SetDeadline(t time.Time) error      { return c.udp.SetDeadline(t) }
func (c *socksPacketConn) SetReadDeadline(t time.Time) error  { return c.udp.SetReadDeadline(t) }
func (c *socksPacketConn) SetWriteDeadline(t time.Time) error { return c.udp.SetWriteDeadline(t) }
func (c *socksPacketConn) SetReadBuffer(size int) error       { return c.udp.SetReadBuffer(size) }
func (c *socksPacketConn) SetWriteBuffer(size int) error      { return c.udp.SetWriteBuffer(size) }
func (c *socksPacketConn) Read(data []byte) (int, error) {
	n, _, err := c.ReadFrom(data)
	return n, err
}
func (c *socksPacketConn) ReadFromUDP(data []byte) (int, *net.UDPAddr, error) {
	n, address, err := c.ReadFrom(data)
	if err != nil {
		return 0, nil, err
	}
	return n, address.(*net.UDPAddr), nil
}
func (c *socksPacketConn) ReadMsgUDP(data, _ []byte) (int, int, int, *net.UDPAddr, error) {
	n, address, err := c.ReadFromUDP(data)
	return n, 0, 0, address, err
}
func (c *socksPacketConn) Write(data []byte) (int, error) {
	if c.remote == nil {
		return 0, errors.New("unconnected SOCKS UDP write")
	}
	return c.WriteTo(data, c.remote)
}
func (c *socksPacketConn) WriteToUDP(data []byte, address *net.UDPAddr) (int, error) {
	return c.WriteTo(data, address)
}
func (c *socksPacketConn) WriteMsgUDP(data, _ []byte, address *net.UDPAddr) (int, int, error) {
	if address == nil {
		address = c.remote
	}
	n, err := c.WriteTo(data, address)
	return n, 0, err
}

var _ transport.Net = (*routedNet)(nil)
var _ transport.UDPConn = (*socksPacketConn)(nil)
var _ net.PacketConn = (*socksPacketConn)(nil)
