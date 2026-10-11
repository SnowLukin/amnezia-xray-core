package main

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pion/logging"
	"github.com/pion/transport/v4/stdnet"
	"github.com/pion/turn/v4"
)

type hostHub struct {
	a, b       *net.UDPConn
	mu         sync.Mutex
	registered [2]*net.UDPAddr
	forwarded  [2]int
}

func newHostHub(portA, portB int) (*hostHub, error) {
	a, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: portA})
	if err != nil {
		return nil, err
	}
	b, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: portB})
	if err != nil {
		a.Close()
		return nil, err
	}
	hub := &hostHub{a: a, b: b}
	go hub.loop(0, a)
	go hub.loop(1, b)
	return hub, nil
}

func (h *hostHub) loop(side int, listener *net.UDPConn) {
	buffer := make([]byte, 4096)
	for {
		n, origin, err := listener.ReadFromUDP(buffer)
		if err != nil {
			return
		}
		if bytes.HasPrefix(buffer[:n], []byte("MATRIX-REGISTER:")) {
			h.mu.Lock()
			h.registered[side] = origin
			h.mu.Unlock()
			continue
		}
		h.mu.Lock()
		destination := h.registered[side]
		h.mu.Unlock()
		if destination == nil {
			continue
		}
		originPort := h.b.LocalAddr().(*net.UDPAddr).Port
		if side == 1 {
			originPort = h.a.LocalAddr().(*net.UDPAddr).Port
		}
		forwarded := append([]byte{'M', 'T', 'R', 'X', byte(originPort >> 8), byte(originPort)}, buffer[:n]...)
		if _, err = listener.WriteToUDP(forwarded, destination); err == nil {
			h.mu.Lock()
			h.forwarded[side]++
			h.mu.Unlock()
		}
	}
}

func (h *hostHub) result() map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	rows := make([]string, 2)
	for i, addr := range h.registered {
		if addr != nil {
			rows[i] = addr.String()
		}
	}
	return map[string]any{"registrations": rows, "forwarded_to_a": h.forwarded[0], "forwarded_to_b": h.forwarded[1]}
}
func (h *hostHub) Close() { h.a.Close(); h.b.Close() }

type turnServer struct {
	server *turn.Server
	packet net.PacketConn
}

func newTURNServer(port int) (*turnServer, error) {
	packet, err := net.ListenPacket("udp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, err
	}
	standard, err := stdnet.NewNet()
	if err != nil {
		packet.Close()
		return nil, err
	}
	server, err := turn.NewServer(turn.ServerConfig{
		PacketConnConfigs: []turn.PacketConnConfig{{PacketConn: packet,
			RelayAddressGenerator: &turn.RelayAddressGeneratorStatic{
				RelayAddress: net.IPv4(127, 0, 0, 1), Address: "127.0.0.1", Net: standard}}},
		Realm: "matrix.test",
		AuthHandler: func(username, realm string, _ net.Addr) ([]byte, bool) {
			if username != "stage1" {
				return nil, false
			}
			return turn.GenerateAuthKey(username, realm, "stage1-secret"), true
		},
		LoggerFactory: logging.NewDefaultLoggerFactory(),
	})
	if err != nil {
		packet.Close()
		return nil, err
	}
	return &turnServer{server, packet}, nil
}

func (s *turnServer) waitForAllocations(count int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.server.AllocationCount() >= count {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
func (s *turnServer) Close() { s.server.Close(); s.packet.Close() }
