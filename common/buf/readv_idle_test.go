//go:build !wasm && !openbsd

package buf_test

import (
	"net"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"testing"
	"time"

	. "github.com/xtls/xray-core/common/buf"
)

// heapObjects is the heap that live objects take, after what is garbage is gone
func heapObjects() uint64 {
	runtime.GC()
	runtime.GC()
	debug.FreeOSMemory()
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}

// idleCost is how much of the heap each of n readers that wait takes, with start making them wait
func idleCost(t *testing.T, n int, start func(i int)) float64 {
	t.Helper()
	before := heapObjects()
	for i := range n {
		start(i)
	}
	time.Sleep(300 * time.Millisecond)
	return (float64(heapObjects()) - float64(before)) / float64(n)
}

func TestReadVReaderHoldsNoBufferWhileWaiting(t *testing.T) {
	if !ReleaseWhileWaiting {
		t.Skip("readv keeps its buffers while it waits here")
	}
	const n = 256
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	clients := make([]net.Conn, n)
	readers := make([]Reader, n)
	for i := range n {
		if clients[i], err = net.Dial("tcp", ln.Addr().String()); err != nil {
			t.Fatal(err)
		}
		defer clients[i].Close()
		server, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		defer server.Close()
		readers[i] = NewReader(server)
		if _, ok := readers[i].(*ReadVReader); !ok {
			t.Fatalf("%T is not read through readv", readers[i])
		}
	}
	got := make(chan string, n)
	cost := idleCost(t, n, func(i int) {
		go func() {
			mb, err := readers[i].ReadMultiBuffer()
			if err != nil {
				got <- err.Error()
				return
			}
			got <- mb.String()
			ReleaseMulti(mb)
		}()
	})
	// a reader that took its buffer before it waited would hold 8 KiB
	if cost > 2048 {
		t.Errorf("a reader that waits holds %.0f bytes of the heap", cost)
	}
	for i := range n {
		clients[i].Write([]byte("ping"))
	}
	for range n {
		if s := <-got; s != "ping" {
			t.Fatalf("read %q instead of ping", s)
		}
	}
}

func TestReadVReaderReadsAfterWaiting(t *testing.T) {
	client, server := tcpPair(t)
	r := NewReader(server)
	payload := make([]byte, 3*Size+100)
	for i := range payload {
		payload[i] = byte(i)
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		client.Write(payload)
		client.Close()
	}()
	var read []byte
	for {
		mb, err := r.ReadMultiBuffer()
		if err != nil {
			break
		}
		for _, b := range mb {
			read = append(read, b.Bytes()...)
		}
		ReleaseMulti(mb)
	}
	if string(read) != string(payload) {
		t.Fatalf("read %d bytes that differ from the %d written", len(read), len(payload))
	}
}

func tcpPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return client, server
}
