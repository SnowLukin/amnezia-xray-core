package splithttp

import (
	"context"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	xtls "github.com/xtls/xray-core/transport/internet/tls"
)

// This same wire-level fixture can be run on the unmodified base to measure
// physical dials. A local TCP proxy stalls ClientHello or gates a healthy server.
func TestXHTTP2TCPDialCounts(t *testing.T) {
	for _, blackhole := range []bool{true, false} {
		for _, clients := range []int32{1, 3} {
			t.Run(fmt.Sprintf("blackhole=%v/xmux=%d", blackhole, clients), func(t *testing.T) {
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
				server.EnableHTTP2 = true
				server.StartTLS()
				defer server.Close()
				target := server.Listener.Addr().String()
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				var accepts atomic.Int32
				var sockets sync.Map
				var workers sync.WaitGroup
				gate := make(chan struct{})
				defer func() {
					listener.Close()
					sockets.Range(func(k, v any) bool { k.(net.Conn).Close(); return true })
					workers.Wait()
				}()
				workers.Add(1)
				go func() {
					defer workers.Done()
					for {
						c, err := listener.Accept()
						if err != nil {
							return
						}
						sockets.Store(c, true)
						accepts.Add(1)
						workers.Add(1)
						go func() {
							defer workers.Done()
							defer c.Close()
							<-gate
							if blackhole {
								_, _ = io.Copy(io.Discard, c)
								return
							}
							upstream, err := net.Dial("tcp", target)
							if err != nil {
								return
							}
							defer upstream.Close()
							done := make(chan struct{})
							go func() { _, _ = io.Copy(upstream, c); upstream.Close(); close(done) }()
							_, _ = io.Copy(c, upstream)
							c.Close()
							<-done
						}()
					}
				}()
				addr := listener.Addr().(*net.TCPAddr)
				config := &Config{Xmux: &XmuxConfig{MaxConnections: &RangeConfig{From: clients, To: clients}}}
				settings := &internet.MemoryStreamConfig{ProtocolName: "splithttp", ProtocolSettings: config, SecurityType: "tls", SecuritySettings: &xtls.Config{ServerName: "example.com", Certificate: []*xtls.Certificate{{Usage: xtls.Certificate_AUTHORITY_VERIFY, Certificate: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}, NextProtocol: []string{"h2"}, Fingerprint: "chrome"}}
				manager := NewXmuxManager(*config.Xmux, func() XmuxConn {
					return createHTTPClient(xnet.TCPDestination(xnet.LocalHostIP, xnet.Port(addr.Port)), settings)
				})
				const requests = 30
				selected := make([]*DefaultDialerClient, requests)
				for i := range selected {
					selected[i] = manager.GetXmuxClient(context.Background()).XmuxConn.(*DefaultDialerClient)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var before, after runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&before)
				done := make(chan error, requests)
				start := make(chan struct{})
				for _, client := range selected {
					go func() {
						<-start
						req, _ := http.NewRequestWithContext(ctx, "GET", "https://xray.test/", nil)
						resp, err := client.client.Do(req)
						if err == nil {
							_, err = io.Copy(io.Discard, resp.Body)
							resp.Body.Close()
						}
						done <- err
					}()
				}
				close(start)
				time.Sleep(300 * time.Millisecond)
				count := accepts.Load()
				runtime.ReadMemStats(&after)
				t.Logf("requests=%d physical_dials=%d heap_delta_bytes=%d", requests, count, int64(after.HeapAlloc)-int64(before.HeapAlloc))
				close(gate)
				if blackhole {
					cancel()
					sockets.Range(func(k, v any) bool { k.(net.Conn).Close(); return true })
				}
				for range requests {
					select {
					case err := <-done:
						if !blackhole && err != nil {
							t.Fatal(err)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("requests did not finish")
					}
				}
				for _, client := range selected {
					client.Close()
					client.client.CloseIdleConnections()
				}
				if os.Getenv("XRAY_RECORD_DIAL_COUNTS") != "1" && count != int32(clients) {
					t.Errorf("physical dials=%d, want %d", count, clients)
				}
			})
		}
	}
}
