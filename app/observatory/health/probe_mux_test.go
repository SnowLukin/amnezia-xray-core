package health_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	gonet "net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/infra/conf/serial"
	_ "github.com/xtls/xray-core/main/distro/all"
)

func TestProbeIsolatedMuxKeepsSlowFirstByte(t *testing.T) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			time.Sleep(1300 * time.Millisecond)
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer httpServer.Close()

	listener, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverPort := listener.Addr().(*gonet.TCPAddr).Port
	listener.Close()
	const userID = "27848739-7e62-4138-9fd3-098a63964b6b"
	serverConfig := fmt.Sprintf(`{
		"log": {"loglevel": "none"},
		"inbounds": [{"tag": "vless-in", "listen": "127.0.0.1", "port": %d,
			"protocol": "vless", "settings": {"clients": [{"id": %q}], "decryption": "none"}}],
		"outbounds": [{"tag": "direct", "protocol": "freedom",
			"settings": {"finalRules": [{"action": "allow"}]}}]
	}`, serverPort, userID)
	server := startProbeMuxInstance(t, serverConfig)
	defer server.Close()
	clientConfig := fmt.Sprintf(`{
		"log": {"loglevel": "none"},
		"outbounds": [{"tag": "v1", "protocol": "vless",
			"settings": {"vnext": [{"address": "127.0.0.1", "port": %d,
				"users": [{"id": %q, "encryption": "none"}]}]},
			"mux": {"enabled": true, "concurrency": 8},
			"selection": {"balancer": "auto", "cost": 1, "probe": "mux-local"}},
			{"tag": "block", "protocol": "blackhole"}],
		"routing": {"balancers": [{"tag": "auto", "fallbackTag":"block", "strategy": {"type": "leastCost"}}]},
		"healthObservatory": {"evidence": {"dialTimeout": {"timeout": "1s"}}, "probes":{"profiles":[{"name":"mux-local","steps":[{"name":"transport","destinations":[%q],"minBytes":2,"timeout":"2s","progressTimeout":"2s"}]}]}}
	}`, serverPort, userID, httpServer.URL+"/configured-probe")
	client := startProbeMuxInstance(t, clientConfig)
	defer client.Close()

	addr := httpServer.Listener.Addr().String()
	warmDeadline, warmCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer warmCancel()
	warmCtx := session.SetForcedOutboundTagToContext(warmDeadline, "v1")
	warm, err := core.Dial(warmCtx, client, net.TCPDestination(net.ParseAddress("127.0.0.1"), net.Port(httpServer.Listener.Addr().(*gonet.TCPAddr).Port)))
	if err != nil {
		t.Fatalf("mux warm-up dial failed: %v", err)
	}
	defer warm.Close()
	stopWarmClose := context.AfterFunc(warmDeadline, func() { _ = warm.Close() })
	defer stopWarmClose()
	warmFailure := func(phase string, err error) {
		if warmDeadline.Err() != nil {
			t.Fatalf("mux warm-up timed out during %s: %v", phase, err)
		}
		t.Fatalf("mux warm-up %s failed: %v", phase, err)
	}
	if _, err := fmt.Fprintf(warm, "GET /warm HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", addr); err != nil {
		warmFailure("write", err)
	}
	response, err := http.ReadResponse(bufio.NewReader(warm), nil)
	if err != nil {
		warmFailure("response", err)
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		warmFailure("status", fmt.Errorf("HTTP %d", response.StatusCode))
	}
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		response.Body.Close()
		warmFailure("body", err)
	}
	response.Body.Close()
	warm.Close()
	stopWarmClose()
	warmCancel()

	o := health.FromInstance(client)
	ok, wire, msg := health.Probe(o, "v1", httpServer.URL+"/slow", 2)
	if !ok {
		if msg == "dial timeout" {
			t.Fatalf("slow first byte through isolated mux: dial timeout, bytes %d", wire)
		}
		t.Fatalf("probe fixture or transport failed before expected RED: %q, bytes %d", msg, wire)
	}
	if wire == 0 {
		t.Fatal("isolated mux probe passed without tracked physical bytes")
	}
}

func startProbeMuxInstance(t *testing.T, raw string) *core.Instance {
	t.Helper()
	cfg, err := serial.LoadJSONConfig(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	inst, err := core.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.Start(); err != nil {
		inst.Close()
		t.Fatal(err)
	}
	return inst
}
