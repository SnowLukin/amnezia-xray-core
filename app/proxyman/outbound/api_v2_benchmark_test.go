package outbound_test

import (
	"context"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/infra/conf/serial"
	_ "github.com/xtls/xray-core/main/distro/all"
	"io"
	gonet "net"
	"strings"
	"testing"
)

// BenchmarkV2Connection measures a complete fresh 32-KiB loopback connection,
// including route choice and outbound processing. Flags describe initialized
// product profiles, not simultaneous trigger activations. No speed threshold.
func BenchmarkV2Connection(b *testing.B) {
	ln, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()
	body := make([]byte, 32*1024)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				p := make([]byte, 32)
				if _, err := c.Read(p); err != nil {
					return
				}
				_, _ = c.Write(body)
			}()
		}
	}()
	dest := xnet.TCPDestination(xnet.LocalHostIP, xnet.Port(ln.Addr().(*gonet.TCPAddr).Port))
	for _, tc := range []struct{ name, config string }{
		{"plain", `{"log":{"loglevel":"none"},"routing":{"rules":[{"type":"field","network":"tcp,udp","outboundTag":"plain"}]},"outbounds":[{"tag":"plain","protocol":"freedom","settings":{"finalRules":[{"action":"allow"}]}}]}`},
		{"observer_none", `{"log":{"loglevel":"none"},"routing":{"rules":[{"type":"field","network":"tcp,udp","balancerTag":"auto"}],"balancers":[{"tag":"auto","fallbackTag":"plain","strategy":{"type":"leastCost","settings":{"failover":{"closePending":{"isEnabled":false},"closeSilent":{"isEnabled":false}}}}}]},"outbounds":[{"tag":"plain","protocol":"freedom","settings":{"finalRules":[{"action":"allow"}]},"selection":{"balancer":"auto","cost":1}}],"healthObservatory":{"evidence":{"dialFailures":{"isEnabled":false},"dialTimeout":{"isEnabled":false},"serverUnverified":{"isEnabled":false},"firstReply":{"isEnabled":false},"earlyClose":{"isEnabled":false},"stalledRequests":{"isEnabled":false}}}}`},
		{"observer_all", `{"log":{"loglevel":"none"},"routing":{"rules":[{"type":"field","network":"tcp,udp","balancerTag":"auto"}],"balancers":[{"tag":"auto","fallbackTag":"plain","strategy":{"type":"leastCost","settings":{"groups":{"isEnabled":true,"items":[{"name":"g","cost":1}]},"failover":{"downAudit":{"isEnabled":true}},"returnToCheaper":{"isEnabled":true,"onNetworkChange":{"isEnabled":true}},"networkChanges":{"isEnabled":true},"startup":{"pin":{"isEnabled":true,"outbound":"plain"}}}}}]},"outbounds":[{"tag":"plain","protocol":"freedom","settings":{"finalRules":[{"action":"allow"}]},"selection":{"balancer":"auto","group":"g"}}],"healthObservatory":{"probes":{"stepProfiles":{"isEnabled":true},"candidates":{"isEnabled":true},"requests":{"isEnabled":true},"udp":{"isEnabled":true,"destinations":["127.0.0.1:19302"]}},"network":{"connectivity":{"isEnabled":true,"outbound":"plain","destination":"http://127.0.0.1:1/204"},"witnesses":{"isEnabled":true,"outbounds":["plain"]},"memory":{"isEnabled":true},"hostLabels":{"isEnabled":true},"labelRequest":{"isEnabled":true,"outbound":"plain","destination":"http://127.0.0.1:1/whoami","labels":{"asn":"X-ASN"}}},"host":{"pinning":{"isEnabled":true},"userPresent":{"isEnabled":true},"memoryPressure":{"isEnabled":true}},"resources":{"tunnelSleep":{"isEnabled":true},"closeUnchosenIdle":{"isEnabled":true}},"telemetry":{"events":{"isEnabled":true},"report":{"isEnabled":true,"sessionLabels":{"isEnabled":true,"labels":{"cohort":"bench"}}},"quality":{"isEnabled":true},"pathLabels":{"isEnabled":true},"probeMeasurements":{"isEnabled":true},"metrics":{"isEnabled":true}},"evidence":{"partialStalls":{"isEnabled":true},"udpReplies":{"isEnabled":true},"loneFailureProbe":{"isEnabled":true}},"providers":[{"isEnabled":true,"tag":"fresh","url":"http://127.0.0.1:1/set","isPlainHttpAllowed":true,"via":["plain"],"apply":{"balancer":"auto","tagPrefix":"p-"},"triggers":{"periodic":{"isEnabled":true},"minHealthy":{"isEnabled":true},"whenCostAbove":{"isEnabled":true,"cost":10},"maxAge":{"isEnabled":true},"networkChange":{"isEnabled":true},"exhausted":{"isEnabled":true},"leaseRenewal":{"isEnabled":true},"backendSchedule":{"isEnabled":true}}}]},"metrics":{"tag":"v2-benchmark-metrics"}}`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			conf, err := serial.DecodeJSONConfig(strings.NewReader(tc.config))
			if err != nil {
				b.Fatal(err)
			}
			pb, err := conf.Build()
			if err != nil {
				b.Fatal(err)
			}
			inst, err := core.New(pb)
			if err != nil {
				b.Fatal(err)
			}
			if err = inst.Start(); err != nil {
				b.Fatal(err)
			}
			defer inst.Close()
			// Resolve startup and first-connection initialization outside the sample.
			transfer := func() {
				c, err := core.Dial(context.Background(), inst, dest)
				if err != nil {
					b.Fatal(err)
				}
				if _, err = c.Write([]byte("request")); err != nil {
					b.Fatal(err)
				}
				if n, err := io.Copy(io.Discard, c); err != nil || n != int64(len(body)) {
					b.Fatalf("copy %d: %v", n, err)
				}
				c.Close()
			}
			transfer()
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				transfer()
			}
			b.StopTimer()
		})
	}
}
