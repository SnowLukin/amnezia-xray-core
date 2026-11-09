package health_test

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	utls "github.com/refraction-networking/utls"
	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/core"
)

func TestProviderLimitsNonProtocolHTTPReplies(t *testing.T) {
	for _, mode := range []string{"bare_status", "broken_json", "protocol", "retry_after", "network", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			var count atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				if mode == "network" {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
					} else {
						conn.Close()
					}
					return
				}
				if mode == "timeout" {
					io.Copy(io.Discard, r.Body)
					<-r.Context().Done()
					return
				}
				name, value := health.AdaptiveHeader()
				if r.Header.Get(name) != value {
					t.Error("missing adaptive header")
				}
				if mode == "broken_json" {
					io.WriteString(w, `{"broken":`)
					return
				}
				if mode == "retry_after" {
					w.Header().Set("Retry-After", "60")
				}
				w.WriteHeader(503)
				if mode == "protocol" {
					io.WriteString(w, `{"protocol":1,"code":"busy"}`)
				}
			}))
			defer server.Close()
			obs := twoViaCore(t, server.URL, fmt.Sprintf(`,"urls":[%q,%q,%q,%q],"via":["direct","direct2","direct"],"timeout":"20ms"`, server.URL, server.URL, server.URL, server.URL))
			health.ConfigureProviderLongCooldownForTest(obs)
			obs.RequestProviderRefresh("fresh")
			waitRefreshed(t, obs)
			// Retry-After without a body of the protocol may come from a
			// proxy, and a server that took the request and did not answer
			// loads the backend like one that answered out of the protocol.
			want := int64(2)
			if mode == "network" {
				want = 12
			}
			if mode == "protocol" {
				want = 1
			}
			if count.Load() != want {
				t.Fatalf("%s got %d attempts, want %d", mode, count.Load(), want)
			}
		})
	}
}

// The URL is deliberately http: the freedom outbound supplies the one TLS
// layer. The HTTP Host and TLS SNI still name the backend, not the entry IP.
func TestProviderHTTPSViaEntryRecipe(t *testing.T) {
	authority, _ := cert.MustGenerate(nil, cert.Authority(true), cert.KeyUsage(x509.KeyUsageCertSign|x509.KeyUsageDigitalSignature))
	rootPEM, _ := authority.ToPEM()
	for _, dynamic := range []bool{false, true} {
		for _, wrong := range []bool{false, true} {
			t.Run(fmt.Sprintf("dynamic=%t/wrong_certificate=%t", dynamic, wrong), func(t *testing.T) {
				dns := "backend.test"
				if wrong {
					dns = "foreign.test"
				}
				leaf, _ := cert.MustGenerate(authority, cert.DNSNames(dns))
				certPEM, keyPEM := leaf.ToPEM()
				pair, err := tls.X509KeyPair(certPEM, keyPEM)
				if err != nil {
					t.Fatal(err)
				}
				var mu sync.Mutex
				var hello *tls.ClientHelloInfo
				var gotHeader, gotHost string
				var requests int
				response := []byte(`{"protocol":1,"version":"fetched","outbounds":[{"tag":"user","protocol":"freedom","selection":{"balancer":"auto","probe":"check","cost":1}}]}`)
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					requests++
					gotHeader = r.Header.Get("Xray-Adaptive")
					gotHost = r.Host
					body := response
					mu.Unlock()
					w.Write(body)
				}))
				server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, NextProtos: []string{"http/1.1"}, GetConfigForClient: func(h *tls.ClientHelloInfo) (*tls.Config, error) { mu.Lock(); hello = h; mu.Unlock(); return nil, nil }}
				server.StartTLS()
				defer server.Close()
				outbound := map[string]any{
					"tag": "entry", "protocol": "freedom", "settings": map[string]any{"redirect": server.Listener.Addr().String()},
					"streamSettings": map[string]any{"network": "raw", "security": "tls", "tlsSettings": map[string]any{
						"serverName": "backend.test", "fingerprint": "chrome", "alpn": []string{"http/1.1"}, "disableSystemRoot": true,
						"certificates": []any{map[string]any{"usage": "verify", "certificate": strings.Split(strings.TrimSpace(string(rootPEM)), "\n")}},
					}},
				}
				apply := map[string]any{"balancers": []string{"auto", "backend"}, "tagPrefix": "p-", "allowedProtocols": []string{"freedom"}}
				provider := map[string]any{"tag": "fresh", "urls": []string{"http://backend.test/set"}, "isPlainHttpAllowed": true, "via": []string{"entry"}, "apply": apply,
					"triggers": map[string]any{"exhausted": map[string]any{"isEnabled": false}, "leaseRenewal": map[string]any{"isEnabled": false}}}
				outbounds := []any{map[string]any{"tag": "direct", "protocol": "freedom"}, map[string]any{"tag": "p-base", "protocol": "freedom", "selection": map[string]any{"balancer": "auto", "probe": "check", "cost": 1}}}
				if dynamic {
					provided := map[string]any{}
					for k, v := range outbound {
						provided[k] = v
					}
					provided["selection"] = map[string]any{"balancer": "backend", "probe": "check", "cost": 1}
					if wrong {
						provider["via"] = []string{"backend"}
						provider["initialAnswer"] = map[string]any{"protocol": 1, "outbounds": []any{provided}}
					} else {
						// Bootstrap through the static entry; the first real HTTP
						// answer installs the road used by the second refresh.
						outbounds = append(outbounds, outbound)
						provider["via"] = []string{"backend", "entry"}
						user := map[string]any{"tag": "user", "protocol": "freedom", "selection": map[string]any{"balancer": "auto", "probe": "check", "cost": 1}}
						mu.Lock()
						response, _ = json.Marshal(map[string]any{"protocol": 1, "version": "fetched", "outbounds": []any{user, provided}})
						mu.Unlock()
					}
				} else {
					outbounds = append(outbounds, outbound)
				}

				document := map[string]any{"log": map[string]any{"loglevel": "none"}, "outbounds": outbounds,
					"routing": map[string]any{"balancers": []any{
						map[string]any{"tag": "auto", "strategy": map[string]any{"type": "leastCost"}, "fallbackTag": "direct"},
						map[string]any{"tag": "backend", "strategy": map[string]any{"type": "leastCost"}, "fallbackTag": "direct"},
					}},
					"healthObservatory": map[string]any{"telemetry": map[string]any{"events": map[string]any{}}, "probes": map[string]any{"profiles": []any{map[string]any{"name": "check", "steps": []any{map[string]any{"name": "transport", "destinations": []string{"http://127.0.0.1/probe"}}}}}}, "providers": []any{provider}},
				}
				raw, _ := json.Marshal(document)
				cfg, err := core.LoadConfig("json", strings.NewReader(string(raw)))
				if err != nil {
					t.Fatal(err)
				}
				instance, err := core.New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if err = instance.Start(); err != nil {
					t.Fatal(err)
				}
				defer instance.Close()
				obs := health.FromInstance(instance)
				health.ConfigureProviderScenarioForTest(obs)
				health.ConfigureProviderLongCooldownForTest(obs)
				selectProviderTraffic(t, obs)
				obs.RequestProviderRefresh("fresh")
				waitRefreshed(t, obs)
				if dynamic && !wrong {
					obs.RequestProviderRefresh("fresh")
					waiter := &eventWaiter{t: t, o: obs}
					waiter.until("provider_fetch", func(e health.Event) bool { return e.Fields["via"] == "backend" && e.Fields["is_ok"] == true })
				}
				mu.Lock()
				defer mu.Unlock()
				if wrong {
					if requests != 0 {
						t.Fatal("foreign certificate accepted")
					}
				} else {
					_, want := health.AdaptiveHeader()
					wantRequests := 1
					if dynamic {
						wantRequests = 2
					}
					if requests != wantRequests || gotHeader != want || gotHost != "backend.test" {
						t.Fatalf("requests=%d header=%q host=%s status=%s", requests, gotHeader, gotHost, obs.Status())
					}
				}
				if hello == nil || hello.ServerName != "backend.test" || !slices.Equal(hello.SupportedProtos, []string{"http/1.1"}) {
					t.Fatalf("hello=%+v", hello)
				}
				// Compare the wire suites and supported groups with the installed Chrome
				// profile, normalizing only GREASE's intentionally random reserved value.
				spec, err := utls.UTLSIdToSpec(utls.HelloChrome_Auto)
				if err != nil {
					t.Fatal(err)
				}
				normalized := func(values []uint16) []uint16 {
					out := slices.Clone(values)
					for i, v := range out {
						if v&0x0f0f == 0x0a0a && byte(v>>8) == byte(v) {
							out[i] = utls.GREASE_PLACEHOLDER
						}
					}
					return out
				}
				if !slices.Equal(normalized(hello.CipherSuites), normalized(spec.CipherSuites)) {
					t.Fatalf("not Chrome cipher suites: %x vs %x", hello.CipherSuites, spec.CipherSuites)
				}
				for _, extension := range spec.Extensions {
					if curves, ok := extension.(*utls.SupportedCurvesExtension); ok {
						got, want := []uint16{}, []uint16{}
						for _, c := range hello.SupportedCurves {
							got = append(got, uint16(c))
						}
						for _, c := range curves.Curves {
							want = append(want, uint16(c))
						}
						if !slices.Equal(normalized(got), normalized(want)) {
							t.Fatalf("not Chrome supported groups: %x vs %x", got, want)
						}
					}
				}
			})
		}
	}
}
