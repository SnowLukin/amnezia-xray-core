package health

import (
	"encoding/json"
	"fmt"
	"github.com/xtls/xray-core/core"
	"strings"
	"testing"
	"time"
)

// An answer may not touch routing or the observatory. Any other section of
// the config file is a key protocol 1 does not know: skipped, and named.
func TestAnswerKeysOfProtocol1(t *testing.T) {
	for _, parse := range []func([]byte) (*providerAnswer, error){parseAnswer, parseInitial} {
		for _, key := range []string{"routing", "dns", "inbounds", "healthObservatory", "api", "policy", "transport", "log", "metrics", "stats", "reverse", "fakeDns", "observatory", "burstObservatory", "geodata"} {
			a, err := parse([]byte(`{"protocol":1,"outbounds":[{"tag":"x","protocol":"vless","selection":{"balancer":"auto","probe":"check","cost":1}}],"` + key + `":{}}`))
			forbidden := key == "routing" || key == "healthObservatory"
			if forbidden && (err == nil || !isAnswerError(err)) {
				t.Fatalf("accepted forbidden %s", key)
			}
			if !forbidden && (err != nil || len(a.ignored) != 1 || a.ignored[0] != key) {
				t.Fatalf("%s: %v, want it ignored", key, err)
			}
		}
	}
}
func TestProviderRoutingRejectionKeepsWholePreviousSet(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); err != nil {
		t.Fatal(err)
	}
	original := f.m.GetHandler("p-x1")
	saved, seq := f.providerSet(t, 0)
	var builds int
	f.o.parseOutbounds = func(json.RawMessage) ([]*core.OutboundHandlerConfig, error) { builds++; return nil, nil }
	_, err := parseAnswer([]byte(`{"protocol":1,"version":"rejected","outbounds":[{"tag":"x2","protocol":"vless","selection":{"balancer":"auto","probe":"check","cost":1}}],"routing":{"rules":[]}}`))
	if err == nil || !strings.Contains(err.Error(), "routing") {
		t.Fatalf("routing refusal: %v", err)
	}
	again, next := f.providerSet(t, 0)
	if builds != 0 || original != f.m.GetHandler("p-x1") || f.m.GetHandler("p-x2") != nil || seq != next || fmt.Sprint(saved) != fmt.Sprint(again) {
		t.Fatal("routing refusal changed previous working set")
	}
}
