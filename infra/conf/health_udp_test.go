package conf_test

import (
	"encoding/json"
	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/infra/conf"
	"testing"
	"time"
)

func TestHealthUDPConfigStrict(t *testing.T) {
	for _, udp := range []string{`null`, `{"isEnabled":true}`, `{"destinations":["stun.test"]}`, `{"destinations":[":3478"]}`, `{"destinations":["stun.test:0"]}`, `{"destinations":["stun.test:65536"]}`, `{"destinations":["stun.test:3478"],"timeout":"0s"}`, `{"interval":"0s"}`, `{"requests":0}`, `{"perHour":-1}`, `{"timeout":2}`, `{"requests":null}`, `{"unknown":1}`} {
		t.Run(udp, func(t *testing.T) {
			if _, err := buildUDPHealth(`{"probes":{"udp":` + udp + `}}`); err == nil {
				t.Fatal("invalid udp accepted")
			}
		})
	}
	c, err := buildUDPHealth(`{"probes":{"udp":{"isEnabled":true,"destinations":["stun.test:3478","[::1]:19302"],"requests":8,"interval":"250ms","timeout":"3s","perHour":2}},"evidence":{"udpReplies":{"isEnabled":false,"timeout":"5s"}}}`)
	if err != nil {
		t.Fatal(err)
	}
	u := c.UdpProbe
	if u == nil || u.Requests != 8 || u.Interval != int64(250*time.Millisecond) || u.Timeout != int64(3*time.Second) || u.PerHour != 2 || c.Features.GetUdpReplies() {
		t.Fatal(c)
	}
	c, err = buildUDPHealth(`{}`)
	if err != nil || c.UdpProbe != nil {
		t.Fatal(c, err)
	}
	for _, raw := range []string{`{"evidence":{"udpReplies":{"timeout":"0s"}}}`, `{"evidence":{"udpReplies":{"timeout":"-1s"}}}`, `{"evidence":{"udpReplies":{"timeout":null}}}`} {
		if _, err := buildUDPHealth(raw); err == nil {
			t.Fatal("bad UDP reply duration accepted")
		}
	}
}
func buildUDPHealth(raw string) (*health.Config, error) {
	var c conf.HealthObservatoryConfig
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return nil, err
	}
	m, err := c.Build()
	if err != nil {
		return nil, err
	}
	return m.(*health.Config), nil
}
