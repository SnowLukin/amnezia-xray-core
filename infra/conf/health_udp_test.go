package conf_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/infra/conf"
)

func TestHealthUDPConfigStrict(t *testing.T) {
	for _, raw := range []string{
		`{"udpProbe":null}`, `{"udpProbe":{}}`,
		`{"udpProbe":{"destinations":["stun.test"]}}`,
		`{"udpProbe":{"destinations":[":3478"]}}`,
		`{"udpProbe":{"destinations":["stun.test:0"]}}`,
		`{"udpProbe":{"destinations":["stun.test:65536"]}}`,
		`{"udpProbe":{"destinations":["stun.test:3478"],"timeout":"0s"}}`,
		`{"udpProbe":{"destinations":["stun.test:3478"],"interval":"0s"}}`,
		`{"udpProbe":{"destinations":["stun.test:3478"],"requests":0}}`,
		`{"udpProbe":{"destinations":["stun.test:3478"],"perHour":-1}}`,
		`{"udpProbe":{"destinations":["stun.test:3478"],"timeout":2}}`,
		`{"udpProbe":{"destinations":["stun.test:3478"],"requests":null}}`,
		`{"udpProbe":{"destinations":["stun.test:3478"],"unknown":1}}`,
		`{"passive":{"udpReplyTimeout":"-1s"}}`, `{"passive":{"udpReplyTimeout":null}}`,
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := buildUDPHealth(raw); err == nil {
				t.Fatal("accepted invalid config")
			}
		})
	}
	c, err := buildUDPHealth(`{"udpProbe":{"destinations":["stun.test:3478","[::1]:19302"],"requests":8,"interval":"250ms","timeout":"3s","perHour":2},"passive":{"udpReplyTimeout":"0s"}}`)
	if err != nil {
		t.Fatal(err)
	}
	u := c.UdpProbe
	if u == nil || u.Requests != 8 || u.Interval != int64(250*time.Millisecond) || u.Timeout != int64(3*time.Second) || u.PerHour != 2 || c.Passive.UdpReplyTimeout != -1 {
		t.Fatalf("%+v", c)
	}
	c, err = buildUDPHealth(`{}`)
	if err != nil || c.UdpProbe != nil {
		t.Fatalf("%+v %v", c, err)
	}
}

func buildUDPHealth(raw string) (*health.Config, error) {
	var c conf.HealthObservatoryConfig
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return nil, err
	}
	message, err := c.Build()
	if err != nil {
		return nil, err
	}
	return message.(*health.Config), nil
}
