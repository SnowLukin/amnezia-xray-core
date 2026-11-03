package conf_test

import (
	"encoding/json"
	"testing"

	"github.com/xtls/xray-core/app/proxyman"
	. "github.com/xtls/xray-core/infra/conf"
	"google.golang.org/protobuf/proto"
)

func TestOutboundSpeedLimit(t *testing.T) {
	build := func(s string) (*proxyman.SpeedLimit, error) {
		c := new(OutboundDetourConfig)
		if err := json.Unmarshal([]byte(`{"protocol": "freedom", `+s+`}`), c); err != nil {
			return nil, err
		}
		h, err := c.Build()
		if err != nil {
			return nil, err
		}
		sender, err := h.SenderSettings.GetInstance()
		if err != nil {
			return nil, err
		}
		return sender.(*proxyman.SenderConfig).SpeedLimit, nil
	}

	for input, want := range map[string]*proxyman.SpeedLimit{
		`"tag": "a"`:                          nil,
		`"speedLimit": {}`:                    nil,
		`"speedLimit": {"downlink": "8mbps"}`: {DownlinkBytesPerSecond: 1 << 20},
		`"speedLimit": {"uplink": "64kbps", "downlink": "1gbps"}`: {UplinkBytesPerSecond: 8 << 10, DownlinkBytesPerSecond: 128 << 20},
	} {
		got, err := build(input)
		if err != nil {
			t.Errorf("%s: %v", input, err)
		} else if !proto.Equal(got, want) {
			t.Errorf("%s: got %v, want %v", input, got, want)
		}
	}
	for _, input := range []string{
		`"speedLimit": {"downlink": "63kbps"}`,
		`"speedLimit": {"uplink": "fast"}`,
		`"speedLimit": {"downlink": "10 parsecs"}`,
	} {
		if _, err := build(input); err == nil {
			t.Errorf("expected an error for %s", input)
		}
	}
}
