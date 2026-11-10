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
		`"tag": "a"`:                                              nil,
		`"speedLimit": {}`:                                        nil,
		`"speedLimit": {"downlink": "8mbps"}`:                     {DownlinkBytesPerSecond: 1_000_000},
		`"speedLimit": {"downlink": "20Mbps"}`:                    {DownlinkBytesPerSecond: 2_500_000},
		`"speedLimit": {"uplink": "64kbps", "downlink": "1gbps"}`: {UplinkBytesPerSecond: 8_000, DownlinkBytesPerSecond: 125_000_000},
		`"speedLimit": {"downlink": "1.5mbps"}`:                   {DownlinkBytesPerSecond: 187_500},
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
		// Only kbps, mbps and gbps: a bare number or a byte unit is a mistake.
		`"speedLimit": {"downlink": "20mb"}`,
		`"speedLimit": {"downlink": "20m"}`,
		`"speedLimit": {"downlink": "1000000"}`,
		`"speedLimit": {"downlink": "1tbps"}`,
		`"speedLimit": {"downlink": "-1mbps"}`,
		// Keys are exact: a typo or another case is not ignored.
		`"speedLimit": {"down": "5mbps"}`,
		`"speedLimit": {"downLink": "5mbps"}`,
		`"speedlimit": {"downlink": "5mbps"}`,
		`"SpeedLimit": {"downlink": "5mbps"}`,
		`"selections": {"balancer": "auto"}`,
		`"seletion": {"balancer": "auto"}`,
		`"Selection": {"balancer": "auto"}`,
		`"speedLimits": {"downlink": "5mbps"}`,
	} {
		if _, err := build(input); err == nil {
			t.Errorf("expected an error for %s", input)
		}
	}
}

// Keys the core does not know stay allowed at the level of an outbound, as
// they always were; only near misses of the adaptive blocks are refused.
func TestOutboundKeepsUnrelatedUnknownKeys(t *testing.T) {
	for _, key := range []string{"comment", "settingz", "x-vendor", "remarks"} {
		c := new(OutboundDetourConfig)
		if err := json.Unmarshal([]byte(`{"protocol": "freedom", "`+key+`": 1}`), c); err != nil {
			t.Errorf("%s: %v", key, err)
		}
	}
}
