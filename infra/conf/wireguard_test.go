package conf_test

import (
	"testing"

	. "github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/proxy/wireguard"
)

const (
	testSecretKey = "0000000000000000000000000000000000000000000000000000000000000001"
	testPublicKey = "0000000000000000000000000000000000000000000000000000000000000002"
)

func TestWireGuardAmneziaParameters(t *testing.T) {
	// The outbound loader builds this config as a client (see the registry in
	// xray.go); only that branch puts peers into Peers.
	creator := func() Buildable {
		return &WireGuardConfig{IsClient: true}
	}

	peers := []*wireguard.PeerConfig{
		{
			PublicKey:  testPublicKey,
			Endpoint:   "1.2.3.4:27789",
			AllowedIps: []string{"0.0.0.0/0", "::0/0"},
		},
	}

	runMultiTestCase(t, []TestCase{
		{
			// Numeric form, as the parameters appear in a wg-quick config.
			Input: `{
				"secretKey": "` + testSecretKey + `",
				"address": ["10.0.0.2/32"],
				"peers": [{
					"publicKey": "` + testPublicKey + `",
					"endpoint": "1.2.3.4:27789"
				}],
				"awg": {
					"jc": 3, "jmin": 50, "jmax": 1000,
					"s1": 20, "s2": 78,
					"h1": 39131278, "h2": 832138185,
					"h3": 1436957857, "h4": 1635877746
				}
			}`,
			Parser: loadJSON(creator),
			Output: &wireguard.DeviceConfig{
				SecretKey: testSecretKey,
				Endpoint:  []string{"10.0.0.2/32"},
				Peers:     peers,
				Mtu:       1420,
				IsClient:  true,
				Parameters: &wireguard.AmneziaParameters{
					Jc: "3", Jmin: "50", Jmax: "1000",
					S1: "20", S2: "78",
					H1: "39131278", H2: "832138185",
					H3: "1436957857", H4: "1635877746",
				},
			},
		},
		{
			// String form: h1-h4 ranges and i1-i5 tags cannot be numbers.
			Input: `{
				"secretKey": "` + testSecretKey + `",
				"address": ["10.0.0.2/32"],
				"peers": [{
					"publicKey": "` + testPublicKey + `",
					"endpoint": "1.2.3.4:27789"
				}],
				"awg": {
					"jc": "3",
					"h1": "1000-2000",
					"i1": "<b 0xc0><r 12>"
				}
			}`,
			Parser: loadJSON(creator),
			Output: &wireguard.DeviceConfig{
				SecretKey: testSecretKey,
				Endpoint:  []string{"10.0.0.2/32"},
				Peers:     peers,
				Mtu:       1420,
				IsClient:  true,
				Parameters: &wireguard.AmneziaParameters{
					Jc: "3",
					H1: "1000-2000",
					I1: "<b 0xc0><r 12>",
				},
			},
		},
		{
			// Without the awg block the device must come up as plain
			// WireGuard instead of failing.
			Input: `{
				"secretKey": "` + testSecretKey + `",
				"address": ["10.0.0.2/32"],
				"peers": [{
					"publicKey": "` + testPublicKey + `",
					"endpoint": "1.2.3.4:27789"
				}]
			}`,
			Parser: loadJSON(creator),
			Output: &wireguard.DeviceConfig{
				SecretKey:  testSecretKey,
				Endpoint:   []string{"10.0.0.2/32"},
				Peers:      peers,
				Mtu:        1420,
				IsClient:   true,
				Parameters: nil,
			},
		},
	})
}
