package conf_test

import (
	"encoding/json"
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
			// AmneziaWG 3.1 flags are booleans next to the parameters.
			Input: `{
				"secretKey": "` + testSecretKey + `",
				"address": ["10.0.0.2/32"],
				"peers": [{
					"publicKey": "` + testPublicKey + `",
					"endpoint": "1.2.3.4:27789"
				}],
				"awg": {
					"jc": 3,
					"randomTrailers": true,
					"disableCookies": true
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
					Jc:             "3",
					RandomTrailers: true,
					DisableCookies: true,
				},
			},
		},
		{
			// Flags may also come as 0 and 1, the way awg configs and UAPI
			// write them, as numbers or strings.
			Input: `{
				"secretKey": "` + testSecretKey + `",
				"address": ["10.0.0.2/32"],
				"peers": [{
					"publicKey": "` + testPublicKey + `",
					"endpoint": "1.2.3.4:27789"
				}],
				"awg": {
					"randomTrailers": "1",
					"disableCookies": 0
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
					RandomTrailers: true,
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

func TestAmneziaFlagRejectsOtherValues(t *testing.T) {
	for _, value := range []string{`2`, `"yes"`, `[]`} {
		var p AmneziaParameters
		if err := json.Unmarshal([]byte(`{"randomTrailers": `+value+`}`), &p); err == nil {
			t.Errorf("accepted %s as an AmneziaWG flag", value)
		}
	}
}

// testHeaderKey is the bytes 0 to 31, as awg genkey would print a key and in
// hex, the form UAPI takes.
const (
	testHeaderKey    = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
	testHeaderKeyHex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
)

func TestWireGuardAmnezia31Keys(t *testing.T) {
	creator := func() Buildable {
		return &WireGuardConfig{IsClient: true}
	}

	runMultiTestCase(t, []TestCase{
		{
			// The device keys of AmneziaWG 3.1 next to the older parameters:
			// the header key turns into hex, ranges stay as they are.
			Input: `{
				"secretKey": "` + testSecretKey + `",
				"address": ["10.0.0.2/32"],
				"peers": [{
					"publicKey": "` + testPublicKey + `",
					"endpoint": "1.2.3.4:27789",
					"keepAlive": "25-35",
					"protocolVersion": 1
				}],
				"awg": {
					"s1": 20, "s2": 78, "s3": 15, "s4": 12,
					"headerProtectionKey": "` + testHeaderKey + `",
					"contentPaddingAddition": "16-64",
					"rekeyAfterTime": "110-130",
					"rekeyTimeout": 5,
					"rejectAfterTime": 180,
					"keepaliveTimeout": "8-12",
					"maxHandshakeAttempts": "15-20"
				}
			}`,
			Parser: loadJSON(creator),
			Output: &wireguard.DeviceConfig{
				SecretKey: testSecretKey,
				Endpoint:  []string{"10.0.0.2/32"},
				Peers: []*wireguard.PeerConfig{{
					PublicKey:       testPublicKey,
					Endpoint:        "1.2.3.4:27789",
					KeepAlive:       "25-35",
					AllowedIps:      []string{"0.0.0.0/0", "::0/0"},
					ProtocolVersion: "1",
				}},
				Mtu:      1420,
				IsClient: true,
				Parameters: &wireguard.AmneziaParameters{
					S1: "20", S2: "78", S3: "15", S4: "12",
					HeaderProtectionKey:    testHeaderKeyHex,
					ContentPaddingAddition: "16-64",
					RekeyAfterTime:         "110-130",
					RekeyTimeout:           "5",
					RejectAfterTime:        "180",
					KeepaliveTimeout:       "8-12",
					MaxHandshakeAttempts:   "15-20",
				},
			},
		},
		{
			// keepAlive as the number it used to be: 25 is written as before,
			// 0 turns it off and writes nothing.
			Input: `{
				"secretKey": "` + testSecretKey + `",
				"address": ["10.0.0.2/32"],
				"peers": [
					{"publicKey": "` + testPublicKey + `", "endpoint": "1.2.3.4:27789", "keepAlive": 25},
					{"publicKey": "` + testPublicKey + `", "endpoint": "1.2.3.4:27790", "keepAlive": 0}
				]
			}`,
			Parser: loadJSON(creator),
			Output: &wireguard.DeviceConfig{
				SecretKey: testSecretKey,
				Endpoint:  []string{"10.0.0.2/32"},
				Peers: []*wireguard.PeerConfig{
					{PublicKey: testPublicKey, Endpoint: "1.2.3.4:27789", KeepAlive: "25", AllowedIps: []string{"0.0.0.0/0", "::0/0"}},
					{PublicKey: testPublicKey, Endpoint: "1.2.3.4:27790", AllowedIps: []string{"0.0.0.0/0", "::0/0"}},
				},
				Mtu:      1420,
				IsClient: true,
			},
		},
		{
			// The header key may also come in hex.
			Input: `{
				"secretKey": "` + testSecretKey + `",
				"peers": [{"publicKey": "` + testPublicKey + `", "endpoint": "1.2.3.4:27789"}],
				"awg": {"s1": 12, "s2": 12, "s3": 12, "s4": 12, "headerProtectionKey": "` + testHeaderKeyHex + `"}
			}`,
			Parser: loadJSON(creator),
			Output: &wireguard.DeviceConfig{
				SecretKey: testSecretKey,
				Endpoint:  []string{"10.0.0.1", "fd59:7153:2388:b5fd:0000:0000:0000:0001"},
				Peers: []*wireguard.PeerConfig{
					{PublicKey: testPublicKey, Endpoint: "1.2.3.4:27789", AllowedIps: []string{"0.0.0.0/0", "::0/0"}},
				},
				Mtu:      1420,
				IsClient: true,
				Parameters: &wireguard.AmneziaParameters{
					S1: "12", S2: "12", S3: "12", S4: "12",
					HeaderProtectionKey: testHeaderKeyHex,
				},
			},
		},
	})
}

func TestWireGuardRejectsBadAmnezia31Values(t *testing.T) {
	peer := func(keepAlive string) string {
		return `{"secretKey": "` + testSecretKey + `", "peers": [{"publicKey": "` + testPublicKey +
			`", "endpoint": "1.2.3.4:27789", "keepAlive": ` + keepAlive + `}]}`
	}
	awg := func(params string) string {
		return `{"secretKey": "` + testSecretKey + `", "peers": [{"publicKey": "` + testPublicKey +
			`", "endpoint": "1.2.3.4:27789"}], "awg": {` + params + `}}`
	}
	for _, input := range []string{
		peer(`-1`),
		peer(`"35-25"`),
		peer(`"25-"`),
		peer(`"soon"`),
		peer(`4294967296`),
		// Header protection takes its nonce from s1-s4.
		awg(`"s1": 20, "s2": 78, "s3": 15, "headerProtectionKey": "` + testHeaderKey + `"`),
		awg(`"s1": 20, "s2": 78, "s3": 11, "s4": 12, "headerProtectionKey": "` + testHeaderKey + `"`),
		awg(`"s1": 20, "s2": 78, "s3": 15, "s4": 12, "headerProtectionKey": "not a key"`),
	} {
		for _, isClient := range []bool{true, false} {
			c := &WireGuardConfig{IsClient: isClient}
			if err := json.Unmarshal([]byte(input), c); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Build(); err == nil {
				t.Errorf("built %s", input)
			}
		}
	}
}
