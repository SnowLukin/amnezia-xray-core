package wireguard

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	stdnet "net"
	"net/netip"
	"strings"
	"testing"

	"github.com/amnezia-vpn/amneziawg-go/v3/device"
	"github.com/xtls/xray-core/common/net"
	"golang.org/x/crypto/curve25519"
)

func testKeys(t testing.TB) (priv, pub string) {
	t.Helper()
	var k [32]byte
	if _, err := rand.Read(k[:]); err != nil {
		t.Fatal(err)
	}
	k[0] &= 248
	k[31] = k[31]&127 | 64
	p, err := curve25519.X25519(k[:], curve25519.Basepoint)
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(k[:]), hex.EncodeToString(p)
}

func TestWriteAmneziaParams(t *testing.T) {
	testCases := []struct {
		name   string
		params *AmneziaParameters
		expect string
	}{
		{
			name:   "nil parameters emit nothing",
			params: nil,
			expect: "",
		},
		{
			name: "empty values are skipped",
			params: &AmneziaParameters{
				Jc: "3",
				H1: "39131278",
			},
			expect: "jc=3\nh1=39131278\n",
		},
		{
			name: "full set keeps device-section order",
			params: &AmneziaParameters{
				Jc: "3", Jmin: "50", Jmax: "1000",
				S1: "20", S2: "78", S3: "0", S4: "0",
				H1: "39131278", H2: "832138185",
				H3: "1436957857", H4: "1635877746",
				I1: "<b 0xc0>", I2: "<r 8>", I3: "<rd 4>", I4: "<rc 4>", I5: "<t>",
			},
			expect: "jc=3\njmin=50\njmax=1000\n" +
				"s1=20\ns2=78\ns3=0\ns4=0\n" +
				"h1=39131278\nh2=832138185\nh3=1436957857\nh4=1635877746\n" +
				"i1=<b 0xc0>\ni2=<r 8>\ni3=<rd 4>\ni4=<rc 4>\ni5=<t>\n",
		},
		{
			name: "3.1 flags follow the string parameters",
			params: &AmneziaParameters{
				Jc: "3", I5: "<t>",
				RandomTrailers: true, DisableCookies: true,
			},
			expect: "jc=3\ni5=<t>\nrandom_trailers=true\ndisable_cookies=true\n",
		},
		{
			// A server without random trailers drops every handshake that
			// carries them, so unset flags must not reach the device.
			name: "unset 3.1 flags emit nothing",
			params: &AmneziaParameters{
				Jc: "3",
			},
			expect: "jc=3\n",
		},
		{
			name: "3.1 keys follow i1-i5 in the order of UAPI get",
			params: &AmneziaParameters{
				S1: "20", I5: "<t>",
				HeaderProtectionKey:    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
				ContentPaddingAddition: "16-64",
				RekeyAfterTime:         "110-130",
				RekeyTimeout:           "5",
				RejectAfterTime:        "180",
				KeepaliveTimeout:       "8-12",
				MaxHandshakeAttempts:   "15-20",
				RandomTrailers:         true,
			},
			expect: "s1=20\ni5=<t>\n" +
				"header_protection_key=000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n" +
				"content_padding_addition=16-64\n" +
				"rekey_after_time=110-130\nrekey_timeout=5\nreject_after_time=180\n" +
				"keepalive_timeout=8-12\nmax_handshake_attempts=15-20\n" +
				"random_trailers=true\n",
		},
		{
			name: "a single 3.1 key alone",
			params: &AmneziaParameters{
				KeepaliveTimeout: "15",
			},
			expect: "keepalive_timeout=15\n",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var cfg strings.Builder
			writeAmneziaParams(&cfg, testCase.params)

			if cfg.String() != testCase.expect {
				t.Errorf("got %q, want %q", cfg.String(), testCase.expect)
			}
		})
	}
}

func TestClientIPC(t *testing.T) {
	conf := &DeviceConfig{
		SecretKey: "aa",
		Peers: []*PeerConfig{{
			PublicKey:    "bb",
			PreSharedKey: "cc",
			Endpoint:     "1.2.3.4:51820",
			AllowedIps:   []string{"0.0.0.0/0", "::/0"},
			KeepAlive:    "25",
		}},
		Parameters: &AmneziaParameters{Jc: "3", S4: "25"},
	}
	// The text a config without the 3.1 keys produced before they existed.
	old := "private_key=aa\njc=3\ns4=25\n" +
		"public_key=bb\npreshared_key=cc\nendpoint=1.2.3.4:51820\n" +
		"allowed_ip=0.0.0.0/0\nallowed_ip=::/0\npersistent_keepalive_interval=25\n"
	if got := clientIPC(conf); got != old {
		t.Errorf("got %q, want %q", got, old)
	}

	conf.Peers[0].KeepAlive = "25-35"
	conf.Peers[0].ProtocolVersion = "1"
	conf.Parameters.RekeyTimeout = "4-6"
	want := "private_key=aa\njc=3\ns4=25\nrekey_timeout=4-6\n" +
		"public_key=bb\nprotocol_version=1\npreshared_key=cc\nendpoint=1.2.3.4:51820\n" +
		"allowed_ip=0.0.0.0/0\nallowed_ip=::/0\npersistent_keepalive_interval=25-35\n"
	if got := clientIPC(conf); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestDeviceTakesAmnezia31Keys hands the IPC text of every 3.1 key to a
// device and reads it back, so the names and formats are those the device
// knows.
func TestDeviceTakesAmnezia31Keys(t *testing.T) {
	priv, _ := testKeys(t)
	_, peer := testKeys(t)
	tun, _, err := CreateNetTUN([]netip.Addr{netip.MustParseAddr("10.9.0.2")}, nil, 1420, nil)
	if err != nil {
		t.Fatal(err)
	}
	dev := device.NewDevice(tun, &bind{
		listenFunc: func(context.Context) (net.PacketConn, error) { return stdnet.ListenPacket("udp", "127.0.0.1:0") },
	}, device.NewLogger(device.LogLevelSilent, ""))
	defer dev.Close()

	params := &AmneziaParameters{
		S1: "20", S2: "78", S3: "15", S4: "12",
		HeaderProtectionKey:    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
		ContentPaddingAddition: "16-64",
		RekeyAfterTime:         "110-130",
		RekeyTimeout:           "5",
		RejectAfterTime:        "180",
		KeepaliveTimeout:       "8-12",
		MaxHandshakeAttempts:   "15-20",
	}
	conf := &DeviceConfig{
		SecretKey: priv,
		Peers: []*PeerConfig{{
			PublicKey:       peer,
			Endpoint:        "127.0.0.1:1",
			AllowedIps:      []string{"10.9.0.1/32"},
			KeepAlive:       "25-35",
			ProtocolVersion: "1",
		}},
		Parameters: params,
	}
	if err := dev.IpcSet(clientIPC(conf)); err != nil {
		t.Fatal(err)
	}
	got, err := dev.IpcGet()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		"header_protection_key=" + params.HeaderProtectionKey,
		"content_padding_addition=16-64",
		"rekey_after_time=110-130",
		"rekey_timeout=5",
		"reject_after_time=180",
		"keepalive_timeout=8-12",
		"max_handshake_attempts=15-20",
		"persistent_keepalive_interval=25-35",
	} {
		if !strings.Contains(got, line+"\n") {
			t.Errorf("the device has no %q:\n%s", line, got)
		}
	}

	// The device refuses any other protocol version.
	conf.Peers[0].ProtocolVersion = "2"
	if err := dev.IpcSet(clientIPC(conf)); err == nil {
		t.Error("the device took protocol_version=2")
	}
}
