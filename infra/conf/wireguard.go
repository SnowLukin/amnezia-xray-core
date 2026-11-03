package conf

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/task"
	"github.com/xtls/xray-core/proxy/wireguard"
	"google.golang.org/protobuf/proto"
)

type WireGuardPeerConfig struct {
	PublicKey    string `json:"publicKey"`
	PreSharedKey string `json:"preSharedKey"`
	Endpoint     string `json:"endpoint"`
	// KeepAlive is the persistent keepalive in seconds: a number, as it
	// always was, or a range such as "25-35" from which AmneziaWG picks
	// every interval anew.
	KeepAlive  awgParam `json:"keepAlive"`
	AllowedIPs []string `json:"allowedIPs,omitempty"`
	// ProtocolVersion is protocol_version of UAPI. AmneziaWG accepts only 1.
	ProtocolVersion awgParam `json:"protocolVersion"`

	Level uint32 `json:"level"`
	Email string `json:"email"`
}

func (c *WireGuardPeerConfig) Build() (*wireguard.PeerConfig, error) {
	var err error
	config := new(wireguard.PeerConfig)

	if c.PublicKey != "" {
		config.PublicKey, err = ParseWireGuardKey(c.PublicKey)
		if err != nil {
			return nil, err
		}
	}

	if c.PreSharedKey != "" {
		config.PreSharedKey, err = ParseWireGuardKey(c.PreSharedKey)
		if err != nil {
			return nil, err
		}
	}

	config.Endpoint = c.Endpoint
	// 0 turns the keepalive off and writes nothing, as it always did. The
	// value used to be a JSON number that the type checked, so a range is
	// checked here rather than by the device of an outbound at its first
	// connection.
	if c.KeepAlive != "" && c.KeepAlive != "0" {
		if err := checkAwgRange(string(c.KeepAlive)); err != nil {
			return nil, errors.New("invalid WireGuard keepAlive ", strconv.Quote(string(c.KeepAlive))).Base(err)
		}
		config.KeepAlive = string(c.KeepAlive)
	}
	if c.AllowedIPs == nil {
		config.AllowedIps = []string{"0.0.0.0/0", "::0/0"}
	} else {
		config.AllowedIps = c.AllowedIPs
	}
	config.ProtocolVersion = string(c.ProtocolVersion)

	return config, nil
}

// checkAwgRange checks a number or a range "lo-hi" of uint32 values, the
// format AmneziaWG reads ranges in.
func checkAwgRange(s string) error {
	lo, hi, isRange := strings.Cut(s, "-")
	l, err := strconv.ParseUint(lo, 10, 32)
	if err != nil {
		return err
	}
	if !isRange {
		return nil
	}
	h, err := strconv.ParseUint(hi, 10, 32)
	if err != nil {
		return err
	}
	if h < l {
		return errors.New("the range ends before it starts")
	}
	return nil
}

// awgParam accepts both a JSON number and a string: h1-h4 may be ranges
// ("123-456") and i1-i5 use a tag language.
type awgParam string

func (p *awgParam) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*p = awgParam(s)
		return nil
	}

	var n json.Number
	if err := json.Unmarshal(data, &n); err != nil {
		return errors.New("AmneziaWG parameter must be a number or a string")
	}
	*p = awgParam(n.String())

	return nil
}

// awgFlag accepts a JSON boolean, and also 0 or 1 as a number or a string,
// which is how awg configs and UAPI write flags.
type awgFlag bool

func (f *awgFlag) UnmarshalJSON(data []byte) error {
	var b bool
	if err := json.Unmarshal(data, &b); err == nil {
		*f = awgFlag(b)
		return nil
	}
	var p awgParam
	if err := p.UnmarshalJSON(data); err == nil {
		if b, err := strconv.ParseBool(string(p)); err == nil {
			*f = awgFlag(b)
			return nil
		}
	}
	return errors.New("AmneziaWG flag must be a boolean, 0 or 1")
}

type AmneziaParameters struct {
	Jc   awgParam `json:"jc"`
	Jmin awgParam `json:"jmin"`
	Jmax awgParam `json:"jmax"`
	S1   awgParam `json:"s1"`
	S2   awgParam `json:"s2"`
	S3   awgParam `json:"s3"`
	S4   awgParam `json:"s4"`
	H1   awgParam `json:"h1"`
	H2   awgParam `json:"h2"`
	H3   awgParam `json:"h3"`
	H4   awgParam `json:"h4"`
	I1   awgParam `json:"i1"`
	I2   awgParam `json:"i2"`
	I3   awgParam `json:"i3"`
	I4   awgParam `json:"i4"`
	I5   awgParam `json:"i5"`

	// AmneziaWG 3.1 flags, named after RandomTrailers and DisableCookies of
	// awg configs. RandomTrailers must match the server.
	RandomTrailers awgFlag `json:"randomTrailers"`
	DisableCookies awgFlag `json:"disableCookies"`

	// AmneziaWG 3.1 keys, named after those of awg configs.
	// HeaderProtectionKey is a key in base64 or hex, like the others; it must
	// match the server and needs s1-s4 of at least 12. The rest are numbers
	// or ranges: of bytes for ContentPaddingAddition, of seconds for the
	// timings, of attempts for MaxHandshakeAttempts.
	HeaderProtectionKey    string   `json:"headerProtectionKey"`
	ContentPaddingAddition awgParam `json:"contentPaddingAddition"`
	RekeyAfterTime         awgParam `json:"rekeyAfterTime"`
	RekeyTimeout           awgParam `json:"rekeyTimeout"`
	RejectAfterTime        awgParam `json:"rejectAfterTime"`
	KeepaliveTimeout       awgParam `json:"keepaliveTimeout"`
	MaxHandshakeAttempts   awgParam `json:"maxHandshakeAttempts"`
}

// awgHeaderNonceSize is the least s1-s4 with header protection: the device
// takes the nonce of every message from its padding.
const awgHeaderNonceSize = 12

func (c *AmneziaParameters) Build() (*wireguard.AmneziaParameters, error) {
	if c == nil {
		return nil, nil
	}

	var headerKey string
	if c.HeaderProtectionKey != "" {
		var err error
		if headerKey, err = ParseWireGuardKey(c.HeaderProtectionKey); err != nil {
			return nil, errors.New("invalid AmneziaWG headerProtectionKey").Base(err)
		}
		// The device refuses the key otherwise, and an outbound would find
		// out only at its first connection.
		for i, s := range []awgParam{c.S1, c.S2, c.S3, c.S4} {
			if n, err := strconv.ParseUint(string(s), 10, 16); err != nil || n < awgHeaderNonceSize {
				return nil, errors.New("AmneziaWG headerProtectionKey needs s", i+1, " of at least ",
					awgHeaderNonceSize, ", not ", strconv.Quote(string(s)))
			}
		}
	}

	return &wireguard.AmneziaParameters{
		Jc:   string(c.Jc),
		Jmin: string(c.Jmin),
		Jmax: string(c.Jmax),
		S1:   string(c.S1),
		S2:   string(c.S2),
		S3:   string(c.S3),
		S4:   string(c.S4),
		H1:   string(c.H1),
		H2:   string(c.H2),
		H3:   string(c.H3),
		H4:   string(c.H4),
		I1:   string(c.I1),
		I2:   string(c.I2),
		I3:   string(c.I3),
		I4:   string(c.I4),
		I5:   string(c.I5),

		RandomTrailers: bool(c.RandomTrailers),
		DisableCookies: bool(c.DisableCookies),

		HeaderProtectionKey:    headerKey,
		ContentPaddingAddition: string(c.ContentPaddingAddition),
		RekeyAfterTime:         string(c.RekeyAfterTime),
		RekeyTimeout:           string(c.RekeyTimeout),
		RejectAfterTime:        string(c.RejectAfterTime),
		KeepaliveTimeout:       string(c.KeepaliveTimeout),
		MaxHandshakeAttempts:   string(c.MaxHandshakeAttempts),
	}, nil
}

type WireGuardConfig struct {
	IsClient bool `json:""`

	NoKernelTun bool                   `json:"noKernelTun"`
	SecretKey   string                 `json:"secretKey"`
	Address     []string               `json:"address"`
	Peers       []*WireGuardPeerConfig `json:"peers"`
	MTU         int32                  `json:"mtu"`
	Reserved    []byte                 `json:"reserved"`
	DNS         []string               `json:"remoteDNS"`
	Parameters  *AmneziaParameters     `json:"awg"`
}

func (c *WireGuardConfig) Build() (proto.Message, error) {
	config := new(wireguard.DeviceConfig)

	var err error
	config.SecretKey, err = ParseWireGuardKey(c.SecretKey)
	if err != nil {
		return nil, errors.New("invalid WireGuard secret key: %w", err)
	}

	if c.Address == nil {
		// bogon ips
		config.Endpoint = []string{"10.0.0.1", "fd59:7153:2388:b5fd:0000:0000:0000:0001"}
	} else {
		config.Endpoint = c.Address
	}

	if c.IsClient {
		config.Peers = make([]*wireguard.PeerConfig, len(c.Peers))
		for i, p := range c.Peers {
			msg, err := p.Build()
			if err != nil {
				return nil, err
			}
			config.Peers[i] = msg
		}
	} else {
		config.Users = make([]*protocol.User, len(c.Peers))
		processUser := func(idx int) error {
			p := c.Peers[idx]
			m, err := p.Build()
			if err != nil {
				return err
			}
			config.Users[idx] = &protocol.User{
				Email:   p.Email,
				Level:   p.Level,
				Account: serial.ToTypedMessage(m),
			}
			return nil
		}
		if err := task.ParallelForN(len(c.Peers), processUser); err != nil {
			return nil, err
		}
	}

	if c.MTU == 0 {
		config.Mtu = 1420
	} else {
		config.Mtu = c.MTU
	}

	if len(c.Reserved) != 0 && len(c.Reserved) != 3 {
		return nil, errors.New(`"reserved" should be empty or 3 bytes`)
	}
	config.Reserved = c.Reserved

	config.IsClient = c.IsClient
	config.NoKernelTun = c.NoKernelTun
	config.DNS = c.DNS
	if config.Parameters, err = c.Parameters.Build(); err != nil {
		return nil, err
	}
	if !c.IsClient && c.Parameters != nil {
		errors.LogWarning(context.Background(), "the WireGuard inbound ignores awg parameters")
	}

	return config, nil
}

func ParseWireGuardKey(str string) (string, error) {
	var err error

	if str == "" {
		return "", errors.New("key must not be empty")
	}

	if len(str) == 64 {
		_, err = hex.DecodeString(str)
		if err == nil {
			return str, nil
		}
	}

	var dat []byte
	str = strings.TrimSuffix(str, "=")
	if strings.ContainsRune(str, '+') || strings.ContainsRune(str, '/') {
		dat, err = base64.RawStdEncoding.DecodeString(str)
	} else {
		dat, err = base64.RawURLEncoding.DecodeString(str)
	}
	if err == nil {
		return hex.EncodeToString(dat), nil
	}

	return "", errors.New("failed to deserialize key").Base(err)
}
