package wireguard

import (
	"strings"
)

// The AmneziaWG keys of the IPC config are written here, apart from
// client.go and server.go, so that those keep following upstream.

// writeAmneziaParams appends AmneziaWG parameters to the device IPC config.
// Must run before the first public_key: the device section ends at the first peer.
func writeAmneziaParams(cfg *strings.Builder, p *AmneziaParameters) {
	if p == nil {
		return
	}

	// Unset values and flags are not written, so older configs produce the
	// same IPC text and the device keeps its defaults.
	for _, param := range []struct{ key, value string }{
		{"jc", p.Jc}, {"jmin", p.Jmin}, {"jmax", p.Jmax},
		{"s1", p.S1}, {"s2", p.S2}, {"s3", p.S3}, {"s4", p.S4},
		{"h1", p.H1}, {"h2", p.H2}, {"h3", p.H3}, {"h4", p.H4},
		{"i1", p.I1}, {"i2", p.I2}, {"i3", p.I3}, {"i4", p.I4}, {"i5", p.I5},
		{"header_protection_key", p.HeaderProtectionKey},
		{"content_padding_addition", p.ContentPaddingAddition},
		{"rekey_after_time", p.RekeyAfterTime},
		{"rekey_timeout", p.RekeyTimeout},
		{"reject_after_time", p.RejectAfterTime},
		{"keepalive_timeout", p.KeepaliveTimeout},
		{"max_handshake_attempts", p.MaxHandshakeAttempts},
	} {
		if param.value != "" {
			cfg.WriteString(param.key + "=" + param.value + "\n")
		}
	}
	if p.RandomTrailers {
		cfg.WriteString("random_trailers=true\n")
	}
	if p.DisableCookies {
		cfg.WriteString("disable_cookies=true\n")
	}
}

// writeProtocolVersion appends the protocol version of a peer, right after
// its public_key, when the config has one; without it the text stays as it
// was before the field existed.
func writeProtocolVersion(cfg *strings.Builder, version string) {
	if version != "" {
		cfg.WriteString("protocol_version=" + version + "\n")
	}
}
