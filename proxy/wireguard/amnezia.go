package wireguard

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
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

// ValidateAmneziaTemplate checks an I1-I5 packet template as the device reads
// it once it is created, which a client does only at its first connection.
// The tags and their arguments are those of amneziawg-go; a test compares
// them with the library.
func ValidateAmneziaTemplate(spec string) error {
	var errs []error
	for rest := spec; ; {
		start := strings.IndexByte(rest, '<')
		if start == -1 {
			break
		}
		end := strings.IndexByte(rest[start:], '>')
		if end == -1 {
			return errors.New("missing enclosing >")
		}
		end += start
		parts := strings.Fields(rest[start+1 : end])
		rest = rest[end+1:]
		if len(parts) == 0 {
			errs = append(errs, errors.New("empty tag"))
			continue
		}
		val := ""
		if len(parts) > 1 {
			val = parts[1]
		}
		if err := checkTemplateTag(parts[0], val); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func checkTemplateTag(key, val string) error {
	switch key {
	case "t", "d", "ds":
		return nil
	case "b":
		val = strings.TrimPrefix(val, "0x")
		if val == "" {
			return errors.New("<b>: empty argument")
		}
		if len(val)%2 != 0 {
			return errors.New("<b>: odd amount of symbols")
		}
		if _, err := hex.DecodeString(val); err != nil {
			return fmt.Errorf("<b>: %w", err)
		}
		return nil
	case "r", "rc", "rd", "dz":
		if _, err := strconv.Atoi(val); err != nil {
			return fmt.Errorf("<%s>: %w", key, err)
		}
		return nil
	}
	return fmt.Errorf("unknown tag <%s>", key)
}

// validateTemplates checks the I1-I5 templates of p.
func validateTemplates(p *AmneziaParameters) error {
	if p == nil {
		return nil
	}
	for _, t := range []struct{ key, spec string }{
		{"i1", p.I1}, {"i2", p.I2}, {"i3", p.I3}, {"i4", p.I4}, {"i5", p.I5},
	} {
		if err := ValidateAmneziaTemplate(t.spec); err != nil {
			return fmt.Errorf("invalid %s: %w", t.key, err)
		}
	}
	return nil
}

// writeProtocolVersion appends the protocol version of a peer, right after
// its public_key, when the config has one; without it the text stays as it
// was before the field existed.
func writeProtocolVersion(cfg *strings.Builder, version string) {
	if version != "" {
		cfg.WriteString("protocol_version=" + version + "\n")
	}
}
