package health

import (
	"fmt"
	"github.com/xtls/xray-core/core"
	"strings"
)

// APIVersion is the public adaptive configuration API version: it grows when
// the meaning of the config changes and an old core would misread it.
const APIVersion = 2

// ConfigRevision grows with every key or value the config gained within
// APIVersion. A core rejects a config with keys it does not know, so the
// backend sends a client only what its revision has; a new one adds a line to
// the table of revisions in PROVIDER.md.
const ConfigRevision = 2

// AdaptiveHeader returns the core-owned RFC 8941 dictionary. Hosts use the
// same pair on connect; providers always send it, including GET requests.
func AdaptiveHeader() (name, value string) {
	version := strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(core.Version())
	return "Xray-Adaptive", fmt.Sprintf(`xray="%s", api=%d, rev=%d, provider=%d`, version, APIVersion, ConfigRevision, providerProtocol)
}
