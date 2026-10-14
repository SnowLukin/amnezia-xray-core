package health

import (
	"fmt"
	"github.com/xtls/xray-core/core"
	"strings"
)

// APIVersion is the public adaptive configuration API version.
const APIVersion = 2

// AdaptiveHeader returns the core-owned RFC 8941 dictionary. Hosts use the
// same pair on connect; providers always send it, including GET requests.
func AdaptiveHeader() (name, value string) {
	version := strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(core.Version())
	return "Xray-Adaptive", fmt.Sprintf(`xray="%s", api=%d, provider=%d`, version, APIVersion, providerProtocol)
}
