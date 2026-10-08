//go:build !linux

package wireguard

import (
	"errors"
	"net/netip"

	"github.com/amnezia-vpn/amneziawg-go/v3/tun"
)

func createKernelTun(name string, _, _ []netip.Addr, _ int) (tdev tun.Device, tnet *Net, _ string, err error) {
	return nil, nil, name, errors.New("not implemented")
}

func releaseKernelTunName(string) {}

func KernelTunSupported() (bool, error) {
	return false, nil
}
