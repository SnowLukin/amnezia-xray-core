package wireguard

import (
	"context"
	"fmt"
	gonet "net"
	"net/netip"
	reflect "reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/amnezia-vpn/amneziawg-go/v3/tun"

	"github.com/amnezia-vpn/amneziawg-go/v3/device"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/dice"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/net/cnc"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/signal"
	"github.com/xtls/xray-core/common/task"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/dns"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
)

type Handler struct {
	conf          *DeviceConfig
	policyManager policy.Manager
	dns           dns.Client

	streamSettings  *internet.MemoryStreamConfig
	uplinkCounter   stats.Counter
	downlinkCounter stats.Counter

	localAddresses []netip.Addr
	kernelTun      bool

	// The stack and the device are created by the first connection, so an
	// outbound that carries none holds neither. Both are set or both are nil.
	// active counts the connections using them; Sleep releases them at zero.
	mu     sync.Mutex
	tnet   *Net
	dev    *device.Device
	active int
	closed bool

	// initDone is closed when the connection creating the stack and the
	// device is done with it, and is nil while none is. The creation runs
	// outside mu: it can take seconds when the endpoint needs DNS, and the
	// connections waiting for it must be able to give up.
	initDone chan struct{}
	// initErr fails the connections that come shortly after a failed init,
	// so that each of them does not try again.
	initErr     error
	initErrTime time.Time
	// inits counts the devices created, for tests.
	inits int
}

// initRetryDelay is how long connections fail with the error of the last init
// before one tries again.
const initRetryDelay = time.Second

// dnsServers resolve domain targets inside the tunnel.
var dnsServers = []netip.Addr{
	netip.MustParseAddr("1.1.1.1"),
	netip.MustParseAddr("1.0.0.1"),
	netip.MustParseAddr("2606:4700:4700::1111"),
	netip.MustParseAddr("2606:4700:4700::1001"),
}

func NewClient(ctx context.Context, conf *DeviceConfig) (*Handler, error) {
	v := core.MustFromContext(ctx)
	p := v.GetFeature(policy.ManagerType()).(policy.Manager)
	d := v.GetFeature(dns.ClientType()).(dns.Client)

	streamSettings := session.StreamSettingsFromContext(ctx).(*internet.MemoryStreamConfig)
	tag := session.FullHandlerFromContext(ctx).Tag()
	var uplinkCounter stats.Counter
	var downlinkCounter stats.Counter
	if len(tag) > 0 && p.ForSystem().Stats.OutboundUplink {
		statsManager := v.GetFeature(stats.ManagerType()).(stats.Manager)
		name := "outbound>>>" + tag + ">>>traffic>>>uplink"
		c, _ := statsManager.GetOrRegisterCounter(name)
		if c != nil {
			uplinkCounter = c
		}
	}
	if len(tag) > 0 && p.ForSystem().Stats.OutboundDownlink {
		statsManager := v.GetFeature(stats.ManagerType()).(stats.Manager)
		name := "outbound>>>" + tag + ">>>traffic>>>downlink"
		c, _ := statsManager.GetOrRegisterCounter(name)
		if c != nil {
			downlinkCounter = c
		}
	}

	if len(conf.Peers) == 0 {
		return nil, errors.New("empty peers")
	}
	for _, peer := range conf.Peers {
		if peer.PublicKey == "" {
			return nil, errors.New("peer without publickey")
		}
		if peer.Endpoint == "" {
			return nil, errors.New("peer without endpoint")
		}
	}

	localAddresses := make([]netip.Addr, 0, len(conf.Endpoint))
	for _, localaddress := range conf.Endpoint {
		addr, err := netip.ParseAddr(localaddress)
		if err == nil {
			localAddresses = append(localAddresses, addr)
			continue
		}
		prefix, err := netip.ParsePrefix(localaddress)
		if err == nil {
			localAddresses = append(localAddresses, prefix.Addr())
			continue
		}
		return nil, err
	}

	kernelTunSupported, err := KernelTunSupported()
	if err != nil {
		errors.LogWarningInner(context.Background(), err, "Failed to check kernel TUN support")
	}
	kernelTun := !conf.NoKernelTun && kernelTunSupported
	if kernelTun {
		errors.LogWarning(context.Background(), "Using kernel TUN")
	} else {
		errors.LogWarning(context.Background(), "Using gVisor TUN")
		// The gVisor stack refuses an address it already has. It is created
		// on first use now, so check here to keep failing at load.
		for i, addr := range localAddresses {
			if slices.Contains(localAddresses[:i], addr) {
				return nil, errors.New("duplicate address ", addr)
			}
		}
	}

	return &Handler{
		conf:          conf,
		policyManager: p,
		dns:           d,

		streamSettings:  streamSettings,
		uplinkCounter:   uplinkCounter,
		downlinkCounter: downlinkCounter,

		localAddresses: localAddresses,
		kernelTun:      kernelTun,
	}, nil
}

// Process implements proxy.Outbound.Process.
func (h *Handler) Process(ctx context.Context, link *transport.Link, dialer internet.Dialer) error {
	outbounds := session.OutboundsFromContext(ctx)
	ob := outbounds[len(outbounds)-1]
	if !ob.Target.IsValid() {
		return errors.New("target not specified")
	}
	ob.Name = "wireguard"
	ob.CanSpliceCopy = 3

	tnet, err := h.acquire(ctx)
	if err != nil {
		return err
	}
	defer h.release()

	var addr netip.Addr
	if ob.Target.Address.Family().IsDomain() {
		ip, err := h.resolveRemote(tnet, ob.Target.Address.String())
		if err != nil {
			return errors.New("failed to resolve domain").Base(err)
		}
		addr, _ = netip.AddrFromSlice(ip)
	} else {
		addr, _ = netip.AddrFromSlice(ob.Target.Address.IP())
	}

	addrPort := netip.AddrPortFrom(addr, ob.Target.Port.Value())
	if !addrPort.IsValid() {
		return errors.New("invalid target ", ob.Target)
	}

	var newCtx context.Context
	var newCancel context.CancelFunc
	if session.TimeoutOnlyFromContext(ctx) {
		newCtx, newCancel = context.WithCancel(context.Background())
	}

	sessionPolicy := h.policyManager.ForLevel(0)
	ctx, cancel := context.WithCancel(ctx)
	timer := signal.CancelAfterInactivity(ctx, func() {
		cancel()
		if newCancel != nil {
			newCancel()
		}
	}, sessionPolicy.Timeouts.ConnectionIdle)

	if newCtx != nil {
		ctx = newCtx
	}

	var reader buf.Reader
	var writer buf.Writer

	switch ob.Target.Network {
	case net.Network_TCP:
		var conn net.Conn
		var err error
		if sessionPolicy.Timeouts.Handshake != 0 {
			timeoutCtx, timeoutCancel := context.WithTimeout(ctx, sessionPolicy.Timeouts.Handshake)
			conn, err = tnet.DialContextTCPAddrPort(timeoutCtx, addrPort)
			timeoutCancel()
		} else {
			conn, err = tnet.DialContextTCPAddrPort(ctx, addrPort)
		}
		if err != nil {
			return errors.New("failed to create TCP connection").Base(err)
		}
		defer conn.Close()
		reader = buf.NewReader(conn)
		writer = buf.NewWriter(conn)
	case net.Network_UDP:
		conn, err := tnet.DialUDPAddrPort(netip.AddrPort{}, addrPort)
		if err != nil {
			return errors.New("failed to create UDP connection").Base(err)
		}
		defer conn.Close()
		c := &udpConnClient{
			PacketConn: conn.(*internet.PacketConnWrapper).PacketConn,
			resolveFunc: func(host string) (net.IP, error) {
				return h.resolveRemote(tnet, host)
			},
			dest: gonet.UDPAddrFromAddrPort(addrPort),
		}
		reader = c
		writer = c
	default:
		panic(ob.Target.Network)
	}

	requestFunc := func() error {
		defer timer.SetTimeout(sessionPolicy.Timeouts.DownlinkOnly)
		return buf.Copy(link.Reader, writer, buf.UpdateActivity(timer))
	}

	responseFunc := func() error {
		defer timer.SetTimeout(sessionPolicy.Timeouts.UplinkOnly)
		return buf.Copy(reader, link.Writer, buf.UpdateActivity(timer))
	}

	responseDonePost := task.OnSuccess(responseFunc, task.Close(link.Writer))
	if err := task.Run(ctx, requestFunc, responseDonePost); err != nil {
		common.Interrupt(link.Reader)
		common.Interrupt(link.Writer)
		return errors.New("connection ends").Base(err)
	}

	return nil
}

// Close ends the connections in flight and releases the stack and the device.
// The handler accepts no connections afterwards.
func (h *Handler) Close() (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	h.closeLocked()
	return nil
}

func (h *Handler) closeLocked() {
	if h.dev != nil {
		// The device closes the stack as well.
		h.dev.Close()
	}
	h.tnet, h.dev = nil, nil
}

// acquire returns the stack for one connection, creating the stack and the
// device on first use. The stack stays until the connection calls release.
func (h *Handler) acquire(ctx context.Context) (*Net, error) {
	h.mu.Lock()
	for h.dev == nil {
		if h.closed {
			h.mu.Unlock()
			return nil, errors.New("outbound closed")
		}
		if h.initErr != nil && time.Since(h.initErrTime) < initRetryDelay {
			err := h.initErr
			h.mu.Unlock()
			return nil, err
		}
		if done := h.initDone; done != nil {
			h.mu.Unlock()
			select {
			case <-done:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			h.mu.Lock()
			continue
		}

		done := make(chan struct{})
		h.initDone = done
		h.mu.Unlock()
		// The device keeps the context for reopening its bind long after
		// this connection ends; it must not end with it. This connection
		// finishes the init even when it is canceled, for those waiting.
		tnet, dev, err := h.init(context.WithoutCancel(ctx))
		h.mu.Lock()
		h.initDone = nil
		close(done)
		if err != nil {
			h.initErr, h.initErrTime = err, time.Now()
			h.mu.Unlock()
			return nil, err
		}
		h.initErr = nil
		h.inits++
		if h.closed {
			h.mu.Unlock()
			dev.Close()
			return nil, errors.New("outbound closed")
		}
		h.tnet, h.dev = tnet, dev
	}
	h.active++
	tnet, dev := h.tnet, h.dev
	h.mu.Unlock()

	// The bind takes the device down when its socket closes unexpectedly.
	if err := dev.Up(); err != nil {
		h.release()
		return nil, err
	}
	return tnet, nil
}

// release ends the use of the stack that acquire returned.
func (h *Handler) release() {
	h.mu.Lock()
	h.active--
	h.mu.Unlock()
}

// init creates the stack and the device. Only one connection runs it at a
// time, without h.mu. On failure it keeps nothing.
func (h *Handler) init(ctx context.Context) (_ *Net, _ *device.Device, err error) {
	var t tun.Device
	var tnet *Net
	if h.kernelTun {
		t, tnet, err = createKernelTun(h.localAddresses, dnsServers, int(h.conf.Mtu))
	} else {
		t, tnet, _, err = CreateNetTUN(h.localAddresses, dnsServers, int(h.conf.Mtu), true)
	}
	if err != nil {
		return nil, nil, err
	}
	listenFunc := func() (net.PacketConn, error) {
		dest, err := net.ParseDestination("udp:" + h.conf.Peers[0].Endpoint)
		if err != nil {
			return nil, err
		}
		conn, err := internet.DialSystem(ctx, dest, h.streamSettings.SocketSettings)
		if err != nil {
			return nil, err
		}
		var pktConn net.PacketConn
		switch c := conn.(type) {
		case *internet.PacketConnWrapper:
			pktConn = c.PacketConn
		case *cnc.Connection:
			pktConn = &internet.FakePacketConn{Conn: c}
		default:
			panic(reflect.TypeOf(c))
		}
		if h.streamSettings.UdpmaskManager != nil {
			newConn, err := h.streamSettings.UdpmaskManager.WrapPacketConnClient(pktConn)
			if err != nil {
				pktConn.Close()
				return nil, errors.New("mask err").Base(err)
			}
			pktConn = newConn
		}
		if h.uplinkCounter != nil || h.downlinkCounter != nil {
			pktConn = &PacketCounterConnection{
				PacketConn:   pktConn,
				ReadCounter:  h.downlinkCounter,
				WriteCounter: h.uplinkCounter,
			}
		}
		return pktConn, nil
	}
	// The device opens the bind as soon as the stack reports itself up, which
	// can happen before NewDevice returns: fill the bind in before.
	bind := &bind{
		resolveFunc: h.resolveLocal,
		listenFunc:  listenFunc,
		reserved:    h.conf.Reserved,
	}
	logger := &device.Logger{
		Verbosef: func(format string, args ...any) {
			log.Record(&log.GeneralMessage{
				Severity: log.Severity_Debug,
				Content:  fmt.Sprintf(format, args...),
			})
		},
		Errorf: func(format string, args ...any) {
			log.Record(&log.GeneralMessage{
				Severity: log.Severity_Error,
				Content:  fmt.Sprintf(format, args...),
			})
		},
	}
	rt := newReadyTun(t)
	dev := device.NewDevice(rt, bind, logger)
	defer func() {
		if err != nil {
			dev.Close()
		}
	}()
	bind.setDownFunc(dev.Down)
	var cfg strings.Builder
	cfg.WriteString("private_key=" + h.conf.SecretKey + "\n")
	writeAmneziaParams(&cfg, h.conf.Parameters)
	for _, peer := range h.conf.Peers {
		cfg.WriteString("public_key=" + peer.PublicKey + "\n")
		if peer.PreSharedKey != "" {
			cfg.WriteString("preshared_key=" + peer.PreSharedKey + "\n")
		}
		cfg.WriteString("endpoint=" + peer.Endpoint + "\n")
		for _, ip := range peer.AllowedIps {
			cfg.WriteString("allowed_ip=" + ip + "\n")
		}
		if peer.KeepAlive != "" {
			cfg.WriteString("persistent_keepalive_interval=" + peer.KeepAlive + "\n")
		}
	}
	err = dev.IpcSet(cfg.String())
	if err != nil {
		return nil, nil, err
	}
	rt.open()
	err = dev.Up()
	if err != nil {
		return nil, nil, err
	}
	return tnet, dev, nil
}

// writeAmneziaParams appends AmneziaWG parameters to the device IPC config.
// Must run before the first public_key: the device section ends at the first peer.
func writeAmneziaParams(cfg *strings.Builder, p *AmneziaParameters) {
	if p == nil {
		return
	}

	for _, param := range []struct{ key, value string }{
		{"jc", p.Jc}, {"jmin", p.Jmin}, {"jmax", p.Jmax},
		{"s1", p.S1}, {"s2", p.S2}, {"s3", p.S3}, {"s4", p.S4},
		{"h1", p.H1}, {"h2", p.H2}, {"h3", p.H3}, {"h4", p.H4},
		{"i1", p.I1}, {"i2", p.I2}, {"i3", p.I3}, {"i4", p.I4}, {"i5", p.I5},
	} {
		if param.value != "" {
			cfg.WriteString(param.key + "=" + param.value + "\n")
		}
	}
	// Unset flags are not written, so older configs produce the same IPC text
	// and the device keeps its defaults.
	if p.RandomTrailers {
		cfg.WriteString("random_trailers=true\n")
	}
	if p.DisableCookies {
		cfg.WriteString("disable_cookies=true\n")
	}
}

func (h *Handler) resolveLocal(host string) (net.IP, error) {
	return resolveDomain(host, h.conf.DomainStrategy, func(host string) ([]net.IP, error) {
		ips, _, err := h.dns.LookupIP(host, dns.IPOption{IPv4Enable: true, IPv6Enable: true})
		return ips, err
	})
}

func (h *Handler) resolveRemote(tnet *Net, host string) (net.IP, error) {
	return resolveDomain(host, h.conf.DomainStrategy, func(host string) ([]net.IP, error) {
		addrs, err := tnet.LookupHost(host)
		if err != nil {
			return nil, err
		}
		ips := make([]net.IP, 0, len(addrs))
		for _, addr := range addrs {
			ips = append(ips, net.ParseIP(addr))
		}
		return ips, nil
	})
}

func resolveDomain(host string, strategy DeviceConfig_DomainStrategy, lookupIP func(host string) ([]net.IP, error)) (net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		return ip, nil
	}
	ips, err := lookupIP(host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, dns.ErrEmptyResponse
	}
	var got4, got6 []net.IP
	for _, ip := range ips {
		if ip.To4() != nil {
			got4 = append(got4, ip)
		} else {
			got6 = append(got6, ip)
		}
	}
	var got []net.IP
	switch strategy {
	case DeviceConfig_FORCE_IP:
		got = ips
		return ips[dice.Roll(len(ips))], nil
	case DeviceConfig_FORCE_IP4:
		got = got4
	case DeviceConfig_FORCE_IP6:
		got = got6
	case DeviceConfig_FORCE_IP46:
		got = got4
		if len(got) == 0 {
			got = got6
		}
	case DeviceConfig_FORCE_IP64:
		got = got6
		if len(got) == 0 {
			got = got4
		}
	default:
		panic(strategy)
	}
	if len(got) == 0 {
		return nil, dns.ErrEmptyResponse
	}
	return got[dice.Roll(len(got))], nil
}

type udpConnClient struct {
	net.PacketConn
	resolveFunc func(host string) (net.IP, error)
	dest        *net.UDPAddr
}

func (c *udpConnClient) ReadMultiBuffer() (buf.MultiBuffer, error) {
	b := buf.New()
	b.Resize(0, buf.Size)
	n, addr, err := c.PacketConn.ReadFrom(b.Bytes())
	if err != nil {
		b.Release()
		return nil, err
	}
	b.Resize(0, int32(n))

	b.UDP = &net.Destination{
		Address: net.IPAddress(addr.(*net.UDPAddr).IP),
		Port:    net.Port(addr.(*net.UDPAddr).Port),
		Network: net.Network_UDP,
	}

	return buf.MultiBuffer{b}, nil
}

func (c *udpConnClient) WriteMultiBuffer(mb buf.MultiBuffer) error {
	for i, b := range mb {
		dst := c.dest
		if b.UDP != nil {
			if b.UDP.Address.Family().IsDomain() {
				ip, err := c.resolveFunc(b.UDP.Address.String())
				if err != nil {
					errors.LogErrorInner(context.Background(), err, "drop packet to ", b.UDP, " with size ", len(b.Bytes()))
					b.Release()
					continue
				}
				dst = &net.UDPAddr{
					IP:   ip,
					Port: int(b.UDP.Port),
				}
			} else {
				dst = b.UDP.RawNetAddr().(*net.UDPAddr)
			}
		}
		_, err := c.PacketConn.WriteTo(b.Bytes(), dst)
		if err != nil {
			buf.ReleaseMulti(mb[i:])
			return err
		}
		b.Release()
	}
	return nil
}

type PacketCounterConnection struct {
	net.PacketConn
	ReadCounter  stats.Counter
	WriteCounter stats.Counter
}

func (c *PacketCounterConnection) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	n, addr, err = c.PacketConn.ReadFrom(p)
	if err == nil && c.ReadCounter != nil {
		c.ReadCounter.Add(int64(n))
	}
	return
}

func (c *PacketCounterConnection) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	n, err = c.PacketConn.WriteTo(p, addr)
	if err == nil && c.WriteCounter != nil {
		c.WriteCounter.Add(int64(n))
	}
	return
}
