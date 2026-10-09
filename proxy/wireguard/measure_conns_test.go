package wireguard

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	stdnet "net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amnezia-vpn/amneziawg-go/v3/device"
	applog "github.com/xtls/xray-core/app/log"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy/freedom"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
)

// The measurements of connections through an outbound run against a server
// in a process of its own, so that the stack and the device of the far end do
// not count in the memory of the client. The server answers TCP on
// measureTargetPort of testServerAddr inside the tunnel, and the same way on
// loopback outside it for the freedom outbound the numbers are compared with.
//
// Each measures the stack CreateNetTUN creates (awg) against the same stack
// set back to the TCP options of gVisor (awg-gvisor) once the outbound is up,
// alternately, so that both see the same load of the machine.
//
//	AWG_MEASURE_CONNS=1       TestMeasureConns
//	AWG_MEASURE_THROUGHPUT=1  TestMeasureThroughput
//	AWG_MEASURE_N=100,250     connections per round
//	AWG_MEASURE_MODES=idle,down,up
//	AWG_MEASURE_PATHS=awg,awg-gvisor,direct
//	AWG_MEASURE_PROFILE=dir   heap profiles of each round, every allocation
//	AWG_MEASURE_SETTLE=70s    how long a round waits for closed endpoints
//	AWG_MEASURE_DELAYS=0,10,25,50  one-way delays of the throughput runs, ms
//	AWG_MEASURE_SIZES=64      MiB per transfer
//	AWG_MEASURE_RUNS=5        transfers per size, delay and stack

const measureTargetPort = 80

// measurePipeLimit is the pipe the dispatcher gives a connection on arm64,
// where phones run: the policy buffer is 4 KiB there.
const measurePipeLimit = 4 << 10

type measureChildConfig struct {
	PeerPub  string
	PeerAddr string
	Delays   []int
}

type measureChildInfo struct {
	Endpoint string
	Pub      string
	Direct   string
	Relays   map[int]string
}

// TestMeasureServer is the far end of the measurements; the parent starts it
// in a process of its own. Its TCP stack keeps the gVisor defaults whatever
// CreateNetTUN sets, so that the sender stays the same while the client is
// tuned.
func TestMeasureServer(t *testing.T) {
	raw := os.Getenv("AWG_MEASURE_CHILD")
	if raw == "" {
		t.Skip("started by the measurements")
	}
	var cfg measureChildConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	conn, err := stdnet.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	setSocketBuffers(conn)
	tun, _, stk, err := CreateNetTUN([]netip.Addr{testServerAddr}, nil, 1420, false)
	if err != nil {
		t.Fatal(err)
	}
	setGVisorTCPDefaults(t, stk)
	priv, pub := testKeys(t)
	rt := newReadyTun(tun)
	dev := device.NewDevice(rt, &bind{
		listenFunc: func() (net.PacketConn, error) { return conn, nil },
	}, device.NewLogger(device.LogLevelSilent, ""))
	var ipc strings.Builder
	ipc.WriteString("private_key=" + priv + "\n")
	writeAmneziaParams(&ipc, testParams)
	ipc.WriteString("public_key=" + cfg.PeerPub + "\n")
	addr := netip.MustParseAddr(cfg.PeerAddr)
	ipc.WriteString("allowed_ip=" + netip.PrefixFrom(addr, addr.BitLen()).String() + "\n")
	if err := dev.IpcSet(ipc.String()); err != nil {
		t.Fatal(err)
	}
	rt.open()
	if err := dev.Up(); err != nil {
		t.Fatal(err)
	}

	full := tcpip.FullAddress{NIC: 1, Addr: tcpip.AddrFrom4(testServerAddr.As4()), Port: measureTargetPort}
	inside, err := gonet.ListenTCP(stk, full, ipv4.ProtocolNumber)
	if err != nil {
		t.Fatal(err)
	}
	go serveMeasureTarget(inside)
	outside, err := stdnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go serveMeasureTarget(outside)

	info := measureChildInfo{Endpoint: conn.LocalAddr().String(), Pub: pub, Direct: outside.Addr().String(), Relays: map[int]string{}}
	for _, d := range cfg.Delays {
		info.Relays[d] = startDelayRelay(t, info.Endpoint, time.Duration(d)*time.Millisecond)
	}
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("READY " + string(b))
	// The parent closes stdin when it is done, or dies.
	io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}

// setGVisorTCPDefaults sets the TCP options CreateNetTUN may tune back to
// what a new gVisor stack has.
func setGVisorTCPDefaults(t testing.TB, stk *stack.Stack) {
	t.Helper()
	opts := []tcpip.SettableTransportProtocolOption{
		&tcpip.TCPReceiveBufferSizeRangeOption{Min: tcp.MinBufferSize, Default: tcp.DefaultReceiveBufferSize, Max: tcp.MaxBufferSize},
		&tcpip.TCPSendBufferSizeRangeOption{Min: tcp.MinBufferSize, Default: tcp.DefaultSendBufferSize, Max: tcp.MaxBufferSize},
		new(tcpip.TCPModerateReceiveBufferOption(true)),
		new(tcpip.TCPTimeWaitTimeoutOption(tcp.DefaultTCPTimeWaitTimeout)),
		new(tcpip.TCPLingerTimeoutOption(tcp.DefaultTCPLingerTimeout)),
	}
	for _, o := range opts {
		if err := stk.SetTransportProtocolOption(tcp.ProtocolNumber, o); err != nil {
			t.Fatalf("%T: %v", o, err)
		}
	}
}

// setSocketBuffers lets a loopback socket take the bursts of a transfer
// without dropping, which would measure the socket rather than the stack.
func setSocketBuffers(c any) {
	type buffered interface {
		SetReadBuffer(int) error
		SetWriteBuffer(int) error
	}
	if b, ok := c.(buffered); ok {
		b.SetReadBuffer(4 << 20)
		b.SetWriteBuffer(4 << 20)
	}
}

func serveMeasureTarget(l stdnet.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			return
		}
		go serveMeasureConn(c)
	}
}

// serveMeasureConn answers one request line:
//
//	echo     echoes what follows
//	send N   sends N bytes, then reads until the client closes
//	recv N   reads N bytes and answers "ok\n"
//	hold S   answers "ok\n", reads nothing for S seconds, then reads until
//	         the client closes
func serveMeasureConn(c stdnet.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	line, err := r.ReadString('\n')
	if err != nil {
		return
	}
	f := strings.Fields(line)
	switch {
	case len(f) == 1 && f[0] == "echo":
		io.Copy(c, r)
	case len(f) == 2 && f[0] == "send":
		n, err := strconv.Atoi(f[1])
		if err != nil {
			return
		}
		chunk := make([]byte, 64<<10)
		for n > 0 {
			k := min(n, len(chunk))
			if _, err := c.Write(chunk[:k]); err != nil {
				return
			}
			n -= k
		}
		io.Copy(io.Discard, r)
	case len(f) == 2 && f[0] == "recv":
		n, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil {
			return
		}
		if _, err := io.CopyN(io.Discard, r, n); err != nil {
			return
		}
		c.Write([]byte("ok\n"))
		io.Copy(io.Discard, r)
	case len(f) == 2 && f[0] == "hold":
		s, err := strconv.Atoi(f[1])
		if err != nil {
			return
		}
		c.Write([]byte("ok\n"))
		time.Sleep(time.Duration(s) * time.Second)
		io.Copy(io.Discard, r)
	}
}

// startDelayRelay forwards datagrams between a client and server after
// delay each way, for a round trip time like that of a phone, and returns
// the address the client sends to.
func startDelayRelay(t testing.TB, server string, delay time.Duration) string {
	t.Helper()
	front, err := stdnet.ListenUDP("udp", &stdnet.UDPAddr{IP: stdnet.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	saddr, err := stdnet.ResolveUDPAddr("udp", server)
	if err != nil {
		t.Fatal(err)
	}
	back, err := stdnet.DialUDP("udp", nil, saddr)
	if err != nil {
		t.Fatal(err)
	}
	setSocketBuffers(front)
	setSocketBuffers(back)
	type datagram struct {
		due time.Time
		b   []byte
	}
	var client atomic.Pointer[stdnet.UDPAddr]
	up := make(chan datagram, 1<<16)
	down := make(chan datagram, 1<<16)
	// A full queue drops, as a router would.
	go func() {
		for {
			b := make([]byte, 4096)
			n, addr, err := front.ReadFromUDP(b)
			if err != nil {
				return
			}
			client.Store(addr)
			select {
			case up <- datagram{time.Now().Add(delay), b[:n]}:
			default:
			}
		}
	}()
	go func() {
		for {
			b := make([]byte, 4096)
			n, err := back.Read(b)
			if err != nil {
				return
			}
			select {
			case down <- datagram{time.Now().Add(delay), b[:n]}:
			default:
			}
		}
	}()
	go func() {
		for d := range up {
			time.Sleep(time.Until(d.due))
			back.Write(d.b)
		}
	}()
	go func() {
		for d := range down {
			time.Sleep(time.Until(d.due))
			if a := client.Load(); a != nil {
				front.WriteToUDP(d.b, a)
			}
		}
	}()
	return front.LocalAddr().String()
}

// startMeasureServer starts TestMeasureServer in a process of its own, with
// a peer of pub at addr.
func startMeasureServer(t testing.TB, pub string, addr netip.Addr, delays []int) measureChildInfo {
	t.Helper()
	cfg, err := json.Marshal(measureChildConfig{PeerPub: pub, PeerAddr: addr.String(), Delays: delays})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestMeasureServer$", "-test.timeout=0")
	cmd.Env = append(os.Environ(), "AWG_MEASURE_CHILD="+string(cfg))
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stdin.Close()
		cmd.Wait()
	})
	ready := make(chan measureChildInfo, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if line, ok := strings.CutPrefix(sc.Text(), "READY "); ok {
				var info measureChildInfo
				if json.Unmarshal([]byte(line), &info) == nil {
					ready <- info
				}
				break
			}
		}
		io.Copy(io.Discard, stdout)
	}()
	select {
	case info := <-ready:
		return info
	case <-time.After(30 * time.Second):
		t.Fatal("the measurement server did not start")
		return measureChildInfo{}
	}
}

// measureConn is one connection dispatched to an outbound with the pipes of
// a phone.
type measureConn struct {
	*testConn
}

func dialMeasured(oh outbound.Handler, dest net.Destination) measureConn {
	ctx, cancel := context.WithCancel(context.Background())
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{Target: dest, Tag: oh.Tag()}})
	upReader, upWriter := pipe.New(pipe.WithSizeLimit(measurePipeLimit))
	downReader, downWriter := pipe.New(pipe.WithSizeLimit(measurePipeLimit))
	c := &testConn{up: upWriter, down: downReader, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(c.done)
		oh.Dispatch(ctx, &transport.Link{Reader: upReader, Writer: downWriter})
	}()
	return measureConn{c}
}

func (c measureConn) send(s string) error {
	return c.up.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte(s))})
}

// sendBytes sends n bytes; it blocks while the outbound does not take them.
func (c measureConn) sendBytes(n int) error {
	for n > 0 {
		b := buf.New()
		k := min(n, int(b.Cap()))
		b.Extend(int32(k))
		if err := c.up.WriteMultiBuffer(buf.MultiBuffer{b}); err != nil {
			return err
		}
		n -= k
	}
	return nil
}

// read reads n bytes and returns how many came.
func (c measureConn) read(n int, timeout time.Duration) (int, error) {
	got := 0
	deadline := time.Now().Add(timeout)
	for got < n {
		mb, err := c.down.ReadMultiBufferTimeout(time.Until(deadline))
		got += int(mb.Len())
		buf.ReleaseMulti(mb)
		if err != nil {
			return got, err
		}
	}
	return got, nil
}

// end ends the connection and waits until the outbound has finished with it.
func (c measureConn) end() error {
	c.cancel()
	c.up.Close()
	c.down.Interrupt()
	return c.wait()
}

// measuredStack returns the stack of the current wake, or nil.
func (c *lazyClient) measuredStack() *stack.Stack {
	c.mu.Lock()
	h := c.h
	c.mu.Unlock()
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if rt, ok := h.tun.(*readyTun); ok {
		if nt, ok := rt.Device.(*netTun); ok {
			return nt.stack
		}
	}
	return nil
}

type memSample struct {
	goroutines                       int
	heapInuse, heapAlloc, stackInuse uint64
	// endpoints counts the TCP endpoints of the stack by state.
	endpoints map[string]int
}

func sampleMem(stk *stack.Stack) memSample {
	// Stopped stacks and devices end some goroutines asynchronously, and a
	// sync.Pool lets go of its buffers only on the second collection.
	for range 3 {
		runtime.GC()
		time.Sleep(300 * time.Millisecond)
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	s := memSample{goroutines: runtime.NumGoroutine(), heapInuse: m.HeapInuse, heapAlloc: m.HeapAlloc, stackInuse: m.StackInuse}
	if stk != nil {
		s.endpoints = map[string]int{}
		for _, ep := range stk.RegisteredEndpoints() {
			if te, ok := ep.(*tcp.Endpoint); ok {
				s.endpoints[tcp.EndpointState(te.State()).String()]++
			}
		}
	}
	return s
}

func (s memSample) tcpEndpoints() int {
	n := 0
	for _, k := range s.endpoints {
		n += k
	}
	return n
}

func (s memSample) states() string {
	if s.endpoints == nil {
		return "-"
	}
	var parts []string
	for k, v := range s.endpoints {
		parts = append(parts, k+" "+strconv.Itoa(v))
	}
	slices.Sort(parts)
	return strings.Join(parts, ", ")
}

func logSample(t testing.TB, label string, s, base memSample, n int) {
	t.Helper()
	kib := func(v, b uint64) float64 { return float64(int64(v)-int64(b)) / 1024 }
	per := func(v, b uint64) string {
		if n == 0 {
			return ""
		}
		return fmt.Sprintf(" (%5.1f/conn)", kib(v, b)/float64(n))
	}
	t.Logf("%-22s goroutines %+5d  HeapInuse %+9.0f KiB%s  HeapAlloc %+9.0f KiB%s  StackInuse %+6.0f KiB  endpoints: %s",
		label, s.goroutines-base.goroutines,
		kib(s.heapInuse, base.heapInuse), per(s.heapInuse, base.heapInuse),
		kib(s.heapAlloc, base.heapAlloc), per(s.heapAlloc, base.heapAlloc),
		kib(s.stackInuse, base.stackInuse), s.states())
}

func writeHeapProfile(t testing.TB, dir, name string) {
	t.Helper()
	if dir == "" {
		return
	}
	runtime.GC()
	f, err := os.Create(filepath.Join(dir, name+".pb.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := pprof.WriteHeapProfile(f); err != nil {
		t.Fatal(err)
	}
}

func envList(name, def string) []string {
	v := os.Getenv(name)
	if v == "" {
		v = def
	}
	return strings.Split(v, ",")
}

func envInts(t testing.TB, name, def string) []int {
	t.Helper()
	var out []int
	for _, s := range envList(name, def) {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		out = append(out, n)
	}
	return out
}

// TestMeasureConns logs what N connections through an AmneziaWG outbound
// hold, against N through freedom over loopback. Each connection sends a
// request line and then stays open:
//
//	idle  gets 1 KiB, which it reads
//	down  gets 4 MiB, more than any window, of which it reads nothing; the
//	      data waits in the outbound as it would for a slow app
//	up    sends 4 MiB to a server that reads nothing for 30 s; the data
//	      waits in the outbound as it would on a slow uplink
//
// The connections then end from the client side, as an app that hangs up
// does.
func TestMeasureConns(t *testing.T) {
	if os.Getenv("AWG_MEASURE_CONNS") != "1" {
		t.Skip("set AWG_MEASURE_CONNS=1 to measure connections through an outbound")
	}
	profileDir := os.Getenv("AWG_MEASURE_PROFILE")
	if profileDir != "" {
		runtime.MemProfileRate = 1
		if err := os.MkdirAll(profileDir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	settle := 70 * time.Second
	if s := os.Getenv("AWG_MEASURE_SETTLE"); s != "" {
		var err error
		if settle, err = time.ParseDuration(s); err != nil {
			t.Fatal(err)
		}
	}
	ns := envInts(t, "AWG_MEASURE_N", "100,250")
	modes := envList("AWG_MEASURE_MODES", "idle,down,up")
	paths := envList("AWG_MEASURE_PATHS", "awg,awg-gvisor,direct")

	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	srv := startMeasureServer(t, pub, addr, nil)
	inst := newMeasureInstance(t)
	direct := addFreedom(t, inst, "direct")
	directPort, err := strconv.Atoi(srv.Direct[strings.LastIndexByte(srv.Direct, ':')+1:])
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("NumCPU %d, GOMAXPROCS %d, pipe limit %d; deltas against the start of each round",
		runtime.NumCPU(), runtime.GOMAXPROCS(0), measurePipeLimit)

	round := 0
	for _, mode := range modes {
		for _, n := range ns {
			for _, path := range paths {
				round++
				label := fmt.Sprintf("%s %s N=%d", path, mode, n)
				var oh outbound.Handler
				var lc *lazyClient
				var dest net.Destination
				switch path {
				case "awg", "awg-gvisor":
					// A fresh outbound each round: a fresh stack, without
					// the endpoints the previous round left.
					oh, lc = addTestClientConf(t, inst, "awg"+strconv.Itoa(round),
						testClientConf(&testServer{endpoint: srv.Endpoint, pub: srv.Pub}, priv, addr, testParams))
					dest = net.TCPDestination(net.IPAddress(testServerAddr.AsSlice()), measureTargetPort)
				case "direct":
					oh = direct
					dest = net.TCPDestination(net.LocalHostIP, net.Port(directPort))
				default:
					t.Fatalf("unknown path %q", path)
				}
				measureRound(t, label, oh, lc, path == "awg-gvisor", dest, mode, n, profileDir, settle)
				if lc != nil {
					lc.Close()
					time.Sleep(handshakeGap)
				}
			}
		}
	}
}

// warmUp wakes the outbound of oh with one echo; with gvisor it then sets the
// stack back to the TCP options of gVisor, which the connections created
// afterwards get. The first handshake through a loaded machine is sometimes
// lost, and the device tries again only after 5 s, past the dial timeout.
func warmUp(t testing.TB, oh outbound.Handler, lc *lazyClient, gvisor bool, dest net.Destination) {
	t.Helper()
	var err error
	for range 3 {
		c := dialMeasured(oh, dest)
		if err = c.send("echo\nping"); err == nil {
			_, err = c.read(4, 10*time.Second)
		}
		c.end()
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("warm-up: %v", err)
	}
	if gvisor {
		setGVisorTCPDefaults(t, lc.measuredStack())
	}
}

// newMeasureInstance returns an instance that logs warnings, as an app does:
// the log of every connection would be measured too.
func newMeasureInstance(t testing.TB) *core.Instance {
	t.Helper()
	inst, err := core.New(&core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&applog.Config{ErrorLogType: applog.LogType_Console, ErrorLogLevel: log.Severity_Warning}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { inst.Close() })
	return inst
}

func addFreedom(t testing.TB, inst *core.Instance, tag string) outbound.Handler {
	t.Helper()
	err := core.AddOutboundHandler(inst, &core.OutboundHandlerConfig{
		Tag:            tag,
		SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{}),
		ProxySettings:  serial.ToTypedMessage(&freedom.Config{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return inst.GetFeature(outbound.ManagerType()).(outbound.Manager).GetHandler(tag)
}

func measureRound(t *testing.T, label string, oh outbound.Handler, lc *lazyClient, gvisor bool, dest net.Destination, mode string, n int, profileDir string, settle time.Duration) {
	t.Helper()
	// One connection first wakes the outbound: its stack and device are not
	// what a connection costs.
	warmUp(t, oh, lc, gvisor, dest)
	var stk *stack.Stack
	if lc != nil {
		stk = lc.measuredStack()
	}
	base := sampleMem(stk)
	name := strings.ReplaceAll(label, " ", "-")
	writeHeapProfile(t, profileDir, name+"-0base")

	var req string
	var want, up int
	switch mode {
	case "idle":
		req, want = "send 1024\n", 1024
	case "down":
		req = "send 4194304\n"
	case "up":
		req, want, up = "hold 30\n", 3, 4<<20
	default:
		t.Fatalf("unknown mode %q", mode)
	}
	stopPeak := samplePeak()
	conns := make([]measureConn, n)
	var wg sync.WaitGroup
	var failed atomic.Int32
	start := time.Now()
	for i := range conns {
		conns[i] = dialMeasured(oh, dest)
		wg.Go(func() {
			c := conns[i]
			if err := c.send(req); err != nil {
				failed.Add(1)
				return
			}
			if want > 0 {
				if _, err := c.read(want, 30*time.Second); err != nil {
					failed.Add(1)
				}
			}
		})
	}
	wg.Wait()
	if up > 0 {
		// Only once every connection is up: uploads that saturate the
		// tunnel delay the handshakes of the others past the dial timeout.
		for _, c := range conns {
			// Blocks until the connection ends.
			go c.sendBytes(up)
		}
	}
	opened := time.Since(start)
	if failed.Load() > 0 {
		t.Errorf("%s: %d connections failed", label, failed.Load())
	}
	if want == 0 || up > 0 {
		waitHeapSteady(20 * time.Second)
	}
	peak := stopPeak()
	open := sampleMem(stk)
	writeHeapProfile(t, profileDir, name+"-1open")

	for _, c := range conns {
		if err := c.end(); err != nil {
			t.Fatal(err)
		}
	}
	closed := sampleMem(stk)
	t.Logf("%s, opened in %v, HeapInuse peaked at %+.0f KiB before the collection", label, opened.Round(time.Millisecond),
		float64(int64(peak)-int64(base.heapInuse))/1024)
	logSample(t, "  open", open, base, n)
	logSample(t, "  just closed", closed, base, n)
	if stk != nil && closed.tcpEndpoints() > base.tcpEndpoints() {
		start := time.Now()
		waitEndpoints(stk, base.tcpEndpoints(), settle)
		gone := sampleMem(stk)
		logSample(t, fmt.Sprintf("  closed +%v", time.Since(start).Round(time.Second)), gone, base, n)
	}
}

// samplePeak samples HeapInuse until the returned function is called, which
// returns the highest: garbage of the burst counts too, as the process holds
// that memory until the runtime returns it.
func samplePeak() func() uint64 {
	var peak atomic.Uint64
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			if m.HeapInuse > peak.Load() {
				peak.Store(m.HeapInuse)
			}
			select {
			case <-done:
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	return func() uint64 {
		close(done)
		<-stopped
		return peak.Load()
	}
}

// waitEndpoints waits until stk has at most n TCP endpoints.
func waitEndpoints(stk *stack.Stack, n int, timeout time.Duration) {
	if stk == nil {
		return
	}
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(250 * time.Millisecond) {
		count := 0
		for _, ep := range stk.RegisteredEndpoints() {
			if _, ok := ep.(*tcp.Endpoint); ok {
				count++
			}
		}
		if count <= n {
			return
		}
	}
}

// waitHeapSteady waits until the heap stops growing: the data sent to the
// connections has arrived and waits in their buffers.
func waitHeapSteady(timeout time.Duration) {
	var last uint64
	steady := 0
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline) && steady < 4; time.Sleep(250 * time.Millisecond) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		if last > 0 && m.HeapInuse < last+last/100 {
			steady++
		} else {
			steady = 0
		}
		last = max(last, m.HeapInuse)
	}
}

// TestMeasureThroughput logs how fast one connection downloads and uploads
// through an AmneziaWG outbound, over loopback and over relays that add a
// round trip time like that of a phone.
func TestMeasureThroughput(t *testing.T) {
	if os.Getenv("AWG_MEASURE_THROUGHPUT") != "1" {
		t.Skip("set AWG_MEASURE_THROUGHPUT=1 to measure the throughput of an outbound")
	}
	delays := envInts(t, "AWG_MEASURE_DELAYS", "0,10,25,50")
	sizes := envInts(t, "AWG_MEASURE_SIZES", "64")
	runs := envInts(t, "AWG_MEASURE_RUNS", "5")[0]
	stacks := envList("AWG_MEASURE_PATHS", "awg,awg-gvisor")
	var relayed []int
	for _, d := range delays {
		if d > 0 {
			relayed = append(relayed, d)
		}
	}
	priv, pub := testKeys(t)
	addr := netip.MustParseAddr("10.9.0.2")
	srv := startMeasureServer(t, pub, addr, relayed)
	inst := newMeasureInstance(t)
	dest := net.TCPDestination(net.IPAddress(testServerAddr.AsSlice()), measureTargetPort)
	tag := 0
	for _, size := range sizes {
		for _, d := range delays {
			endpoint := srv.Endpoint
			if d > 0 {
				endpoint = srv.Relays[d]
			}
			mbps := map[string][]float64{}
			for range runs {
				for _, st := range stacks {
					tag++
					oh, lc := addTestClientConf(t, inst, "tp"+strconv.Itoa(tag),
						testClientConf(&testServer{endpoint: endpoint, pub: srv.Pub}, priv, addr, testParams))
					warmUp(t, oh, lc, st == "awg-gvisor", dest)
					for _, dir := range []string{"down", "up"} {
						mbps[st+" "+dir] = append(mbps[st+" "+dir], transfer(t, oh, dest, dir, size<<20))
					}
					lc.Close()
					time.Sleep(handshakeGap)
				}
			}
			for _, dir := range []string{"down", "up"} {
				for _, st := range stacks {
					v := mbps[st+" "+dir]
					slices.Sort(v)
					t.Logf("%3d MiB %-4s one-way delay %3d ms %-10s median %6.1f Mbit/s, max %6.1f; runs %s",
						size, dir, d, st, v[len(v)/2], v[len(v)-1], fmtFloats(v))
				}
			}
		}
	}
}

// transfer moves size bytes through a new connection, down from the server
// or up to it, and returns the speed in Mbit/s.
func transfer(t testing.TB, oh outbound.Handler, dest net.Destination, dir string, size int) float64 {
	t.Helper()
	c := dialMeasured(oh, dest)
	defer c.end()
	start := time.Now()
	var err error
	if dir == "down" {
		if err = c.send("send " + strconv.Itoa(size) + "\n"); err == nil {
			_, err = c.read(size, 120*time.Second)
		}
	} else {
		if err = c.send("recv " + strconv.Itoa(size) + "\n"); err == nil {
			go c.sendBytes(size)
			_, err = c.read(3, 120*time.Second)
		}
	}
	if err != nil {
		t.Fatalf("%s %d bytes: %v", dir, size, err)
	}
	return float64(size) * 8 / time.Since(start).Seconds() / 1e6
}

func fmtFloats(fs []float64) string {
	parts := make([]string, len(fs))
	for i, f := range fs {
		parts[i] = strconv.FormatFloat(f, 'f', 1, 64)
	}
	return strings.Join(parts, " ")
}
