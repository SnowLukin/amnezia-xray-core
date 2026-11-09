package wireguard

import (
	"bufio"
	"context"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
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

	applog "github.com/xtls/xray-core/app/log"
	"github.com/xtls/xray-core/app/policy"
	"github.com/xtls/xray-core/app/proxyman"
	_ "github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/proxy/freedom"
	"github.com/xtls/xray-core/transport"
	_ "github.com/xtls/xray-core/transport/internet/tcp"
	"github.com/xtls/xray-core/transport/pipe"
	"google.golang.org/protobuf/proto"
)

// The measurements of connections through an outbound run against a server in a process of its own, so that the
// stack and the device of the far end do not count in the memory of the client. The server answers TCP on
// measureTargetPort and UDP on measureUDPPort of measureServerAddr inside the tunnel, and the same way on loopback
// outside it for the freedom outbound the numbers are compared with. What the outbound runs on, and how its stack
// is looked into, is in measure_stack_test.go.
//
//	AWG_MEASURE_CONNS=1       TestMeasureConns
//	AWG_MEASURE_THROUGHPUT=1  TestMeasureThroughput
//	AWG_MEASURE_UDP_RATES=100,200  TestMeasureUDPThroughput, Mbit/s that the server sends at
//	AWG_MEASURE_JITTER=0      how much longer each way of a relay takes at most, ms
//	AWG_MEASURE_UDP_STALL=0   ms that the reader of TestMeasureUDPThroughput takes nothing every 50 ms
//	AWG_MEASURE_IDLE=1        TestMeasureIdle
//	AWG_MEASURE_N=100,250     connections per round
//	AWG_MEASURE_MODES=idle,down,up       also abandon, dead, stop, udp, udp-flood, udp-abandon
//	AWG_MEASURE_PATHS=awg,direct
//	AWG_MEASURE_CONN_IDLE=300s           connIdle of the policy, after which an abandoned connection ends
//	AWG_MEASURE_CONN_DELAY=0  one-way delay of the way of TestMeasureConns, ms
//	AWG_MEASURE_READ_FOR=5s   how long the connections of mode stop read before they stop
//	AWG_MEASURE_OUTBOUNDS=1   outbounds of TestMeasureIdle
//	AWG_MEASURE_IDLE_FOR=30s  how long TestMeasureIdle leaves them without connections
//	AWG_MEASURE_SERVER=path   a test binary of another revision to be the server, so that every revision
//	                          measured talks to the same far end
//	AWG_MEASURE_VARIANT=name  what the RESULT lines are labelled with
//	AWG_MEASURE_PROFILE=dir   heap profiles of each round, every allocation
//	AWG_MEASURE_SETTLE=70s    how long a round waits for its connections to leave the stack
//	AWG_MEASURE_DELAYS=0,10,25,50  one-way delays of the throughput runs, ms
//	AWG_MEASURE_SIZES=64      MiB per transfer
//	AWG_MEASURE_RUNS=5        transfers per size, delay and path

const (
	measureTargetPort = 80
	measureUDPPort    = 7
	// measurePipeLimit is the pipe the dispatcher gives a connection on arm64, where phones run: the policy
	// buffer is 4 KiB there.
	measurePipeLimit = 4 << 10
	// measureGap separates the handshakes of a device and of the next one: the server drops an initiation that
	// comes within 20 ms of the previous one, and the client retries only after 5 s.
	measureGap = 50 * time.Millisecond
	// measureDatagram is the size of what the server floods a UDP flow with.
	measureDatagram = 1200
)

// measureParams are the AmneziaWG parameters of both ends.
var measureParams = &AmneziaParameters{
	Jc: "3", Jmin: "50", Jmax: "1000",
	S1: "20", S2: "78", S3: "15",
	H1: "39131278", H2: "832138185", H3: "1436957857", H4: "1635877746",
}

var (
	measureServerAddr = netip.MustParseAddr("10.9.0.1")
	measureClientAddr = netip.MustParseAddr("10.9.0.2")
)

type measureChildConfig struct {
	PeerPub  string
	PeerAddr string
	Delays   []int
	// Jitter is how much longer each way of a relay takes at most, ms: see startDelayRelay
	Jitter int
}

type measureChildInfo struct {
	Endpoint  string
	Pub       string
	Direct    string
	DirectUDP string
	Relays    map[int]string
}

// TestMeasureServer is the far end of the measurements; the parent starts it in a process of its own.
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
	pub := measureServeTunnel(t, conn, cfg.PeerPub, netip.MustParseAddr(cfg.PeerAddr))

	outside, err := stdnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go serveMeasureTarget(outside)
	outsideUDP, err := stdnet.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go serveMeasurePackets(outsideUDP)

	info := measureChildInfo{
		Endpoint: conn.LocalAddr().String(), Pub: pub,
		Direct: outside.Addr().String(), DirectUDP: outsideUDP.LocalAddr().String(),
		Relays: map[int]string{},
	}
	for _, d := range cfg.Delays {
		info.Relays[d] = startDelayRelay(t, info.Endpoint, time.Duration(d)*time.Millisecond, time.Duration(cfg.Jitter)*time.Millisecond)
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

// setSocketBuffers lets a loopback socket take the bursts of a transfer without dropping, which would measure
// the socket rather than the stack.
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
//	hold S   answers "ok\n", reads nothing for S seconds, then reads until the client closes
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

// answerMeasureDatagram answers a datagram: "flood N" with N datagrams of measureDatagram bytes, "pace R S" with
// datagrams of measureDatagram bytes at R Mbit/s for S seconds, a millisecond's worth at a time, anything else
// with itself.
func answerMeasureDatagram(b []byte, send func([]byte) error) {
	if f := strings.Fields(string(b)); len(f) == 3 && f[0] == "pace" {
		rate, err1 := strconv.Atoi(f[1])
		secs, err2 := strconv.Atoi(f[2])
		if err1 != nil || err2 != nil {
			return
		}
		go func() {
			p := make([]byte, measureDatagram)
			start := time.Now()
			sent := 0
			for time.Since(start) < time.Duration(secs)*time.Second {
				for due := int(time.Since(start).Seconds() * float64(rate) * 1e6 / 8); sent < due; sent += len(p) {
					if send(p) != nil {
						return
					}
				}
				time.Sleep(time.Millisecond)
			}
		}()
		return
	}
	if n, ok := strings.CutPrefix(string(b), "flood "); ok {
		k, err := strconv.Atoi(n)
		if err != nil {
			return
		}
		p := make([]byte, measureDatagram)
		for range k {
			if send(p) != nil {
				return
			}
		}
		return
	}
	send(b)
}

func serveMeasurePackets(c stdnet.PacketConn) {
	b := make([]byte, 2048)
	for {
		n, addr, err := c.ReadFrom(b)
		if err != nil {
			return
		}
		answerMeasureDatagram(b[:n], func(p []byte) error {
			_, err := c.WriteTo(p, addr)
			return err
		})
	}
}

// startDelayRelay forwards datagrams between a client and server after delay each way, for a round trip time
// like that of a phone, and returns the address the client sends to. With jitter each way takes up to that much
// longer, a new share of it every 50 ms, as a wireless link or a queue with traffic of others makes it; datagrams
// keep their order.
func startDelayRelay(t testing.TB, server string, delay, jitter time.Duration) string {
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
	var extra atomic.Int64
	if jitter > 0 {
		go func() {
			for {
				extra.Store(rand.Int64N(int64(jitter)))
				time.Sleep(50 * time.Millisecond)
			}
		}()
	}
	// due is when a datagram that came now goes on, not before the one that came before it
	due := func(last *time.Time) time.Time {
		at := time.Now().Add(delay + time.Duration(extra.Load()))
		if at.Before(*last) {
			at = *last
		}
		*last = at
		return at
	}
	up := make(chan datagram, 1<<16)
	down := make(chan datagram, 1<<16)
	// A full queue drops, as a router would.
	go func() {
		var last time.Time
		for {
			b := make([]byte, 4096)
			n, addr, err := front.ReadFromUDP(b)
			if err != nil {
				return
			}
			client.Store(addr)
			select {
			case up <- datagram{due(&last), b[:n]}:
			default:
			}
		}
	}()
	go func() {
		var last time.Time
		for {
			b := make([]byte, 4096)
			n, err := back.Read(b)
			if err != nil {
				return
			}
			select {
			case down <- datagram{due(&last), b[:n]}:
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

// startMeasureServer starts TestMeasureServer in a process of its own, with a peer of pub at addr, and returns
// what it listens on and what ends it.
func startMeasureServer(t testing.TB, pub string, addr netip.Addr, delays []int) (measureChildInfo, func()) {
	t.Helper()
	cfg, err := json.Marshal(measureChildConfig{PeerPub: pub, PeerAddr: addr.String(), Delays: delays, Jitter: envInts(t, "AWG_MEASURE_JITTER", "0")[0]})
	if err != nil {
		t.Fatal(err)
	}
	bin := os.Getenv("AWG_MEASURE_SERVER")
	if bin == "" {
		bin = os.Args[0]
	}
	cmd := exec.Command(bin, "-test.run=^TestMeasureServer$", "-test.timeout=0")
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
	var once sync.Once
	stop := func() {
		once.Do(func() {
			stdin.Close()
			cmd.Wait()
		})
	}
	t.Cleanup(stop)
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
		return info, stop
	case <-time.After(30 * time.Second):
		t.Fatal("the measurement server did not start")
		return measureChildInfo{}, stop
	}
}

// measureConn is one connection dispatched to an outbound with the pipes of a phone.
type measureConn struct {
	up     *pipe.Writer
	down   *pipe.Reader
	cancel context.CancelFunc
	done   chan struct{}
}

func dialMeasured(oh outbound.Handler, dest net.Destination) *measureConn {
	ctx, cancel := context.WithCancel(context.Background())
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{Target: dest, Tag: oh.Tag()}})
	upReader, upWriter := pipe.New(pipe.WithSizeLimit(measurePipeLimit))
	downReader, downWriter := pipe.New(pipe.WithSizeLimit(measurePipeLimit))
	c := &measureConn{up: upWriter, down: downReader, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(c.done)
		oh.Dispatch(ctx, &transport.Link{Reader: upReader, Writer: downWriter})
	}()
	return c
}

func (c *measureConn) send(s string) error {
	return c.up.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte(s))})
}

// sendBytes sends n bytes; it blocks while the outbound does not take them.
func (c *measureConn) sendBytes(n int) error {
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
func (c *measureConn) read(n int, timeout time.Duration) (int, error) {
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

// end ends the connection from the side of the app and waits until the outbound has finished with it.
func (c *measureConn) end() error {
	c.cancel()
	c.up.Close()
	c.down.Interrupt()
	return c.wait(15 * time.Second)
}

// wait waits until the outbound has finished with the connection.
func (c *measureConn) wait(timeout time.Duration) error {
	select {
	case <-c.done:
		return nil
	case <-time.After(timeout):
		return goerrors.New("the outbound did not finish the connection")
	}
}

// measureStack is the stack of an outbound as far as the measurements look into it.
type measureStack interface {
	// states counts the TCP connections of the stack by state, and its UDP sockets as "udp"
	states() map[string]int
}

type memSample struct {
	goroutines                       int
	heapInuse, heapAlloc, stackInuse uint64
	endpoints                        map[string]int
}

func sampleMem(stk measureStack) memSample {
	// Stopped stacks and devices end some goroutines asynchronously, and a sync.Pool lets go of its buffers
	// only on the second collection.
	for range 3 {
		runtime.GC()
		time.Sleep(300 * time.Millisecond)
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	s := memSample{goroutines: runtime.NumGoroutine(), heapInuse: m.HeapInuse, heapAlloc: m.HeapAlloc, stackInuse: m.StackInuse}
	if stk != nil {
		s.endpoints = stk.states()
	}
	return s
}

func countEndpoints(stk measureStack) int {
	if stk == nil {
		return 0
	}
	n := 0
	for _, k := range stk.states() {
		n += k
	}
	return n
}

func (s memSample) total() int {
	n := 0
	for _, k := range s.endpoints {
		n += k
	}
	return n
}

func (s memSample) statesText() string {
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

// measurePoint is a sample against the start of its round, as the RESULT lines carry it.
type measurePoint struct {
	Goroutines    int
	HeapInuseKiB  float64
	HeapAllocKiB  float64
	StackInuseKiB float64
	States        map[string]int `json:",omitempty"`
	AfterS        float64        `json:",omitempty"`
}

func (s memSample) against(base memSample) *measurePoint {
	kib := func(v, b uint64) float64 { return float64(int64(v)-int64(b)) / 1024 }
	return &measurePoint{
		Goroutines:    s.goroutines - base.goroutines,
		HeapInuseKiB:  kib(s.heapInuse, base.heapInuse),
		HeapAllocKiB:  kib(s.heapAlloc, base.heapAlloc),
		StackInuseKiB: kib(s.stackInuse, base.stackInuse),
		States:        s.endpoints,
	}
}

func logSample(t testing.TB, label string, s, base memSample, n int) {
	t.Helper()
	p := s.against(base)
	per := func(v float64) string {
		if n == 0 {
			return ""
		}
		return fmt.Sprintf(" (%5.1f/conn)", v/float64(n))
	}
	t.Logf("%-22s goroutines %+5d  HeapInuse %+9.0f KiB%s  HeapAlloc %+9.0f KiB%s  StackInuse %+6.0f KiB  endpoints: %s",
		label, p.Goroutines, p.HeapInuseKiB, per(p.HeapInuseKiB), p.HeapAllocKiB, per(p.HeapAllocKiB), p.StackInuseKiB, s.statesText())
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

func envDuration(t testing.TB, name string, def time.Duration) time.Duration {
	t.Helper()
	s := os.Getenv(name)
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return d
}

// logResult logs a line that a script can gather the rounds of several runs from.
func logResult(t testing.TB, r any) {
	t.Helper()
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RESULT %s", b)
}

// measureClientConf is the config of an outbound on the built-in stack that connects to the server.
func measureClientConf(endpoint, pub, priv string) *DeviceConfig {
	return &DeviceConfig{
		IsClient:    true,
		SecretKey:   priv,
		Endpoint:    []string{measureClientAddr.String()},
		Mtu:         1420,
		NoKernelTun: true,
		Peers: []*PeerConfig{{
			PublicKey:  pub,
			Endpoint:   endpoint,
			AllowedIps: []string{"0.0.0.0/0"},
		}},
		Parameters: measureParams,
	}
}

func addMeasureOutbound(t testing.TB, inst *core.Instance, tag string, conf proto.Message) outbound.Handler {
	t.Helper()
	err := core.AddOutboundHandler(inst, &core.OutboundHandlerConfig{
		Tag:            tag,
		SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{}),
		ProxySettings:  serial.ToTypedMessage(conf),
	})
	if err != nil {
		t.Fatal(err)
	}
	return inst.GetFeature(outbound.ManagerType()).(outbound.Manager).GetHandler(tag)
}

// closeMeasureOutbound closes the proxy of oh, its device and stack with it.
func closeMeasureOutbound(oh outbound.Handler) {
	if c, ok := oh.(proxy.GetOutbound).GetOutbound().(common.Closable); ok {
		c.Close()
	}
}

// newMeasureInstance returns an instance that logs warnings, as an app does: the log of every connection would be
// measured too. connIdle is the policy after which a connection without traffic ends.
func newMeasureInstance(t testing.TB, connIdle time.Duration) *core.Instance {
	t.Helper()
	inst, err := core.New(&core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&applog.Config{ErrorLogType: applog.LogType_Console, ErrorLogLevel: log.Severity_Warning}),
			serial.ToTypedMessage(&policy.Config{Level: map[uint32]*policy.Policy{0: {
				Timeout: &policy.Policy_Timeout{ConnectionIdle: &policy.Second{Value: uint32(connIdle / time.Second)}},
			}}}),
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
	return addMeasureOutbound(t, inst, tag, &freedom.Config{})
}

// warmUp wakes the outbound of oh with one echo. The first handshake through a loaded machine is sometimes lost,
// and the device tries again only after 5 s, past the dial timeout.
func warmUp(t testing.TB, oh outbound.Handler, dest net.Destination) {
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
}

// measureMode is what the connections of a round do.
type measureMode struct {
	udp bool
	// req is what each sends first, want how much of the answer it reads, up how much it sends after that
	req      string
	want, up int
	keep     bool // the app abandons the connections: nothing ends them but the idle timeout of the policy
	kill     bool // and the server goes away first
	unread   bool // the answer is not read: it waits in the outbound
}

var measureModes = map[string]measureMode{
	// gets 1 KiB, which it reads
	"idle": {req: "send 1024\n", want: 1024},
	// gets 4 MiB, more than any window, of which it reads nothing; the data waits in the outbound as it would
	// for a slow app
	"down": {req: "send 4194304\n", unread: true},
	// sends 4 MiB to a server that reads nothing for 30 s; the data waits in the outbound as it would on a slow
	// uplink
	"up": {req: "hold 30\n", want: 3, up: 4 << 20},
	// the app goes away without closing, the server says nothing more
	"abandon": {req: "hold 86400\n", want: 3, keep: true},
	// the same, and then the server is gone too, so that nothing answers the FIN
	"dead": {req: "hold 86400\n", want: 3, keep: true, kill: true},
	// one datagram there and back
	"udp": {udp: true, req: "ping", want: 4},
	// the server sends 2000 datagrams that the app does not read
	"udp-flood":   {udp: true, req: "flood 2000", unread: true},
	"udp-abandon": {udp: true, req: "ping", want: 4, keep: true},
}

type measureRoundResult struct {
	Variant, Path, Mode string
	N                   int
	Failed              int
	OpenedMs            int64
	PeakKiB             float64
	Open, Closed, Gone  *measurePoint
}

// TestMeasureConns logs what N connections through an AmneziaWG outbound hold, against N through freedom over
// loopback; see measureModes. The connections then end from the side of the app, as an app that hangs up does,
// or are left to the idle timeout.
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
	settle := envDuration(t, "AWG_MEASURE_SETTLE", 70*time.Second)
	connIdle := envDuration(t, "AWG_MEASURE_CONN_IDLE", 300*time.Second)
	ns := envInts(t, "AWG_MEASURE_N", "100,250")
	modes := envList("AWG_MEASURE_MODES", "idle,down,up")
	paths := envList("AWG_MEASURE_PATHS", "awg,direct")
	variant := os.Getenv("AWG_MEASURE_VARIANT")

	priv, pub := testKeys(t)
	delay := envInts(t, "AWG_MEASURE_CONN_DELAY", "0")[0]
	var delays []int
	if delay > 0 {
		delays = []int{delay}
	}
	srv, _ := startMeasureServer(t, pub, measureClientAddr, delays)
	endpoint := srv.Endpoint
	if delay > 0 {
		endpoint = srv.Relays[delay]
	}
	inst := newMeasureInstance(t, connIdle)
	direct := addFreedom(t, inst, "direct")
	t.Logf("NumCPU %d, GOMAXPROCS %d, pipe limit %d, connIdle %v; deltas against the start of each round",
		runtime.NumCPU(), runtime.GOMAXPROCS(0), measurePipeLimit, connIdle)

	round := 0
	for _, name := range modes {
		mode, ok := measureModes[name]
		if name == "stop" {
			for _, n := range ns {
				round++
				oh := addMeasureOutbound(t, inst, "awg"+strconv.Itoa(round), measureClientConf(endpoint, srv.Pub, priv))
				r := measureStopRound(t, fmt.Sprintf("awg stop N=%d", n), oh, n)
				r.Variant, r.Path, r.Mode, r.N = variant, "awg", name, n
				logResult(t, r)
				closeMeasureOutbound(oh)
				time.Sleep(measureGap)
			}
			continue
		}
		if !ok {
			t.Fatalf("unknown mode %q", name)
		}
		for _, n := range ns {
			for _, path := range paths {
				round++
				label := fmt.Sprintf("%s %s N=%d", path, name, n)
				var oh outbound.Handler
				var tcpDest, udpDest net.Destination
				stopServer := func() {}
				switch path {
				case "awg":
					s := srv
					if mode.kill {
						// a server of its own, to be gone
						s, stopServer = startMeasureServer(t, pub, measureClientAddr, nil)
					}
					// A fresh outbound each round: a fresh stack, without what the previous round left.
					e := s.Endpoint
					if !mode.kill {
						e = endpoint
					}
					oh = addMeasureOutbound(t, inst, "awg"+strconv.Itoa(round), measureClientConf(e, s.Pub, priv))
					tcpDest = net.TCPDestination(net.IPAddress(measureServerAddr.AsSlice()), measureTargetPort)
					udpDest = net.UDPDestination(net.IPAddress(measureServerAddr.AsSlice()), measureUDPPort)
				case "direct":
					if mode.kill {
						continue
					}
					oh = direct
					tcpDest = measureLoopback(t, net.Network_TCP, srv.Direct)
					udpDest = measureLoopback(t, net.Network_UDP, srv.DirectUDP)
				default:
					t.Fatalf("unknown path %q", path)
				}
				r := measureRound(t, label, oh, tcpDest, udpDest, mode, n, profileDir, settle, connIdle, stopServer)
				r.Variant, r.Path, r.Mode, r.N = variant, path, name, n
				logResult(t, r)
				if path != "direct" {
					closeMeasureOutbound(oh)
					stopServer()
					time.Sleep(measureGap)
				}
			}
		}
	}
}

func measureLoopback(t testing.TB, network net.Network, addr string) net.Destination {
	t.Helper()
	port, err := strconv.Atoi(addr[strings.LastIndexByte(addr, ':')+1:])
	if err != nil {
		t.Fatal(err)
	}
	return net.Destination{Network: network, Address: net.LocalHostIP, Port: net.Port(port)}
}

func measureRound(t *testing.T, label string, oh outbound.Handler, tcpDest, udpDest net.Destination, mode measureMode, n int,
	profileDir string, settle, connIdle time.Duration, stopServer func(),
) measureRoundResult {
	t.Helper()
	// One connection first wakes the outbound: its stack and device are not what a connection costs.
	warmUp(t, oh, tcpDest)
	stk := measureStackOf(oh)
	base := sampleMem(stk)
	name := strings.ReplaceAll(label, " ", "-")
	writeHeapProfile(t, profileDir, name+"-0base")

	dest := tcpDest
	if mode.udp {
		dest = udpDest
	}
	stopPeak := samplePeak()
	conns := make([]*measureConn, n)
	var wg sync.WaitGroup
	var failed atomic.Int32
	start := time.Now()
	for i := range conns {
		conns[i] = dialMeasured(oh, dest)
		wg.Go(func() {
			c := conns[i]
			if err := c.send(mode.req); err != nil {
				failed.Add(1)
				return
			}
			if mode.want > 0 {
				if _, err := c.read(mode.want, 30*time.Second); err != nil {
					failed.Add(1)
				}
			}
		})
	}
	wg.Wait()
	if mode.up > 0 {
		// Only once every connection is up: uploads that saturate the tunnel delay the handshakes of the others
		// past the dial timeout.
		for _, c := range conns {
			// Blocks until the connection ends.
			go c.sendBytes(mode.up)
		}
	}
	opened := time.Since(start)
	if failed.Load() > 0 {
		t.Errorf("%s: %d connections failed", label, failed.Load())
	}
	if mode.unread || mode.up > 0 {
		waitHeapSteady(20 * time.Second)
	}
	peak := stopPeak()
	open := sampleMem(stk)
	writeHeapProfile(t, profileDir, name+"-1open")
	r := measureRoundResult{Failed: int(failed.Load()), OpenedMs: opened.Milliseconds(), PeakKiB: float64(int64(peak)-int64(base.heapInuse)) / 1024}
	r.Open = open.against(base)

	closedLabel := "  just closed"
	closeStart := time.Now()
	if mode.keep {
		if mode.kill {
			stopServer()
		}
		// nothing but the idle timeout of the policy ends them
		for _, c := range conns {
			if err := c.wait(connIdle + 30*time.Second); err != nil {
				t.Fatalf("%s: %v after the idle timeout", label, err)
			}
		}
		closedLabel = fmt.Sprintf("  timed out +%v", time.Since(closeStart).Round(time.Second))
	} else {
		for _, c := range conns {
			if err := c.end(); err != nil {
				t.Fatal(err)
			}
		}
	}
	closed := sampleMem(stk)
	r.Closed = closed.against(base)
	r.Closed.AfterS = time.Since(closeStart).Seconds()
	t.Logf("%s, opened in %v, HeapInuse peaked at %+.0f KiB before the collection", label, opened.Round(time.Millisecond), r.PeakKiB)
	logSample(t, "  open", open, base, n)
	logSample(t, closedLabel, closed, base, n)
	if stk != nil && closed.total() > base.total() {
		start := time.Now()
		waitEndpoints(stk, base.total(), settle)
		gone := sampleMem(stk)
		r.Gone = gone.against(base)
		r.Gone.AfterS = time.Since(start).Seconds()
		logSample(t, fmt.Sprintf("  left +%v", time.Since(start).Round(time.Second)), gone, base, n)
	}
	for _, c := range conns {
		c.end()
	}
	return r
}

// samplePeak samples HeapInuse until the returned function is called, which returns the highest: garbage of the
// burst counts too, as the process holds that memory until the runtime returns it.
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

// waitEndpoints waits until stk has at most n connections and sockets.
func waitEndpoints(stk measureStack, n int, timeout time.Duration) {
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(250 * time.Millisecond) {
		if countEndpoints(stk) <= n {
			return
		}
	}
}

// waitHeapSteady waits until the heap stops growing: the data sent to the connections has arrived and waits in
// their buffers.
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

// TestMeasureIdle logs what outbounds hold while they carry little: created, with one and two idle connections
// open, once those ended and nothing came for a while, and closed.
func TestMeasureIdle(t *testing.T) {
	if os.Getenv("AWG_MEASURE_IDLE") != "1" {
		t.Skip("set AWG_MEASURE_IDLE=1 to measure idle outbounds")
	}
	n := envInts(t, "AWG_MEASURE_OUTBOUNDS", "1")[0]
	idleFor := envDuration(t, "AWG_MEASURE_IDLE_FOR", 30*time.Second)
	priv, pub := testKeys(t)
	srv, _ := startMeasureServer(t, pub, measureClientAddr, nil)
	inst := newMeasureInstance(t, 300*time.Second)
	// The first log line starts a writer goroutine; count it from the start.
	log.Record(&log.GeneralMessage{Severity: log.Severity_Warning, Content: "measuring idle outbounds"})
	t.Logf("NumCPU %d, GOMAXPROCS %d, %d outbounds; deltas against the first line, per outbound",
		runtime.NumCPU(), runtime.GOMAXPROCS(0), n)
	base := sampleMem(nil)
	point := func(label string) {
		s := sampleMem(nil)
		logSample(t, label, s, base, n)
		logResult(t, struct {
			Variant, Point string
			Outbounds      int
			*measurePoint
		}{os.Getenv("AWG_MEASURE_VARIANT"), label, n, s.against(base)})
	}
	ohs := make([]outbound.Handler, n)
	for i := range ohs {
		// Every outbound is the same peer to the server: they take turns, measureGap apart.
		ohs[i] = addMeasureOutbound(t, inst, "idle"+strconv.Itoa(i), measureClientConf(srv.Endpoint, srv.Pub, priv))
	}
	point("created")
	dest := net.TCPDestination(net.IPAddress(measureServerAddr.AsSlice()), measureTargetPort)
	var open []*measureConn
	for k := 1; k <= 2; k++ {
		for _, oh := range ohs {
			c := dialMeasured(oh, dest)
			err := c.send("send 1024\n")
			if err == nil {
				_, err = c.read(1024, 15*time.Second)
			}
			if err != nil {
				t.Fatal(err)
			}
			open = append(open, c)
			time.Sleep(measureGap)
		}
		point(strconv.Itoa(k) + " open")
	}
	for _, c := range open {
		c.end()
	}
	time.Sleep(idleFor)
	point("idle " + idleFor.String())
	for _, oh := range ohs {
		closeMeasureOutbound(oh)
	}
	point("closed")
}

// TestMeasureThroughput logs how fast one connection downloads and uploads through an AmneziaWG outbound, over
// loopback and over relays that add a round trip time like that of a phone, and how much CPU the process of the
// outbound takes for it.
func TestMeasureThroughput(t *testing.T) {
	if os.Getenv("AWG_MEASURE_THROUGHPUT") != "1" {
		t.Skip("set AWG_MEASURE_THROUGHPUT=1 to measure the throughput of an outbound")
	}
	delays := envInts(t, "AWG_MEASURE_DELAYS", "0,10,25,50")
	sizes := envInts(t, "AWG_MEASURE_SIZES", "64")
	runs := envInts(t, "AWG_MEASURE_RUNS", "5")[0]
	paths := envList("AWG_MEASURE_PATHS", "awg")
	variant := os.Getenv("AWG_MEASURE_VARIANT")
	var relayed []int
	for _, d := range delays {
		if d > 0 {
			relayed = append(relayed, d)
		}
	}
	priv, pub := testKeys(t)
	srv, _ := startMeasureServer(t, pub, measureClientAddr, relayed)
	inst := newMeasureInstance(t, 300*time.Second)
	dest := net.TCPDestination(net.IPAddress(measureServerAddr.AsSlice()), measureTargetPort)
	tag := 0
	for _, size := range sizes {
		for _, d := range delays {
			endpoint := srv.Endpoint
			if d > 0 {
				endpoint = srv.Relays[d]
			}
			mbps := map[string][]float64{}
			cpu := map[string][]float64{}
			for range runs {
				for _, path := range paths {
					tag++
					oh := addMeasureOutbound(t, inst, "tp"+strconv.Itoa(tag), measureClientConf(endpoint, srv.Pub, priv))
					warmUp(t, oh, dest)
					for _, dir := range []string{"down", "up"} {
						speed, ms := transfer(t, oh, dest, dir, size<<20)
						mbps[path+" "+dir] = append(mbps[path+" "+dir], speed)
						cpu[path+" "+dir] = append(cpu[path+" "+dir], ms/float64(size))
					}
					closeMeasureOutbound(oh)
					time.Sleep(measureGap)
				}
			}
			for _, dir := range []string{"down", "up"} {
				for _, path := range paths {
					v, c := mbps[path+" "+dir], cpu[path+" "+dir]
					slices.Sort(v)
					slices.Sort(c)
					t.Logf("%3d MiB %-4s one-way delay %3d ms %-10s median %6.1f Mbit/s, max %6.1f, CPU %5.2f ms/MiB; runs %s",
						size, dir, d, path, v[len(v)/2], v[len(v)-1], c[len(c)/2], fmtFloats(v))
					logResult(t, struct {
						Variant, Path, Dir string
						SizeMiB, DelayMs   int
						Mbps, CPUMsPerMiB  []float64
					}{variant, path, dir, size, d, v, c})
				}
			}
		}
	}
}

// transfer moves size bytes through a new connection, down from the server or up to it, and returns the speed in
// Mbit/s and the CPU time of the process in ms.
func transfer(t testing.TB, oh outbound.Handler, dest net.Destination, dir string, size int) (float64, float64) {
	t.Helper()
	c := dialMeasured(oh, dest)
	defer c.end()
	start, cpuStart := time.Now(), cpuTime()
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
	took, cpu := time.Since(start), cpuTime()-cpuStart
	return float64(size) * 8 / took.Seconds() / 1e6, float64(cpu) / float64(time.Millisecond)
}

func fmtFloats(fs []float64) string {
	parts := make([]string, len(fs))
	for i, f := range fs {
		parts[i] = strconv.FormatFloat(f, 'f', 1, 64)
	}
	return strings.Join(parts, " ")
}

// TestMeasureUDPThroughput logs how much of a UDP flow that the server sends at a rate comes through an AmneziaWG
// outbound to a reader that takes it as it comes, as a download over QUIC does.
func TestMeasureUDPThroughput(t *testing.T) {
	if os.Getenv("AWG_MEASURE_UDP_RATES") == "" {
		t.Skip("set AWG_MEASURE_UDP_RATES=100,200 to measure UDP through an outbound")
	}
	rates := envInts(t, "AWG_MEASURE_UDP_RATES", "")
	delays := envInts(t, "AWG_MEASURE_DELAYS", "0,25")
	runs := envInts(t, "AWG_MEASURE_RUNS", "3")[0]
	secs := envInts(t, "AWG_MEASURE_SECONDS", "5")[0]
	stall := time.Duration(envInts(t, "AWG_MEASURE_UDP_STALL", "0")[0]) * time.Millisecond
	variant := os.Getenv("AWG_MEASURE_VARIANT")
	var relayed []int
	for _, d := range delays {
		if d > 0 {
			relayed = append(relayed, d)
		}
	}
	priv, pub := testKeys(t)
	srv, _ := startMeasureServer(t, pub, measureClientAddr, relayed)
	inst := newMeasureInstance(t, 300*time.Second)
	tcpDest := net.TCPDestination(net.IPAddress(measureServerAddr.AsSlice()), measureTargetPort)
	udpDest := net.UDPDestination(net.IPAddress(measureServerAddr.AsSlice()), measureUDPPort)
	tag := 0
	for _, d := range delays {
		endpoint := srv.Endpoint
		if d > 0 {
			endpoint = srv.Relays[d]
		}
		for _, rate := range rates {
			var got []float64
			for range runs {
				tag++
				oh := addMeasureOutbound(t, inst, "udp"+strconv.Itoa(tag), measureClientConf(endpoint, srv.Pub, priv))
				warmUp(t, oh, tcpDest)
				c := dialMeasured(oh, udpDest)
				if err := c.send("pace " + strconv.Itoa(rate) + " " + strconv.Itoa(secs)); err != nil {
					t.Fatal(err)
				}
				// a reader that now and then takes nothing for a while, as one that the system makes wait
				n := 0
				for end := time.Now().Add(time.Duration(secs)*time.Second + 2*time.Duration(d)*time.Millisecond + time.Second); time.Now().Before(end); {
					k, _ := c.read(math.MaxInt, min(50*time.Millisecond, time.Until(end)))
					n += k
					time.Sleep(stall)
				}
				got = append(got, float64(n)/(float64(rate)*1e6/8*float64(secs)))
				c.end()
				closeMeasureOutbound(oh)
				time.Sleep(measureGap)
			}
			slices.Sort(got)
			t.Logf("UDP %4d Mbit/s one-way delay %3d ms, reader stalls %v: median %.1f%% came; runs %s", rate, d, stall, 100*got[len(got)/2], fmtFloats(got))
			logResult(t, struct {
				Variant                    string
				RateMbps, DelayMs, StallMs int
				Came                       []float64
			}{variant, rate, d, int(stall / time.Millisecond), got})
		}
	}
}

// measureStopRound has n connections download as fast as they can for a while, and then stop reading, as an app that
// went to the background, while the server goes on sending. It logs how fast they went together and what they hold
// once they stopped.
func measureStopRound(t *testing.T, label string, oh outbound.Handler, n int) measureRoundResult {
	t.Helper()
	readFor := envDuration(t, "AWG_MEASURE_READ_FOR", 5*time.Second)
	dest := net.TCPDestination(net.IPAddress(measureServerAddr.AsSlice()), measureTargetPort)
	warmUp(t, oh, dest)
	base := sampleMem(nil)
	stopPeak := samplePeak()
	conns := make([]*measureConn, n)
	var read atomic.Int64
	var wg sync.WaitGroup
	end := time.Now().Add(readFor)
	for i := range conns {
		conns[i] = dialMeasured(oh, dest)
		wg.Go(func() {
			c := conns[i]
			if c.send("send 1073741824\n") != nil {
				return
			}
			for time.Now().Before(end) {
				k, err := c.read(math.MaxInt, time.Until(end))
				read.Add(int64(k))
				if err != nil && time.Now().Before(end) {
					return
				}
			}
		})
	}
	wg.Wait()
	mbps := float64(read.Load()) * 8 / readFor.Seconds() / 1e6
	waitHeapSteady(20 * time.Second)
	peak := stopPeak()
	held := sampleMem(nil)
	r := measureRoundResult{OpenedMs: int64(mbps), PeakKiB: float64(int64(peak)-int64(base.heapInuse)) / 1024}
	r.Open = held.against(base)
	t.Logf("%s: %.0f Mbit/s together while they read, HeapInuse peaked at %+.0f KiB", label, mbps, r.PeakKiB)
	logSample(t, "  stopped", held, base, n)
	for _, c := range conns {
		c.end()
	}
	closed := sampleMem(nil)
	r.Closed = closed.against(base)
	return r
}
