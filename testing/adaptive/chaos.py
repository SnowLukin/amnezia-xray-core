"""TCP and UDP proxy with failures on command. Every listen port stands in for
one server address.

Run:      chaos.py <control_port> <listen:upstream>... [udp:<listen:upstream>]...
Control:  GET http://127.0.0.1:<control_port>/set?port=21001,21002&mode=blackhole
          GET http://127.0.0.1:<control_port>/set   (status only)
Listen:   the server addresses listen on BENCH_LISTEN (127.0.0.1 by default);
          0.0.0.0 lets a phone on the LAN reach them. The control port stays
          on loopback.

Modes:
  pass            forward everything
  blackhole       accept TCP, never answer, never close (a drop after SYN-ACK)
  reset           accept, then RST right away
  refuse          close the listener: a new connection gets RST to its SYN and
                  is never counted; connections already open go on
  stall_after:N   forward N bytes each way, then silently swallow (DPI freeze)
  reset_after:N   RST after N bytes in either direction
  delay:MS        delay every chunk by MS milliseconds
  pause           hold data until the mode changes, like loss that TCP
                  retransmits recover from
  redirect:PORT   new connections go to another upstream port, like an
                  address that now belongs to someone else
Live connections follow a new mode at once, like a block in the middle of a
session.

The status line of a port: "<port> <mode> conns=N up=U down=D". up counts the
bytes the client sent, whatever became of them; down the bytes that reached the
client. Together they are what a client pays for traffic on the port.

A udp: port forwards datagrams. Each client address gets a socket of its own
to the upstream, as a NAT would give it; conns counts those addresses. It
takes the modes that apply to datagrams, and refuses the others:
  pass            forward everything
  blackhole       drop every datagram both ways, silently
  drop_after:N    forward N datagrams each way per client address, then drop
  lose_reply:N    drop the first N datagrams back to each client address, as
                  the network loses the handshake response of a new socket
  delay:MS        delay every datagram by MS milliseconds
Its status line adds the datagrams: "... pkts_up=P pkts_down=Q", counted as
the bytes are.
"""
import asyncio
import socket
import struct
import sys
import os
import time
from urllib.parse import parse_qs, urlparse

MODES = {}
CONNS = {}
BYTES = {}
LISTEN = {}
SERVERS = {}
# UDP ports: listen port -> upstream port, and datagrams [up, down] per port.
UDP = {}
PACKETS = {}
UDP_MODES = ("pass", "blackhole", "drop_after", "lose_reply", "delay")


def rst(writer):
    sock = writer.get_extra_info("socket")
    if sock is not None:
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        except OSError:
            pass
    writer.transport.abort()


def mode_of(port):
    name, _, arg = MODES.get(port, "pass").partition(":")
    return name, int(arg) if arg else 0


def count(port, direction, n):
    BYTES.setdefault(port, [0, 0])[direction] += n


async def pump(port, reader, writer, counter, other_writer, direction):
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            if direction == 0:
                count(port, 0, len(data))
            while mode_of(port)[0] == "pause":
                await asyncio.sleep(0.05)
            name, arg = mode_of(port)
            if name == "blackhole":
                continue
            if name == "reset":
                rst(writer)
                rst(other_writer)
                return
            if name == "delay":
                await asyncio.sleep(arg / 1000)
            if name in ("stall_after", "reset_after"):
                allowed = max(0, arg - counter[0])
                if allowed < len(data):
                    if name == "reset_after":
                        rst(writer)
                        rst(other_writer)
                        return
                    data = data[:allowed]
            counter[0] += len(data)
            if data:
                if direction == 1:
                    count(port, 1, len(data))
                other_writer.write(data)
                await other_writer.drain()
    except (ConnectionError, asyncio.CancelledError, OSError):
        pass
    finally:
        # A frozen connection never tells the other side that it ended.
        name, arg = mode_of(port)
        frozen = name == "blackhole" or (name == "stall_after" and counter[0] >= arg)
        if frozen:
            try:
                while not other_writer.is_closing():
                    await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                pass
        try:
            other_writer.close()
        except Exception:
            pass


async def handle(port, upstream, reader, writer):
    CONNS[port] = CONNS.get(port, 0) + 1
    name, _ = mode_of(port)
    if name in ("reset", "refuse"):
        rst(writer)
        return
    if name == "blackhole":
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                count(port, 0, len(data))
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()
        return
    name, arg = mode_of(port)
    if name == "redirect":
        upstream = arg
    try:
        ur, uw = await asyncio.open_connection("127.0.0.1", upstream)
    except OSError:
        rst(writer)
        return
    up, down = [0], [0]
    await asyncio.gather(pump(port, reader, writer, up, uw, 0), pump(port, ur, uw, down, writer, 1))


def count_packet(port, direction, n):
    count(port, direction, n)
    PACKETS.setdefault(port, [0, 0])[direction] += 1


class UdpSession:
    """One client address of a UDP port with its own socket to the upstream."""

    def __init__(self, port, listener, client):
        self.port = port
        self.listener = listener
        self.client = client
        self.upstream = None
        # Datagrams of the client that came before its socket to the upstream.
        self.pending = []
        # Datagrams forwarded each way, for drop_after.
        self.forwarded = [0, 0]
        # Datagrams back to the client, whatever became of them, for lose_reply.
        self.replies = 0

    async def open(self):
        loop = asyncio.get_running_loop()
        try:
            transport, _ = await loop.create_datagram_endpoint(
                lambda: UdpUpstream(self), remote_addr=("127.0.0.1", UDP[self.port]))
        except OSError:
            self.pending = []
            return
        self.upstream = transport
        for data in self.pending:
            transport.sendto(data)
        self.pending = []

    def forward(self, direction, data):
        name, arg = mode_of(self.port)
        self.replies += direction
        if name == "blackhole" or (name == "drop_after" and self.forwarded[direction] >= arg) or (
                name == "lose_reply" and direction == 1 and self.replies <= arg):
            return
        self.forwarded[direction] += 1
        if name == "delay":
            asyncio.get_running_loop().call_later(arg / 1000, self.deliver, direction, data)
        else:
            self.deliver(direction, data)

    def deliver(self, direction, data):
        if direction == 0:
            if self.upstream is None:
                self.pending.append(data)
            else:
                self.upstream.sendto(data)
        elif self.listener.transport is not None:
            self.listener.transport.sendto(data, self.client)
            count_packet(self.port, 1, len(data))


class UdpUpstream(asyncio.DatagramProtocol):
    def __init__(self, session):
        self.session = session

    def datagram_received(self, data, addr):
        self.session.forward(1, data)

    def error_received(self, exc):
        # The upstream port is closed: to the client that is loss.
        pass


class UdpListener(asyncio.DatagramProtocol):
    def __init__(self, port):
        self.port = port
        self.transport = None
        self.sessions = {}

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        count_packet(self.port, 0, len(data))
        session = self.sessions.get(addr)
        if session is None:
            session = self.sessions[addr] = UdpSession(self.port, self, addr)
            CONNS[self.port] = CONNS.get(self.port, 0) + 1
            asyncio.ensure_future(session.open())
        session.forward(0, data)

    def error_received(self, exc):
        # A client went away; its socket answers with an ICMP error.
        pass


# Where the server addresses listen. A client on another device, such as a
# phone on the LAN, needs 0.0.0.0; the upstreams and the control stay local.
LISTEN_HOST = os.environ.get("BENCH_LISTEN", "127.0.0.1")


async def listen_udp(lp):
    await asyncio.get_running_loop().create_datagram_endpoint(lambda: UdpListener(lp), local_addr=(LISTEN_HOST, lp))


async def listen(lp):
    up = LISTEN[lp]
    SERVERS[lp] = await asyncio.start_server(lambda r, w, lp=lp, up=up: handle(lp, up, r, w), LISTEN_HOST, lp)


async def follow_mode(port):
    """The listener exists exactly when the port does not refuse."""
    refusing = mode_of(port)[0] == "refuse"
    if refusing and SERVERS.get(port) is not None:
        SERVERS.pop(port).close()
    elif not refusing and SERVERS.get(port) is None:
        await listen(port)


async def control(reader, writer):
    line = (await reader.readline()).decode()
    while (await reader.readline()) not in (b"\r\n", b"\n", b""):
        pass
    q = parse_qs(urlparse(line.split(" ")[1]).query)
    body = ""
    status = "200 OK"
    if "port" in q and "mode" in q:
        ports = [int(p) for p in q["port"][0].split(",")]
        mode = q["mode"][0]
        # A mode that does not apply to datagrams changes nothing, so a
        # scenario that asks for it fails instead of testing nothing.
        wrong = [p for p in ports if p in UDP and mode.partition(":")[0] not in UDP_MODES]
        if wrong:
            status = "400 Bad Request"
            body = f"mode {mode} does not apply to the udp ports {wrong}\n"
        else:
            for p in ports:
                MODES[p] = mode
                if p in LISTEN:
                    await follow_mode(p)
            body = f"{time.time():.3f} set {q['port'][0]} {mode}\n"
    for p in sorted(set(LISTEN) | set(UDP)):
        body += (f"{p} {MODES.get(p, 'pass')} conns={CONNS.get(p, 0)} up={BYTES.get(p, [0, 0])[0]} "
                 f"down={BYTES.get(p, [0, 0])[1]}")
        if p in UDP:
            body += f" pkts_up={PACKETS.get(p, [0, 0])[0]} pkts_down={PACKETS.get(p, [0, 0])[1]}"
        body += "\n"
    writer.write(f"HTTP/1.1 {status}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n{body}".encode())
    await writer.drain()
    writer.close()


async def main():
    control_port = int(sys.argv[1])
    for spec in sys.argv[2:]:
        if spec.startswith("udp:"):
            lp, up = map(int, spec[len("udp:"):].split(":"))
            UDP[lp] = up
            await listen_udp(lp)
            continue
        lp, up = map(int, spec.split(":"))
        LISTEN[lp] = up
        await listen(lp)
    await asyncio.start_server(control, "127.0.0.1", control_port)
    print("chaos up", LISTEN, "udp", UDP, flush=True)
    await asyncio.Event().wait()


asyncio.run(main())
