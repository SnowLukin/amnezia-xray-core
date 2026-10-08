"""TCP proxy with failures on command. Every listen port stands in for one
server address.

Run:      chaos.py <control_port> <listen:upstream>...
Control:  GET http://127.0.0.1:<control_port>/set?port=21001,21002&mode=blackhole
          GET http://127.0.0.1:<control_port>/set   (status only)

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
"""
import asyncio
import socket
import struct
import sys
import time
from urllib.parse import parse_qs, urlparse

MODES = {}
CONNS = {}
BYTES = {}
LISTEN = {}
SERVERS = {}


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


async def listen(lp):
    up = LISTEN[lp]
    SERVERS[lp] = await asyncio.start_server(lambda r, w, lp=lp, up=up: handle(lp, up, r, w), "127.0.0.1", lp)


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
    if "port" in q and "mode" in q:
        for p in q["port"][0].split(","):
            MODES[int(p)] = q["mode"][0]
            if int(p) in LISTEN:
                await follow_mode(int(p))
        body = f"{time.time():.3f} set {q['port'][0]} {q['mode'][0]}\n"
    body += "".join(f"{p} {MODES.get(p, 'pass')} conns={CONNS.get(p, 0)} up={BYTES.get(p, [0, 0])[0]} "
                    f"down={BYTES.get(p, [0, 0])[1]}\n" for p in sorted(LISTEN))
    writer.write(f"HTTP/1.1 200 OK\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n{body}".encode())
    await writer.drain()
    writer.close()


async def main():
    control_port = int(sys.argv[1])
    for spec in sys.argv[2:]:
        lp, up = map(int, spec.split(":"))
        LISTEN[lp] = up
        await listen(lp)
    await asyncio.start_server(control, "127.0.0.1", control_port)
    print("chaos up", LISTEN, flush=True)
    await asyncio.Event().wait()


asyncio.run(main())
