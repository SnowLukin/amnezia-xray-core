"""Local UDP proxy regressions; no Xray process or network listener is started."""
import asyncio
import random
import unittest
from unittest.mock import patch

import chaos


PORT = 54321


class Transport:
    def __init__(self):
        self.sent = []
        self.closed = False

    def sendto(self, data, addr=None):
        self.sent.append((bytes(data), addr))

    def close(self):
        self.closed = True


class Listener:
    def __init__(self):
        self.transport = Transport()
        self.sessions = {}


class Reader:
    def __init__(self, mode):
        self.lines = iter((f"GET /set?port={PORT}&mode={mode} HTTP/1.1\r\n".encode(), b"\r\n"))

    async def readline(self):
        return next(self.lines, b"")


class Writer:
    def __init__(self):
        self.body = b""

    def write(self, data):
        self.body += data

    async def drain(self):
        pass

    def close(self):
        pass


class ChaosUdpTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        for mapping in (chaos.MODES, chaos.CONNS, chaos.BYTES, chaos.UDP, chaos.PACKETS,
                        chaos.NET, chaos.NET_STATS, chaos.UDP_LISTENERS):
            mapping.clear()
        chaos.UDP[PORT] = 54322
        self.listener = Listener()
        chaos.UDP_LISTENERS[PORT] = self.listener

    def session(self):
        client = ("127.0.0.1", 54323 + len(self.listener.sessions))
        session = chaos.UdpSession(PORT, self.listener, client)
        self.listener.sessions[client] = session
        return session

    async def open(self, session):
        upstream = Transport()

        async def endpoint(factory, remote_addr):
            self.assertEqual(remote_addr, ("127.0.0.1", chaos.UDP[PORT]))
            factory()
            return upstream, None

        loop = asyncio.get_running_loop()
        with patch.object(loop, "create_datagram_endpoint", side_effect=endpoint):
            await session.open()
        return upstream

    async def test_pass_pending_counts_once_and_drop_after_one(self):
        chaos.MODES[PORT] = "pass"
        session = self.session()
        session.forward(0, b"first")
        self.assertEqual(session.forwarded, [1, 0])
        upstream = await self.open(session)
        self.assertEqual(upstream.sent, [(b"first", None)])
        self.assertEqual(session.forwarded, [1, 0])

        chaos.MODES[PORT] = "drop_after:1"
        later = self.session()
        later.forward(0, b"allowed")
        later.forward(0, b"dropped")
        self.assertEqual(later.forwarded, [1, 0])
        upstream = await self.open(later)
        self.assertEqual(upstream.sent, [(b"allowed", None)])
        self.assertEqual(later.forwarded, [1, 0])

    async def test_seeded_net_preserves_bytes_order_and_counters(self):
        seed = 17
        chaos.MODES[PORT] = f"net:0:50:{seed}"
        chaos.NET[PORT] = {"delay": 0, "loss": 50,
                           "rng": [random.Random(seed * 2 + direction) for direction in (0, 1)],
                           "seed": seed}
        chaos.NET_STATS[PORT] = {key: [0, 0] for key in
                                 ("offered", "dropped", "forwarded", "pending", "cancelled")}
        session = self.session()
        upstream = await self.open(session)
        sent_up = [bytes((i, 0, 255 - i)) for i in range(20)]
        sent_down = [bytes((255 - i, i, 0)) for i in range(20)]
        for data in sent_up:
            session.forward(0, data)
        for data in sent_down:
            session.forward(1, data)
        await asyncio.sleep(0.02)

        expected = []
        for direction, offered in enumerate((sent_up, sent_down)):
            rng = random.Random(seed * 2 + direction)
            expected.append([data for data in offered if rng.random() >= 0.5])
        self.assertEqual([data for data, _ in upstream.sent], expected[0])
        self.assertEqual([data for data, _ in self.listener.transport.sent], expected[1])
        stats = chaos.NET_STATS[PORT]
        for direction in (0, 1):
            self.assertEqual(stats["offered"][direction], 20)
            self.assertEqual(stats["dropped"][direction], 20 - len(expected[direction]))
            self.assertEqual(stats["forwarded"][direction], len(expected[direction]))
            self.assertEqual(stats["pending"][direction], 0)
            self.assertEqual(stats["cancelled"][direction], 0)

    async def test_mode_change_cancels_delayed_datagram(self):
        chaos.MODES[PORT] = "net:100:0:7"
        chaos.NET[PORT] = {"delay": 100, "loss": 0,
                           "rng": [random.Random(14), random.Random(15)], "seed": 7}
        chaos.NET_STATS[PORT] = {key: [0, 0] for key in
                                 ("offered", "dropped", "forwarded", "pending", "cancelled")}
        session = self.session()
        upstream = await self.open(session)
        session.forward(0, b"cancel me")
        stats = chaos.NET_STATS[PORT]
        self.assertEqual(stats["pending"], [1, 0])
        writer = Writer()
        await chaos.control(Reader("pass"), writer)
        await asyncio.sleep(0.12)
        self.assertTrue(writer.body.startswith(b"HTTP/1.1 200"))
        self.assertEqual(upstream.sent, [])
        self.assertEqual(stats["offered"], [1, 0])
        self.assertEqual(stats["cancelled"], [1, 0])
        self.assertEqual(stats["pending"], [0, 0])

    async def test_net_mode_change_uses_new_deadline_and_keeps_packet_order(self):
        chaos.MODES[PORT] = "net:5000:0:7"
        chaos.NET[PORT] = {"delay": 5000, "loss": 0,
                           "rng": [random.Random(14), random.Random(15)], "seed": 7}
        chaos.NET_STATS[PORT] = {key: [0, 0] for key in
                                 ("offered", "dropped", "forwarded", "pending", "cancelled")}
        session = self.session()
        upstream = await self.open(session)
        session.forward(0, b"old deadline")
        old = chaos.NET_STATS[PORT]
        self.assertEqual(old["pending"], [1, 0])
        writer = Writer()
        await chaos.control(Reader("net:10:0:7"), writer)
        self.assertTrue(writer.body.startswith(b"HTTP/1.1 200"))
        self.assertEqual((old["cancelled"], old["pending"]), ([1, 0], [0, 0]))
        self.assertEqual(session.last_delivery, [0.0, 0.0])
        session.forward(0, b"new first")
        session.forward(0, b"new second")
        await asyncio.sleep(0.03)
        self.assertEqual(upstream.sent, [(b"new first", None), (b"new second", None)])
        current = chaos.NET_STATS[PORT]
        self.assertEqual(current["offered"], [2, 0])
        self.assertEqual(current["dropped"], [0, 0])
        self.assertEqual(current["forwarded"], [2, 0])
        self.assertEqual(current["pending"], [0, 0])
        self.assertEqual(current["cancelled"], [0, 0])

    async def test_net_datagram_waits_for_upstream_and_close_cancels_it(self):
        chaos.MODES[PORT] = "net:0:0:9"
        chaos.NET[PORT] = {"delay": 0, "loss": 0,
                           "rng": [random.Random(18), random.Random(19)], "seed": 9}
        chaos.NET_STATS[PORT] = {key: [0, 0] for key in
                                 ("offered", "dropped", "forwarded", "pending", "cancelled")}
        session = self.session()
        session.forward(0, b"\x00\x01hello")
        await asyncio.sleep(0.01)
        stats = chaos.NET_STATS[PORT]
        self.assertEqual((stats["pending"], stats["forwarded"]), ([1, 0], [0, 0]))
        upstream = await self.open(session)
        self.assertEqual(upstream.sent, [(b"\x00\x01hello", None)])
        self.assertEqual((stats["pending"], stats["forwarded"]), ([0, 0], [1, 0]))

        another = self.session()
        another.forward(0, b"cancel before open")
        await asyncio.sleep(0.01)
        another.close()
        self.assertEqual(stats["offered"], [2, 0])
        self.assertEqual(stats["forwarded"], [1, 0])
        self.assertEqual(stats["cancelled"], [1, 0])
        self.assertEqual(stats["pending"], [0, 0])


if __name__ == "__main__":
    unittest.main()
