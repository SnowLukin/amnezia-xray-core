"""WebSocket survival control over the same selected outbound path."""

import base64
import hashlib
import os
import select
import socket
import struct
import threading
import time

import bench
from udp_matrix import HELPER_PORTS, WS_SECONDS, recv_exact, read_socks_address, save_json, socks_address


GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def read_header(sock):
    data = bytearray()
    while b"\r\n\r\n" not in data:
        part = sock.recv(4096)
        if not part:
            raise ConnectionError("HTTP header ended early")
        data.extend(part)
        if len(data) > 16384:
            raise ValueError("HTTP header too long")
    return bytes(data)


def read_frame(sock):
    head = recv_exact(sock, 2)
    opcode = head[0] & 0x0f
    length = head[1] & 0x7f
    masked = bool(head[1] & 0x80)
    if length == 126:
        length = struct.unpack("!H", recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", recv_exact(sock, 8))[0]
    if length > 65536:
        raise ValueError("WebSocket frame too large")
    key = recv_exact(sock, 4) if masked else b""
    payload = bytearray(recv_exact(sock, length))
    if masked:
        for index in range(len(payload)):
            payload[index] ^= key[index % 4]
    return opcode, bytes(payload)


def send_frame(sock, opcode, payload, masked):
    if len(payload) >= 126:
        raise ValueError("control frame too large")
    first = bytes([0x80 | opcode, len(payload) | (0x80 if masked else 0)])
    if masked:
        key = os.urandom(4)
        payload = bytes(value ^ key[index % 4] for index, value in enumerate(payload))
        sock.sendall(first + key + payload)
    else:
        sock.sendall(first + payload)


class WebSocketEcho:
    def __init__(self, work):
        self.work = work
        self.stop = threading.Event()
        self.rows = []

    def __enter__(self):
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", HELPER_PORTS["ws_echo"]))
        self.listener.listen(8)
        self.listener.settimeout(0.2)
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=3)
        self.listener.close()
        save_json(os.path.join(self.work, "ws-target.json"), self.rows)

    def _accept(self):
        while not self.stop.is_set():
            try:
                connection, source = self.listener.accept()
            except socket.timeout:
                continue
            threading.Thread(target=self._handle, args=(connection, source), daemon=True).start()

    def _handle(self, connection, source):
        row = {"source": source, "opened_ns": time.monotonic_ns(), "pings": 0}
        self.rows.append(row)
        try:
            with connection:
                request = read_header(connection)
                headers = {}
                for line in request.split(b"\r\n")[1:]:
                    if b":" in line:
                        name, value = line.split(b":", 1)
                        headers[name.strip().lower()] = value.strip()
                key = headers[b"sec-websocket-key"]
                accept = base64.b64encode(hashlib.sha1(key + GUID).digest())
                connection.sendall(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                                   b"Connection: Upgrade\r\nSec-WebSocket-Accept: " + accept + b"\r\n\r\n")
                row["handshake_ns"] = time.monotonic_ns()
                while True:
                    opcode, payload = read_frame(connection)
                    if opcode == 9:
                        row["pings"] += 1
                        send_frame(connection, 10, payload, False)
                    elif opcode == 8:
                        send_frame(connection, 8, payload, False)
                        break
                    elif opcode == 1:
                        send_frame(connection, 1, payload, False)
        except Exception as error:
            row["error"] = repr(error)
        finally:
            row["closed_ns"] = time.monotonic_ns()


def socks_tcp(host, port, timeout=10):
    sock = socket.create_connection(("127.0.0.1", bench.SOCKS_PORT), timeout)
    sock.settimeout(timeout)
    sock.sendall(b"\x05\x01\x00")
    if recv_exact(sock, 2) != b"\x05\x00":
        raise RuntimeError("SOCKS greeting failed")
    sock.sendall(b"\x05\x01\x00" + socks_address(host, port))
    response = recv_exact(sock, 4)
    if response[:3] != b"\x05\x00\x00":
        raise RuntimeError(f"SOCKS CONNECT failed: {response!r}")
    read_socks_address(sock, response[3])
    return sock


def websocket_case(host, duration=WS_SECONDS, ping=True, required_duration=WS_SECONDS):
    started = time.monotonic_ns()
    result = {"required_seconds": required_duration, "configured_seconds": duration, "ping": ping,
              "pongs": 0, "pong_records": [], "close_code": None, "final_pong": False}
    with socks_tcp(host, HELPER_PORTS["ws_echo"]) as sock:
        key = base64.b64encode(os.urandom(16))
        sock.sendall(b"GET /stage1 HTTP/1.1\r\nHost: matrix.test\r\nUpgrade: websocket\r\n"
                     b"Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: "
                     + key + b"\r\n\r\n")
        response = read_header(sock)
        expected = base64.b64encode(hashlib.sha1(key + GUID).digest())
        if not response.startswith(b"HTTP/1.1 101 ") or expected not in response:
            raise RuntimeError("WebSocket handshake invalid")
        opened = time.monotonic()
        result["handshake_ms"] = (time.monotonic_ns() - started) / 1e6
        deadline = opened + duration
        next_ping = opened
        try:
            while time.monotonic() < deadline:
                if ping and time.monotonic() >= next_ping:
                    token = os.urandom(8)
                    sent = time.monotonic_ns()
                    sock.settimeout(5)
                    send_frame(sock, 9, token, True)
                    opcode, payload = read_frame(sock)
                    if opcode != 10 or payload != token:
                        raise ConnectionError("WebSocket pong mismatch or close frame")
                    pong = time.monotonic_ns()
                    result["pongs"] += 1
                    result["pong_records"].append({"sent_ns": sent, "pong_ns": pong,
                                                   "rtt_seconds": (pong - sent) / 1e9})
                    next_ping += 20
                wait = min(1, deadline - time.monotonic())
                if ping:
                    wait = min(wait, next_ping - time.monotonic())
                if wait > 0 and select.select([sock], [], [], wait)[0]:
                    sock.settimeout(5)
                    opcode, payload = read_frame(sock)
                    if opcode == 8:
                        result["close_code"] = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else None
                    raise ConnectionError(f"unexpected WebSocket frame {opcode}")
            result["survived_seconds"] = time.monotonic() - opened
            token = os.urandom(8)
            sock.settimeout(5)
            final_ping_ns = time.monotonic_ns()
            send_frame(sock, 9, token, True)
            opcode, payload = read_frame(sock)
            result["final_pong_ns"] = time.monotonic_ns()
            result["final_pong_rtt_seconds"] = (result["final_pong_ns"] - final_ping_ns) / 1e9
            result["final_pong"] = opcode == 10 and payload == token and result["final_pong_rtt_seconds"] <= 5
            if not result["final_pong"]:
                raise ConnectionError("WebSocket final liveness pong missing")
            send_frame(sock, 8, struct.pack("!H", 1000), True)
            try:
                opcode, payload = read_frame(sock)
                if opcode == 8 and len(payload) >= 2:
                    result["close_code"] = struct.unpack("!H", payload[:2])[0]
            except (OSError, ConnectionError):
                pass
        except (OSError, ConnectionError) as error:
            result["remote_closed_ns"] = time.monotonic_ns()
            result["close_error"] = repr(error)
            result.setdefault("survived_seconds", time.monotonic() - opened)
    pong_times = [record["pong_ns"] for record in result["pong_records"]]
    gaps = [(right - left) / 1e9 for left, right in zip(pong_times, pong_times[1:])]
    if ping and pong_times and result["final_pong"]:
        gaps.append((result["final_pong_ns"] - pong_times[-1]) / 1e9)
    result["max_pong_gap_seconds"] = max(gaps, default=0)
    result["required_pongs"] = (required_duration + 19) // 20 if ping else 0
    result["ok"] = (duration == required_duration and result["survived_seconds"] >= required_duration and
                    "remote_closed_ns" not in result and result["final_pong"] and
                    result["pongs"] >= result["required_pongs"] and result["max_pong_gap_seconds"] <= 25 and
                    all(record["rtt_seconds"] <= 5 for record in result["pong_records"]))
    return result
