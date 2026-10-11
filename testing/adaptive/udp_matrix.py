"""Stage 1 UDP oracle and route checks. This module never starts a workload on import."""

import collections
import contextlib
import csv
import hashlib
import json
import os
import socket
import struct
import threading
import time
import uuid
from dataclasses import dataclass, field

import bench


STUN_DESTINATION = 19302
STREAM_SECONDS = 20 * 60
STREAM_RATE = 50
STREAM_PACKETS = STREAM_SECONDS * STREAM_RATE
WS_SECONDS = 10 * 60
FUNCTIONAL_STREAM_SECONDS = 60
FUNCTIONAL_WS_PING_SECONDS = 60
FUNCTIONAL_WS_IDLE_SECONDS = 30


def run_settings(functional=False):
    udp = FUNCTIONAL_STREAM_SECONDS if functional else STREAM_SECONDS
    ping = FUNCTIONAL_WS_PING_SECONDS if functional else WS_SECONDS
    idle = FUNCTIONAL_WS_IDLE_SECONDS if functional else WS_SECONDS
    return {"mode": "functional" if functional else "full",
            "udp_seconds": udp, "udp_rate_pps": STREAM_RATE, "udp_packets": udp * STREAM_RATE,
            "ws_ping_seconds": ping, "ws_idle_seconds": idle,
            "timed_seconds_per_path": udp + ping + idle,
            "removed_soak_seconds_per_path": {"udp": STREAM_SECONDS - udp,
                                              "ws_ping": WS_SECONDS - ping,
                                              "ws_idle": WS_SECONDS - idle},
            "performance_acceptance": False if functional else None}


PORT_BASE = 22000 + bench.OFFSET
HELPER_PORTS = {
    "relay": PORT_BASE + 2,
    "ws_proxy": PORT_BASE + 3,
    "xhttp_proxy": PORT_BASE + 4,
    "stun": PORT_BASE + 7,
    "echo": PORT_BASE + 8,
    "ws_echo": PORT_BASE + 10,
    "blackhole": PORT_BASE + 11,
    "turn": PORT_BASE + 12,
    "rtc_a": PORT_BASE + 13,
    "rtc_b": PORT_BASE + 14,
    "switch_old": PORT_BASE + 15,
    "switch_new": PORT_BASE + 16,
    "switch_old_echo": PORT_BASE + 17,
    "switch_new_echo": PORT_BASE + 18,
}
PATHS = ("reality", "relay", "ws_local_proxy", "xhttp_local_proxy", "awg")


def bucket(port):
    if port == 53:
        return "dns"
    if port == 443:
        return "quic"
    if port in (3478, 3479, 19302):
        return "stun"
    if port >= 50000:
        return "rtc"
    return "other"


def port_inventory():
    tcp = {bench.TARGET_PORT, bench.TARGET_TLS_PORT, bench.CHAOS_CONTROL,
           bench.PROVIDER_PORT, bench.SOCKS_PORT, bench.API_PORT, bench.METRICS_PORT,
           *(bench.server_port(k) for k in bench.TRANSPORTS), *bench.all_address_ports(),
           HELPER_PORTS["relay"], HELPER_PORTS["ws_proxy"],
           HELPER_PORTS["xhttp_proxy"], HELPER_PORTS["ws_echo"],
           HELPER_PORTS["switch_old"], HELPER_PORTS["switch_new"]}
    udp = {*bench.UDP_PORTS, HELPER_PORTS["stun"], HELPER_PORTS["echo"],
           HELPER_PORTS["blackhole"], HELPER_PORTS["turn"],
           HELPER_PORTS["rtc_a"], HELPER_PORTS["rtc_b"],
           HELPER_PORTS["switch_old_echo"], HELPER_PORTS["switch_new_echo"]}
    return tcp, udp


def assert_ports_available():
    tcp, udp = port_inventory()
    if any(not 1 <= port <= 65535 for port in tcp | udp):
        raise RuntimeError("matrix port outside 1..65535")
    occupied = [f"tcp:{p}" for p in sorted(tcp) if bench.port_open(p)]
    occupied += [f"udp:{p}" for p in sorted(udp) if bench.udp_bound(p)]
    if occupied:
        raise RuntimeError("matrix port collision: " + ", ".join(occupied))


def save_json(path, value):
    with open(path, "w") as output:
        json.dump(value, output, indent=2, sort_keys=True)


@dataclass
class Flow:
    name: str
    tag: str
    source: str
    first_destination: str
    opened_ns: int
    sent: list = field(default_factory=list)
    received: list = field(default_factory=list)
    closed_ns: int | None = None
    window_index: int | None = None

    @property
    def first_port(self):
        return int(self.first_destination.rsplit(":", 1)[1])

    def expected(self):
        if self.closed_ns is None:
            return None
        return {"bucket": bucket(self.first_port), "unanswered": bool(self.sent and not self.received)}

    def as_json(self):
        return {"name": self.name, "tag": self.tag, "source": self.source,
                "first_destination": self.first_destination, "opened_ns": self.opened_ns,
                "closed_ns": self.closed_ns, "window_index": self.window_index,
                "sent": self.sent, "received": self.received, "expected": self.expected()}


class Ledger:
    def __init__(self, output):
        self.output = output
        self.flows = []
        self.notes = []
        self.lock = threading.Lock()

    def new_flow(self, name, tag, source, destination):
        flow = Flow(name, tag, source, destination, time.monotonic_ns())
        with self.lock:
            self.flows.append(flow)
        return flow

    def note(self, kind, **fields):
        with self.lock:
            self.notes.append({"when_ns": time.monotonic_ns(), "kind": kind, **fields})

    def flush(self):
        with self.lock:
            save_json(self.output, {"flows": [f.as_json() for f in self.flows],
                                    "notes": self.notes})
            timeline = self.output.removesuffix(".json") + "-timeline.csv"
            with open(timeline, "w", newline="") as output:
                writer = csv.DictWriter(output, fieldnames=("at_ns", "flow", "kind", "destination",
                                                             "bytes", "sequence"))
                writer.writeheader()
                for flow in self.flows:
                    for sent in flow.sent:
                        writer.writerow({"at_ns": sent["when_ns"], "flow": flow.name, "kind": "send",
                                         "destination": sent["destination"], "bytes": sent["bytes"],
                                         "sequence": sent.get("sequence")})
                    for received in flow.received:
                        writer.writerow({"at_ns": received["when_ns"], "flow": flow.name, "kind": "receive",
                                         "destination": received["source"], "bytes": received["bytes"]})


def recv_exact(sock, size):
    data = bytearray()
    while len(data) < size:
        part = sock.recv(size - len(data))
        if not part:
            raise ConnectionError("SOCKS control closed")
        data.extend(part)
    return bytes(data)


def socks_address(host, port):
    try:
        ip = socket.inet_aton(host)
    except OSError:
        encoded = host.encode("ascii")
        if len(encoded) > 255:
            raise ValueError("SOCKS host too long")
        return b"\x03" + bytes([len(encoded)]) + encoded + struct.pack("!H", port)
    return b"\x01" + ip + struct.pack("!H", port)


def read_socks_address(sock, atyp):
    if atyp == 1:
        host = socket.inet_ntoa(recv_exact(sock, 4))
    elif atyp == 3:
        host = recv_exact(sock, recv_exact(sock, 1)[0]).decode("ascii")
    elif atyp == 4:
        host = socket.inet_ntop(socket.AF_INET6, recv_exact(sock, 16))
    else:
        raise ValueError(f"unsupported SOCKS address type {atyp}")
    return host, struct.unpack("!H", recv_exact(sock, 2))[0]


class SocksUDP:
    """One real SOCKS UDP association: the TCP control stays open until close."""

    def __init__(self, ledger, tag, name, timeout=5):
        self.ledger, self.tag, self.name, self.timeout = ledger, tag, name, timeout
        self.control = None
        self.packet = None
        self.flow = None
        self.relay = None

    def __enter__(self):
        if bench.ADB_SERIAL:
            raise RuntimeError("adb TCP forwarding cannot carry SOCKS UDP; use a TUN/device driver")
        self.control = socket.create_connection(("127.0.0.1", bench.SOCKS_PORT), self.timeout)
        self.control.settimeout(self.timeout)
        self.control.sendall(b"\x05\x01\x00")
        if recv_exact(self.control, 2) != b"\x05\x00":
            raise RuntimeError("SOCKS no-auth greeting rejected")
        self.packet = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.packet.bind(("127.0.0.1", 0))
        self.packet.settimeout(self.timeout)
        self.control.sendall(b"\x05\x03\x00" + socks_address("0.0.0.0", self.packet.getsockname()[1]))
        header = recv_exact(self.control, 4)
        if header[:3] != b"\x05\x00\x00":
            raise RuntimeError(f"SOCKS UDP associate failed: {header!r}")
        relay = read_socks_address(self.control, header[3])
        self.relay = ("127.0.0.1" if relay[0] == "0.0.0.0" else relay[0], relay[1])
        return self

    def send(self, host, port, payload, sequence=None):
        if self.flow is None:
            source = f"{self.packet.getsockname()[0]}:{self.packet.getsockname()[1]}"
            self.flow = self.ledger.new_flow(self.name, self.tag, source, f"{host}:{port}")
        frame = b"\x00\x00\x00" + socks_address(host, port) + payload
        when = time.monotonic_ns()
        self.packet.sendto(frame, self.relay)
        self.flow.sent.append({"when_ns": when, "destination": f"{host}:{port}",
                               "bytes": len(payload), "sequence": sequence})

    def receive(self, timeout=None):
        self.packet.settimeout(self.timeout if timeout is None else timeout)
        frame, peer = self.packet.recvfrom(65535)
        if peer != self.relay or frame[:3] != b"\x00\x00\x00":
            raise RuntimeError("unexpected SOCKS UDP response")
        atyp = frame[3]
        size = {1: 4, 4: 16}.get(atyp)
        if atyp == 3:
            size = 1 + frame[4]
        if size is None:
            raise RuntimeError("bad SOCKS UDP response address")
        pos = 4 + size + 2
        if atyp == 1:
            source_host = socket.inet_ntoa(frame[4:8])
        elif atyp == 4:
            source_host = socket.inet_ntop(socket.AF_INET6, frame[4:20])
        else:
            source_host = frame[5:5 + frame[4]].decode("ascii")
        source_port = struct.unpack("!H", frame[pos - 2:pos])[0]
        payload = frame[pos:]
        self.flow.received.append({"when_ns": time.monotonic_ns(), "source":
                                   f"{source_host}:{source_port}", "bytes": len(payload)})
        return payload

    def __exit__(self, *_):
        if self.packet:
            self.packet.close()
        if self.control:
            self.control.close()
        if self.flow:
            self.flow.closed_ns = time.monotonic_ns()
        self.ledger.flush()


STUN_COOKIE = 0x2112A442
STUN_HEADER = struct.Struct("!HHI12s")


def stun_binding_success(transaction, sender):
    if len(transaction) != 12:
        raise ValueError("STUN transaction ID must have 12 bytes")
    host, port = sender
    address = struct.unpack("!I", socket.inet_aton(host))[0]
    mapped = struct.pack("!BBHI", 0, 1, port ^ (STUN_COOKIE >> 16), address ^ STUN_COOKIE)
    attribute = struct.pack("!HH", 0x0020, len(mapped)) + mapped
    return STUN_HEADER.pack(0x0101, len(attribute), STUN_COOKIE, transaction) + attribute


def parse_stun_binding_success(response, transaction):
    if len(response) < STUN_HEADER.size or len(transaction) != 12:
        raise ValueError("STUN header or transaction ID length invalid")
    message_type, length, cookie, response_id = STUN_HEADER.unpack_from(response)
    if message_type != 0x0101 or cookie != STUN_COOKIE or response_id != transaction:
        raise ValueError("STUN response type, cookie or transaction ID mismatch")
    if length % 4 or len(response) != STUN_HEADER.size + length:
        raise ValueError("STUN message length invalid")
    mapped = None
    offset = STUN_HEADER.size
    while offset < len(response):
        if len(response) - offset < 4:
            raise ValueError("STUN attribute header truncated")
        kind, size = struct.unpack_from("!HH", response, offset)
        offset += 4
        padded_end = offset + ((size + 3) // 4) * 4
        if padded_end > len(response):
            raise ValueError("STUN attribute length invalid")
        if kind == 0x0020:
            if mapped is not None or size != 8:
                raise ValueError("STUN XOR-MAPPED-ADDRESS size or multiplicity invalid")
            reserved, family, xor_port, xor_address = struct.unpack_from("!BBHI", response, offset)
            if reserved != 0 or family != 1:
                raise ValueError("controlled STUN oracle requires IPv4 XOR-MAPPED-ADDRESS")
            mapped = (socket.inet_ntoa(struct.pack("!I", xor_address ^ cookie)),
                      xor_port ^ (cookie >> 16))
            if mapped[1] == 0:
                raise ValueError("STUN mapped source port is zero")
        offset = padded_end
    if mapped is None:
        raise ValueError("STUN Binding Success lacks XOR-MAPPED-ADDRESS")
    return mapped


class UDPOracle:
    """Controlled target with a raw receipt log for echo, STUN and silence."""

    def __init__(self, port, mode, output):
        self.port, self.mode, self.output = port, mode, output
        self.socket = None
        self.thread = None
        self.stop = threading.Event()
        self.receipts = []

    def __enter__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("127.0.0.1", self.port))
        self.socket.settimeout(0.2)
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=2)
        self.socket.close()
        save_json(self.output, self.receipts)

    def _loop(self):
        while not self.stop.is_set():
            try:
                payload, sender = self.socket.recvfrom(65535)
            except socket.timeout:
                continue
            event = {"when_ns": time.monotonic_ns(), "from": list(sender),
                     "bytes": len(payload), "payload_prefix_hex": payload[:24].hex(),
                     "payload_sha256": hashlib.sha256(payload).hexdigest()}
            self.receipts.append(event)
            if self.mode == "blackhole":
                continue
            if self.mode == "stun":
                if (len(payload) != STUN_HEADER.size or payload[:4] != b"\x00\x01\x00\x00" or
                        payload[4:8] != struct.pack("!I", STUN_COOKIE)):
                    continue
                answer = stun_binding_success(payload[8:20], sender)
                event["mapped_source"] = list(sender)
                event["response_hex"] = answer.hex()
            else:
                answer = payload
            try:
                self.socket.sendto(answer, sender)
                event["reply_ns"] = time.monotonic_ns()
            except OSError as error:
                event["reply_error"] = repr(error)


def stun_case(ledger, tag, target_host, attempts=5):
    replies = []
    used_transactions = set()
    with SocksUDP(ledger, tag, "stun") as channel:
        for _ in range(attempts):
            transaction = os.urandom(12)
            while transaction in used_transactions:
                transaction = os.urandom(12)
            used_transactions.add(transaction)
            request = b"\x00\x01\x00\x00\x21\x12\xa4\x42" + transaction
            started = time.monotonic_ns()
            channel.send(target_host, STUN_DESTINATION, request, transaction.hex())
            mapped_source, error = None, None
            try:
                response = channel.receive()
                mapped_source = parse_stun_binding_success(response, transaction)
                valid = True
            except (socket.timeout, ValueError) as failure:
                valid, error = False, repr(failure)
            replies.append({"transaction": transaction.hex(), "valid": valid,
                            "mapped_source": mapped_source, "error": error,
                            "rtt_ms": (time.monotonic_ns() - started) / 1e6 if valid else None})
    return {"scope": "controlled IPv4 STUN Binding; no IPv6 or NAT claim",
            "attempts": attempts, "valid": sum(item["valid"] for item in replies), "replies": replies}


def dns_question(name):
    labels = name.split(".")
    return b"".join(bytes([len(label)]) + label.encode("ascii") for label in labels) + b"\x00\x00\x01\x00\x01"


def dns_case(ledger, tag, attempts=5):
    records = []
    names = ("one.one.one.one", "cloudflare.com", "example.com", "iana.org", "dns.google")
    used_ids = set()
    with SocksUDP(ledger, tag, "dns-cloudflare", timeout=4) as channel:
        for i in range(attempts):
            txid = os.urandom(2)
            while txid in used_ids:
                txid = os.urandom(2)
            used_ids.add(txid)
            name = names[i % len(names)]
            request = txid + b"\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00" + dns_question(name)
            started = time.monotonic_ns()
            channel.send("1.1.1.1", 53, request, i)
            try:
                response = channel.receive()
                answered = len(response) >= 12 and response[:2] == txid and response[2] & 0x80 != 0
                rcode = response[3] & 0x0f if answered else None
                answer_count = struct.unpack("!H", response[6:8])[0] if answered else 0
                valid = answered and rcode == 0 and answer_count > 0
            except socket.timeout:
                valid, rcode, answer_count = False, None, 0
            records.append({"id": txid.hex(), "name": name, "valid": valid, "rcode": rcode,
                            "answer_count": answer_count,
                            "rtt_ms": (time.monotonic_ns() - started) / 1e6 if valid else None})
    return {"destination": "1.1.1.1:53", "external": True, "valid": sum(r["valid"] for r in records),
            "records": records}


STREAM_HEADER = struct.Struct("!8sIQ")


def stream_case(ledger, tag, target_host, duration=STREAM_SECONDS, rate=STREAM_RATE,
                required_duration=STREAM_SECONDS):
    expected = duration * rate
    identity = os.urandom(8)
    arrivals = {}
    with SocksUDP(ledger, tag, "stream-50pps", timeout=0.01) as channel:
        start = time.monotonic()
        last_sent = start
        for sequence in range(expected):
            due = start + sequence / rate
            pause = due - time.monotonic()
            if pause > 0:
                time.sleep(pause)
            payload = STREAM_HEADER.pack(identity, sequence, time.monotonic_ns()) + bytes(128)
            channel.send(target_host, HELPER_PORTS["echo"], payload, sequence)
            last_sent = time.monotonic()
            while True:
                try:
                    answer = channel.receive(timeout=0)
                except (socket.timeout, BlockingIOError):
                    break
                received_ns = time.monotonic_ns()
                if len(answer) < STREAM_HEADER.size:
                    continue
                token, number, sent_ns = STREAM_HEADER.unpack_from(answer)
                if token == identity and number < expected and number not in arrivals:
                    arrivals[number] = (received_ns, sent_ns)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                answer = channel.receive(timeout=0.1)
            except socket.timeout:
                continue
            if len(answer) >= STREAM_HEADER.size:
                token, number, sent_ns = STREAM_HEADER.unpack_from(answer)
                if token == identity and number < expected and number not in arrivals:
                    arrivals[number] = (time.monotonic_ns(), sent_ns)
    seen = sorted(arrivals.items(), key=lambda item: item[1][0])
    order = [number for number, _ in seen]
    largest = -1
    reorder = 0
    for number in order:
        if number < largest:
            reorder += 1
        largest = max(largest, number)
    rtts = sorted((received - sent) / 1e6 for received, sent in arrivals.values())
    intervals = [(seen[i][1][0] - seen[i - 1][1][0]) / 1e6 for i in range(1, len(seen))]
    jitter = sum(abs(value - 1000 / rate) for value in intervals) / len(intervals) if intervals else None
    span = last_sent - start
    return {"identity_hex": identity.hex(), "required_packets": required_duration * STREAM_RATE,
            "required_seconds": required_duration, "configured_seconds": duration,
            "required_rate_pps": STREAM_RATE, "configured_rate_pps": rate,
            "sent": expected, "received_unique": len(arrivals), "received_sequences": sorted(arrivals),
            "lost": expected - len(arrivals), "reordered": reorder, "jitter_ms_mean_abs": jitter,
            "rtt_p50_ms": rtts[len(rtts) // 2] if rtts else None,
            "rtt_p95_ms": rtts[int((len(rtts) - 1) * 0.95)] if rtts else None,
            "send_span_seconds": span, "effective_rate_pps": (expected - 1) / span if span > 0 else None,
            "meets_required_duration": duration == required_duration and rate == STREAM_RATE and
            required_duration - 0.5 <= span <= required_duration * 1.01}


def stream_target_proof(receipts, stream):
    identity = bytes.fromhex(stream["identity_hex"])
    seen = set()
    duplicates, foreign, invalid = [], [], []
    valid_receipts = []
    for receipt in receipts:
        try:
            prefix = bytes.fromhex(receipt["payload_prefix_hex"])
        except (KeyError, ValueError):
            invalid.append(receipt)
            continue
        if prefix[:8] != identity:
            foreign.append(receipt)
            continue
        if len(prefix) < STREAM_HEADER.size:
            invalid.append(receipt)
            continue
        token, sequence, sent_ns = STREAM_HEADER.unpack_from(prefix)
        if not 0 <= sequence < stream["sent"] or receipt["bytes"] != STREAM_HEADER.size + 128:
            invalid.append(receipt)
            continue
        expected = STREAM_HEADER.pack(token, sequence, sent_ns) + bytes(128)
        if receipt["payload_sha256"] != hashlib.sha256(expected).hexdigest():
            invalid.append(receipt)
            continue
        valid_receipts.append(receipt)
        if sequence in seen:
            duplicates.append(sequence)
        seen.add(sequence)
    received = set(stream["received_sequences"])
    return {"received_unique": len(seen), "sequences": sorted(seen),
            "duplicate_count": len(duplicates), "duplicate_sequences": duplicates,
            "foreign_count": len(foreign), "foreign_receipts": foreign,
            "invalid_count": len(invalid), "invalid_receipts": invalid,
            "receipts": valid_receipts, "missing_before_target": sorted(set(range(stream["sent"])) - seen),
            "missing_after_target": sorted(seen - received),
            "client_sequences_without_target": sorted(received - seen),
            "ok": not invalid and received <= seen and len(seen) <= stream["sent"]}


def mixed_port_case(ledger, tag, target_host, reverse=False):
    destinations = [("1.1.1.1", 53), (target_host, STUN_DESTINATION)]
    if reverse:
        destinations.reverse()
    with SocksUDP(ledger, tag, "mixed-reverse" if reverse else "mixed-forward") as channel:
        for index, (host, port) in enumerate(destinations):
            payload = (b"\x00\x01\x00\x00\x21\x12\xa4\x42" + os.urandom(12)
                       if port == STUN_DESTINATION else
                       os.urandom(2) + b"\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00" +
                       dns_question(f"mixed-{uuid.uuid4().hex[:8]}.example.com"))
            channel.send(host, port, payload, index)
            try:
                channel.receive()
            except socket.timeout:
                pass
    return {"first_destination": f"{destinations[0][0]}:{destinations[0][1]}",
            "second_destination": f"{destinations[1][0]}:{destinations[1][1]}"}


def association_cases(ledger, tag, target_host):
    results = {}
    with SocksUDP(ledger, tag, "answered-once") as channel:
        token = os.urandom(12)
        channel.send(target_host, HELPER_PORTS["echo"], token)
        results["answered_once"] = channel.receive() == token
    with SocksUDP(ledger, tag, "answered-five") as channel:
        replies = 0
        for index in range(5):
            token = os.urandom(12)
            channel.send(target_host, HELPER_PORTS["echo"], token, index)
            replies += channel.receive() == token
        results["answered_five"] = replies
    with SocksUDP(ledger, tag, "unanswered") as channel:
        channel.send(target_host, HELPER_PORTS["blackhole"], os.urandom(12))
        try:
            channel.receive(timeout=1)
            results["unanswered_timeout"] = False
        except socket.timeout:
            results["unanswered_timeout"] = True
    with contextlib.ExitStack() as stack:
        first = stack.enter_context(SocksUDP(ledger, tag, "parallel-a"))
        second = stack.enter_context(SocksUDP(ledger, tag, "parallel-b"))
        for channel in (first, second):
            token = os.urandom(12)
            channel.send(target_host, HELPER_PORTS["echo"], token)
            if channel.receive() != token:
                raise RuntimeError("parallel association echo mismatch")
        results["parallel_sources"] = [first.flow.source, second.flow.source]
    return results


def import_pion_flows(ledger, tag, mode, result):
    imported = []
    for peer in ("a", "b"):
        events = result[f"peer_{peer}"]["events"]
        by_source = collections.defaultdict(list)
        for event in events:
            by_source[event["source_port"]].append(event)
        for source_port, records in by_source.items():
            sends = [item for item in records if item["direction"] == "send"]
            if not sends:
                continue
            first = sends[0]
            flow = ledger.new_flow(f"pion-{mode}-{peer}-{source_port}", tag,
                                   f"127.0.0.1:{source_port}", first["destination"])
            flow.sent = [{"when_ns": item["at_unix_ns"], "destination": item["destination"],
                          "bytes": item["bytes"]} for item in sends]
            flow.received = [{"when_ns": item["at_unix_ns"], "source": item["destination"],
                              "bytes": item["bytes"]} for item in records if item["direction"] == "recv"]
            flow.closed_ns = time.monotonic_ns()
            imported.append(flow)
    if len(imported) < 2:
        raise RuntimeError(f"Pion {mode} has fewer than two routed logical associations")
    ledger.flush()
    return [flow.name for flow in imported]


def reconcile_flows(flows, report, tag):
    relevant = [f for f in flows if f.tag == tag and f.closed_ns is not None]
    expected = collections.Counter(f.expected()["bucket"] for f in relevant)
    unanswered = collections.Counter(f.expected()["bucket"] for f in relevant if f.expected()["unanswered"])
    actual = report.get("paths", {}).get(tag, {})
    ports = actual.get("udp_ports", {})
    issues = []
    if actual.get("udp_flows") != len(relevant):
        issues.append(f"udp_flows={actual.get('udp_flows')} expected={len(relevant)}")
    if actual.get("udp_flows_unanswered") != sum(unanswered.values()):
        issues.append("udp_flows_unanswered differs from association ledger")
    for name in set(expected) | set(ports):
        observed = ports.get(name, {})
        if observed.get("flows", 0) != expected[name] or observed.get("unanswered", 0) != unanswered[name]:
            issues.append(f"udp_ports.{name}={observed} expected=({expected[name]},{unanswered[name]})")
    sent = sum(len(f.sent) for f in relevant)
    received = sum(len(f.received) for f in relevant)
    sent_bytes = sum(item["bytes"] for flow in relevant for item in flow.sent)
    received_bytes = sum(item["bytes"] for flow in relevant for item in flow.received)
    if actual.get("udp_writes", -1) < sent or actual.get("udp_reads", -1) < received:
        issues.append("UDP packet counters below application ledger; inspect packet boundary")
    if actual.get("udp_bytes_up", -1) < sent_bytes or actual.get("udp_bytes_down", -1) < received_bytes:
        issues.append("UDP byte counters below application ledger; inspect payload boundary")
    first_reply_ms = sorted((flow.received[0]["when_ns"] - flow.sent[0]["when_ns"]) / 1e6
                            for flow in relevant if flow.sent and flow.received
                            and flow.received[0]["when_ns"] >= flow.sent[0]["when_ns"])
    return {"ok": not issues, "issues": issues, "expected_flows": len(relevant),
            "expected_ports": dict(expected), "expected_unanswered": dict(unanswered),
            "minimum_udp_writes": sent, "minimum_udp_reads": received,
            "minimum_udp_bytes_up": sent_bytes, "minimum_udp_bytes_down": received_bytes,
            "ledger_first_reply_p50_ms": first_reply_ms[len(first_reply_ms) // 2] if first_reply_ms else None,
            "actual": actual}
