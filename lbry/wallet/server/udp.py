import asyncio
import typing
import struct
import time
import logging
from typing import Optional, Tuple, NamedTuple
# from prometheus_client import Counter


log = logging.getLogger(__name__)
_MAGIC = 1446058291  # genesis blocktime (which is actually wrong)
# ping_count_metric = Counter("ping_count", "Number of pings received", namespace='wallet_server_status')


class PingPacket(NamedTuple):
    magic: int
    protocol_version: int

    def encode(self):
        return struct.pack(b'!lB', *self)

    @staticmethod
    def make(protocol_version=1) -> bytes:
        return PingPacket(_MAGIC, protocol_version).encode()

    @classmethod
    def decode(cls, packet: bytes):
        decoded = cls(*struct.unpack(b'!lB', packet[:5]))
        if decoded.magic != _MAGIC:
            raise ValueError("invalid magic bytes")
        return decoded


class PongPacket(NamedTuple):
    protocol_version: int
    available: int
    height: int
    tip: bytes

    def encode(self):
        return struct.pack(b'!BBl32s', *self)

    @staticmethod
    def make(height: int, tip: bytes, available: int, protocol_version: int = 1) -> bytes:
        return PongPacket(protocol_version, available, height, tip).encode()

    @classmethod
    def decode(cls, packet: bytes):
        return cls(*struct.unpack(b'!BBl32s', packet[:38]))


class SPVServerStatusProtocol(asyncio.DatagramProtocol):
    PROTOCOL_VERSION = 1

    def __init__(self, height: int, tip: bytes):
        super().__init__()
        self.transport: Optional[asyncio.transports.DatagramTransport] = None
        self._height = height
        self._tip = tip
        self._flags = 0
        self._cached_response = None
        self.update_cached_response()

    def update_cached_response(self):
        self._cached_response = PongPacket.make(self._height, self._tip, self._flags, self.PROTOCOL_VERSION)

    def set_unavailable(self):
        self._flags &= 0b11111110
        self.update_cached_response()

    def set_available(self):
        self._flags |= 0b00000001
        self.update_cached_response()

    def set_height(self, height: int, tip: bytes):
        self._height, self._tip = height, tip
        self.update_cached_response()

    def datagram_received(self, data: bytes, addr: Tuple[str, int]):
        try:
            PingPacket.decode(data)
        except (ValueError, struct.error, AttributeError, TypeError):
            log.exception("derp")
            return
        self.transport.sendto(self._cached_response, addr)
        # ping_count_metric.inc()

    def connection_made(self, transport) -> None:
        self.transport = transport

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.transport = None

    def close(self):
        if self.transport:
            self.transport.close()


class StatusServer:
    def __init__(self):
        self._protocol: Optional[SPVServerStatusProtocol] = None

    async def start(self, height: int, tip: bytes, interface: str, port: int):
        if self._protocol:
            return
        loop = asyncio.get_event_loop()
        self._protocol = SPVServerStatusProtocol(height, tip)
        await loop.create_datagram_endpoint(lambda: self._protocol, (interface, port), reuse_port=True)
        log.info("started udp status server on %s:%i", interface, port)

    def stop(self):
        if self._protocol:
            self._protocol.close()
            self._protocol = None

    def set_unavailable(self):
        self._protocol.set_unavailable()

    def set_available(self):
        self._protocol.set_available()

    def set_height(self, height: int, tip: bytes):
        self._protocol.set_height(height, tip)
