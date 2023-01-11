import asyncio
import ipaddress
import socket
import struct
from time import perf_counter
import logging
from typing import Optional, Tuple, NamedTuple, List, Union
from hub.schema.attrs import country_str_to_int, country_int_to_str
from hub.common import (
    LRUCache,
    is_valid_public_ip,
    is_valid_public_ipv4,
    is_valid_public_ipv6,
)


log = logging.getLogger(__name__)
_MAGIC = 1446058291  # genesis blocktime (which is actually wrong)
# ping_count_metric = Counter("ping_count", "Number of pings received", namespace='wallet_server_status')
_PAD_BYTES = b'\x00' * 64


PROTOCOL_VERSION = 1


class SPVPing(NamedTuple):
    magic: int
    protocol_version: int
    pad_bytes: bytes

    def encode(self):
        return struct.pack(b'!lB64s', *self)

    @staticmethod
    def make() -> bytes:
        return SPVPing(_MAGIC, PROTOCOL_VERSION, _PAD_BYTES).encode()

    @classmethod
    def decode(cls, packet: bytes):
        decoded = cls(*struct.unpack(b'!lB64s', packet[:69]))
        if decoded.magic != _MAGIC:
            raise ValueError("invalid magic bytes")
        return decoded


PONG_ENCODING_PRE = b'!BBL32s'
PONG_ENCODING_POST = b'!H'

class SPVPong(NamedTuple):
    protocol_version: int
    flags: int
    height: int
    tip: bytes
    ipaddr: Union[ipaddress.IPv4Address, ipaddress.IPv6Address]
    country: int

    FLAG_AVAILABLE = 0b00000001
    FLAG_IPV6 = 0b00000010

    def encode(self):
        return (struct.pack(PONG_ENCODING_PRE, self.protocol_version, self.flags, self.height, self.tip) +
                self.encode_address(self.ipaddr) +
                struct.pack(PONG_ENCODING_POST, self.country))

    @staticmethod
    def encode_address(address: Union[str, ipaddress.IPv4Address, ipaddress.IPv6Address]):
        if not isinstance(address, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
            address = ipaddress.ip_address(address)
        return address.packed

    @classmethod
    def make(cls, flags: int, height: int, tip: bytes, source_address: str, country: str) -> bytes:
        ipaddr = ipaddress.ip_address(source_address)
        flags = (flags | cls.FLAG_IPV6) if ipaddr.version == 6 else (flags & ~cls.FLAG_IPV6)
        return SPVPong(
            PROTOCOL_VERSION, flags, height, tip,
            ipaddr,
            country_str_to_int(country)
        )

    @classmethod
    def make_sans_source_address(cls, flags: int, height: int, tip: bytes, country: str) -> Tuple[bytes, bytes]:
        pong = cls.make(flags, height, tip, '0.0.0.0', country)
        pong = pong.encode()
        return pong[0:1], pong[2:38], pong[42:]

    @classmethod
    def decode(cls, packet: bytes):
        offset = 0
        protocol_version, flags, height, tip = struct.unpack(PONG_ENCODING_PRE, packet[offset:offset+38])
        offset += 38
        if flags & cls.FLAG_IPV6:
            addr_len = ipaddress.IPV6LENGTH // 8
            ipaddr = ipaddress.ip_address(packet[offset:offset+addr_len])
            offset += addr_len
        else:
            addr_len = ipaddress.IPV4LENGTH // 8
            ipaddr = ipaddress.ip_address(packet[offset:offset+addr_len])
            offset += addr_len
        country, = struct.unpack(PONG_ENCODING_POST, packet[offset:offset+2])
        offset += 2
        return cls(protocol_version, flags, height, tip, ipaddr, country)

    @property
    def available(self) -> bool:
        return (self.flags & self.FLAG_AVAILABLE) > 0

    @property
    def ipv6(self) -> bool:
        return (self.flags & self.FLAG_IPV6) > 0

    @property
    def ip_address(self) -> str:
        return self.ipaddr.compressed

    @property
    def country_name(self):
        return country_int_to_str(self.country)

    def __repr__(self) -> str:
        return f"SPVPong(external_ip={self.ip_address}, version={self.protocol_version}, " \
               f"available={'True' if self.flags & 1 > 0 else 'False'}," \
               f" height={self.height}, tip={self.tip[::-1].hex()}, country={self.country_name})"


class SPVServerStatusProtocol(asyncio.DatagramProtocol):

    def __init__(
        self, height: int, tip: bytes, country: str,
        throttle_cache_size: int = 1024, throttle_reqs_per_sec: int = 10,
        allow_localhost: bool = False, allow_lan: bool = False,
        is_valid_ip = is_valid_public_ip,
    ):
        super().__init__()
        self.transport: Optional[asyncio.transports.DatagramTransport] = None
        self._height = height
        self._tip = tip
        self._flags = 0
        self._country = country
        self._cache0 = self._cache1 = self.cache2 = None
        self.update_cached_response()
        self._throttle = LRUCache(throttle_cache_size)
        self._should_log = LRUCache(throttle_cache_size)
        self._min_delay = 1 / throttle_reqs_per_sec
        self._allow_localhost = allow_localhost
        self._allow_lan = allow_lan
        self._is_valid_ip = is_valid_ip
        self.closed = asyncio.Event()

    def update_cached_response(self):
        self._cache0, self._cache1, self._cache2 = SPVPong.make_sans_source_address(
            self._flags, max(0, self._height), self._tip, self._country
        )

    def set_unavailable(self):
        self._flags &= ~SPVPong.FLAG_AVAILABLE
        self.update_cached_response()

    def set_available(self):
        self._flags |= SPVPong.FLAG_AVAILABLE
        self.update_cached_response()

    def set_height(self, height: int, tip: bytes):
        self._height, self._tip = height, tip
        self.update_cached_response()

    def should_throttle(self, host: str):
        now = perf_counter()
        last_requested = self._throttle.get(host, default=0)
        self._throttle[host] = now
        if now - last_requested < self._min_delay:
            log_cnt = self._should_log.get(host, default=0) + 1
            if log_cnt % 100 == 0:
                log.warning("throttle spv status to %s", host)
            self._should_log[host] = log_cnt
            return True
        return False

    def make_pong(self, host):
        ipaddr = ipaddress.ip_address(host)
        if ipaddr.version == 6:
            flags = self._flags | SPVPong.FLAG_IPV6
        else:
            flags = self._flags & ~SPVPong.FLAG_IPV6
        return (self._cache0 + flags.to_bytes(1, 'big') +
                self._cache1 + SPVPong.encode_address(ipaddr) +
                self._cache2)

    def datagram_received(self, data: bytes, addr: Union[Tuple[str, int], Tuple[str, int, int, int]]):
        if self.should_throttle(addr[0]):
            # print(f"throttled: {addr}")
            return
        try:
            SPVPing.decode(data)
        except (ValueError, struct.error, AttributeError, TypeError):
            # log.exception("derp")
            return
        if addr[1] >= 1024 and self._is_valid_ip(
                addr[0], allow_localhost=self._allow_localhost, allow_lan=self._allow_lan):
            self.transport.sendto(self.make_pong(addr[0]), addr)
        else:
            log.warning("odd packet from %s:%i", addr[0], addr[1])
        # ping_count_metric.inc()

    def connection_made(self, transport) -> None:
        self.transport = transport
        self.closed.clear()

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.transport = None
        self.closed.set()

    async def close(self):
        if self.transport:
            self.transport.close()
        await self.closed.wait()


class StatusServer:
    def __init__(self):
        self._protocols: List[SPVServerStatusProtocol] = []

    async def start(self, height: int, tip: bytes, country: str, interface: str, port: int, allow_lan: bool = False):
        if self.is_running:
            return
        loop = asyncio.get_event_loop()
        addr = interface if interface.lower() != 'localhost' else '127.0.0.1'
        proto = SPVServerStatusProtocol(
            height, tip, country, allow_localhost=addr == '127.0.0.1', allow_lan=allow_lan,
            is_valid_ip=is_valid_public_ipv4,
        )
        await loop.create_datagram_endpoint(lambda: proto, (addr, port), family=socket.AF_INET)
        log.warning("started udp4 status server on %s", proto.transport.get_extra_info('sockname')[:2])
        self._protocols.append(proto)
        if not socket.has_ipv6:
            return
        addr = interface if interface.lower() != 'localhost' else '::1'
        proto = SPVServerStatusProtocol(
            height, tip, country, allow_localhost=addr == '::1', allow_lan=allow_lan,
            is_valid_ip=is_valid_public_ipv6,
        )
        await loop.create_datagram_endpoint(lambda: proto, (addr, port), family=socket.AF_INET6)
        log.warning("started udp6 status server on %s", proto.transport.get_extra_info('sockname')[:2])
        self._protocols.append(proto)

    async def stop(self):
        for proto in self._protocols:
            await proto.close()
        self._protocols.clear()

    @property
    def is_running(self):
        return self._protocols

    def set_unavailable(self):
        for proto in self._protocols:
            proto.set_unavailable()

    def set_available(self):
        for proto in self._protocols:
            proto.set_available()

    def set_height(self, height: int, tip: bytes):
        for proto in self._protocols:
            proto.set_height(height, tip)


class SPVStatusClientProtocol(asyncio.DatagramProtocol):

    def __init__(self, responses: asyncio.Queue):
        super().__init__()
        self.transport: Optional[asyncio.transports.DatagramTransport] = None
        self.responses = responses
        self._ping_packet = SPVPing.make()

    def datagram_received(self, data: bytes, addr: Union[Tuple[str, int], Tuple[str, int, int, int]]):
        try:
            if len(addr) > 2: # IPv6 with possible mapped IPv4
                ipaddr = ipaddress.ip_address(addr[0])
                if ipaddr.ipv4_mapped:
                    # mapped IPv4 address identified
                    addr = (ipaddr.ipv4_mapped.compressed, addr[1])
            self.responses.put_nowait(((addr[:2], perf_counter()), SPVPong.decode(data)))
        except (ValueError, struct.error, AttributeError, TypeError, RuntimeError):
            return

    def connection_made(self, transport) -> None:
        self.transport = transport

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.transport = None
        log.info("closed udp spv server selection client")

    def ping(self, server: Union[Tuple[str, int], Tuple[str, int, int, int]]):
        self.transport.sendto(self._ping_packet, server)

    def close(self):
        # log.info("close udp client")
        if self.transport:
            self.transport.close()
