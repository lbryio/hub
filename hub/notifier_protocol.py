import typing
import struct
import asyncio
import logging
from typing import Deque, Tuple


log = logging.getLogger(__name__)


class ElasticNotifierProtocol(asyncio.Protocol):
    """notifies the reader when ES has written updates"""

    def __init__(self, listeners):
        self._listeners = listeners
        self.transport: typing.Optional[asyncio.Transport] = None

    def connection_made(self, transport):
        self.transport = transport
        self._listeners.append(self)
        log.info("got es notifier connection")

    def connection_lost(self, exc) -> None:
        self._listeners.remove(self)
        self.transport = None

    def send_height(self, height: int, block_hash: bytes):
        log.info("notify es update '%s'", height)
        self.transport.write(struct.pack(b'>Q32s', height, block_hash))


class ElasticNotifierClientProtocol(asyncio.Protocol):
    """notifies the reader when ES has written updates"""

    def __init__(self, notifications: asyncio.Queue, notifier_hosts: Deque[Tuple[Tuple[str, int], Tuple[str, int]]]):
        assert len(notifier_hosts) > 0, 'no elastic notifier clients given'
        self.notifications = notifications
        self.transport: typing.Optional[asyncio.Transport] = None
        self._notifier_hosts = notifier_hosts
        self.lost_connection = asyncio.Event()
        self.lost_connection.set()

    @property
    def host(self):
        return self._notifier_hosts[0][1][0]

    @property
    def port(self):
        return self._notifier_hosts[0][1][1]

    async def connect(self):
        if self.lost_connection.is_set():
            await asyncio.get_event_loop().create_connection(
                lambda: self, self.host, self.port
            )

    def close(self):
        if self.transport and not self.transport.is_closing():
            self.transport.close()

    def connection_made(self, transport):
        self.transport = transport
        self.lost_connection.clear()

    def connection_lost(self, exc) -> None:
        self.transport = None
        self.lost_connection.set()

    def data_received(self, data: bytes) -> None:
        try:
            height, block_hash = struct.unpack(b'>Q32s', data)
        except:
            log.exception("failed to decode %s", (data or b'').hex())
            raise
        self.notifications.put_nowait((height, block_hash))
