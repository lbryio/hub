import typing
import struct
import asyncio
import logging


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

    def __init__(self, notifications: asyncio.Queue, host: str, port: int):
        self.notifications = notifications
        self.transport: typing.Optional[asyncio.Transport] = None
        self.host = host
        self.port = port
        self._lost_connection = asyncio.Event()
        self._lost_connection.set()

    async def connect(self):
        if self._lost_connection.is_set():
            await asyncio.get_event_loop().create_connection(
                lambda: self, self.host, self.port
            )

    async def maintain_connection(self, synchronized: asyncio.Event):
        first_connect = True
        if not self._lost_connection.is_set():
            synchronized.set()
        while True:
            try:
                await self._lost_connection.wait()
                if not first_connect:
                    log.warning("lost connection to scribe-elastic-sync notifier")
                await self.connect()
                first_connect = False
                synchronized.set()
                log.info("connected to es notifier")
            except Exception as e:
                if not isinstance(e, asyncio.CancelledError):
                    log.warning("waiting 30s for scribe-elastic-sync notifier to become available (%s:%i)", self.host, self.port)
                    await asyncio.sleep(30)
                else:
                    log.info("stopping the notifier loop")
                    raise e

    def close(self):
        if self.transport and not self.transport.is_closing():
            self.transport.close()

    def connection_made(self, transport):
        self.transport = transport
        self._lost_connection.clear()

    def connection_lost(self, exc) -> None:
        self.transport = None
        self._lost_connection.set()

    def data_received(self, data: bytes) -> None:
        try:
            height, block_hash = struct.unpack(b'>Q32s', data)
        except:
            log.exception("failed to decode %s", (data or b'').hex())
            raise
        self.notifications.put_nowait((height, block_hash))
