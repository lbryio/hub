import signal
import asyncio
import typing
from scribe import __version__
from scribe.blockchain.daemon import LBCDaemon
from scribe.readers import BaseBlockchainReader
from scribe.elasticsearch import ElasticNotifierClientProtocol
from scribe.hub.session import SessionManager
from scribe.hub.mempool import MemPool
from scribe.hub.udp import StatusServer
from scribe.hub.prometheus import PrometheusServer


class BlockchainReaderServer(BaseBlockchainReader):
    def __init__(self, env):
        super().__init__(env, 'lbry-reader', thread_workers=max(1, env.max_query_workers), thread_prefix='hub-worker')
        self.history_cache = {}
        self.resolve_outputs_cache = {}
        self.resolve_cache = {}
        self.notifications_to_send = []
        self.mempool_notifications = set()
        self.status_server = StatusServer()
        self.daemon = LBCDaemon(env.coin, env.daemon_url)  # only needed for broadcasting txs
        self.prometheus_server: typing.Optional[PrometheusServer] = None
        self.mempool = MemPool(self.env.coin, self.db)
        self.session_manager = SessionManager(
            env, self.db, self.mempool, self.history_cache, self.resolve_cache,
            self.resolve_outputs_cache, self.daemon,
            self.shutdown_event,
            on_available_callback=self.status_server.set_available,
            on_unavailable_callback=self.status_server.set_unavailable
        )
        self.mempool.session_manager = self.session_manager
        self.es_notifications = asyncio.Queue()
        self.es_notification_client = ElasticNotifierClientProtocol(self.es_notifications)
        self.synchronized = asyncio.Event()
        self._es_height = None
        self._es_block_hash = None

    def clear_caches(self):
        self.history_cache.clear()
        self.resolve_outputs_cache.clear()
        self.resolve_cache.clear()
        # self.clear_search_cache()
        # self.mempool.notified_mempool_txs.clear()

    def clear_search_cache(self):
        self.session_manager.search_index.clear_caches()

    def advance(self, height: int):
        super().advance(height)
        touched_hashXs = self.db.prefix_db.touched_hashX.get(height).touched_hashXs
        self.notifications_to_send.append((set(touched_hashXs), height))

    def _detect_changes(self):
        super()._detect_changes()
        self.mempool_notifications.update(self.mempool.refresh())

    async def poll_for_changes(self):
        await super().poll_for_changes()
        if self.db.fs_height <= 0:
            return
        self.status_server.set_height(self.db.fs_height, self.db.db_tip)
        if self.notifications_to_send:
            for (touched, height) in self.notifications_to_send:
                await self.mempool.on_block(touched, height)
                self.log.info("reader advanced to %i", height)
                if self._es_height == self.db.db_height:
                    self.synchronized.set()
        if self.mempool_notifications:
            await self.mempool.on_mempool(
                set(self.mempool.touched_hashXs), self.mempool_notifications, self.db.db_height
            )
        self.mempool_notifications.clear()
        self.notifications_to_send.clear()

    async def receive_es_notifications(self, synchronized: asyncio.Event):
        await asyncio.get_event_loop().create_connection(
            lambda: self.es_notification_client, '127.0.0.1', self.env.elastic_notifier_port
        )
        synchronized.set()
        try:
            while True:
                self._es_height, self._es_block_hash = await self.es_notifications.get()
                self.clear_search_cache()
                if self.last_state and self._es_block_hash == self.last_state.tip:
                    self.synchronized.set()
                    self.log.info("es and reader are in sync at block %i", self.last_state.height)
                else:
                    self.log.info("es and reader are not yet in sync (block %s vs %s)", self._es_height,
                                  self.db.db_height)
        finally:
            self.es_notification_client.close()

    async def start(self):
        await super().start()
        env = self.env
        # min_str, max_str = env.coin.SESSIONCLS.protocol_min_max_strings()
        self.log.info(f'software version: {__version__}')
        # self.log.info(f'supported protocol versions: {min_str}-{max_str}')
        self.log.info(f'event loop policy: {env.loop_policy}')
        self.log.info(f'reorg limit is {env.reorg_limit:,d} blocks')
        await self.daemon.height()

        def _start_cancellable(run, *args):
            _flag = asyncio.Event()
            self.cancellable_tasks.append(asyncio.ensure_future(run(*args, _flag)))
            return _flag.wait()

        self.db.open_db()
        await self.db.initialize_caches()

        self.last_state = self.db.read_db_state()

        await self.start_prometheus()
        if self.env.udp_port and int(self.env.udp_port):
            await self.status_server.start(
                0, bytes.fromhex(self.env.coin.GENESIS_HASH)[::-1], self.env.country,
                self.env.host, self.env.udp_port, self.env.allow_lan_udp
            )
        await _start_cancellable(self.receive_es_notifications)
        await _start_cancellable(self.refresh_blocks_forever)
        await self.session_manager.search_index.start()
        await _start_cancellable(self.session_manager.serve, self.mempool)

    async def stop(self):
        await self.status_server.stop()
        async with self._lock:
            while self.cancellable_tasks:
                t = self.cancellable_tasks.pop()
                if not t.done():
                    t.cancel()
        await self.session_manager.search_index.stop()
        self.db.close()
        if self.prometheus_server:
            await self.prometheus_server.stop()
            self.prometheus_server = None
        await self.daemon.close()
        self._executor.shutdown(wait=True)
        self._executor = None
        self.shutdown_event.set()

    def run(self):
        loop = asyncio.get_event_loop()
        loop.set_default_executor(self._executor)

        def __exit():
            raise SystemExit()
        try:
            loop.add_signal_handler(signal.SIGINT, __exit)
            loop.add_signal_handler(signal.SIGTERM, __exit)
            loop.run_until_complete(self.start())
            loop.run_until_complete(self.shutdown_event.wait())
        except (SystemExit, KeyboardInterrupt):
            pass
        finally:
            loop.run_until_complete(self.stop())

    async def start_prometheus(self):
        if not self.prometheus_server and self.env.prometheus_port:
            self.prometheus_server = PrometheusServer()
            await self.prometheus_server.start("0.0.0.0", self.env.prometheus_port)
