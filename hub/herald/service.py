import time
import typing
import asyncio
from hub.scribe.daemon import LBCDaemon
from hub.herald.session import SessionManager
from hub.herald.mempool import HubMemPool
from hub.herald.udp import StatusServer
from hub.service import BlockchainReaderService
from hub.notifier_protocol import ElasticNotifierClientProtocol
if typing.TYPE_CHECKING:
    from hub.herald.env import ServerEnv


class HubServerService(BlockchainReaderService):
    def __init__(self, env: 'ServerEnv'):
        super().__init__(env, 'lbry-reader', thread_workers=max(1, env.max_query_workers), thread_prefix='hub-worker')
        self.env = env
        self.notifications_to_send = []
        self.mempool_notifications = set()
        self.status_server = StatusServer()
        self.daemon = LBCDaemon(env.coin, env.daemon_url)  # only needed for broadcasting txs
        self.mempool = HubMemPool(self.env.coin, self.db)
        self.session_manager = SessionManager(
            env, self.db, self.mempool, self.daemon,
            self.shutdown_event,
            on_available_callback=self.status_server.set_available,
            on_unavailable_callback=self.status_server.set_unavailable
        )
        self.mempool.session_manager = self.session_manager
        self.es_notifications = asyncio.Queue()
        self.es_notification_client = ElasticNotifierClientProtocol(
            self.es_notifications, self.env.elastic_notifier_host, self.env.elastic_notifier_port
        )
        self.synchronized = asyncio.Event()
        self._es_height = None
        self._es_block_hash = None

    def clear_caches(self):
        self.session_manager.clear_caches()
        # self.clear_search_cache()
        # self.mempool.notified_mempool_txs.clear()

    def clear_search_cache(self):
        self.session_manager.search_index.clear_caches()

    def advance(self, height: int):
        super().advance(height)
        touched_hashXs = self.db.prefix_db.touched_hashX.get(height).touched_hashXs
        self.notifications_to_send.append((set(touched_hashXs), height))

    def unwind(self):
        prev_count = self.db.tx_counts.pop()
        tx_count = self.db.tx_counts[-1]
        self.db.headers.pop()
        self.db.block_hashes.pop()
        current_count = prev_count
        for _ in range(prev_count - tx_count):
            if current_count in self.session_manager.history_tx_info_cache:
                self.session_manager.history_tx_info_cache.pop(current_count)
            current_count -= 1
        if self.db._cache_all_tx_hashes:
            for _ in range(prev_count - tx_count):
                tx_hash = self.db.tx_num_mapping.pop(self.db.total_transactions.pop())
                if tx_hash in self.db.tx_cache:
                    self.db.tx_cache.pop(tx_hash)
            assert len(self.db.total_transactions) == tx_count, f"{len(self.db.total_transactions)} vs {tx_count}"
        self.db.merkle_cache.clear()

    def _detect_changes(self):
        super()._detect_changes()
        start = time.perf_counter()
        self.mempool_notifications.update(self.mempool.refresh())
        self.mempool.mempool_process_time_metric.observe(time.perf_counter() - start)

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

    async def start_status_server(self):
        if self.env.udp_port and int(self.env.udp_port):
            await self.status_server.start(
                0, bytes.fromhex(self.env.coin.GENESIS_HASH)[::-1], self.env.country,
                self.env.host, self.env.udp_port, self.env.allow_lan_udp
            )

    def _iter_start_tasks(self):
        yield self.start_status_server()
        yield self.start_cancellable(self.es_notification_client.maintain_connection)
        yield self.start_cancellable(self.mempool.send_notifications_forever)
        yield self.start_cancellable(self.refresh_blocks_forever)
        yield self.finished_initial_catch_up.wait()
        self.block_count_metric.set(self.last_state.height)
        yield self.start_prometheus()
        yield self.start_cancellable(self.receive_es_notifications)
        yield self.session_manager.search_index.start()
        yield self.start_cancellable(self.session_manager.serve, self.mempool)

    def _iter_stop_tasks(self):
        yield self.stop_prometheus()
        yield self.status_server.stop()
        yield self._stop_cancellable_tasks()
        yield self.session_manager.search_index.stop()
        yield self.daemon.close()
