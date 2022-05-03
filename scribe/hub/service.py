import time
import typing
import asyncio
from bisect import bisect_right
from scribe.blockchain.daemon import LBCDaemon
from scribe.hub.session import SessionManager
from scribe.hub.mempool import HubMemPool
from scribe.hub.udp import StatusServer
from scribe.service import BlockchainReaderService
from scribe.elasticsearch import ElasticNotifierClientProtocol
from scribe.common import LargestValueCache
if typing.TYPE_CHECKING:
    from scribe.hub.env import ServerEnv


class HubServerService(BlockchainReaderService):
    def __init__(self, env: 'ServerEnv'):
        super().__init__(env, 'lbry-reader', thread_workers=max(1, env.max_query_workers), thread_prefix='hub-worker')
        self.env = env
        self.notifications_to_send = []
        self.mempool_notifications = set()
        self.status_server = StatusServer()
        self.daemon = LBCDaemon(env.coin, env.daemon_url)  # only needed for broadcasting txs
        self.mempool = HubMemPool(self.env.coin, self.db)
        self.large_history_cache = LargestValueCache(10000)  # stores histories as lists of tx nums
        self.large_full_history_cache = LargestValueCache(10000)  # stores serialized histories
        self.session_manager = SessionManager(
            env, self.db, self.mempool, self.daemon, self.large_full_history_cache,
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

    def _reorg_detected(self):
        self.large_history_cache.clear()  # TODO: these could be pruned instead of cleared
        self.large_full_history_cache.clear()

    def advance(self, height: int):
        super().advance(height)
        touched_hashXs = self.db.prefix_db.touched_hashX.get(height).touched_hashXs

        # update cached address histories
        for hashX in touched_hashXs:
            history = self.large_history_cache.get(hashX) or []
            extend_history = history.extend
            full_history = self.large_full_history_cache.get(hashX) or []
            start_height = 0
            if full_history:
                start_height = full_history[-1][1] + 1
            for hist in self.db.prefix_db.hashX_history.iterate(start=(hashX, start_height),
                                                                stop=(hashX, height + 1), include_key=False):
                extend_history(hist)
            if not history:
                continue
            if self.large_history_cache.set(hashX, list(history)):  # only the largest will be cached:
                precached_offset = len(full_history or [])
                needed_history = history[precached_offset:]
                if needed_history:
                    append_full_history = full_history.append
                    for tx_num, tx_hash in zip(needed_history, self.db.get_tx_hashes(needed_history)):
                        append_full_history((tx_hash, bisect_right(self.db.tx_counts, tx_num)))
                    self.large_full_history_cache.set(hashX, full_history)

        self.notifications_to_send.append((set(touched_hashXs), height))

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

    def populate_largest_histories_cache(self):
        self.log.info("populating address history cache")
        for hashX, history in self.db.prefix_db.hashX_history.iterate():
            self.large_history_cache[hashX] = history
        self.log.info("sorted %i largest address histories", len(self.large_history_cache))
        for hashX, history in self.large_history_cache.items():
            full_history = []
            append_full_history = full_history.append
            for tx_num, tx_hash in zip(history, self.db.get_tx_hashes(history)):
                append_full_history((tx_hash, bisect_right(self.db.tx_counts, tx_num)))
            self.large_full_history_cache.set(hashX, full_history)
        self.log.info("cached %i largest address histories ranging from %i to %i txs in length",
                      len(self.large_history_cache), self.large_history_cache._sizes[-1],
                      self.large_history_cache._sizes[0])

    def _iter_start_tasks(self):
        # self.populate_largest_histories_cache()
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
