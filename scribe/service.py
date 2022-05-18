import logging
import asyncio
import typing
import signal
from concurrent.futures.thread import ThreadPoolExecutor
from prometheus_client import Gauge, Histogram

from scribe import __version__, PROMETHEUS_NAMESPACE
from scribe.env import Env
from scribe.db import HubDB
from scribe.db.prefixes import DBState
from scribe.common import HISTOGRAM_BUCKETS
from scribe.metrics import PrometheusServer


class BlockchainService:
    """
    Base class for blockchain readers as well as the block processor
    """
    def __init__(self, env: Env, secondary_name: str, thread_workers: int = 1, thread_prefix: str = 'scribe'):
        self.env = env
        self.log = logging.getLogger(__name__).getChild(self.__class__.__name__)
        self.shutdown_event = asyncio.Event()
        self.cancellable_tasks = []
        self._thread_workers = thread_workers
        self._thread_prefix = thread_prefix
        self._executor = ThreadPoolExecutor(thread_workers, thread_name_prefix=thread_prefix)
        self.lock = asyncio.Lock()
        self.last_state: typing.Optional[DBState] = None
        self.db = HubDB(
            env.coin, env.db_dir, env.reorg_limit, env.cache_all_claim_txos, env.cache_all_tx_hashes,
            secondary_name=secondary_name, max_open_files=-1, blocking_channel_ids=env.blocking_channel_ids,
            filtering_channel_ids=env.filtering_channel_ids, executor=self._executor,
            index_address_status=env.index_address_status
        )
        self._stopping = False

    def start_cancellable(self, run, *args):
        _flag = asyncio.Event()
        self.cancellable_tasks.append(asyncio.ensure_future(run(*args, _flag)))
        return _flag.wait()

    def _iter_start_tasks(self):
        raise NotImplementedError()

    def _iter_stop_tasks(self):
        yield self._stop_cancellable_tasks()

    async def _stop_cancellable_tasks(self):
        async with self.lock:
            while self.cancellable_tasks:
                t = self.cancellable_tasks.pop()
                if not t.done():
                    t.cancel()

    async def start(self):
        if not self._executor:
            self._executor = ThreadPoolExecutor(self._thread_workers, thread_name_prefix=self._thread_prefix)
            self.db._executor = self._executor

        env = self.env
        # min_str, max_str = env.coin.SESSIONCLS.protocol_min_max_strings()
        self.log.info(f'software version: {__version__}')
        # self.log.info(f'supported protocol versions: {min_str}-{max_str}')
        self.log.info(f'reorg limit is {env.reorg_limit:,d} blocks')

        self.db.open_db()
        self.log.info(f'initializing caches')
        await self.db.initialize_caches()
        self.last_state = self.db.read_db_state()
        self.log.info(f'opened db at block {self.db.db_height}')

        for start_task in self._iter_start_tasks():
            await start_task

    async def stop(self):
        self.log.info("stopping")
        self._stopping = True
        for stop_task in self._iter_stop_tasks():
            await stop_task
        self.db.close()
        self._executor.shutdown(wait=True)
        self._executor = None
        self.shutdown_event.set()

    async def _run(self):
        try:
            await self.start()
            await self.shutdown_event.wait()
        except (SystemExit, KeyboardInterrupt, asyncio.CancelledError):
            self._stopping = True
        except Exception as err:
            self.log.exception("unexpected fatal error: %s", err)
            self._stopping = True

    def run(self):
        def __exit():
            raise SystemExit()

        loop = asyncio.get_event_loop()
        loop.set_default_executor(self._executor)
        loop.add_signal_handler(signal.SIGINT, __exit)
        loop.add_signal_handler(signal.SIGTERM, __exit)
        try:
            loop.run_until_complete(self._run())
        finally:
            loop.run_until_complete(self.stop())


NAMESPACE = f"{PROMETHEUS_NAMESPACE}_reader"


class BlockchainReaderService(BlockchainService):
    block_count_metric = Gauge(
        "block_count", "Number of processed blocks", namespace=NAMESPACE
    )
    block_update_time_metric = Histogram(
        "block_time", "Block update times", namespace=NAMESPACE, buckets=HISTOGRAM_BUCKETS
    )
    reorg_count_metric = Gauge(
        "reorg_count", "Number of reorgs", namespace=NAMESPACE
    )

    def __init__(self, env, secondary_name: str, thread_workers: int = 1, thread_prefix: str = 'blockchain-reader'):
        super().__init__(env, secondary_name, thread_workers, thread_prefix)
        self._refresh_interval = 0.1
        self.prometheus_server: typing.Optional[PrometheusServer] = None
        self.finished_initial_catch_up = asyncio.Event()

    async def poll_for_changes(self):
        """
        Detect and handle if the db has advanced to a new block or unwound during a chain reorganization

        If a reorg is detected, this will first unwind() to the branching height and then advance() forward
        to the new block(s).
        """
        await asyncio.get_event_loop().run_in_executor(self._executor, self._detect_changes)

    def clear_caches(self):
        """
        Called after finished advancing, used for invalidating caches
        """
        pass

    def advance(self, height: int):
        """
        Called when advancing to the given block height
        Callbacks that look up new values from the added block can be put here
        """
        tx_count = self.db.prefix_db.tx_count.get(height).tx_count
        assert tx_count not in self.db.tx_counts, f'boom {tx_count} in {len(self.db.tx_counts)} tx counts'
        assert len(self.db.tx_counts) == height, f"{len(self.db.tx_counts)} != {height}"
        prev_count = self.db.tx_counts[-1]
        self.db.tx_counts.append(tx_count)

        # precache all of the txs from this block
        block_tx_hashes = self.db.prefix_db.block_txs.get(height).tx_hashes
        block_txs = self.db.prefix_db.tx.multi_get(
            [(tx_hash,) for tx_hash in block_tx_hashes], deserialize_value=False
        )
        for tx_pos, (tx_num, tx_hash, tx) in enumerate(zip(range(prev_count, tx_count), block_tx_hashes, block_txs)):
            self.db.tx_cache[tx_hash] = tx, tx_num, tx_pos, height
            if self.db._cache_all_tx_hashes:
                self.db.total_transactions.append(tx_hash)
                self.db.tx_num_mapping[tx_hash] = tx_count
        if self.db._cache_all_tx_hashes:
            assert len(self.db.total_transactions) == tx_count, f"{len(self.db.total_transactions)} vs {tx_count}"

        header = self.db.prefix_db.header.get(height, deserialize_value=False)
        self.db.headers.append(header)
        self.db.block_hashes.append(self.env.coin.header_hash(header))

    def unwind(self):
        """
        Go backwards one block
        """
        prev_count = self.db.tx_counts.pop()
        tx_count = self.db.tx_counts[-1]
        self.db.headers.pop()
        self.db.block_hashes.pop()
        if self.db._cache_all_tx_hashes:
            for _ in range(prev_count - tx_count):
                tx_hash = self.db.tx_num_mapping.pop(self.db.total_transactions.pop())
                if tx_hash in self.db.tx_cache:
                    self.db.tx_cache.pop(tx_hash)
            assert len(self.db.total_transactions) == tx_count, f"{len(self.db.total_transactions)} vs {tx_count}"
        self.db.merkle_cache.clear()

    def _detect_changes(self):
        try:
            self.db.prefix_db.try_catch_up_with_primary()
        except Exception as err:
            self.log.exception('failed to update secondary db: %s', err)
            raise err
        state = self.db.prefix_db.db_state.get()
        if not state or state.height <= 0:
            return
        if self.last_state and self.last_state.height > state.height:
            self.log.warning("reorg detected, waiting until the writer has flushed the new blocks to advance")
            return
        last_height = 0 if not self.last_state else self.last_state.height
        rewound = False
        if self.last_state:
            while True:
                if self.db.headers[-1] == self.db.prefix_db.header.get(last_height, deserialize_value=False):
                    self.log.debug("connects to block %i", last_height)
                    break
                else:
                    self.log.warning("disconnect block %i", last_height)
                    self.unwind()
                    rewound = True
                    last_height -= 1
        if rewound:
            self.reorg_count_metric.inc()
        self.db.read_db_state()
        if not self.last_state or last_height < state.height:
            for height in range(last_height + 1, state.height + 1):
                self.log.info("advancing to %i", height)
                self.advance(height)
            self.clear_caches()
            self.last_state = state
            self.block_count_metric.set(self.last_state.height)
            self.db.blocked_streams, self.db.blocked_channels = self.db.get_streams_and_channels_reposted_by_channel_hashes(
                self.db.blocking_channel_hashes
            )
            self.db.filtered_streams, self.db.filtered_channels = self.db.get_streams_and_channels_reposted_by_channel_hashes(
                self.db.filtering_channel_hashes
            )

    async def refresh_blocks_forever(self, synchronized: asyncio.Event):
        while True:
            try:
                async with self.lock:
                    await self.poll_for_changes()
                    if not self.db.catching_up and not self.finished_initial_catch_up.is_set():
                        self.finished_initial_catch_up.set()
            except asyncio.CancelledError:
                raise
            except:
                self.log.exception("blockchain reader main loop encountered an unexpected error")
                self.shutdown_event.set()
                raise
            await asyncio.sleep(self._refresh_interval)
            synchronized.set()

    def _iter_start_tasks(self):
        self.block_count_metric.set(self.last_state.height)
        yield self.start_prometheus()
        yield self.start_cancellable(self.refresh_blocks_forever)

    def _iter_stop_tasks(self):
        yield self.stop_prometheus()
        yield self._stop_cancellable_tasks()

    async def start_prometheus(self):
        if not self.prometheus_server and self.env.prometheus_port:
            self.prometheus_server = PrometheusServer()
            await self.prometheus_server.start("0.0.0.0", self.env.prometheus_port)

    async def stop_prometheus(self):
        if self.prometheus_server:
            await self.prometheus_server.stop()
            self.prometheus_server = None
