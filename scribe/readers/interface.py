import logging
import asyncio
import typing
from concurrent.futures.thread import ThreadPoolExecutor
from prometheus_client import Gauge, Histogram
from scribe import PROMETHEUS_NAMESPACE
from scribe.db.prefixes import DBState
from scribe.db import HubDB

HISTOGRAM_BUCKETS = (
    .005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0, 15.0, 20.0, 30.0, 60.0, float('inf')
)

NAMESPACE = f"{PROMETHEUS_NAMESPACE}_reader"


class BaseBlockchainReader:
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
        self.env = env
        self.log = logging.getLogger(__name__).getChild(self.__class__.__name__)
        self.shutdown_event = asyncio.Event()
        self.cancellable_tasks = []
        self._thread_workers = thread_workers
        self._thread_prefix = thread_prefix
        self._executor = ThreadPoolExecutor(thread_workers, thread_name_prefix=thread_prefix)
        self.db = HubDB(
            env.coin, env.db_dir, env.cache_MB, env.reorg_limit, env.cache_all_claim_txos, env.cache_all_tx_hashes,
            secondary_name=secondary_name, max_open_files=-1, blocking_channel_ids=env.blocking_channel_ids,
            filtering_channel_ids=env.filtering_channel_ids, executor=self._executor
        )
        self.last_state: typing.Optional[DBState] = None
        self._refresh_interval = 0.1
        self._lock = asyncio.Lock()

    def _detect_changes(self):
        try:
            self.db.prefix_db.try_catch_up_with_primary()
        except:
            self.log.exception('failed to update secondary db')
            raise
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

    async def poll_for_changes(self):
        await asyncio.get_event_loop().run_in_executor(self._executor, self._detect_changes)

    async def refresh_blocks_forever(self, synchronized: asyncio.Event):
        while True:
            try:
                async with self._lock:
                    await self.poll_for_changes()
            except asyncio.CancelledError:
                raise
            except:
                self.log.exception("blockchain reader main loop encountered an unexpected error")
                raise
            await asyncio.sleep(self._refresh_interval)
            synchronized.set()

    def clear_caches(self):
        pass

    def advance(self, height: int):
        tx_count = self.db.prefix_db.tx_count.get(height).tx_count
        assert tx_count not in self.db.tx_counts, f'boom {tx_count} in {len(self.db.tx_counts)} tx counts'
        assert len(self.db.tx_counts) == height, f"{len(self.db.tx_counts)} != {height}"
        self.db.tx_counts.append(tx_count)
        self.db.headers.append(self.db.prefix_db.header.get(height, deserialize_value=False))

    def unwind(self):
        self.db.tx_counts.pop()
        self.db.headers.pop()

    async def start(self):
        if not self._executor:
            self._executor = ThreadPoolExecutor(self._thread_workers, thread_name_prefix=self._thread_prefix)
            self.db._executor = self._executor
