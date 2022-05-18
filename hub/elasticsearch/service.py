import os
import json
import typing
import asyncio
from collections import defaultdict
from elasticsearch import AsyncElasticsearch, NotFoundError
from elasticsearch.helpers import async_streaming_bulk
from hub.schema.result import Censor
from hub.service import BlockchainReaderService
from hub.db.revertable import RevertableOp
from hub.db.common import TrendingNotification, DB_PREFIXES
from hub.elasticsearch.notifier_protocol import ElasticNotifierProtocol
from hub.elasticsearch.search import IndexVersionMismatch, expand_query
from hub.elasticsearch.constants import ALL_FIELDS, INDEX_DEFAULT_SETTINGS
from hub.elasticsearch.fast_ar_trending import FAST_AR_TRENDING_SCRIPT
if typing.TYPE_CHECKING:
    from hub.elasticsearch.env import ElasticEnv


class ElasticSyncService(BlockchainReaderService):
    VERSION = 1

    def __init__(self, env: 'ElasticEnv'):
        super().__init__(env, 'lbry-elastic-writer', thread_workers=1, thread_prefix='lbry-elastic-writer')
        self.env = env
        # self._refresh_interval = 0.1
        self._task = None
        self.index = self.env.es_index_prefix + 'claims'
        self._elastic_host = env.elastic_host
        self._elastic_port = env.elastic_port
        self.sync_timeout = 1800
        self.sync_client = None
        self._es_info_path = os.path.join(env.db_dir, 'es_info')
        self._last_wrote_height = 0
        self._last_wrote_block_hash = None

        self._touched_claims = set()
        self._deleted_claims = set()

        self._removed_during_undo = set()

        self._trending = defaultdict(list)
        self._advanced = True
        self.synchronized = asyncio.Event()
        self._listeners: typing.List[ElasticNotifierProtocol] = []
        self._force_reindex = False

    async def run_es_notifier(self, synchronized: asyncio.Event):
        server = await asyncio.get_event_loop().create_server(
            lambda: ElasticNotifierProtocol(self._listeners), self.env.elastic_notifier_host, self.env.elastic_notifier_port
        )
        self.log.info("ES notifier server listening on TCP %s:%i", self.env.elastic_notifier_host,
                      self.env.elastic_notifier_port)
        synchronized.set()
        async with server:
            await server.serve_forever()

    def notify_es_notification_listeners(self, height: int, block_hash: bytes):
        for p in self._listeners:
            p.send_height(height, block_hash)
            self.log.info("notify listener %i", height)

    def _read_es_height(self):
        info = {}
        if os.path.exists(self._es_info_path):
            with open(self._es_info_path, 'r') as f:
                info.update(json.loads(f.read()))
        self._last_wrote_height = int(info.get('height', 0))
        self._last_wrote_block_hash = info.get('block_hash', None)

    async def read_es_height(self):
        await asyncio.get_event_loop().run_in_executor(self._executor, self._read_es_height)

    def write_es_height(self, height: int, block_hash: str):
        with open(self._es_info_path, 'w') as f:
            f.write(json.dumps({'height': height, 'block_hash': block_hash}, indent=2))
        self._last_wrote_height = height
        self._last_wrote_block_hash = block_hash

    async def get_index_version(self) -> int:
        try:
            template = await self.sync_client.indices.get_template(self.index)
            return template[self.index]['version']
        except NotFoundError:
            return 0

    async def set_index_version(self, version):
        await self.sync_client.indices.put_template(
            self.index, body={'version': version, 'index_patterns': ['ignored']}, ignore=400
        )

    async def start_index(self) -> bool:
        if self.sync_client:
            return False
        hosts = [{'host': self._elastic_host, 'port': self._elastic_port}]
        self.sync_client = AsyncElasticsearch(hosts, timeout=self.sync_timeout)
        while True:
            try:
                await self.sync_client.cluster.health(wait_for_status='yellow')
                self.log.info("ES is ready to connect to")
                break
            except ConnectionError:
                self.log.warning("Failed to connect to Elasticsearch. Waiting for it!")
                await asyncio.sleep(1)

        index_version = await self.get_index_version()

        res = await self.sync_client.indices.create(self.index, INDEX_DEFAULT_SETTINGS, ignore=400)
        acked = res.get('acknowledged', False)

        if acked:
            await self.set_index_version(self.VERSION)
            return True
        elif index_version != self.VERSION:
            self.log.error("es search index has an incompatible version: %s vs %s", index_version, self.VERSION)
            raise IndexVersionMismatch(index_version, self.VERSION)
        else:
            await self.sync_client.indices.refresh(self.index)
            return False

    async def stop_index(self, delete=False):
        if self.sync_client:
            if delete:
                await self.delete_index()
            await self.sync_client.close()
        self.sync_client = None

    async def delete_index(self):
        if self.sync_client:
            return await self.sync_client.indices.delete(self.index, ignore_unavailable=True)

    def update_filter_query(self, censor_type, blockdict, channels=False):
        blockdict = {blocked.hex(): blocker.hex() for blocked, blocker in blockdict.items()}
        if channels:
            update = expand_query(channel_id__in=list(blockdict.keys()), censor_type=f"<{censor_type}")
        else:
            update = expand_query(claim_id__in=list(blockdict.keys()), censor_type=f"<{censor_type}")
        key = 'channel_id' if channels else 'claim_id'
        update['script'] = {
            "source": f"ctx._source.censor_type={censor_type}; "
                      f"ctx._source.censoring_channel_id=params[ctx._source.{key}];",
            "lang": "painless",
            "params": blockdict
        }
        return update

    async def apply_filters(self, blocked_streams, blocked_channels, filtered_streams, filtered_channels):
        if filtered_streams:
            await self.sync_client.update_by_query(
                self.index, body=self.update_filter_query(Censor.SEARCH, filtered_streams), slices=4)
            await self.sync_client.indices.refresh(self.index)
        if filtered_channels:
            await self.sync_client.update_by_query(
                self.index, body=self.update_filter_query(Censor.SEARCH, filtered_channels), slices=4)
            await self.sync_client.indices.refresh(self.index)
            await self.sync_client.update_by_query(
                self.index, body=self.update_filter_query(Censor.SEARCH, filtered_channels, True), slices=4)
            await self.sync_client.indices.refresh(self.index)
        if blocked_streams:
            await self.sync_client.update_by_query(
                self.index, body=self.update_filter_query(Censor.RESOLVE, blocked_streams), slices=4)
            await self.sync_client.indices.refresh(self.index)
        if blocked_channels:
            await self.sync_client.update_by_query(
                self.index, body=self.update_filter_query(Censor.RESOLVE, blocked_channels), slices=4)
            await self.sync_client.indices.refresh(self.index)
            await self.sync_client.update_by_query(
                self.index, body=self.update_filter_query(Censor.RESOLVE, blocked_channels, True), slices=4)
            await self.sync_client.indices.refresh(self.index)

    @staticmethod
    def _upsert_claim_query(index, claim):
        return {
            'doc': {key: value for key, value in claim.items() if key in ALL_FIELDS},
            '_id': claim['claim_id'],
            '_index': index,
            '_op_type': 'update',
            'doc_as_upsert': True
        }

    @staticmethod
    def _delete_claim_query(index, claim_hash: bytes):
        return {
            '_index': index,
            '_op_type': 'delete',
            '_id': claim_hash.hex()
        }

    @staticmethod
    def _update_trending_query(index, claim_hash, notifications):
        return {
            '_id': claim_hash.hex(),
            '_index': index,
            '_op_type': 'update',
            'script': {
                'lang': 'painless',
                'source': FAST_AR_TRENDING_SCRIPT,
                'params': {'src': {
                    'changes': [
                        {
                            'height': notification.height,
                            'prev_amount': notification.prev_amount / 1E8,
                            'new_amount': notification.new_amount / 1E8,
                        } for notification in notifications
                    ]
                }}
            },
        }

    async def _claim_producer(self):
        for deleted in self._deleted_claims:
            yield self._delete_claim_query(self.index, deleted)
        for touched in self._touched_claims:
            claim = self.db.claim_producer(touched)
            if claim:
                yield self._upsert_claim_query(self.index, claim)
        for claim_hash, notifications in self._trending.items():
            yield self._update_trending_query(self.index, claim_hash, notifications)

    def advance(self, height: int):
        super().advance(height)
        touched_or_deleted = self.db.prefix_db.touched_or_deleted.get(height)
        for k, v in self.db.prefix_db.trending_notification.iterate((height,)):
            self._trending[k.claim_hash].append(TrendingNotification(k.height, v.previous_amount, v.new_amount))
        if touched_or_deleted:
            readded_after_reorg = self._removed_during_undo.intersection(touched_or_deleted.touched_claims)
            self._deleted_claims.difference_update(readded_after_reorg)
            self._touched_claims.update(touched_or_deleted.touched_claims)
            self._deleted_claims.update(touched_or_deleted.deleted_claims)
            self._touched_claims.difference_update(self._deleted_claims)
            for to_del in touched_or_deleted.deleted_claims:
                if to_del in self._trending:
                    self._trending.pop(to_del)
        self._advanced = True

    def unwind(self):
        reverted_block_hash = self.db.block_hashes[-1]
        super().unwind()
        packed = self.db.prefix_db.undo.get(len(self.db.tx_counts), reverted_block_hash)
        touched_or_deleted = None
        claims_to_delete = []
        # find and apply the touched_or_deleted items in the undos for the reverted blocks
        assert packed, f'missing undo information for block {len(self.db.tx_counts)}'
        while packed:
            op, packed = RevertableOp.unpack(packed)
            if op.is_delete and op.key.startswith(DB_PREFIXES.touched_or_deleted.value):
                assert touched_or_deleted is None, 'only should have one match'
                touched_or_deleted = self.db.prefix_db.touched_or_deleted.unpack_value(op.value)
            elif op.is_delete and op.key.startswith(DB_PREFIXES.claim_to_txo.value):
                v = self.db.prefix_db.claim_to_txo.unpack_value(op.value)
                if v.root_tx_num == v.tx_num and v.root_tx_num > self.db.tx_counts[-1]:
                    claims_to_delete.append(self.db.prefix_db.claim_to_txo.unpack_key(op.key).claim_hash)
        if touched_or_deleted:
            self._touched_claims.update(set(touched_or_deleted.deleted_claims).union(
                touched_or_deleted.touched_claims.difference(set(claims_to_delete))))
            self._deleted_claims.update(claims_to_delete)
            self._removed_during_undo.update(claims_to_delete)
        self._advanced = True
        self.log.warning("delete %i claim and upsert %i from reorg", len(self._deleted_claims), len(self._touched_claims))

    async def poll_for_changes(self):
        await super().poll_for_changes()
        cnt = 0
        success = 0
        if self._advanced:
            if self._touched_claims or self._deleted_claims or self._trending:
                async for ok, item in async_streaming_bulk(
                        self.sync_client, self._claim_producer(),
                        raise_on_error=False):
                    cnt += 1
                    if not ok:
                        self.log.warning("indexing failed for an item: %s", item)
                    else:
                        success += 1
                await self.sync_client.indices.refresh(self.index)
                await self.apply_filters(
                    self.db.blocked_streams, self.db.blocked_channels, self.db.filtered_streams,
                    self.db.filtered_channels
                )
            self.write_es_height(self.db.db_height, self.db.db_tip[::-1].hex())
            self.log.info("Indexing block %i done. %i/%i successful", self._last_wrote_height, success, cnt)
            self._touched_claims.clear()
            self._deleted_claims.clear()
            self._removed_during_undo.clear()
            self._trending.clear()
            self._advanced = False
            self.synchronized.set()
            self.notify_es_notification_listeners(self._last_wrote_height, self.db.db_tip)

    @property
    def last_synced_height(self) -> int:
        return self._last_wrote_height

    async def catch_up(self):
        last_state = self.db.prefix_db.db_state.get()
        db_height = last_state.height
        if last_state and self._last_wrote_height and db_height > self._last_wrote_height:
            self.log.warning(
                "syncing ES from block %i to rocksdb height of %i (%i blocks to sync)",
                self._last_wrote_height, last_state.height, last_state.height - self._last_wrote_height
            )
            for _ in range(self._last_wrote_height + 1, last_state.height + 1):
                super().unwind()
            for height in range(self._last_wrote_height + 1, last_state.height + 1):
                self.advance(height)
        else:
            return
        success = 0
        cnt = 0
        if self._touched_claims or self._deleted_claims or self._trending:
            async for ok, item in async_streaming_bulk(
                    self.sync_client, self._claim_producer(),
                    raise_on_error=False):
                cnt += 1
                if not ok:
                    self.log.warning("indexing failed for an item: %s", item)
                else:
                    success += 1
            await self.sync_client.indices.refresh(self.index)
            await self.apply_filters(
                self.db.blocked_streams, self.db.blocked_channels, self.db.filtered_streams,
                self.db.filtered_channels
            )
        self.write_es_height(db_height, last_state.tip[::-1].hex())
        self._touched_claims.clear()
        self._deleted_claims.clear()
        self._removed_during_undo.clear()
        self._trending.clear()
        self._advanced = False
        self.notify_es_notification_listeners(self._last_wrote_height, last_state.tip)
        self.log.info("Indexing block %i done. %i/%i successful", self._last_wrote_height, success, cnt)

    async def reindex(self, force=False):
        if force or self._last_wrote_height == 0 and self.db.db_height > 0:
            if self._last_wrote_height == 0:
                self.log.info("running initial ES indexing of rocksdb at block height %i", self.db.db_height)
            else:
                self.log.info("reindex (last wrote: %i, db height: %i)", self._last_wrote_height, self.db.db_height)
            await self._reindex()

    async def block_bulk_sync_on_writer_catchup(self):
        def _check_if_catching_up():
            self.db.prefix_db.try_catch_up_with_primary()
            state = self.db.prefix_db.db_state.get()
            return state.catching_up

        loop = asyncio.get_event_loop()

        catching_up = True
        while catching_up:
            catching_up = await loop.run_in_executor(self._executor, _check_if_catching_up)
            if catching_up:
                await asyncio.sleep(1)
            else:
                return

    def _iter_start_tasks(self):
        yield self.block_bulk_sync_on_writer_catchup()
        yield self.read_es_height()
        yield self.start_index()
        yield self.start_cancellable(self.run_es_notifier)
        yield self.reindex(force=self._force_reindex)
        yield self.catch_up()
        self.block_count_metric.set(self.last_state.height)
        yield self.start_prometheus()
        yield self.start_cancellable(self.refresh_blocks_forever)

    def _iter_stop_tasks(self):
        yield self._stop_cancellable_tasks()
        yield self.stop_index()

    def run(self, reindex=False):
        self._force_reindex = reindex
        return super().run()

    async def start(self, reindex=False):
        self._force_reindex = reindex
        try:
            return await super().start()
        finally:
            self._force_reindex = False

    async def _reindex(self):
        async with self.lock:
            self.log.info("reindexing %i claims (estimate)", self.db.prefix_db.claim_to_txo.estimate_num_keys())
            await self.delete_index()
            res = await self.sync_client.indices.create(self.index, INDEX_DEFAULT_SETTINGS, ignore=400)
            acked = res.get('acknowledged', False)
            if acked:
                await self.set_index_version(self.VERSION)
            await self.sync_client.indices.refresh(self.index)
            self.write_es_height(0, self.env.coin.GENESIS_HASH)
            await self._sync_all_claims()
            await self.sync_client.indices.refresh(self.index)
            self.write_es_height(self.db.db_height, self.db.db_tip[::-1].hex())
            self.notify_es_notification_listeners(self.db.db_height, self.db.db_tip)
            self.log.info("finished reindexing")

    async def _sync_all_claims(self, batch_size=100000):
        def load_historic_trending():
            notifications = self._trending
            for k, v in self.db.prefix_db.trending_notification.iterate():
                notifications[k.claim_hash].append(TrendingNotification(k.height, v.previous_amount, v.new_amount))

        async def all_claims_producer():
            async for claim in self.db.all_claims_producer(batch_size=batch_size):
                yield self._upsert_claim_query(self.index, claim)
                claim_hash = bytes.fromhex(claim['claim_id'])
                if claim_hash in self._trending:
                    yield self._update_trending_query(self.index, claim_hash, self._trending.pop(claim_hash))
            self._trending.clear()

        self.log.info("loading about %i historic trending updates", self.db.prefix_db.trending_notification.estimate_num_keys())
        await asyncio.get_event_loop().run_in_executor(self._executor, load_historic_trending)
        self.log.info("loaded historic trending updates for %i claims", len(self._trending))

        cnt = 0
        success = 0
        producer = all_claims_producer()

        finished = False
        try:
            async for ok, item in async_streaming_bulk(self.sync_client, producer, raise_on_error=False):
                cnt += 1
                if not ok:
                    self.log.warning("indexing failed for an item: %s", item)
                else:
                    success += 1
                if cnt % batch_size == 0:
                    self.log.info(f"indexed {success} claims")
            finished = True
            await self.sync_client.indices.refresh(self.index)
            self.log.info("indexed %i/%i claims", success, cnt)
        finally:
            if not finished:
                await producer.aclose()
            self.shutdown_event.set()
