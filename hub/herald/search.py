import logging
import asyncio
from bisect import bisect_right
from collections import Counter, deque
from operator import itemgetter
from typing import Optional, List, TYPE_CHECKING, Deque, Tuple

from elasticsearch import AsyncElasticsearch, NotFoundError, ConnectionError
from hub.schema.result import Censor, Outputs
from hub.common import LRUCache, IndexVersionMismatch, INDEX_DEFAULT_SETTINGS, expand_query, expand_result
from hub.db.common import ResolveResult
if TYPE_CHECKING:
    from prometheus_client import Counter as PrometheusCounter
    from hub.db import SecondaryDB


class ChannelResolution(str):
    @classmethod
    def lookup_error(cls, url):
        return LookupError(f'Could not find channel in "{url}".')


class StreamResolution(str):
    @classmethod
    def lookup_error(cls, url):
        return LookupError(f'Could not find claim at "{url}".')


class SearchIndex:
    VERSION = 1

    def __init__(self, hub_db: 'SecondaryDB', index_prefix: str, search_timeout=3.0,
                 elastic_services: Optional[Deque[Tuple[Tuple[str, int], Tuple[str, int]]]] = None,
                 timeout_counter: Optional['PrometheusCounter'] = None):
        self.hub_db = hub_db
        self.search_timeout = search_timeout
        self.timeout_counter: Optional['PrometheusCounter'] = timeout_counter
        self.sync_timeout = 600  # wont hit that 99% of the time, but can hit on a fresh import
        self.search_client: Optional[AsyncElasticsearch] = None
        self.sync_client: Optional[AsyncElasticsearch] = None
        self.index = index_prefix + 'claims'
        self.logger = logging.getLogger(__name__)
        self.claim_cache = LRUCache(2 ** 15)
        self.search_cache = LRUCache(2 ** 17)
        self._elastic_services = elastic_services
        self.lost_connection = asyncio.Event()

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

    async def start(self) -> bool:
        if self.sync_client:
            return False
        hosts = [{'host': self._elastic_services[0][0][0], 'port': self._elastic_services[0][0][1]}]
        self.sync_client = AsyncElasticsearch(hosts, timeout=self.sync_timeout)
        self.search_client = AsyncElasticsearch(hosts, timeout=self.search_timeout+1)
        while True:
            try:
                await self.sync_client.cluster.health(wait_for_status='yellow')
                break
            except ConnectionError:
                self.logger.warning("Failed to connect to Elasticsearch. Waiting for it!")
                await asyncio.sleep(1)

        res = await self.sync_client.indices.create(self.index, INDEX_DEFAULT_SETTINGS, ignore=400)
        acked = res.get('acknowledged', False)
        if acked:
            await self.set_index_version(self.VERSION)
            return acked
        index_version = await self.get_index_version()
        if index_version != self.VERSION:
            self.logger.error("es search index has an incompatible version: %s vs %s", index_version, self.VERSION)
            raise IndexVersionMismatch(index_version, self.VERSION)
        await self.sync_client.indices.refresh(self.index)
        return True

    async def stop(self):
        clients = [c for c in (self.sync_client, self.search_client) if c is not None]
        self.sync_client, self.search_client = None, None
        if clients:
            await asyncio.gather(*(client.close() for client in clients))

    def clear_caches(self):
        self.search_cache.clear()
        self.claim_cache.clear()

    def _make_resolve_result(self, es_result):
        channel_hash = es_result['channel_hash']
        reposted_claim_hash = es_result['reposted_claim_hash']
        channel_tx_hash = None
        channel_tx_position = None
        channel_height = None
        reposted_tx_hash = None
        reposted_tx_position = None
        reposted_height = None
        if channel_hash:  # FIXME: do this inside ES in a script
            channel_txo = self.hub_db.get_cached_claim_txo(channel_hash[::-1])
            if channel_txo:
                channel_tx_hash = self.hub_db.get_tx_hash(channel_txo.tx_num)
                channel_tx_position = channel_txo.position
                channel_height = bisect_right(self.hub_db.tx_counts, channel_txo.tx_num)
        if reposted_claim_hash:
            repost_txo = self.hub_db.get_cached_claim_txo(reposted_claim_hash[::-1])
            if repost_txo:
                reposted_tx_hash = self.hub_db.get_tx_hash(repost_txo.tx_num)
                reposted_tx_position = repost_txo.position
                reposted_height = bisect_right(self.hub_db.tx_counts, repost_txo.tx_num)
        return ResolveResult(
            name=es_result['claim_name'],
            normalized_name=es_result['normalized_name'],
            claim_hash=es_result['claim_hash'],
            tx_num=es_result['tx_num'],
            position=es_result['tx_nout'],
            tx_hash=es_result['tx_hash'],
            height=es_result['height'],
            amount=es_result['amount'],
            short_url=es_result['short_url'],
            is_controlling=es_result['is_controlling'],
            canonical_url=es_result['canonical_url'],
            creation_height=es_result['creation_height'],
            activation_height=es_result['activation_height'],
            expiration_height=es_result['expiration_height'],
            effective_amount=es_result['effective_amount'],
            support_amount=es_result['support_amount'],
            last_takeover_height=es_result['last_take_over_height'],
            claims_in_channel=es_result['claims_in_channel'],
            channel_hash=channel_hash,
            reposted_claim_hash=reposted_claim_hash,
            reposted=es_result['reposted'],
            signature_valid=es_result['signature_valid'],
            reposted_tx_hash=reposted_tx_hash,
            reposted_tx_position=reposted_tx_position,
            reposted_height=reposted_height,
            channel_tx_hash=channel_tx_hash,
            channel_tx_position=channel_tx_position,
            channel_height=channel_height,
        )

    async def cached_search(self, kwargs):
        total_referenced = []
        cache_item = ResultCacheItem.from_cache(str(kwargs), self.search_cache)
        if cache_item.result is not None:
            return cache_item.result
        async with cache_item.lock:
            if cache_item.result:
                return cache_item.result
            response, offset, total = await self.search(**kwargs)
            censored = {}
            for row in response:
                if (row.get('censor_type') or 0) >= Censor.SEARCH:
                    censoring_channel_hash = bytes.fromhex(row['censoring_channel_id'])[::-1]
                    censored.setdefault(censoring_channel_hash, set())
                    censored[censoring_channel_hash].add(row['tx_hash'])
            total_referenced.extend(response)
            if censored:
                response, _, _ = await self.search(**kwargs, censor_type=Censor.NOT_CENSORED)
                total_referenced.extend(response)
            response = [self._make_resolve_result(r) for r in response]
            extra = [self._make_resolve_result(r) for r in await self._get_referenced_rows(total_referenced)]
            result = Outputs.to_base64(
                response, extra, offset, total, censored
            )
            cache_item.result = result
            return result

    async def get_many(self, *claim_ids):
        await self.populate_claim_cache(*claim_ids)
        return filter(None, map(self.claim_cache.get, claim_ids))

    async def populate_claim_cache(self, *claim_ids):
        missing = [claim_id for claim_id in claim_ids if self.claim_cache.get(claim_id) is None]
        if missing:
            results = await self.search_client.mget(
                index=self.index, body={"ids": missing}
            )
            for result in expand_result(filter(lambda doc: doc['found'], results["docs"])):
                self.claim_cache.set(result['claim_id'], result)

    async def search(self, **kwargs):
        try:
            return await self.search_ahead(**kwargs)
        except NotFoundError:
            return [], 0, 0
        # return expand_result(result['hits']), 0, result.get('total', {}).get('value', 0)

    async def search_ahead(self, **kwargs):
        # 'limit_claims_per_channel' case. Fetch 1000 results, reorder, slice, inflate and return
        per_channel_per_page = kwargs.pop('limit_claims_per_channel', 0) or 0
        remove_duplicates = kwargs.pop('remove_duplicates', False)
        page_size = kwargs.pop('limit', 10)
        offset = kwargs.pop('offset', 0)
        kwargs['limit'] = 1000
        cache_item = ResultCacheItem.from_cache(f"ahead{per_channel_per_page}{kwargs}", self.search_cache)
        if cache_item.result is not None:
            reordered_hits = cache_item.result
        else:
            async with cache_item.lock:
                if cache_item.result:
                    reordered_hits = cache_item.result
                else:
                    query = expand_query(**kwargs)
                    es_resp = await self.search_client.search(
                        query, index=self.index, track_total_hits=False,
                        timeout=f'{int(1000*self.search_timeout)}ms',
                        _source_includes=['_id', 'channel_id', 'reposted_claim_id', 'creation_height']
                    )
                    search_hits = deque(es_resp['hits']['hits'])
                    if self.timeout_counter and es_resp['timed_out']:
                        self.timeout_counter.inc()
                    if remove_duplicates:
                        search_hits = self.__remove_duplicates(search_hits)
                    if per_channel_per_page > 0:
                        reordered_hits = self.__search_ahead(search_hits, page_size, per_channel_per_page)
                    else:
                        reordered_hits = [(hit['_id'], hit['_source']['channel_id']) for hit in search_hits]
                    cache_item.result = reordered_hits
        result = list(await self.get_many(*(claim_id for claim_id, _ in reordered_hits[offset:(offset + page_size)])))
        return result, 0, len(reordered_hits)

    def __remove_duplicates(self, search_hits: deque) -> deque:
        known_ids = {}  # claim_id -> (creation_height, hit_id), where hit_id is either reposted claim id or original
        dropped = set()
        for hit in search_hits:
            hit_height, hit_id = hit['_source']['creation_height'], hit['_source']['reposted_claim_id'] or hit['_id']
            if hit_id not in known_ids:
                known_ids[hit_id] = (hit_height, hit['_id'])
            else:
                previous_height, previous_id = known_ids[hit_id]
                if hit_height < previous_height:
                    known_ids[hit_id] = (hit_height, hit['_id'])
                    dropped.add(previous_id)
                else:
                    dropped.add(hit['_id'])
        return deque(hit for hit in search_hits if hit['_id'] not in dropped)

    def __search_ahead(self, search_hits: deque, page_size: int, per_channel_per_page: int) -> list:
        reordered_hits = []
        channel_counters = Counter()
        next_page_hits_maybe_check_later = deque()
        while search_hits or next_page_hits_maybe_check_later:
            if reordered_hits and len(reordered_hits) % page_size == 0:
                channel_counters.clear()
            elif not reordered_hits:
                pass
            else:
                break  # means last page was incomplete and we are left with bad replacements
            for _ in range(len(next_page_hits_maybe_check_later)):
                claim_id, channel_id = next_page_hits_maybe_check_later.popleft()
                if per_channel_per_page > 0 and channel_counters[channel_id] < per_channel_per_page:
                    reordered_hits.append((claim_id, channel_id))
                    channel_counters[channel_id] += 1
                else:
                    next_page_hits_maybe_check_later.append((claim_id, channel_id))
            while search_hits:
                hit = search_hits.popleft()
                hit_id, hit_channel_id = hit['_id'], hit['_source']['channel_id']
                if hit_channel_id is None or per_channel_per_page <= 0:
                    reordered_hits.append((hit_id, hit_channel_id))
                elif channel_counters[hit_channel_id] < per_channel_per_page:
                    reordered_hits.append((hit_id, hit_channel_id))
                    channel_counters[hit_channel_id] += 1
                    if len(reordered_hits) % page_size == 0:
                        break
                else:
                    next_page_hits_maybe_check_later.append((hit_id, hit_channel_id))
        return reordered_hits

    async def _get_referenced_rows(self, txo_rows: List[dict]):
        txo_rows = [row for row in txo_rows if isinstance(row, dict)]
        referenced_ids = set(filter(None, map(itemgetter('reposted_claim_id'), txo_rows)))
        referenced_ids |= set(filter(None, (row['channel_id'] for row in txo_rows)))
        referenced_ids |= set(filter(None, (row['censoring_channel_id'] for row in txo_rows)))

        referenced_txos = []
        if referenced_ids:
            referenced_txos.extend(await self.get_many(*referenced_ids))
            referenced_ids = set(filter(None, (row['channel_id'] for row in referenced_txos)))

        if referenced_ids:
            referenced_txos.extend(await self.get_many(*referenced_ids))

        return referenced_txos


class ResultCacheItem:
    __slots__ = '_result', 'lock', 'has_result'

    def __init__(self):
        self.has_result = asyncio.Event()
        self.lock = asyncio.Lock()
        self._result = None

    @property
    def result(self) -> str:
        return self._result

    @result.setter
    def result(self, result: str):
        self._result = result
        if result is not None:
            self.has_result.set()

    @classmethod
    def from_cache(cls, cache_key, cache):
        cache_item = cache.get(cache_key)
        if cache_item is None:
            cache_item = cache[cache_key] = ResultCacheItem()
        return cache_item
