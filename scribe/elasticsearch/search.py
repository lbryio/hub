import logging
import asyncio
import struct
from bisect import bisect_right
from collections import Counter, deque
from decimal import Decimal
from operator import itemgetter
from typing import Optional, List, Iterable, TYPE_CHECKING

from elasticsearch import AsyncElasticsearch, NotFoundError, ConnectionError
from scribe.schema.result import Censor, Outputs
from scribe.schema.tags import clean_tags
from scribe.schema.url import normalize_name
from scribe.error import TooManyClaimSearchParametersError
from scribe.common import LRUCache
from scribe.db.common import CLAIM_TYPES, STREAM_TYPES
from scribe.elasticsearch.constants import INDEX_DEFAULT_SETTINGS, REPLACEMENTS, FIELDS, TEXT_FIELDS,  RANGE_FIELDS
from scribe.db.common import ResolveResult
if TYPE_CHECKING:
    from scribe.db import HubDB


class ChannelResolution(str):
    @classmethod
    def lookup_error(cls, url):
        return LookupError(f'Could not find channel in "{url}".')


class StreamResolution(str):
    @classmethod
    def lookup_error(cls, url):
        return LookupError(f'Could not find claim at "{url}".')


class IndexVersionMismatch(Exception):
    def __init__(self, got_version, expected_version):
        self.got_version = got_version
        self.expected_version = expected_version


class SearchIndex:
    VERSION = 1

    def __init__(self, hub_db: 'HubDB', index_prefix: str, search_timeout=3.0, elastic_host='localhost',
                 elastic_port=9200):
        self.hub_db = hub_db
        self.search_timeout = search_timeout
        self.sync_timeout = 600  # wont hit that 99% of the time, but can hit on a fresh import
        self.search_client: Optional[AsyncElasticsearch] = None
        self.sync_client: Optional[AsyncElasticsearch] = None
        self.index = index_prefix + 'claims'
        self.logger = logging.getLogger(__name__)
        self.claim_cache = LRUCache(2 ** 15)
        self.search_cache = LRUCache(2 ** 17)
        self._elastic_host = elastic_host
        self._elastic_port = elastic_port

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
        hosts = [{'host': self._elastic_host, 'port': self._elastic_port}]
        self.sync_client = AsyncElasticsearch(hosts, timeout=self.sync_timeout)
        self.search_client = AsyncElasticsearch(hosts, timeout=self.search_timeout)
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
        return acked

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
                    search_hits = deque((await self.search_client.search(
                        query, index=self.index, track_total_hits=False,
                        _source_includes=['_id', 'channel_id', 'reposted_claim_id', 'creation_height']
                    ))['hits']['hits'])
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

    def __search_ahead(self, search_hits: list, page_size: int, per_channel_per_page: int):
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


def expand_query(**kwargs):
    if "amount_order" in kwargs:
        kwargs["limit"] = 1
        kwargs["order_by"] = "effective_amount"
        kwargs["offset"] = int(kwargs["amount_order"]) - 1
    if 'name' in kwargs:
        kwargs['name'] = normalize_name(kwargs.pop('name'))
    if kwargs.get('is_controlling') is False:
        kwargs.pop('is_controlling')
    query = {'must': [], 'must_not': []}
    collapse = None
    if 'fee_currency' in kwargs and kwargs['fee_currency'] is not None:
        kwargs['fee_currency'] = kwargs['fee_currency'].upper()
    for key, value in kwargs.items():
        key = key.replace('claim.', '')
        many = key.endswith('__in') or isinstance(value, list)
        if many and len(value) > 2048:
            raise TooManyClaimSearchParametersError(key, 2048)
        if many:
            key = key.replace('__in', '')
            value = list(filter(None, value))
        if value is None or isinstance(value, list) and len(value) == 0:
            continue
        key = REPLACEMENTS.get(key, key)
        if key in FIELDS:
            partial_id = False
            if key == 'claim_type':
                if isinstance(value, str):
                    value = CLAIM_TYPES[value]
                else:
                    value = [CLAIM_TYPES[claim_type] for claim_type in value]
            elif key == 'stream_type':
                value = [STREAM_TYPES[value]] if isinstance(value, str) else list(map(STREAM_TYPES.get, value))
            if key == '_id':
                if isinstance(value, Iterable):
                    value = [item[::-1].hex() for item in value]
                else:
                    value = value[::-1].hex()
            if not many and key in ('_id', 'claim_id', 'sd_hash') and len(value) < 20:
                partial_id = True
            if key in ('signature_valid', 'has_source'):
                continue  # handled later
            if key in TEXT_FIELDS:
                key += '.keyword'
            ops = {'<=': 'lte', '>=': 'gte', '<': 'lt', '>': 'gt'}
            if partial_id:
                query['must'].append({"prefix": {key: value}})
            elif key in RANGE_FIELDS and isinstance(value, str) and value[0] in ops:
                operator_length = 2 if value[:2] in ops else 1
                operator, value = value[:operator_length], value[operator_length:]
                if key == 'fee_amount':
                    value = str(Decimal(value)*1000)
                query['must'].append({"range": {key: {ops[operator]: value}}})
            elif key in RANGE_FIELDS and isinstance(value, list) and all(v[0] in ops for v in value):
                range_constraints = []
                release_times = []
                for v in value:
                    operator_length = 2 if v[:2] in ops else 1
                    operator, stripped_op_v = v[:operator_length], v[operator_length:]
                    if key == 'fee_amount':
                        stripped_op_v = str(Decimal(stripped_op_v)*1000)
                    if key == 'release_time':
                        release_times.append((operator, stripped_op_v))
                    else:
                        range_constraints.append((operator, stripped_op_v))
                if key != 'release_time':
                    query['must'].append({"range": {key: {ops[operator]: v for operator, v in range_constraints}}})
                else:
                    query['must'].append(
                        {"bool":
                            {"should": [
                                {"bool": {
                                    "must_not": {
                                        "exists": {
                                            "field": "release_time"
                                        }
                                    }
                                }},
                                {"bool": {
                                    "must": [
                                        {"exists": {"field": "release_time"}},
                                        {'range': {key: {ops[operator]: v for operator, v in release_times}}},
                                ]}},
                            ]}
                        }
                    )
            elif many:
                query['must'].append({"terms": {key: value}})
            else:
                if key == 'fee_amount':
                    value = str(Decimal(value)*1000)
                query['must'].append({"term": {key: {"value": value}}})
        elif key == 'not_channel_ids':
            for channel_id in value:
                query['must_not'].append({"term": {'channel_id.keyword': channel_id}})
                query['must_not'].append({"term": {'_id': channel_id}})
        elif key == 'channel_ids':
            query['must'].append({"terms": {'channel_id.keyword': value}})
        elif key == 'claim_ids':
            query['must'].append({"terms": {'claim_id.keyword': value}})
        elif key == 'media_types':
            query['must'].append({"terms": {'media_type.keyword': value}})
        elif key == 'any_languages':
            query['must'].append({"terms": {'languages': clean_tags(value)}})
        elif key == 'any_languages':
            query['must'].append({"terms": {'languages': value}})
        elif key == 'all_languages':
            query['must'].extend([{"term": {'languages': tag}} for tag in value])
        elif key == 'any_tags':
            query['must'].append({"terms": {'tags.keyword': clean_tags(value)}})
        elif key == 'all_tags':
            query['must'].extend([{"term": {'tags.keyword': tag}} for tag in clean_tags(value)])
        elif key == 'not_tags':
            query['must_not'].extend([{"term": {'tags.keyword': tag}} for tag in clean_tags(value)])
        elif key == 'not_claim_id':
            query['must_not'].extend([{"term": {'claim_id.keyword': cid}} for cid in value])
        elif key == 'limit_claims_per_channel':
            collapse = ('channel_id.keyword', value)
    if kwargs.get('has_channel_signature'):
        query['must'].append({"exists": {"field": "signature"}})
        if 'signature_valid' in kwargs:
            query['must'].append({"term": {"is_signature_valid": bool(kwargs["signature_valid"])}})
    elif 'signature_valid' in kwargs:
        query['must'].append(
            {"bool":
                {"should": [
                    {"bool": {"must_not": {"exists": {"field": "signature"}}}},
                    {"bool" : {"must" : {"term": {"is_signature_valid": bool(kwargs["signature_valid"])}}}}
                ]}
             }
        )
    if 'has_source' in kwargs:
        is_stream_or_repost_terms = {"terms": {"claim_type": [CLAIM_TYPES['stream'], CLAIM_TYPES['repost']]}}
        query['must'].append(
            {"bool":
                {"should": [
                    {"bool": # when is_stream_or_repost AND has_source
                        {"must": [
                            {"match": {"has_source": kwargs['has_source']}},
                            is_stream_or_repost_terms,
                        ]
                        },
                     },
                    {"bool": # when not is_stream_or_repost
                        {"must_not": is_stream_or_repost_terms}
                     },
                    {"bool": # when reposted_claim_type wouldn't have source
                        {"must_not":
                            [
                                {"term": {"reposted_claim_type": CLAIM_TYPES['stream']}}
                            ],
                        "must":
                            [
                                {"term": {"claim_type": CLAIM_TYPES['repost']}}
                            ]
                        }
                     }
                ]}
             }
        )
    if kwargs.get('text'):
        query['must'].append(
                    {"simple_query_string":
                         {"query": kwargs["text"], "fields": [
                             "claim_name^4", "channel_name^8", "title^1", "description^.5", "author^1", "tags^.5"
                         ]}})
    query = {
        "_source": {"excludes": ["description", "title"]},
        'query': {'bool': query},
        "sort": [],
    }
    if "limit" in kwargs:
        query["size"] = kwargs["limit"]
    if 'offset' in kwargs:
        query["from"] = kwargs["offset"]
    if 'order_by' in kwargs:
        if isinstance(kwargs["order_by"], str):
            kwargs["order_by"] = [kwargs["order_by"]]
        for value in kwargs['order_by']:
            if 'trending_group' in value:
                # fixme: trending_mixed is 0 for all records on variable decay, making sort slow.
                continue
            is_asc = value.startswith('^')
            value = value[1:] if is_asc else value
            value = REPLACEMENTS.get(value, value)
            if value in TEXT_FIELDS:
                value += '.keyword'
            query['sort'].append({value: "asc" if is_asc else "desc"})
    if collapse:
        query["collapse"] = {
            "field": collapse[0],
            "inner_hits": {
                "name": collapse[0],
                "size": collapse[1],
                "sort": query["sort"]
            }
        }
    return query


def expand_result(results):
    inner_hits = []
    expanded = []
    for result in results:
        if result.get("inner_hits"):
            for _, inner_hit in result["inner_hits"].items():
                inner_hits.extend(inner_hit["hits"]["hits"])
            continue
        result = result['_source']
        result['claim_hash'] = bytes.fromhex(result['claim_id'])[::-1]
        if result['reposted_claim_id']:
            result['reposted_claim_hash'] = bytes.fromhex(result['reposted_claim_id'])[::-1]
        else:
            result['reposted_claim_hash'] = None
        result['channel_hash'] = bytes.fromhex(result['channel_id'])[::-1] if result['channel_id'] else None
        result['txo_hash'] = bytes.fromhex(result['tx_id'])[::-1] + struct.pack('<I', result['tx_nout'])
        result['tx_hash'] = bytes.fromhex(result['tx_id'])[::-1]
        result['reposted'] = result.pop('repost_count')
        result['signature_valid'] = result.pop('is_signature_valid')
        # result['normalized'] = result.pop('normalized_name')
        # if result['censoring_channel_hash']:
        #     result['censoring_channel_hash'] = unhexlify(result['censoring_channel_hash'])[::-1]
        expanded.append(result)
    if inner_hits:
        return expand_result(inner_hits)
    return expanded


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
