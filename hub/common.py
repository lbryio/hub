import struct
import asyncio
import hashlib
import hmac
import ipaddress
import logging
import logging.handlers
import typing
import collections
from decimal import Decimal
from typing import Iterable
from asyncio import get_event_loop, Event
from prometheus_client import Counter
from hub.schema.tags import clean_tags
from hub.schema.url import normalize_name
from hub.error import TooManyClaimSearchParametersError

log = logging.getLogger(__name__)


_sha256 = hashlib.sha256
_sha512 = hashlib.sha512
_new_hash = hashlib.new
_new_hmac = hmac.new
HASHX_LEN = 11
CLAIM_HASH_LEN = 20


HISTOGRAM_BUCKETS = (
    .005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0, 15.0, 20.0, 30.0, 60.0, float('inf')
)
SIZE_BUCKETS = (
    1, 10, 100, 500, 1000, 2000, 4000, 7500, 10000, 15000, 25000, 50000, 75000, 100000, 150000, 250000, float('inf')
)

CLAIM_TYPES = {
    'stream': 1,
    'channel': 2,
    'repost': 3,
    'collection': 4,
}

STREAM_TYPES = {
    'video': 1,
    'audio': 2,
    'image': 3,
    'document': 4,
    'binary': 5,
    'model': 6,
}


def setup_logging(log_path: str):
    log = logging.getLogger()
    fmt = logging.Formatter("%(asctime)s %(levelname)-4s %(name)s:%(lineno)d: %(message)s")
    handler = logging.handlers.RotatingFileHandler(log_path, maxBytes=1024*1024*5, backupCount=2)
    handler.setFormatter(fmt)
    log.addHandler(handler)
    handler = logging.StreamHandler()
    handler.setFormatter(fmt)
    log.addHandler(handler)

    log.setLevel(logging.INFO)
    logging.getLogger('aiohttp').setLevel(logging.WARNING)
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)


class StagedClaimtrieItem(typing.NamedTuple):
    """
    Represents a claim TXO, used internally by the block processor
    """
    name: str
    normalized_name: str
    claim_hash: bytes
    amount: int
    expiration_height: int
    tx_num: int
    position: int
    root_tx_num: int
    root_position: int
    channel_signature_is_valid: bool
    signing_hash: typing.Optional[bytes]
    reposted_claim_hash: typing.Optional[bytes]

    @property
    def is_update(self) -> bool:
        return (self.tx_num, self.position) != (self.root_tx_num, self.root_position)

    def invalidate_signature(self) -> 'StagedClaimtrieItem':
        return StagedClaimtrieItem(
            self.name, self.normalized_name, self.claim_hash, self.amount, self.expiration_height, self.tx_num,
            self.position, self.root_tx_num, self.root_position, False, None, self.reposted_claim_hash
        )


def formatted_time(t, sep=' '):
    """Return a number of seconds as a string in days, hours, mins and
    maybe secs."""
    t = int(t)
    fmts = (('{:d}d', 86400), ('{:02d}h', 3600), ('{:02d}m', 60))
    parts = []
    for fmt, n in fmts:
        val = t // n
        if parts or val:
            parts.append(fmt.format(val))
        t %= n
    if len(parts) < 3:
        parts.append(f'{t:02d}s')
    return sep.join(parts)


def protocol_tuple(s):
    """Converts a protocol version number, such as "1.0" to a tuple (1, 0).

    If the version number is bad, (0, ) indicating version 0 is returned."""
    try:
        return tuple(int(part) for part in s.split('.'))
    except Exception:
        return (0, )


def version_string(ptuple):
    """Convert a version tuple such as (1, 2) to "1.2".
    There is always at least one dot, so (1, ) becomes "1.0"."""
    while len(ptuple) < 2:
        ptuple += (0, )
    return '.'.join(str(p) for p in ptuple)


def protocol_version(client_req, min_tuple, max_tuple):
    """Given a client's protocol version string, return a pair of
    protocol tuples:
           (negotiated version, client min request)
    If the request is unsupported, the negotiated protocol tuple is
    None.
    """
    if client_req is None:
        client_min = client_max = min_tuple
    else:
        if isinstance(client_req, list) and len(client_req) == 2:
            client_min, client_max = client_req
        else:
            client_min = client_max = client_req
        client_min = protocol_tuple(client_min)
        client_max = protocol_tuple(client_max)

    result = min(client_max, max_tuple)
    if result < max(client_min, min_tuple) or result == (0, ):
        result = None

    return result, client_min


class LRUCacheWithMetrics:
    __slots__ = [
        'capacity',
        'cache',
        '_track_metrics',
        'hits',
        'misses'
    ]

    def __init__(self, capacity: int, metric_name: typing.Optional[str] = None, namespace: str = "daemon_cache"):
        self.capacity = capacity
        self.cache = collections.OrderedDict()
        if metric_name is None:
            self._track_metrics = False
            self.hits = self.misses = None
        else:
            self._track_metrics = True
            try:
                self.hits = Counter(
                    f"{metric_name}_cache_hit_count", "Number of cache hits", namespace=namespace
                )
                self.misses = Counter(
                    f"{metric_name}_cache_miss_count", "Number of cache misses", namespace=namespace
                )
            except ValueError as err:
                log.debug("failed to set up prometheus %s_cache_miss_count metric: %s", metric_name, err)
                self._track_metrics = False
                self.hits = self.misses = None

    def get(self, key, default=None):
        try:
            value = self.cache.pop(key)
            if self._track_metrics:
                self.hits.inc()
        except KeyError:
            if self._track_metrics:
                self.misses.inc()
            return default
        self.cache[key] = value
        return value

    def set(self, key, value):
        try:
            self.cache.pop(key)
        except KeyError:
            if len(self.cache) >= self.capacity:
                self.cache.popitem(last=False)
        self.cache[key] = value

    def clear(self):
        self.cache.clear()

    def pop(self, key):
        return self.cache.pop(key)

    def __setitem__(self, key, value):
        return self.set(key, value)

    def __getitem__(self, item):
        return self.get(item)

    def __contains__(self, item) -> bool:
        return item in self.cache

    def __len__(self):
        return len(self.cache)

    def __delitem__(self, key):
        self.cache.pop(key)

    def __del__(self):
        self.clear()


class LRUCache:
    __slots__ = [
        'capacity',
        'cache'
    ]

    def __init__(self, capacity: int):
        self.capacity = capacity
        self.cache = collections.OrderedDict()

    def get(self, key, default=None):
        try:
            value = self.cache.pop(key)
        except KeyError:
            return default
        self.cache[key] = value
        return value

    def set(self, key, value):
        try:
            self.cache.pop(key)
        except KeyError:
            if len(self.cache) >= self.capacity:
                self.cache.popitem(last=False)
        self.cache[key] = value

    def items(self):
        return self.cache.items()

    def clear(self):
        self.cache.clear()

    def pop(self, key, default=None):
        return self.cache.pop(key, default)

    def __setitem__(self, key, value):
        return self.set(key, value)

    def __getitem__(self, item):
        return self.get(item)

    def __contains__(self, item) -> bool:
        return item in self.cache

    def __len__(self):
        return len(self.cache)

    def __delitem__(self, key):
        self.cache.pop(key)

    def __del__(self):
        self.clear()


# the ipaddress module does not show these subnets as reserved
CARRIER_GRADE_NAT_SUBNET = ipaddress.ip_network('100.64.0.0/10')
IPV4_TO_6_RELAY_SUBNET = ipaddress.ip_network('192.88.99.0/24')


def is_valid_public_ipv4(address, allow_localhost: bool = False, allow_lan: bool = False):
    try:
        parsed_ip = ipaddress.ip_address(address)
        if parsed_ip.is_loopback and allow_localhost:
            return True
        if allow_lan and parsed_ip.is_private:
            return True
        if any((parsed_ip.version != 4, parsed_ip.is_unspecified, parsed_ip.is_link_local, parsed_ip.is_loopback,
                parsed_ip.is_multicast, parsed_ip.is_reserved, parsed_ip.is_private)):
            return False
        else:
            return not any((CARRIER_GRADE_NAT_SUBNET.supernet_of(ipaddress.ip_network(f"{address}/32")),
                            IPV4_TO_6_RELAY_SUBNET.supernet_of(ipaddress.ip_network(f"{address}/32"))))
    except (ipaddress.AddressValueError, ValueError):
        return False


def sha256(x):
    """Simple wrapper of hashlib sha256."""
    return _sha256(x).digest()


def ripemd160(x):
    """Simple wrapper of hashlib ripemd160."""
    h = _new_hash('ripemd160')
    h.update(x)
    return h.digest()


def double_sha256(x):
    """SHA-256 of SHA-256, as used extensively in bitcoin."""
    return sha256(sha256(x))


def hmac_sha512(key, msg):
    """Use SHA-512 to provide an HMAC."""
    return _new_hmac(key, msg, _sha512).digest()


def hash160(x):
    """RIPEMD-160 of SHA-256. Used to make bitcoin addresses from pubkeys."""
    return ripemd160(sha256(x))


def hash_to_hex_str(x: bytes) -> str:
    """Convert a big-endian binary hash to displayed hex string.

    Display form of a binary hash is reversed and converted to hex.
    """
    return x[::-1].hex()


def hex_str_to_hash(x: str) -> bytes:
    """Convert a displayed hex string to a binary hash."""
    return bytes.fromhex(x)[::-1]



INVALID_REQUEST = -32600
INVALID_ARGS = -32602


class CodeMessageError(Exception):

    @property
    def code(self):
        return self.args[0]

    @property
    def message(self):
        return self.args[1]

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                self.code == other.code and self.message == other.message)

    def __hash__(self):
        # overridden to make the exception hashable
        # see https://bugs.python.org/issue28603
        return hash((self.code, self.message))

    @classmethod
    def invalid_args(cls, message):
        return cls(INVALID_ARGS, message)

    @classmethod
    def invalid_request(cls, message):
        return cls(INVALID_REQUEST, message)

    @classmethod
    def empty_batch(cls):
        return cls.invalid_request('batch is empty')


class RPCError(CodeMessageError):
    pass



class DaemonError(Exception):
    """Raised when the daemon returns an error in its results."""


class WarmingUpError(Exception):
    """Internal - when the daemon is warming up."""


class WorkQueueFullError(Exception):
    """Internal - when the daemon's work queue is full."""


class TaskGroup:
    def __init__(self, loop=None):
        self._loop = loop or get_event_loop()
        self._tasks = set()
        self.done = Event()
        self.started = Event()

    def __len__(self):
        return len(self._tasks)

    def add(self, coro):
        task = self._loop.create_task(coro)
        self._tasks.add(task)
        self.started.set()
        self.done.clear()
        task.add_done_callback(self._remove)
        return task

    def _remove(self, task):
        self._tasks.remove(task)
        if len(self._tasks) < 1:
            self.done.set()
            self.started.clear()

    def cancel(self):
        for task in self._tasks:
            task.cancel()
        self.done.set()
        self.started.clear()


class IndexVersionMismatch(Exception):
    def __init__(self, got_version, expected_version):
        self.got_version = got_version
        self.expected_version = expected_version


# Elasticsearch constants

INDEX_DEFAULT_SETTINGS = {
    "settings":
        {"analysis":
            {"analyzer": {
                "default": {"tokenizer": "whitespace", "filter": ["lowercase", "porter_stem"]}}},
            "index":
                {"refresh_interval": -1,
                 "number_of_shards": 1,
                 "number_of_replicas": 0,
                 "sort": {
                     "field": ["trending_score", "release_time"],
                     "order": ["desc", "desc"]
                 }}
        },
    "mappings": {
        "properties": {
            "claim_id": {
                "fields": {
                    "keyword": {
                        "ignore_above": 256,
                        "type": "keyword"
                    }
                },
                "type": "text",
                "index_prefixes": {
                    "min_chars": 1,
                    "max_chars": 10
                }
            },
            "sd_hash": {
                "fields": {
                    "keyword": {
                        "ignore_above": 96,
                        "type": "keyword"
                    }
                },
                "type": "text",
                "index_prefixes": {
                    "min_chars": 1,
                    "max_chars": 4
                }
            },
            "height": {"type": "integer"},
            "claim_type": {"type": "byte"},
            "censor_type": {"type": "byte"},
            "trending_score": {"type": "double"},
            "release_time": {"type": "long"}
        }
    }
}

FIELDS = {
    '_id',
    'claim_id', 'claim_type', 'claim_name', 'normalized_name',
    'tx_id', 'tx_nout', 'tx_position',
    'short_url', 'canonical_url',
    'is_controlling', 'last_take_over_height',
    'public_key_bytes', 'public_key_id', 'claims_in_channel',
    'channel_id', 'signature', 'signature_digest', 'is_signature_valid',
    'amount', 'effective_amount', 'support_amount',
    'fee_amount', 'fee_currency',
    'height', 'creation_height', 'activation_height', 'expiration_height',
    'stream_type', 'media_type', 'censor_type',
    'title', 'author', 'description',
    'timestamp', 'creation_timestamp',
    'duration', 'release_time',
    'tags', 'languages', 'has_source', 'reposted_claim_type',
    'reposted_claim_id', 'repost_count', 'sd_hash',
    'trending_score', 'tx_num',
    'channel_tx_id', 'channel_tx_position', 'channel_height',  'reposted_tx_id',
    'reposted_tx_position', 'reposted_height',
}

TEXT_FIELDS = {
    'author', 'canonical_url', 'channel_id', 'description', 'claim_id', 'censoring_channel_id',
    'media_type', 'normalized_name', 'public_key_bytes', 'public_key_id', 'short_url', 'signature',
    'claim_name', 'signature_digest', 'title', 'tx_id', 'fee_currency', 'reposted_claim_id',
    'tags', 'sd_hash', 'channel_tx_id', 'reposted_tx_id',
}

RANGE_FIELDS = {
    'height', 'creation_height', 'activation_height', 'expiration_height',
    'timestamp', 'creation_timestamp', 'duration', 'release_time', 'fee_amount',
    'tx_position', 'repost_count', 'limit_claims_per_channel',
    'amount', 'effective_amount', 'support_amount',
    'trending_score', 'censor_type', 'tx_num', 'reposted_tx_position', 'reposted_height',
    'channel_tx_position', 'channel_height',
}

ALL_FIELDS = RANGE_FIELDS | TEXT_FIELDS | FIELDS

REPLACEMENTS = {
    'claim_name': 'normalized_name',
    'name': 'normalized_name',
    'txid': 'tx_id',
    'nout': 'tx_nout',
    'trending_group': 'trending_score',
    'trending_mixed': 'trending_score',
    'trending_global': 'trending_score',
    'trending_local': 'trending_score',
    'reposted': 'repost_count',
    'stream_types': 'stream_type',
    'media_types': 'media_type',
    'valid_channel_signature': 'is_signature_valid'
}


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


async def asyncify_for_loop(gen, ticks_per_sleep: int = 1000):
    async_sleep = asyncio.sleep
    for cnt, item in enumerate(gen):
        yield item
        if cnt % ticks_per_sleep == 0:
            await async_sleep(0)
