import hashlib
import hmac
import ipaddress
import logging
import logging.handlers
import typing
import collections
from bisect import bisect_right
from asyncio import get_event_loop, Event
from prometheus_client import Counter

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


def setup_logging(log_path: str):
    log = logging.getLogger('scribe')
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


class LargestValueCache:
    __slots__ = [
        '_capacity',
        '_cache',
        '_sizes',
        '_keys'
    ]

    def __init__(self, capacity: int):
        self._capacity = capacity
        self._cache = {}
        self._keys = []
        self._sizes = []

    def items(self):
        return self._cache.items()

    def get(self, key, default=None):
        return self._cache.get(key, default)

    @property
    def full(self):
        return len(self._cache) >= self._capacity

    def set(self, key, value) -> bool:
        if not self.full:
            idx = len(self._sizes) - bisect_right(list(reversed(self._sizes)), len(value))
            self._sizes.insert(idx, len(value))
            self._keys.insert(idx, key)
            self._cache[key] = value
            return True
        elif key in self._cache:  # item is already cached, update it
            existing = self._keys.index(key)
            if len(value) != self._sizes[existing]:
                self._keys.pop(existing)
                self._sizes.pop(existing)
                idx = len(self._sizes) - bisect_right(list(reversed(self._sizes)), len(value))
                self._sizes.insert(idx, len(value))
                self._keys.insert(idx, key)
            self._cache[key] = value
            return True
        elif len(value) > self._sizes[-1]:
            self._sizes.pop()
            self._cache.pop(self._keys.pop())
            idx = len(self._sizes) - bisect_right(list(reversed(self._sizes)), len(value))
            self._sizes.insert(idx, len(value))
            self._keys.insert(idx, key)
            self._cache[key] = value
            return True
        return False

    def clear(self):
        self._cache.clear()
        self._sizes.clear()
        self._keys.clear()

    def pop(self, key, default=None):
        return self._cache.pop(key, default)

    def __setitem__(self, key, value):
        return self.set(key, value)

    def __getitem__(self, item):
        return self.get(item)

    def __contains__(self, item) -> bool:
        return item in self._cache

    def __len__(self):
        return len(self._cache)


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
