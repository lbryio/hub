import os
import ssl
import math
import time
import codecs
import typing
import asyncio
import logging
import itertools
import collections
from bisect import bisect_right
from asyncio import Event, sleep
from collections import defaultdict, namedtuple
from contextlib import suppress
from functools import partial
from elasticsearch import ConnectionTimeout
from prometheus_client import Counter, Info, Histogram, Gauge
from scribe.schema.result import Outputs
from scribe.error import ResolveCensoredError, TooManyClaimSearchParametersError
from scribe import __version__, PROMETHEUS_NAMESPACE
from scribe.hub import PROTOCOL_MIN, PROTOCOL_MAX, HUB_PROTOCOL_VERSION
from scribe.build_info import BUILD, COMMIT_HASH, DOCKER_TAG
from scribe.elasticsearch import SearchIndex
from scribe.common import sha256, hash_to_hex_str, hex_str_to_hash, HASHX_LEN, version_string, formatted_time
from scribe.common import protocol_version, RPCError, DaemonError, TaskGroup, HISTOGRAM_BUCKETS
from scribe.hub.jsonrpc import JSONRPCAutoDetect, JSONRPCConnection, JSONRPCv2, JSONRPC
from scribe.hub.common import BatchRequest, ProtocolError, Request, Batch, Notification
from scribe.hub.framer import NewlineFramer
if typing.TYPE_CHECKING:
    from scribe.db import HubDB
    from scribe.hub.env import ServerEnv
    from scribe.blockchain.daemon import LBCDaemon
    from scribe.hub.mempool import HubMemPool

BAD_REQUEST = 1
DAEMON_ERROR = 2

log = logging.getLogger(__name__)


SignatureInfo = namedtuple('SignatureInfo', 'min_args max_args '
                           'required_names other_names')


def scripthash_to_hashX(scripthash: str) -> bytes:
    try:
        bin_hash = hex_str_to_hash(scripthash)
        if len(bin_hash) == 32:
            return bin_hash[:HASHX_LEN]
    except Exception:
        pass
    raise RPCError(BAD_REQUEST, f'{scripthash} is not a valid script hash')


def non_negative_integer(value) -> int:
    """Return param value it is or can be converted to a non-negative
    integer, otherwise raise an RPCError."""
    try:
        value = int(value)
        if value >= 0:
            return value
    except ValueError:
        pass
    raise RPCError(BAD_REQUEST,
                   f'{value} should be a non-negative integer')


def assert_boolean(value) -> bool:
    """Return param value it is boolean otherwise raise an RPCError."""
    if value in (False, True):
        return value
    raise RPCError(BAD_REQUEST, f'{value} should be a boolean value')


def assert_tx_hash(value: str) -> None:
    """Raise an RPCError if the value is not a valid transaction
    hash."""
    try:
        if len(bytes.fromhex(value)) == 32:
            return
    except Exception:
        pass
    raise RPCError(BAD_REQUEST, f'{value} should be a transaction hash')


class Semaphores:
    """For aiorpcX's semaphore handling."""

    def __init__(self, semaphores):
        self.semaphores = semaphores
        self.acquired = []

    async def __aenter__(self):
        for semaphore in self.semaphores:
            await semaphore.acquire()
            self.acquired.append(semaphore)

    async def __aexit__(self, exc_type, exc_value, traceback):
        for semaphore in self.acquired:
            semaphore.release()


class SessionGroup:

    def __init__(self, gid: int):
        self.gid = gid
        # Concurrency per group
        self.semaphore = asyncio.Semaphore(20)


NAMESPACE = f"{PROMETHEUS_NAMESPACE}_hub"


class SessionManager:
    """Holds global state about all sessions."""

    version_info_metric = Info(
        'build', 'Wallet server build info (e.g. version, commit hash)', namespace=NAMESPACE
    )
    version_info_metric.info({
        'build': BUILD,
        "commit": COMMIT_HASH,
        "docker_tag": DOCKER_TAG,
        'version': __version__,
        "min_version": version_string(PROTOCOL_MIN),
        "cpu_count": str(os.cpu_count())
    })
    session_count_metric = Gauge("session_count", "Number of connected client sessions", namespace=NAMESPACE,
                                 labelnames=("version",))
    request_count_metric = Counter("requests_count", "Number of requests received", namespace=NAMESPACE,
                                   labelnames=("method", "version"))
    tx_request_count_metric = Counter("requested_transaction", "Number of transactions requested", namespace=NAMESPACE)
    tx_replied_count_metric = Counter("replied_transaction", "Number of transactions responded", namespace=NAMESPACE)
    urls_to_resolve_count_metric = Counter("urls_to_resolve", "Number of urls to resolve", namespace=NAMESPACE)
    resolved_url_count_metric = Counter("resolved_url", "Number of resolved urls", namespace=NAMESPACE)
    interrupt_count_metric = Counter("interrupt", "Number of interrupted queries", namespace=NAMESPACE)
    db_operational_error_metric = Counter(
        "operational_error", "Number of queries that raised operational errors", namespace=NAMESPACE
    )
    db_error_metric = Counter(
        "internal_error", "Number of queries raising unexpected errors", namespace=NAMESPACE
    )
    executor_time_metric = Histogram(
        "executor_time", "SQLite executor times", namespace=NAMESPACE, buckets=HISTOGRAM_BUCKETS
    )
    pending_query_metric = Gauge(
        "pending_queries_count", "Number of pending and running sqlite queries", namespace=NAMESPACE
    )

    client_version_metric = Counter(
        "clients", "Number of connections received per client version",
        namespace=NAMESPACE, labelnames=("version",)
    )
    address_history_metric = Histogram(
        "address_history", "Time to fetch an address history",
        namespace=NAMESPACE, buckets=HISTOGRAM_BUCKETS
    )
    notifications_in_flight_metric = Gauge(
        "notifications_in_flight", "Count of notifications in flight",
        namespace=NAMESPACE
    )
    notifications_sent_metric = Histogram(
        "notifications_sent", "Time to send an address notification",
        namespace=NAMESPACE, buckets=HISTOGRAM_BUCKETS
    )

    def __init__(self, env: 'ServerEnv', db: 'HubDB', mempool: 'HubMemPool',
                 daemon: 'LBCDaemon', shutdown_event: asyncio.Event,
                 on_available_callback: typing.Callable[[], None], on_unavailable_callback: typing.Callable[[], None]):
        env.max_send = max(350000, env.max_send)
        self.env = env
        self.db = db
        self.on_available_callback = on_available_callback
        self.on_unavailable_callback = on_unavailable_callback
        self.daemon = daemon
        self.mempool = mempool
        self.shutdown_event = shutdown_event
        self.logger = logging.getLogger(__name__)
        self.servers: typing.Dict[str, asyncio.AbstractServer] = {}
        self.sessions: typing.Dict[int, 'LBRYElectrumX'] = {}
        self.hashx_subscriptions_by_session: typing.DefaultDict[str, typing.Set[int]] = defaultdict(set)
        self.mempool_statuses = {}
        self.cur_group = SessionGroup(0)
        self.txs_sent = 0
        self.start_time = time.time()
        self.history_cache = {}
        self.resolve_outputs_cache = {}
        self.resolve_cache = {}
        self.notified_height: typing.Optional[int] = None
        # Cache some idea of room to avoid recounting on each subscription
        self.subs_room = 0

        self.session_event = Event()

        # Search index
        self.search_index = SearchIndex(
            self.db, self.env.es_index_prefix, self.env.database_query_timeout,
            elastic_host=env.elastic_host, elastic_port=env.elastic_port
        )
        self.running = False

    def clear_caches(self):
        self.history_cache.clear()
        self.resolve_outputs_cache.clear()
        self.resolve_cache.clear()

    async def _start_server(self, kind, *args, **kw_args):
        loop = asyncio.get_event_loop()

        if kind == 'TCP':
            protocol_class = LBRYElectrumX
        else:
            raise ValueError(kind)
        protocol_factory = partial(protocol_class, self, kind)

        host, port = args[:2]
        try:
            self.servers[kind] = await loop.create_server(protocol_factory, *args, **kw_args)
        except OSError as e:    # don't suppress CancelledError
            self.logger.error(f'{kind} server failed to listen on {host}:'
                              f'{port:d} :{e!r}')
        else:
            self.logger.info(f'{kind} server listening on {host}:{port:d}')

    async def _start_external_servers(self):
        """Start listening on TCP and SSL ports, but only if the respective
        port was given in the environment.
        """
        env = self.env
        host = env.cs_host()
        if env.tcp_port is not None:
            await self._start_server('TCP', host, env.tcp_port)

    async def _close_servers(self, kinds):
        """Close the servers of the given kinds (TCP etc.)."""
        if kinds:
            self.logger.info('closing down {} listening servers'
                             .format(', '.join(kinds)))
        for kind in kinds:
            server = self.servers.pop(kind, None)
            if server:
                server.close()
                await server.wait_closed()

    async def _manage_servers(self):
        paused = False
        max_sessions = self.env.max_sessions
        low_watermark = int(max_sessions * 0.95)
        while True:
            await self.session_event.wait()
            self.session_event.clear()
            if not paused and len(self.sessions) >= max_sessions:
                self.on_unavailable_callback()
                self.logger.info(f'maximum sessions {max_sessions:,d} '
                                 f'reached, stopping new connections until '
                                 f'count drops to {low_watermark:,d}')
                await self._close_servers(['TCP', 'SSL'])
                paused = True
            # Start listening for incoming connections if paused and
            # session count has fallen
            if paused and len(self.sessions) <= low_watermark:
                self.on_available_callback()
                self.logger.info('resuming listening for incoming connections')
                await self._start_external_servers()
                paused = False

    def _group_map(self):
        group_map = defaultdict(list)
        for session in self.sessions.values():
            group_map[session.group].append(session)
        return group_map

    def _sub_count(self) -> int:
        return sum(s.sub_count() for s in self.sessions.values())

    def _lookup_session(self, session_id):
        try:
            session_id = int(session_id)
        except Exception:
            pass
        else:
            for session in self.sessions.values():
                if session.session_id == session_id:
                    return session
        return None

    async def _for_each_session(self, session_ids, operation):
        if not isinstance(session_ids, list):
            raise RPCError(BAD_REQUEST, 'expected a list of session IDs')

        result = []
        for session_id in session_ids:
            session = self._lookup_session(session_id)
            if session:
                result.append(await operation(session))
            else:
                result.append(f'unknown session: {session_id}')
        return result

    async def _clear_stale_sessions(self):
        """Cut off sessions that haven't done anything for 10 minutes."""
        session_timeout = self.env.session_timeout
        while True:
            await sleep(session_timeout // 10)
            stale_cutoff = time.perf_counter() - session_timeout
            stale_sessions = [session for session in self.sessions.values()
                              if session.last_recv < stale_cutoff]
            if stale_sessions:
                text = ', '.join(str(session.session_id)
                                 for session in stale_sessions)
                self.logger.info(f'closing stale connections {text}')
                # Give the sockets some time to close gracefully
                if stale_sessions:
                    await asyncio.wait([
                        session.close(force_after=session_timeout // 10) for session in stale_sessions
                    ])

            # Consolidate small groups
            group_map = self._group_map()
            groups = [group for group, sessions in group_map.items()
                      if len(sessions) <= 5]  # fixme: apply session cost here
            if len(groups) > 1:
                new_group = groups[-1]
                for group in groups:
                    for session in group_map[group]:
                        session.group = new_group

    def _get_info(self):
        """A summary of server state."""
        group_map = self._group_map()
        method_counts = collections.defaultdict(int)
        error_count = 0
        logged = 0
        paused = 0
        pending_requests = 0
        closing = 0

        for s in self.sessions.values():
            error_count += s.errors
            if s.log_me:
                logged += 1
            if not s._can_send.is_set():
                paused += 1
            pending_requests += s.count_pending_items()
            if s.is_closing():
                closing += 1
            for request, _ in s.connection._requests.values():
                method_counts[request.method] += 1
        return {
            'closing': closing,
            'daemon': self.daemon.logged_url(),
            'daemon_height': self.daemon.cached_height(),
            'db_height': self.db.db_height,
            'errors': error_count,
            'groups': len(group_map),
            'logged': logged,
            'paused': paused,
            'pid': os.getpid(),
            'peers': [],
            'requests': pending_requests,
            'method_counts': method_counts,
            'sessions': self.session_count(),
            'subs': self._sub_count(),
            'txs_sent': self.txs_sent,
            'uptime': formatted_time(time.time() - self.start_time),
            'version': __version__,
        }

    def _group_data(self):
        """Returned to the RPC 'groups' call."""
        result = []
        group_map = self._group_map()
        for group, sessions in group_map.items():
            result.append([group.gid,
                           len(sessions),
                           sum(s.bw_charge for s in sessions),
                           sum(s.count_pending_items() for s in sessions),
                           sum(s.txs_sent for s in sessions),
                           sum(s.sub_count() for s in sessions),
                           sum(s.recv_count for s in sessions),
                           sum(s.recv_size for s in sessions),
                           sum(s.send_count for s in sessions),
                           sum(s.send_size for s in sessions),
                           ])
        return result

    async def _electrum_and_raw_headers(self, height):
        raw_header = await self.raw_header(height)
        electrum_header = self.env.coin.electrum_header(raw_header, height)
        return electrum_header, raw_header

    async def _refresh_hsub_results(self, height):
        """Refresh the cached header subscription responses to be for height,
        and record that as notified_height.
        """
        # Paranoia: a reorg could race and leave db_height lower
        height = min(height, self.db.db_height)
        electrum, raw = await self._electrum_and_raw_headers(height)
        self.hsub_results = (electrum, {'hex': raw.hex(), 'height': height})
        self.notified_height = height

    # --- LocalRPC command handlers
    #
    # async def rpc_add_peer(self, real_name):
    #     """Add a peer.
    #
    #     real_name: "bch.electrumx.cash t50001 s50002" for example
    #     """
    #     await self._notify_peer(real_name)
    #     return f"peer '{real_name}' added"
    #
    # async def rpc_disconnect(self, session_ids):
    #     """Disconnect sessions.
    #
    #     session_ids: array of session IDs
    #     """
    #     async def close(session):
    #         """Close the session's transport."""
    #         await session.close(force_after=2)
    #         return f'disconnected {session.session_id}'
    #
    #     return await self._for_each_session(session_ids, close)
    #
    # async def rpc_log(self, session_ids):
    #     """Toggle logging of sessions.
    #
    #     session_ids: array of session IDs
    #     """
    #     async def toggle_logging(session):
    #         """Toggle logging of the session."""
    #         session.toggle_logging()
    #         return f'log {session.session_id}: {session.log_me}'
    #
    #     return await self._for_each_session(session_ids, toggle_logging)
    #
    # async def rpc_daemon_url(self, daemon_url):
    #     """Replace the daemon URL."""
    #     daemon_url = daemon_url or self.env.daemon_url
    #     try:
    #         self.daemon.set_url(daemon_url)
    #     except Exception as e:
    #         raise RPCError(BAD_REQUEST, f'an error occurred: {e!r}')
    #     return f'now using daemon at {self.daemon.logged_url()}'
    #
    # async def rpc_stop(self):
    #     """Shut down the server cleanly."""
    #     self.shutdown_event.set()
    #     return 'stopping'
    #
    # async def rpc_getinfo(self):
    #     """Return summary information about the server process."""
    #     return self._get_info()
    #
    # async def rpc_groups(self):
    #     """Return statistics about the session groups."""
    #     return self._group_data()
    #
    # async def rpc_peers(self):
    #     """Return a list of data about server peers."""
    #     return self.env.peer_hubs
    #
    # async def rpc_query(self, items, limit):
    #     """Return a list of data about server peers."""
    #     coin = self.env.coin
    #     db = self.db
    #     lines = []
    #
    #     def arg_to_hashX(arg):
    #         try:
    #             script = bytes.fromhex(arg)
    #             lines.append(f'Script: {arg}')
    #             return coin.hashX_from_script(script)
    #         except ValueError:
    #             pass
    #
    #         try:
    #             hashX = coin.address_to_hashX(arg)
    #         except Base58Error as e:
    #             lines.append(e.args[0])
    #             return None
    #         lines.append(f'Address: {arg}')
    #         return hashX
    #
    #     for arg in items:
    #         hashX = arg_to_hashX(arg)
    #         if not hashX:
    #             continue
    #         n = None
    #         history = await db.limited_history(hashX, limit=limit)
    #         for n, (tx_hash, height) in enumerate(history):
    #             lines.append(f'History #{n:,d}: height {height:,d} '
    #                          f'tx_hash {hash_to_hex_str(tx_hash)}')
    #         if n is None:
    #             lines.append('No history found')
    #         n = None
    #         utxos = await db.all_utxos(hashX)
    #         for n, utxo in enumerate(utxos, start=1):
    #             lines.append(f'UTXO #{n:,d}: tx_hash '
    #                          f'{hash_to_hex_str(utxo.tx_hash)} '
    #                          f'tx_pos {utxo.tx_pos:,d} height '
    #                          f'{utxo.height:,d} value {utxo.value:,d}')
    #             if n == limit:
    #                 break
    #         if n is None:
    #             lines.append('No UTXOs found')
    #
    #         balance = sum(utxo.value for utxo in utxos)
    #         lines.append(f'Balance: {coin.decimal_value(balance):,f} '
    #                      f'{coin.SHORTNAME}')
    #
    #     return lines

    # async def rpc_reorg(self, count):
    #     """Force a reorg of the given number of blocks.
    #
    #     count: number of blocks to reorg
    #     """
    #     count = non_negative_integer(count)
    #     if not self.bp.force_chain_reorg(count):
    #         raise RPCError(BAD_REQUEST, 'still catching up with daemon')
    #     return f'scheduled a reorg of {count:,d} blocks'

    # --- External Interface

    async def serve(self, mempool, server_listening_event):
        """Start the RPC server if enabled.  When the event is triggered,
        start TCP and SSL servers."""
        try:
            self.logger.info(f'max session count: {self.env.max_sessions:,d}')
            self.logger.info(f'session timeout: '
                             f'{self.env.session_timeout:,d} seconds')
            self.logger.info(f'max response size {self.env.max_send:,d} bytes')
            if self.env.drop_client is not None:
                self.logger.info(f'drop clients matching: {self.env.drop_client.pattern}')
            # Start notifications; initialize hsub_results
            await mempool.start(self.db.db_height, self)
            await self.start_other()
            await self._start_external_servers()
            server_listening_event.set()
            self.on_available_callback()
            # Peer discovery should start after the external servers
            # because we connect to ourself
            await asyncio.wait([
                self._clear_stale_sessions(),
                self._manage_servers()
            ])
        except Exception as err:
            if not isinstance(err, asyncio.CancelledError):
                log.exception("hub server died")
            raise err
        finally:
            await self._close_servers(list(self.servers.keys()))
            log.info("disconnect %i sessions", len(self.sessions))
            if self.sessions:
                await asyncio.wait([
                    session.close(force_after=1) for session in self.sessions.values()
                ])
            await self.stop_other()

    async def start_other(self):
        self.running = True

    async def stop_other(self):
        self.running = False

    def session_count(self) -> int:
        """The number of connections that we've sent something to."""
        return len(self.sessions)

    async def daemon_request(self, method, *args):
        """Catch a DaemonError and convert it to an RPCError."""
        try:
            return await getattr(self.daemon, method)(*args)
        except DaemonError as e:
            raise RPCError(DAEMON_ERROR, f'daemon error: {e!r}') from None

    async def raw_header(self, height):
        """Return the binary header at the given height."""
        try:
            return await self.db.raw_header(height)
        except IndexError:
            raise RPCError(BAD_REQUEST, f'height {height:,d} '
                                        'out of range') from None

    async def electrum_header(self, height):
        """Return the deserialized header at the given height."""
        electrum_header, _ = await self._electrum_and_raw_headers(height)
        return electrum_header

    async def broadcast_transaction(self, raw_tx):
        hex_hash = await self.daemon.broadcast_transaction(raw_tx)
        self.txs_sent += 1
        return hex_hash

    async def limited_history(self, hashX):
        """A caching layer."""
        if hashX not in self.history_cache:
            # History DoS limit.  Each element of history is about 99
            # bytes when encoded as JSON.  This limits resource usage
            # on bloated history requests, and uses a smaller divisor
            # so large requests are logged before refusing them.
            limit = self.env.max_send // 97
            self.history_cache[hashX] = await self.db.limited_history(hashX, limit=limit)
        return self.history_cache[hashX]

    def _notify_peer(self, peer):
        notify_tasks = [
            session.send_notification('blockchain.peers.subscribe', [peer])
            for session in self.sessions.values() if session.subscribe_peers
        ]
        if notify_tasks:
            self.logger.info(f'notify {len(notify_tasks)} sessions of new peers')
            asyncio.create_task(asyncio.wait(notify_tasks))

    def add_session(self, session):
        self.sessions[id(session)] = session
        self.session_event.set()
        gid = int(session.start_time - self.start_time) // 900
        if self.cur_group.gid != gid:
            self.cur_group = SessionGroup(gid)
        return self.cur_group

    def remove_session(self, session):
        """Remove a session from our sessions list if there."""
        session_id = id(session)
        for hashX in session.hashX_subs:
            sessions = self.hashx_subscriptions_by_session[hashX]
            sessions.remove(session_id)
            if not sessions:
                self.hashx_subscriptions_by_session.pop(hashX)
        self.sessions.pop(session_id)
        self.session_event.set()


class LBRYElectrumX(asyncio.Protocol):
    """A TCP server that handles incoming Electrum connections."""

    PROTOCOL_MIN = PROTOCOL_MIN
    PROTOCOL_MAX = PROTOCOL_MAX
    max_errors = math.inf  # don't disconnect people for errors! let them happen...
    version = HUB_PROTOCOL_VERSION
    cached_server_features = {}

    MAX_CHUNK_SIZE = 40960
    session_counter = itertools.count()
    RESPONSE_TIMES = Histogram("response_time", "Response times", namespace=NAMESPACE,
                               labelnames=("method", "version"), buckets=HISTOGRAM_BUCKETS)
    NOTIFICATION_COUNT = Counter("notification", "Number of notifications sent (for subscriptions)",
                                 namespace=NAMESPACE, labelnames=("method", "version"))
    REQUEST_ERRORS_COUNT = Counter(
        "request_error", "Number of requests that returned errors", namespace=NAMESPACE,
        labelnames=("method", "version")
    )
    RESET_CONNECTIONS = Counter(
        "reset_clients", "Number of reset connections by client version",
        namespace=NAMESPACE, labelnames=("version",)
    )
    max_errors = 10

    def __init__(self, session_manager: SessionManager, kind: str):
        connection = JSONRPCConnection(JSONRPCAutoDetect)
        self.env = session_manager.env
        self.framer = self.default_framer()
        self.loop = asyncio.get_event_loop()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.transport = None
        # Set when a connection is made
        self._address = None
        self._proxy_address = None
        # For logger.debug messages
        self.verbosity = 0
        # Cleared when the send socket is full
        self._can_send = Event()
        self._can_send.set()
        self._pm_task = None
        self._task_group = TaskGroup(self.loop)
        # Force-close a connection if a send doesn't succeed in this time
        self.max_send_delay = 60
        # Statistics.  The RPC object also keeps its own statistics.
        self.start_time = time.perf_counter()
        self.errors = 0
        self.send_count = 0
        self.send_size = 0
        self.last_send = self.start_time
        self.recv_count = 0
        self.recv_size = 0
        self.last_recv = self.start_time
        self.last_packet_received = self.start_time
        self.connection = connection or self.default_connection()
        self.client_version = 'unknown'

        self.logger = logging.getLogger(__name__)
        self.session_manager = session_manager

        self.kind = kind  # 'RPC', 'TCP' etc.
        self.coin = self.env.coin
        self.txs_sent = 0
        self.log_me = False
        self.daemon_request = self.session_manager.daemon_request

        if not LBRYElectrumX.cached_server_features:
            LBRYElectrumX.set_server_features(self.env)
        self.subscribe_headers = False
        self.subscribe_headers_raw = False
        self.subscribe_peers = False
        self.connection.max_response_size = self.env.max_send
        self.hashX_subs = {}
        self.sv_seen = False
        self.protocol_tuple = self.PROTOCOL_MIN
        self.protocol_string = None

        self.daemon = self.session_manager.daemon
        self.db = self.session_manager.db
        self.mempool = self.session_manager.mempool

    def data_received(self, framed_message):
        """Called by asyncio when a message comes in."""
        self.last_packet_received = time.perf_counter()
        if self.verbosity >= 4:
            self.logger.debug(f'Received framed message {framed_message}')
        self.recv_size += len(framed_message)
        self.framer.received_bytes(framed_message)

    def pause_writing(self):
        """Transport calls when the send buffer is full."""
        if not self.is_closing():
            self._can_send.clear()
            self.transport.pause_reading()

    def resume_writing(self):
        """Transport calls when the send buffer has room."""
        if not self._can_send.is_set():
            self._can_send.set()
            self.transport.resume_reading()

    def connection_made(self, transport):
        """Handle an incoming client connection."""
        self.transport = transport
        # This would throw if called on a closed SSL transport.  Fixed
        # in asyncio in Python 3.6.1 and 3.5.4
        peer_address = transport.get_extra_info('peername')
        # If the Socks proxy was used then _address is already set to
        # the remote address
        if self._address:
            self._proxy_address = peer_address
        else:
            self._address = peer_address
        self._pm_task = self.loop.create_task(self._receive_messages())

        self.session_id = next(self.session_counter)
        # context = {'conn_id': f'{self.session_id}'}
        # self.logger = logging.getLogger(__name__)  # util.ConnectionLogger(self.logger, context)
        self.group = self.session_manager.add_session(self)
        self.session_manager.session_count_metric.labels(version=self.client_version).inc()
        # self.logger.info(f'{self.kind} {self.peer_address_str()}, {self.session_manager.session_count():,d} total')

    def connection_lost(self, exc):
        """Handle client disconnection."""
        self.connection.raise_pending_requests(exc)
        self._address = None
        self.transport = None
        self._task_group.cancel()
        if self._pm_task:
            self._pm_task.cancel()
        # Release waiting tasks
        self._can_send.set()

        self.session_manager.remove_session(self)
        self.session_manager.session_count_metric.labels(version=self.client_version).dec()
        msg = ''
        if not self._can_send.is_set():
            msg += ' whilst paused'
        if self.send_size >= 1024 * 1024:
            msg += ('.  Sent {:,d} bytes in {:,d} messages'
                    .format(self.send_size, self.send_count))
        if msg:
            msg = 'disconnected' + msg
            self.logger.info(msg)

    def default_framer(self):
        return NewlineFramer(self.env.max_receive)

    def toggle_logging(self):
        self.log_me = not self.log_me

    def count_pending_items(self):
        return len(self.connection.pending_requests())

    def semaphore(self):
        return Semaphores([self.group.semaphore])

    async def handle_request(self, request):
        """Handle an incoming request.  ElectrumX doesn't receive
        notifications from client sessions.
        """
        self.session_manager.request_count_metric.labels(method=request.method, version=self.client_version).inc()

        if isinstance(request, Request):
            method = request.method
            if method == 'blockchain.block.get_chunk':
                coro = self.block_get_chunk
            elif method == 'blockchain.block.get_header':
                coro = self.block_get_header
            elif method == 'blockchain.block.get_server_height':
                coro = self.get_server_height
            elif method == 'blockchain.scripthash.get_history':
                coro = self.scripthash_get_history
            elif method == 'blockchain.scripthash.get_mempool':
                coro = self.scripthash_get_mempool
            elif method == 'blockchain.scripthash.subscribe':
                coro = self.scripthash_subscribe
            elif method == 'blockchain.scripthash.unsubscribe':
                coro = self.scripthash_unsubscribe
            elif method == 'blockchain.scripthash.get_balance':
                coro = self.scripthash_get_balance
            elif method == 'blockchain.scripthash.listunspent':
                coro = self.scripthash_listunspent
            elif method == 'blockchain.transaction.broadcast':
                coro = self.transaction_broadcast
            elif method == 'blockchain.transaction.get':
                coro = self.transaction_get
            elif method == 'blockchain.transaction.get_batch':
                coro = self.transaction_get_batch
            elif method == 'blockchain.transaction.info':
                coro = self.transaction_info
            elif method == 'blockchain.transaction.get_merkle':
                coro = self.transaction_merkle
            elif method == 'blockchain.transaction.get_height':
                coro = self.transaction_get_height
            elif method == 'blockchain.block.headers':
                coro = self.block_headers
            elif method == 'server.banner':
                coro = self.banner
            elif method == 'server.payment_address':
                coro = self.payment_address
            elif method == 'server.donation_address':
                coro = self.donation_address
            elif method == 'server.features':
                coro = self.server_features_async
            elif method == 'server.peers.subscribe':
                coro = self.peers_subscribe
            elif method == 'server.version':
                coro = self.server_version
            elif method == 'blockchain.claimtrie.search':
                coro = self.claimtrie_search
            elif method == 'blockchain.claimtrie.resolve':
                coro = self.claimtrie_resolve
            elif method == 'blockchain.claimtrie.getclaimbyid':
                coro = self.claimtrie_getclaimbyid
            elif method == 'mempool.get_fee_histogram':
                coro = self.mempool_compact_histogram
            elif method == 'server.ping':
                coro = self.ping
            elif method == 'blockchain.headers.subscribe':
                coro = self.headers_subscribe_False
            elif method == 'blockchain.address.get_history':
                coro = self.address_get_history
            elif method == 'blockchain.address.get_mempool':
                coro = self.address_get_mempool
            elif method == 'blockchain.address.subscribe':
                coro = self.address_subscribe
            elif method == 'blockchain.address.unsubscribe':
                coro = self.address_unsubscribe
            elif method == 'blockchain.address.listunspent':
                coro = self.address_listunspent
            elif method == 'blockchain.address.getbalance':
                coro = self.address_get_balance
            elif method == 'blockchain.estimatefee':
                coro = self.estimatefee
            elif method == 'blockchain.relayfee':
                coro = self.relayfee
            else:
                raise RPCError(JSONRPC.METHOD_NOT_FOUND, f'unknown method "{method}"')
        else:
            raise ValueError
        if isinstance(request.args, dict):
            return await coro(**request.args)
        return await coro(*request.args)

    async def _limited_wait(self, secs):
        try:
            await asyncio.wait_for(self._can_send.wait(), secs)
        except asyncio.TimeoutError:
            self.abort()
            raise asyncio.TimeoutError(f'task timed out after {secs}s')

    async def _send_message(self, message):
        if not self._can_send.is_set():
            await self._limited_wait(self.max_send_delay)
        if not self.is_closing():
            framed_message = self.framer.frame(message)
            self.send_size += len(framed_message)
            self.send_count += 1
            self.last_send = time.perf_counter()
            if self.verbosity >= 4:
                self.logger.debug(f'Sending framed message {framed_message}')
            self.transport.write(framed_message)

    def _bump_errors(self):
        self.errors += 1
        if self.errors >= self.max_errors:
            # Don't await self.close() because that is self-cancelling
            self._close()

    def _close(self):
        if self.transport:
            self.transport.close()

    def peer_address(self):
        """Returns the peer's address (Python networking address), or None if
        no connection or an error.

        This is the result of socket.getpeername() when the connection
        was made.
        """
        return self._address

    def is_closing(self):
        """Return True if the connection is closing."""
        return not self.transport or self.transport.is_closing()

    def abort(self):
        """Forcefully close the connection."""
        if self.transport:
            self.transport.abort()

    # TODO: replace with synchronous_close
    async def close(self, *, force_after=30):
        """Close the connection and return when closed."""
        self._close()
        if self._pm_task:
            with suppress(asyncio.CancelledError):
                await asyncio.wait([self._pm_task], timeout=force_after)
                self.abort()
                await self._pm_task

    def synchronous_close(self):
        self._close()
        if self._pm_task and not self._pm_task.done():
            self._pm_task.cancel()

    async def _receive_messages(self):
        while not self.is_closing():
            try:
                message = await self.framer.receive_message()
            except MemoryError:
                self.logger.warning('received oversized message from %s:%s, dropping connection',
                                    self._address[0], self._address[1])
                self.RESET_CONNECTIONS.labels(version=self.client_version).inc()
                self._close()
                return

            self.last_recv = time.perf_counter()
            self.recv_count += 1

            try:
                requests = self.connection.receive_message(message)
            except ProtocolError as e:
                self.logger.debug(f'{e}')
                if e.error_message:
                    await self._send_message(e.error_message)
                if e.code == JSONRPC.PARSE_ERROR:
                    self.max_errors = 0
                self._bump_errors()
            else:
                for request in requests:
                    await self._task_group.add(self._handle_request(request))

    async def _handle_request(self, request):
        start = time.perf_counter()
        try:
            result = await self.handle_request(request)
        except (ProtocolError, RPCError) as e:
            result = e
        except asyncio.CancelledError:
            raise
        except Exception:
            reqstr = str(request)
            self.logger.exception(f'exception handling {reqstr[:16_000]}')
            result = RPCError(JSONRPC.INTERNAL_ERROR,
                              'internal server error')
        if isinstance(request, Request):
            message = request.send_result(result)
            self.RESPONSE_TIMES.labels(
                method=request.method,
                version=self.client_version
            ).observe(time.perf_counter() - start)
            if message:
                await self._send_message(message)
        if isinstance(result, Exception):
            self._bump_errors()
            self.REQUEST_ERRORS_COUNT.labels(
                method=request.method,
                version=self.client_version
            ).inc()

    # External API
    def default_connection(self):
        """Return a default connection if the user provides none."""
        return JSONRPCConnection(JSONRPCv2)

    async def send_request(self, method, args=()):
        """Send an RPC request over the network."""
        if self.is_closing():
            raise asyncio.TimeoutError("Trying to send request on a recently dropped connection.")
        message, event = self.connection.send_request(Request(method, args))
        await self._send_message(message)
        await event.wait()
        result = event.result
        if isinstance(result, Exception):
            raise result
        return result

    async def send_notification(self, method, args=()) -> bool:
        """Send an RPC notification over the network."""
        message = self.connection.send_notification(Notification(method, args))
        self.NOTIFICATION_COUNT.labels(method=method, version=self.client_version).inc()
        try:
            await self._send_message(message)
            return True
        except asyncio.TimeoutError:
            self.logger.info(f"timeout sending address notification to {self._address[0]}:{self._address[1]}")
            self.abort()
            return False

    async def send_notifications(self, notifications) -> bool:
        """Send an RPC notification over the network."""
        message, _ = self.connection.send_batch(notifications)
        try:
            await self._send_message(message)
            return True
        except asyncio.TimeoutError:
            self.logger.info(f"timeout sending address notification to {self._address[0]}:{self._address[1]}")
            self.abort()
            return False

    def send_batch(self, raise_errors=False):
        """Return a BatchRequest.  Intended to be used like so:

           async with session.send_batch() as batch:
               batch.add_request("method1")
               batch.add_request("sum", (x, y))
               batch.add_notification("updated")

           for result in batch.results:
              ...

        Note that in some circumstances exceptions can be raised; see
        BatchRequest doc string.
        """
        return BatchRequest(self, raise_errors)

    @classmethod
    def protocol_min_max_strings(cls):
        return [version_string(ver)
                for ver in (cls.PROTOCOL_MIN, cls.PROTOCOL_MAX)]

    @classmethod
    def set_server_features(cls, env):
        """Return the server features dictionary."""
        min_str, max_str = cls.protocol_min_max_strings()
        cls.cached_server_features.update({
            'hosts': {},
            'pruning': None,
            'server_version': cls.version,
            'protocol_min': min_str,
            'protocol_max': max_str,
            'genesis_hash': env.coin.GENESIS_HASH,
            'description': env.description,
            'payment_address': env.payment_address,
            'donation_address': env.donation_address,
            'daily_fee': env.daily_fee,
            'hash_function': 'sha256',
            'trending_algorithm': 'fast_ar'
        })

    async def server_features_async(self):
        return self.cached_server_features

    @classmethod
    def server_version_args(cls):
        """The arguments to a server.version RPC call to a peer."""
        return [cls.version, cls.protocol_min_max_strings()]

    def protocol_version_string(self):
        return version_string(self.protocol_tuple)

    def sub_count(self):
        return len(self.hashX_subs)

    async def get_hashX_status(self, hashX: bytes):
        self.session_manager.db.last_flush
        if self.env.index_address_status:
            loop = self.loop
            return await loop.run_in_executor(None, self.db.get_hashX_status, hashX)
        history = ''.join(
            f"{tx_hash[::-1].hex()}:{height:d}:"
            for tx_hash, height in await self.db.limited_history(hashX, limit=None)
        ) + self.mempool.mempool_history(hashX)
        if not history:
            return
        status = sha256(history.encode())
        return status.hex()

    async def send_history_notifications(self, *hashXes: typing.Iterable[bytes]):
        notifications = []
        for hashX in hashXes:
            alias = self.hashX_subs[hashX]
            if len(alias) == 64:
                method = 'blockchain.scripthash.subscribe'
            else:
                method = 'blockchain.address.subscribe'
            start = time.perf_counter()
            status = await self.get_hashX_status(hashX)
            duration = time.perf_counter() - start
            self.session_manager.address_history_metric.observe(duration)
            notifications.append((method, (alias, status)))
            if duration > 30:
                self.logger.warning("slow history notification (%s) for '%s'", duration, alias)

        start = time.perf_counter()
        self.session_manager.notifications_in_flight_metric.inc()
        for method, args in notifications:
            self.NOTIFICATION_COUNT.labels(method=method, version=self.client_version).inc()
        try:
            await self.send_notifications(
                Batch([Notification(method, (alias, status)) for (method, (alias, status)) in notifications])
            )
            self.session_manager.notifications_sent_metric.observe(time.perf_counter() - start)
        finally:
            self.session_manager.notifications_in_flight_metric.dec()

    # def get_metrics_or_placeholder_for_api(self, query_name):
    #     """ Do not hold on to a reference to the metrics
    #         returned by this method past an `await` or
    #         you may be working with a stale metrics object.
    #     """
    #     if self.env.track_metrics:
    #         # return self.session_manager.metrics.for_api(query_name)
    #     else:
    #         return APICallMetrics(query_name)


    # async def run_and_cache_query(self, query_name, kwargs):
    #     start = time.perf_counter()
    #     if isinstance(kwargs, dict):
    #         kwargs['release_time'] = format_release_time(kwargs.get('release_time'))
    #     try:
    #         self.session_manager.pending_query_metric.inc()
    #         return await self.db.search_index.session_query(query_name, kwargs)
    #     except ConnectionTimeout:
    #         self.session_manager.interrupt_count_metric.inc()
    #         raise RPCError(JSONRPC.QUERY_TIMEOUT, 'query timed out')
    #     finally:
    #         self.session_manager.pending_query_metric.dec()
    #         self.session_manager.executor_time_metric.observe(time.perf_counter() - start)

    async def mempool_compact_histogram(self):  # TODO: fix this
        return [] #self.mempool.compact_fee_histogram()

    async def claimtrie_search(self, **kwargs):
        start = time.perf_counter()
        if 'release_time' in kwargs:
            release_time = kwargs.pop('release_time')
            release_times = release_time if isinstance(release_time, list) else [release_time]
            try:
                kwargs['release_time'] = [format_release_time(release_time) for release_time in release_times]
            except ValueError:
                pass
        try:
            self.session_manager.pending_query_metric.inc()
            if 'channel' in kwargs:
                channel_url = kwargs.pop('channel')
                _, channel_claim, _, _ = await self.db.resolve(channel_url)
                if not channel_claim or isinstance(channel_claim, (ResolveCensoredError, LookupError, ValueError)):
                    return Outputs.to_base64([], [])
                kwargs['channel_id'] = channel_claim.claim_hash.hex()
            return await self.session_manager.search_index.cached_search(kwargs)
        except ConnectionTimeout:
            self.session_manager.interrupt_count_metric.inc()
            raise RPCError(JSONRPC.QUERY_TIMEOUT, 'query timed out')
        except TooManyClaimSearchParametersError as err:
            await asyncio.sleep(2)
            self.logger.warning("Got an invalid query from %s, for %s with more than %d elements.",
                                self.peer_address()[0], err.key, err.limit)
            return RPCError(1, str(err))
        finally:
            self.session_manager.pending_query_metric.dec()
            self.session_manager.executor_time_metric.observe(time.perf_counter() - start)

    async def _cached_resolve_url(self, url):
        if url not in self.session_manager.resolve_cache:
            self.session_manager.resolve_cache[url] = await self.loop.run_in_executor(self.db._executor, self.db._resolve, url)
        return self.session_manager.resolve_cache[url]

    async def claimtrie_resolve(self, *urls) -> str:
        sorted_urls = tuple(sorted(urls))
        self.session_manager.urls_to_resolve_count_metric.inc(len(sorted_urls))
        try:
            if sorted_urls in self.session_manager.resolve_outputs_cache:
                return self.session_manager.resolve_outputs_cache[sorted_urls]
            rows, extra = [], []
            for url in urls:
                if url not in self.session_manager.resolve_cache:
                    self.session_manager.resolve_cache[url] = await self._cached_resolve_url(url)
                stream, channel, repost, reposted_channel = self.session_manager.resolve_cache[url]
                if isinstance(channel, ResolveCensoredError):
                    rows.append(channel)
                    extra.append(channel.censor_row)
                elif isinstance(stream, ResolveCensoredError):
                    rows.append(stream)
                    extra.append(stream.censor_row)
                elif isinstance(repost, ResolveCensoredError):
                    rows.append(repost)
                    extra.append(repost.censor_row)
                elif isinstance(reposted_channel, ResolveCensoredError):
                    rows.append(reposted_channel)
                    extra.append(reposted_channel.censor_row)
                elif channel and not stream:
                    rows.append(channel)
                    # print("resolved channel", channel.name.decode())
                    if repost:
                        extra.append(repost)
                    if reposted_channel:
                        extra.append(reposted_channel)
                elif stream:
                    # print("resolved stream", stream.name.decode())
                    rows.append(stream)
                    if channel:
                        # print("and channel", channel.name.decode())
                        extra.append(channel)
                    if repost:
                        extra.append(repost)
                    if reposted_channel:
                        extra.append(reposted_channel)
                await asyncio.sleep(0)
            self.session_manager.resolve_outputs_cache[sorted_urls] = result = await self.loop.run_in_executor(
                None, Outputs.to_base64, rows, extra
            )
            return result
        finally:
            self.session_manager.resolved_url_count_metric.inc(len(sorted_urls))

    async def get_server_height(self):
        return self.db.db_height

    async def transaction_get_height(self, tx_hash):
        self.assert_tx_hash(tx_hash)

        def get_height():
            v = self.db.prefix_db.tx_num.get(tx_hash)
            if v:
                return bisect_right(self.db.tx_counts, v.tx_num)
            return self.mempool.get_mempool_height(tx_hash)

        return await asyncio.get_event_loop().run_in_executor(self.db._executor, get_height)

    def _getclaimbyid(self, claim_id: str):
        rows = []
        extra = []
        claim_hash = bytes.fromhex(claim_id)
        stream = self.db._fs_get_claim_by_hash(claim_hash)
        rows.append(stream or LookupError(f"Could not find claim at {claim_id}"))
        if stream and stream.channel_hash:
            channel = self.db._fs_get_claim_by_hash(stream.channel_hash)
            extra.append(channel or LookupError(f"Could not find channel at {stream.channel_hash.hex()}"))
        if stream and stream.reposted_claim_hash:
            repost = self.db._fs_get_claim_by_hash(stream.reposted_claim_hash)
            if repost:
                extra.append(repost)
        return Outputs.to_base64(rows, extra, 0, None, None)

    async def claimtrie_getclaimbyid(self, claim_id):
        assert len(claim_id) == 40, f"{len(claim_id)}: '{claim_id}'"
        return await self.loop.run_in_executor(None, self._getclaimbyid, claim_id)

    def assert_tx_hash(self, value):
        '''Raise an RPCError if the value is not a valid transaction
        hash.'''
        try:
            if len(bytes.fromhex(value)) == 32:
                return
        except Exception:
            pass
        raise RPCError(1, f'{value} should be a transaction hash')

    async def subscribe_headers_result(self):
        """The result of a header subscription or notification."""
        return self.session_manager.hsub_results[self.subscribe_headers_raw]

    async def _headers_subscribe(self, raw):
        """Subscribe to get headers of new blocks."""
        self.subscribe_headers_raw = assert_boolean(raw)
        self.subscribe_headers = True
        return await self.subscribe_headers_result()

    async def headers_subscribe(self):
        """Subscribe to get raw headers of new blocks."""
        return await self._headers_subscribe(True)

    async def headers_subscribe_True(self, raw=True):
        """Subscribe to get headers of new blocks."""
        return await self._headers_subscribe(raw)

    async def headers_subscribe_False(self, raw=False):
        """Subscribe to get headers of new blocks."""
        return await self._headers_subscribe(raw)

    async def add_peer(self, features):
        """Add a peer (but only if the peer resolves to the source)."""
        return await self.peer_mgr.on_add_peer(features, self.peer_address())

    async def peers_subscribe(self):
        """Return the server peers as a list of (ip, host, details) tuples."""
        self.subscribe_peers = True
        return self.env.peer_hubs

    async def address_status(self, hashX):
        """Returns an address status.

        Status is a hex string, but must be None if there is no history.
        """
        # Note history is ordered and mempool unordered in electrum-server
        # For mempool, height is -1 if it has unconfirmed inputs, otherwise 0
        status = await self.get_hashX_status(hashX)
        return status

    async def hashX_listunspent(self, hashX: bytes):
        """Return the list of UTXOs of a script hash, including mempool
        effects."""
        utxos = await self.db.all_utxos(hashX)
        utxos = sorted(utxos)
        utxos.extend(self.mempool.unordered_UTXOs(hashX))
        spends = self.mempool.potential_spends(hashX)

        return [{'tx_hash': hash_to_hex_str(utxo.tx_hash),
                 'tx_pos': utxo.tx_pos,
                 'height': utxo.height, 'value': utxo.value}
                for utxo in utxos
                if (utxo.tx_hash, utxo.tx_pos) not in spends]

    async def address_listunspent(self, address: str):
        return await self.hashX_listunspent(self.address_to_hashX(address))

    async def hashX_subscribe(self, hashX, alias):
        self.hashX_subs[hashX] = alias
        self.session_manager.hashx_subscriptions_by_session[hashX].add(id(self))
        return await self.address_status(hashX)

    async def hashX_unsubscribe(self, hashX, alias):
        sessions = self.session_manager.hashx_subscriptions_by_session[hashX]
        try:
            sessions.remove(id(self))
        except KeyError:
            pass
        if not sessions:
            self.hashX_subs.pop(hashX, None)

    def address_to_hashX(self, address):
        try:
            return self.coin.address_to_hashX(address)
        except Exception:
            pass
        raise RPCError(BAD_REQUEST, f'{address} is not a valid address')

    async def address_get_balance(self, address):
        """Return the confirmed and unconfirmed balance of an address."""
        hashX = self.address_to_hashX(address)
        return await self.get_balance(hashX)

    async def address_get_history(self, address):
        """Return the confirmed and unconfirmed history of an address."""
        hashX = self.address_to_hashX(address)
        return await self.confirmed_and_unconfirmed_history(hashX)

    async def address_get_mempool(self, address):
        """Return the mempool transactions touching an address."""
        hashX = self.address_to_hashX(address)
        return self.unconfirmed_history(hashX)

    # async def address_listunspent(self, address):
    #     """Return the list of UTXOs of an address."""
    #     hashX = self.address_to_hashX(address)
    #     return await self.hashX_listunspent(hashX)

    async def address_subscribe(self, *addresses):
        """Subscribe to an address.

        address: the address to subscribe to"""
        if len(addresses) > 1000:
            raise RPCError(BAD_REQUEST, f'too many addresses in subscription request: {len(addresses)}')
        results = []
        for address in addresses:
            results.append(await self.hashX_subscribe(self.address_to_hashX(address), address))
            await asyncio.sleep(0)
        return results

    async def address_unsubscribe(self, address):
        """Unsubscribe an address.

        address: the address to unsubscribe"""
        hashX = self.address_to_hashX(address)
        return await self.hashX_unsubscribe(hashX, address)

    async def get_balance(self, hashX):
        utxos = await self.db.all_utxos(hashX)
        confirmed = sum(utxo.value for utxo in utxos)
        unconfirmed = self.mempool.balance_delta(hashX)
        return {'confirmed': confirmed, 'unconfirmed': unconfirmed}

    async def scripthash_get_balance(self, scripthash):
        """Return the confirmed and unconfirmed balance of a scripthash."""
        hashX = scripthash_to_hashX(scripthash)
        return await self.get_balance(hashX)

    def unconfirmed_history(self, hashX):
        # Note unconfirmed history is unordered in electrum-server
        # height is -1 if it has unconfirmed inputs, otherwise 0
        return [{'tx_hash': hash_to_hex_str(tx.hash),
                 'height': -tx.has_unconfirmed_inputs,
                 'fee': tx.fee}
                for tx in self.mempool.transaction_summaries(hashX)]

    async def confirmed_and_unconfirmed_history(self, hashX):
        # Note history is ordered but unconfirmed is unordered in e-s
        history = await self.session_manager.limited_history(hashX)
        conf = [{'tx_hash': hash_to_hex_str(tx_hash), 'height': height}
                for tx_hash, height in history]
        return conf + self.unconfirmed_history(hashX)

    async def scripthash_get_history(self, scripthash):
        """Return the confirmed and unconfirmed history of a scripthash."""
        hashX = scripthash_to_hashX(scripthash)
        return await self.confirmed_and_unconfirmed_history(hashX)

    async def scripthash_get_mempool(self, scripthash):
        """Return the mempool transactions touching a scripthash."""
        hashX = scripthash_to_hashX(scripthash)
        return self.unconfirmed_history(hashX)

    async def scripthash_listunspent(self, scripthash):
        """Return the list of UTXOs of a scripthash."""
        hashX = scripthash_to_hashX(scripthash)
        return await self.hashX_listunspent(hashX)

    async def scripthash_subscribe(self, scripthash):
        """Subscribe to a script hash.

        scripthash: the SHA256 hash of the script to subscribe to"""
        hashX = scripthash_to_hashX(scripthash)
        return await self.hashX_subscribe(hashX, scripthash)

    async def scripthash_unsubscribe(self, scripthash: str):
        hashX = scripthash_to_hashX(scripthash)
        return await self.hashX_unsubscribe(hashX, scripthash)

    async def _merkle_proof(self, cp_height, height):
        max_height = self.db.db_height
        if not height <= cp_height <= max_height:
            raise RPCError(BAD_REQUEST,
                           f'require header height {height:,d} <= '
                           f'cp_height {cp_height:,d} <= '
                           f'chain height {max_height:,d}')
        branch, root = await self.db.header_branch_and_root(cp_height + 1, height)
        return {
            'branch': [hash_to_hex_str(elt) for elt in branch],
            'root': hash_to_hex_str(root),
        }

    async def block_headers(self, start_height, count, cp_height=0, b64=False):
        """Return count concatenated block headers as hex for the main chain;
        starting at start_height.

        start_height and count must be non-negative integers.  At most
        MAX_CHUNK_SIZE headers will be returned.
        """
        start_height = non_negative_integer(start_height)
        count = non_negative_integer(count)
        cp_height = non_negative_integer(cp_height)

        max_size = self.MAX_CHUNK_SIZE
        count = min(count, max_size)
        headers, count = await self.db.read_headers(start_height, count)

        if b64:
            headers = self.db.encode_headers(start_height, count, headers)
        else:
            headers = headers.hex()
        result = {
            'base64' if b64 else 'hex': headers,
            'count': count,
            'max': max_size
        }
        if count and cp_height:
            last_height = start_height + count - 1
            result.update(await self._merkle_proof(cp_height, last_height))
        return result

    async def block_get_chunk(self, index):
        """Return a chunk of block headers as a hexadecimal string.

        index: the chunk index"""
        index = non_negative_integer(index)
        size = self.coin.CHUNK_SIZE
        start_height = index * size
        headers, _ = await self.db.read_headers(start_height, size)
        return headers.hex()

    async def block_get_header(self, height):
        """The deserialized header at a given height.

        height: the header's height"""
        height = non_negative_integer(height)
        return await self.session_manager.electrum_header(height)

    # def is_tor(self):
    #     """Try to detect if the connection is to a tor hidden service we are
    #     running."""
    #     peername = self.peer_mgr.proxy_peername()
    #     if not peername:
    #         return False
    #     peer_address = self.peer_address()
    #     return peer_address and peer_address[0] == peername[0]

    async def replaced_banner(self, banner):
        network_info = await self.daemon_request('getnetworkinfo')
        ni_version = network_info['version']
        major, minor = divmod(ni_version, 1000000)
        minor, revision = divmod(minor, 10000)
        revision //= 100
        daemon_version = f'{major:d}.{minor:d}.{revision:d}'
        for pair in [
            ('$SERVER_VERSION', self.version),
            ('$DAEMON_VERSION', daemon_version),
            ('$DAEMON_SUBVERSION', network_info['subversion']),
            ('$PAYMENT_ADDRESS', self.env.payment_address),
            ('$DONATION_ADDRESS', self.env.donation_address),
        ]:
            banner = banner.replace(*pair)
        return banner

    async def payment_address(self):
        """Return the payment address as a string, empty if there is none."""
        return self.env.payment_address

    async def donation_address(self):
        """Return the donation address as a string, empty if there is none."""
        return self.env.donation_address

    async def banner(self):
        """Return the server banner text."""
        banner = f'You are connected to an {self.version} server.'
        banner_file = self.env.banner_file
        if banner_file:
            try:
                with codecs.open(banner_file, 'r', 'utf-8') as f:
                    banner = f.read()
            except Exception as e:
                self.logger.error(f'reading banner file {banner_file}: {e!r}')
            else:
                banner = await self.replaced_banner(banner)

        return banner

    async def relayfee(self):
        """The minimum fee a low-priority tx must pay in order to be accepted
        to the daemon's memory pool."""
        return await self.daemon_request('relayfee')

    async def estimatefee(self, number):
        """The estimated transaction fee per kilobyte to be paid for a
        transaction to be included within a certain number of blocks.

        number: the number of blocks
        """
        number = non_negative_integer(number)
        return await self.daemon_request('estimatefee', number)

    async def ping(self):
        """Serves as a connection keep-alive mechanism and for the client to
        confirm the server is still responding.
        """
        return None

    async def server_version(self, client_name='', client_version=None):
        """Returns the server version as a string.

        client_name: a string identifying the client
        client_version: the protocol version spoken by the client
        """
        if self.protocol_string is not None:
            return self.version, self.protocol_string
        if self.sv_seen and self.protocol_tuple >= (1, 4):
            raise RPCError(BAD_REQUEST, f'server.version already sent')
        self.sv_seen = True

        if client_name:
            client_name = str(client_name)
            if self.env.drop_client is not None and \
                    self.env.drop_client.match(client_name):
                self.close_after_send = True
                raise RPCError(BAD_REQUEST, f'unsupported client: {client_name}')
            if self.client_version != client_name[:17]:
                self.session_manager.session_count_metric.labels(version=self.client_version).dec()
                self.client_version = client_name[:17]
                self.session_manager.session_count_metric.labels(version=self.client_version).inc()
        self.session_manager.client_version_metric.labels(version=self.client_version).inc()

        # Find the highest common protocol version.  Disconnect if
        # that protocol version in unsupported.
        ptuple, client_min = protocol_version(client_version, self.PROTOCOL_MIN, self.PROTOCOL_MAX)
        if ptuple is None:
            ptuple, client_min = protocol_version(client_version, (1, 1, 0), (1, 4, 0))
            if ptuple is None:
                self.close_after_send = True
                raise RPCError(BAD_REQUEST, f'unsupported protocol version: {client_version}')

        self.protocol_tuple = ptuple
        self.protocol_string = version_string(ptuple)
        return self.version, self.protocol_string

    async def transaction_broadcast(self, raw_tx):
        """Broadcast a raw transaction to the network.

        raw_tx: the raw transaction as a hexadecimal string"""
        # This returns errors as JSON RPC errors, as is natural
        try:
            hex_hash = await self.session_manager.broadcast_transaction(raw_tx)
            self.txs_sent += 1
            # self.mempool.wakeup.set()
            # await asyncio.sleep(0.5)
            self.logger.info(f'sent tx: {hex_hash}')
            return hex_hash
        except DaemonError as e:
            error, = e.args
            message = error['message']
            self.logger.info(f'error sending transaction: {message}')
            raise RPCError(BAD_REQUEST, 'the transaction was rejected by '
                                        f'network rules.\n\n{message}\n[{raw_tx}]')

    async def transaction_info(self, tx_hash: str):
        tx_info = (await self.transaction_get_batch(tx_hash))[tx_hash]
        if tx_info[0] is None and tx_info[1]['block_height'] == -1:
            return RPCError(BAD_REQUEST, "No such mempool or blockchain transaction.")
        return tx_info

    async def transaction_get_batch(self, *tx_hashes):
        self.session_manager.tx_request_count_metric.inc(len(tx_hashes))
        if len(tx_hashes) > 100:
            raise RPCError(BAD_REQUEST, f'too many tx hashes in request: {len(tx_hashes)}')
        for tx_hash in tx_hashes:
            assert_tx_hash(tx_hash)
        batch_result = await self.db.get_transactions_and_merkles(tx_hashes)
        self.session_manager.tx_replied_count_metric.inc(len(tx_hashes))
        return batch_result

    async def transaction_get(self, txid: str, verbose=False):
        """Return the serialized raw transaction given its hash

        txid: the transaction hash as a hexadecimal string
        verbose: passed on to the daemon
        """
        assert_tx_hash(txid)
        verbose = bool(verbose)
        tx_hash_bytes = bytes.fromhex(txid)[::-1]

        raw_tx = await asyncio.get_event_loop().run_in_executor(None, self.db.get_raw_tx, tx_hash_bytes)
        if raw_tx:
            if not verbose:
                return raw_tx.hex()
            return self.coin.transaction(raw_tx).as_dict(self.coin)
        return RPCError("No such mempool or blockchain transaction.")

    def _get_merkle_branch(self, tx_hashes, tx_pos):
        """Return a merkle branch to a transaction.

        tx_hashes: ordered list of hex strings of tx hashes in a block
        tx_pos: index of transaction in tx_hashes to create branch for
        """
        hashes = [hex_str_to_hash(_hash) for _hash in tx_hashes]
        branch, root = self.db.merkle.branch_and_root(hashes, tx_pos)
        branch = [hash_to_hex_str(_hash) for _hash in branch]
        return branch

    async def transaction_merkle(self, tx_hash, height):
        """Return the markle branch to a confirmed transaction given its hash
        and height.

        tx_hash: the transaction hash as a hexadecimal string
        height: the height of the block it is in
        """
        assert_tx_hash(tx_hash)
        result = await self.transaction_get_batch(tx_hash)
        if tx_hash not in result or result[tx_hash][1]['block_height'] <= 0:
            raise RPCError(BAD_REQUEST, f'tx hash {tx_hash} not in '
                                        f'block at height {height:,d}')
        return result[tx_hash][1]


def get_from_possible_keys(dictionary, *keys):
    for key in keys:
        if key in dictionary:
            return dictionary[key]


def format_release_time(release_time):
    # round release time to 1000 so it caches better
    # also set a default so we dont show claims in the future
    def roundup_time(number, factor=360):
        return int(1 + int(number / factor)) * factor
    if isinstance(release_time, str) and len(release_time) > 0:
        time_digits = ''.join(filter(str.isdigit, release_time))
        time_prefix = release_time[:-len(time_digits)]
        return time_prefix + str(roundup_time(int(time_digits)))
    elif isinstance(release_time, int):
        return roundup_time(release_time)
