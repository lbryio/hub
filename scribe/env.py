import os
import re
import resource
import logging
from collections import namedtuple
from ipaddress import ip_address
from scribe.blockchain.network import LBCMainNet, LBCTestNet, LBCRegTest


NetIdentity = namedtuple('NetIdentity', 'host tcp_port ssl_port nick_suffix')


SEGMENT_REGEX = re.compile("(?!-)[A-Z_\\d-]{1,63}(?<!-)$", re.IGNORECASE)


def is_valid_hostname(hostname):
    if len(hostname) > 255:
        return False
    # strip exactly one dot from the right, if present
    if hostname and hostname[-1] == ".":
        hostname = hostname[:-1]
    return all(SEGMENT_REGEX.match(x) for x in hostname.split("."))


class Env:

    # Peer discovery
    PD_OFF, PD_SELF, PD_ON = range(3)

    class Error(Exception):
        pass

    def __init__(self, db_dir=None, daemon_url=None, host=None, elastic_host=None, elastic_port=None,
                 max_query_workers=None, chain=None, es_index_prefix=None, reorg_limit=None,
                 tcp_port=None, udp_port=None, prometheus_port=None, banner_file=None,
                 allow_lan_udp=None, cache_all_tx_hashes=None, cache_all_claim_txos=None, country=None,
                 payment_address=None, donation_address=None, max_send=None, max_receive=None, max_sessions=None,
                 session_timeout=None, drop_client=None, description=None, daily_fee=None,
                 database_query_timeout=None, db_max_open_files=64, elastic_notifier_host=None,
                 elastic_notifier_port=None, blocking_channel_ids=None, filtering_channel_ids=None, peer_hubs=None,
                 peer_announce=None):
        self.logger = logging.getLogger(__name__)
        self.db_dir = db_dir if db_dir is not None else self.required('DB_DIRECTORY')
        self.daemon_url = daemon_url if daemon_url is not None else self.required('DAEMON_URL')
        self.db_max_open_files = db_max_open_files
        self.host = host if host is not None else self.default('HOST', 'localhost')
        self.elastic_host = elastic_host if elastic_host is not None else self.default('ELASTIC_HOST', 'localhost')
        self.elastic_port = elastic_port if elastic_port is not None else self.integer('ELASTIC_PORT', 9200)
        self.elastic_notifier_host = elastic_notifier_host if elastic_notifier_host is not None else self.default('ELASTIC_NOTIFIER_HOST', 'localhost')
        self.elastic_notifier_port = elastic_notifier_port if elastic_notifier_port is not None else self.integer('ELASTIC_NOTIFIER_PORT', 19080)

        self.obsolete(['UTXO_MB', 'HIST_MB', 'NETWORK'])
        self.max_query_workers = max_query_workers if max_query_workers is not None else self.integer('MAX_QUERY_WORKERS', 4)
        if chain == 'mainnet':
            self.coin = LBCMainNet
        elif chain == 'testnet':
            self.coin = LBCTestNet
        else:
            self.coin = LBCRegTest
        self.es_index_prefix = es_index_prefix if es_index_prefix is not None else self.default('ES_INDEX_PREFIX', '')
        self.reorg_limit = reorg_limit if reorg_limit is not None else self.integer('REORG_LIMIT', self.coin.REORG_LIMIT)
        # Server stuff
        self.tcp_port = tcp_port if tcp_port is not None else self.integer('TCP_PORT', None)
        self.udp_port = udp_port if udp_port is not None else self.integer('UDP_PORT', self.tcp_port)
        self.prometheus_port = prometheus_port if prometheus_port is not None else self.integer('PROMETHEUS_PORT', 0)
        self.banner_file = banner_file if banner_file is not None else self.default('BANNER_FILE', None)
        self.allow_lan_udp = allow_lan_udp if allow_lan_udp is not None else self.boolean('ALLOW_LAN_UDP', False)
        self.cache_all_tx_hashes = cache_all_tx_hashes if cache_all_tx_hashes is not None else self.boolean('CACHE_ALL_TX_HASHES', False)
        self.cache_all_claim_txos = cache_all_claim_txos if cache_all_claim_txos is not None else self.boolean('CACHE_ALL_CLAIM_TXOS', False)
        self.country = country if country is not None else self.default('COUNTRY', 'US')
        # Peer discovery
        self.peer_discovery = self.peer_discovery_enum()
        self.peer_announce = peer_announce if peer_announce is not None else self.boolean('PEER_ANNOUNCE', True)
        if peer_hubs is not None:
            self.peer_hubs = [p.strip("") for p in peer_hubs.split(",")]
        else:
            self.peer_hubs = self.extract_peer_hubs()
        # The electrum client takes the empty string as unspecified
        self.payment_address = payment_address if payment_address is not None else self.default('PAYMENT_ADDRESS', '')
        self.donation_address = donation_address if donation_address is not None else self.default('DONATION_ADDRESS', '')
        # Server limits to help prevent DoS
        self.max_send = max_send if max_send is not None else self.integer('MAX_SEND', 1000000)
        self.max_receive = max_receive if max_receive is not None else self.integer('MAX_RECEIVE', 1000000)
        self.max_sessions = max_sessions if max_sessions is not None else self.sane_max_sessions()
        self.session_timeout = session_timeout if session_timeout is not None else self.integer('SESSION_TIMEOUT', 600)
        self.drop_client = drop_client if drop_client is not None else self.custom("DROP_CLIENT", None, re.compile)
        self.description = description if description is not None else self.default('DESCRIPTION', '')
        self.daily_fee = daily_fee if daily_fee is not None else self.string_amount('DAILY_FEE', '0')
        self.database_query_timeout = database_query_timeout if database_query_timeout is not None else \
            (float(self.integer('QUERY_TIMEOUT_MS', 10000)) / 1000.0)

        # Filtering / Blocking
        self.blocking_channel_ids = blocking_channel_ids if blocking_channel_ids is not None else self.default('BLOCKING_CHANNEL_IDS', '').split(' ')
        self.filtering_channel_ids = filtering_channel_ids if filtering_channel_ids is not None else self.default('FILTERING_CHANNEL_IDS', '').split(' ')

    @classmethod
    def default(cls, envvar, default):
        return os.environ.get(envvar, default)

    @classmethod
    def boolean(cls, envvar, default):
        default = 'Yes' if default else ''
        return bool(cls.default(envvar, default).strip())

    @classmethod
    def required(cls, envvar):
        value = os.environ.get(envvar)
        if value is None:
            raise cls.Error(f'required envvar {envvar} not set')
        return value

    @classmethod
    def string_amount(cls, envvar, default):
        value = os.environ.get(envvar, default)
        amount_pattern = re.compile("[0-9]{0,10}(\.[0-9]{1,8})?")
        if len(value) > 0 and not amount_pattern.fullmatch(value):
            raise cls.Error(f'{value} is not a valid amount for {envvar}')
        return value

    @classmethod
    def integer(cls, envvar, default):
        value = os.environ.get(envvar)
        if value is None:
            return default
        try:
            return int(value)
        except Exception:
            raise cls.Error(f'cannot convert envvar {envvar} value {value} to an integer')

    @classmethod
    def custom(cls, envvar, default, parse):
        value = os.environ.get(envvar)
        if value is None:
            return default
        try:
            return parse(value)
        except Exception as e:
            raise cls.Error(f'cannot parse envvar {envvar} value {value}') from e

    @classmethod
    def obsolete(cls, envvars):
        bad = [envvar for envvar in envvars if os.environ.get(envvar)]
        if bad:
            raise cls.Error(f'remove obsolete os.environment variables {bad}')

    def cs_host(self):
        """Returns the 'host' argument to pass to asyncio's create_server
        call.  The result can be a single host name string, a list of
        host name strings, or an empty string to bind to all interfaces.

        If rpc is True the host to use for the RPC server is returned.
        Otherwise the host to use for SSL/TCP servers is returned.
        """
        host = self.host
        result = [part.strip() for part in host.split(',')]
        if len(result) == 1:
            result = result[0]
        if result == 'localhost':
            # 'localhost' resolves to ::1 (ipv6) on many systems, which fails on default setup of
            # docker, using 127.0.0.1 instead forces ipv4
            result = '127.0.0.1'
        return result

    def sane_max_sessions(self):
        """Return the maximum number of sessions to permit.  Normally this
        is MAX_SESSIONS.  However, to prevent open file exhaustion, ajdust
        downwards if running with a small open file rlimit."""
        env_value = self.integer('MAX_SESSIONS', 1000)
        nofile_limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
        # We give the DB 250 files; allow ElectrumX 100 for itself
        value = max(0, min(env_value, nofile_limit - 350))
        if value < env_value:
            self.logger.warning(f'lowered maximum sessions from {env_value:,d} to {value:,d} '
                                f'because your open file limit is {nofile_limit:,d}')
        return value

    def peer_discovery_enum(self):
        pd = self.default('PEER_DISCOVERY', 'on').strip().lower()
        if pd in ('off', ''):
            return self.PD_OFF
        elif pd == 'self':
            return self.PD_SELF
        else:
            return self.PD_ON

    def extract_peer_hubs(self):
        peer_hubs = self.default('PEER_HUBS', '')
        if not peer_hubs:
            return []
        return [hub.strip() for hub in peer_hubs.split(',')]

    @classmethod
    def contribute_common_settings_to_arg_parser(cls, parser):
        """
        Settings used by all services
        """
        env_db_dir = cls.default('DB_DIRECTORY', None)
        parser.add_argument('--db_dir', type=str, required=env_db_dir is None,
                            help="Path of the directory containing lbry-rocksdb. ", default=env_db_dir)
        parser.add_argument('--reorg_limit', default=cls.integer('REORG_LIMIT', 200), type=int, help='Max reorg depth')
        parser.add_argument('--chain', type=str, default=cls.default('NET', 'mainnet'),
                            help="Which chain to use, default is mainnet, others are used for testing",
                            choices=['mainnet', 'regtest', 'testnet'])
        parser.add_argument('--max_query_workers', type=int, default=cls.integer('MAX_QUERY_WORKERS', 4),
                            help="Size of the thread pool. Can be set in env with 'MAX_QUERY_WORKERS'")
        parser.add_argument('--cache_all_tx_hashes', action='store_true',
                            help="Load all tx hashes into memory. This will make address subscriptions and sync, "
                                 "resolve, transaction fetching, and block sync all faster at the expense of higher "
                                 "memory usage (at least 10GB more). Can be set in env with 'CACHE_ALL_TX_HASHES'.",
                            default=cls.boolean('CACHE_ALL_TX_HASHES', False))
        parser.add_argument('--cache_all_claim_txos', action='store_true',
                            help="Load all claim txos into memory. This will make address subscriptions and sync, "
                                 "resolve, transaction fetching, and block sync all faster at the expense of higher "
                                 "memory usage. Can be set in env with 'CACHE_ALL_CLAIM_TXOS'.",
                            default=cls.boolean('CACHE_ALL_CLAIM_TXOS', False))
        parser.add_argument('--prometheus_port', type=int, default=cls.integer('PROMETHEUS_PORT', 0),
                            help="Port for prometheus metrics to listen on, disabled by default. "
                                 "Can be set in env with 'PROMETHEUS_PORT'.")

    @classmethod
    def contribute_writer_settings_to_arg_parser(cls, parser):
        env_daemon_url = cls.default('DAEMON_URL', None)
        parser.add_argument('--daemon_url', required=env_daemon_url is None,
                            help="URL for rpc from lbrycrd or lbcd, "
                                 "<rpcuser>:<rpcpassword>@<lbrycrd rpc ip><lbrycrd rpc port>.",
                            default=env_daemon_url)
        parser.add_argument('--db_max_open_files', type=int, default=64,
                            help='This setting translates into the max_open_files option given to rocksdb. '
                                 'A higher number will use more memory. Defaults to 64.')

    @classmethod
    def contribute_server_settings_to_arg_parser(cls, parser):
        env_daemon_url = cls.default('DAEMON_URL', None)
        parser.add_argument('--daemon_url', required=env_daemon_url is None,
                            help="URL for rpc from lbrycrd or lbcd, "
                                 "<rpcuser>:<rpcpassword>@<lbrycrd rpc ip><lbrycrd rpc port>.",
                            default=env_daemon_url)
        parser.add_argument('--reindex', default=False, help="Drop and rebuild the elasticsearch index.",
                            action='store_true')
        parser.add_argument('--host', type=str, default=cls.default('HOST', 'localhost'),
                            help="Interface for hub server to listen on, use 0.0.0.0 to listen on the external "
                                 "interface. Can be set in env with 'HOST'")
        parser.add_argument('--tcp_port', type=int, default=cls.integer('TCP_PORT', 50001),
                            help="Electrum TCP port to listen on for hub server. Can be set in env with 'TCP_PORT'")
        parser.add_argument('--udp_port', type=int, default=cls.integer('UDP_PORT', 50001),
                            help="'UDP port to listen on for hub server. Can be set in env with 'UDP_PORT'")
        parser.add_argument('--elastic_host', default=cls.default('ELASTIC_HOST', 'localhost'), type=str,
                            help="Hostname or ip address of the elasticsearch instance to connect to. "
                                 "Can be set in env with 'ELASTIC_HOST'")
        parser.add_argument('--elastic_port', default=cls.integer('ELASTIC_PORT', 9200), type=int,
                            help="Elasticsearch port to connect to. Can be set in env with 'ELASTIC_PORT'")
        parser.add_argument('--elastic_notifier_host', default=cls.default('ELASTIC_NOTIFIER_HOST', 'localhost'),
                            type=str, help='elasticsearch sync notifier host, defaults to localhost')
        parser.add_argument('--elastic_notifier_port', default=cls.integer('ELASTIC_NOTIFIER_PORT', 19080), type=int,
                            help='elasticsearch sync notifier port')
        parser.add_argument('--es_index_prefix', default=cls.default('ES_INDEX_PREFIX', ''), type=str)

    @classmethod
    def contribute_elastic_sync_settings_to_arg_parser(cls, parser):
        parser.add_argument('--elastic_host', default=cls.default('ELASTIC_HOST', 'localhost'), type=str,
                            help="Hostname or ip address of the elasticsearch instance to connect to. "
                                 "Can be set in env with 'ELASTIC_HOST'")
        parser.add_argument('--elastic_port', default=cls.integer('ELASTIC_PORT', 9200), type=int,
                            help="Elasticsearch port to connect to. Can be set in env with 'ELASTIC_PORT'")
        parser.add_argument('--elastic_notifier_host', default=cls.default('ELASTIC_NOTIFIER_HOST', 'localhost'),
                            type=str, help='elasticsearch sync notifier host, defaults to localhost')
        parser.add_argument('--elastic_notifier_port', default=cls.integer('ELASTIC_NOTIFIER_PORT', 19080), type=int,
                            help='elasticsearch sync notifier port')
        parser.add_argument('--es_index_prefix', default=cls.default('ES_INDEX_PREFIX', ''), type=str)

        parser.add_argument('--query_timeout_ms', type=int, default=cls.integer('QUERY_TIMEOUT_MS', 10000),
                            help="Elasticsearch query timeout, in ms. Can be set in env with 'QUERY_TIMEOUT_MS'")
        parser.add_argument('--blocking_channel_ids', nargs='*',
                            help="Space separated list of channel claim ids used for blocking. "
                                 "Claims that are reposted by these channels can't be resolved "
                                 "or returned in search results. Can be set in env with 'BLOCKING_CHANNEL_IDS'",
                            default=cls.default('BLOCKING_CHANNEL_IDS', '').split(' '))
        parser.add_argument('--filtering_channel_ids', nargs='*',
                            help="Space separated list of channel claim ids used for blocking. "
                                 "Claims that are reposted by these channels aren't returned in search results. "
                                 "Can be set in env with 'FILTERING_CHANNEL_IDS'",
                            default=cls.default('FILTERING_CHANNEL_IDS', '').split(' '))

    @classmethod
    def from_arg_parser(cls, args):
        return cls(
            db_dir=args.db_dir, daemon_url=args.daemon_url, db_max_open_files=args.db_max_open_files,
            host=args.host, elastic_host=args.elastic_host, elastic_port=args.elastic_port,
            max_query_workers=args.max_query_workers, chain=args.chain, es_index_prefix=args.es_index_prefix,
            reorg_limit=args.reorg_limit, tcp_port=args.tcp_port,
            udp_port=args.udp_port, prometheus_port=args.prometheus_port,
            banner_file=args.banner_file, allow_lan_udp=args.allow_lan_udp,
            cache_all_tx_hashes=args.cache_all_tx_hashes, cache_all_claim_txos=args.cache_all_claim_txos,
            country=args.country, payment_address=args.payment_address, donation_address=args.donation_address,
            max_send=args.max_send, max_receive=args.max_receive, max_sessions=args.max_sessions,
            session_timeout=args.session_timeout, drop_client=args.drop_client, description=args.description,
            daily_fee=args.daily_fee, database_query_timeout=(args.query_timeout_ms / 1000),
            blocking_channel_ids=args.blocking_channel_ids, filtering_channel_ids=args.filtering_channel_ids,
            elastic_notifier_host=args.elastic_notifier_host, elastic_notifier_port=args.elastic_notifier_port
        )
