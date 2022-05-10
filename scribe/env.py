import os
import re
import resource
import logging
from collections import namedtuple
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

    def __init__(self, db_dir=None, max_query_workers=None, chain=None, reorg_limit=None,
                 prometheus_port=None, cache_all_tx_hashes=None, cache_all_claim_txos=None,
                 blocking_channel_ids=None, filtering_channel_ids=None, index_address_status=None):

        self.logger = logging.getLogger(__name__)
        self.db_dir = db_dir if db_dir is not None else self.required('DB_DIRECTORY')
        self.obsolete(['UTXO_MB', 'HIST_MB', 'NETWORK'])
        self.max_query_workers = max_query_workers if max_query_workers is not None else self.integer('MAX_QUERY_WORKERS', 4)
        if chain == 'mainnet':
            self.coin = LBCMainNet
        elif chain == 'testnet':
            self.coin = LBCTestNet
        else:
            self.coin = LBCRegTest
        self.reorg_limit = reorg_limit if reorg_limit is not None else self.integer('REORG_LIMIT', self.coin.REORG_LIMIT)
        self.prometheus_port = prometheus_port if prometheus_port is not None else self.integer('PROMETHEUS_PORT', 0)
        self.cache_all_tx_hashes = cache_all_tx_hashes if cache_all_tx_hashes is not None else self.boolean('CACHE_ALL_TX_HASHES', False)
        self.cache_all_claim_txos = cache_all_claim_txos if cache_all_claim_txos is not None else self.boolean('CACHE_ALL_CLAIM_TXOS', False)
        # Filtering / Blocking
        self.blocking_channel_ids = blocking_channel_ids if blocking_channel_ids is not None else self.default(
            'BLOCKING_CHANNEL_IDS', '').split(' ')
        self.filtering_channel_ids = filtering_channel_ids if filtering_channel_ids is not None else self.default(
            'FILTERING_CHANNEL_IDS', '').split(' ')
        self.index_address_status = index_address_status if index_address_status is not None else \
            self.boolean('INDEX_ADDRESS_STATUS', False)

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
    def contribute_to_arg_parser(cls, parser):
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
        parser.add_argument('--index_address_statuses', action='store_true',
                            help="Use precomputed address statuses, must be enabled in the reader and the writer to "
                                 "use it. If disabled (the default), the status of an address must be calculated at "
                                 "runtime when clients request it (address subscriptions, address history sync). "
                                 "If enabled, scribe will maintain an index of precomputed statuses",
                            default=cls.boolean('INDEX_ADDRESS_STATUS', False))

    @classmethod
    def from_arg_parser(cls, args):
        raise NotImplementedError()
