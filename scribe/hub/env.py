import re
from scribe.env import Env


class ServerEnv(Env):
    def __init__(self, db_dir=None, max_query_workers=None, chain=None, reorg_limit=None,
                 prometheus_port=None, cache_all_tx_hashes=None, cache_all_claim_txos=None,
                 daemon_url=None, host=None, elastic_host=None, elastic_port=None, es_index_prefix=None,
                 tcp_port=None, udp_port=None, banner_file=None, allow_lan_udp=None, country=None,
                 payment_address=None, donation_address=None, max_send=None, max_receive=None, max_sessions=None,
                 session_timeout=None, drop_client=None, description=None, daily_fee=None,
                 database_query_timeout=None, elastic_notifier_host=None, elastic_notifier_port=None,
                 blocking_channel_ids=None, filtering_channel_ids=None, peer_hubs=None, peer_announce=None,
                 index_address_status=None):
        super().__init__(db_dir, max_query_workers, chain, reorg_limit, prometheus_port, cache_all_tx_hashes,
                         cache_all_claim_txos, blocking_channel_ids, filtering_channel_ids, index_address_status)
        self.daemon_url = daemon_url if daemon_url is not None else self.required('DAEMON_URL')
        self.host = host if host is not None else self.default('HOST', 'localhost')
        self.elastic_host = elastic_host if elastic_host is not None else self.default('ELASTIC_HOST', 'localhost')
        self.elastic_port = elastic_port if elastic_port is not None else self.integer('ELASTIC_PORT', 9200)
        self.elastic_notifier_host = elastic_notifier_host if elastic_notifier_host is not None else self.default(
            'ELASTIC_NOTIFIER_HOST', 'localhost')
        self.elastic_notifier_port = elastic_notifier_port if elastic_notifier_port is not None else self.integer(
            'ELASTIC_NOTIFIER_PORT', 19080)
        self.es_index_prefix = es_index_prefix if es_index_prefix is not None else self.default('ES_INDEX_PREFIX', '')
        # Server stuff
        self.tcp_port = tcp_port if tcp_port is not None else self.integer('TCP_PORT', None)
        self.udp_port = udp_port if udp_port is not None else self.integer('UDP_PORT', self.tcp_port)
        self.banner_file = banner_file if banner_file is not None else self.default('BANNER_FILE', None)
        self.allow_lan_udp = allow_lan_udp if allow_lan_udp is not None else self.boolean('ALLOW_LAN_UDP', False)
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
        self.donation_address = donation_address if donation_address is not None else self.default('DONATION_ADDRESS',
                                                                                                   '')
        # Server limits to help prevent DoS
        self.max_send = max_send if max_send is not None else self.integer('MAX_SEND', 1000000000000000000)
        self.max_receive = max_receive if max_receive is not None else self.integer('MAX_RECEIVE', 1000000000000000000)
        self.max_sessions = max_sessions if max_sessions is not None else self.sane_max_sessions()
        self.session_timeout = session_timeout if session_timeout is not None else self.integer('SESSION_TIMEOUT', 600)
        self.drop_client = re.compile(drop_client) if drop_client is not None else self.custom("DROP_CLIENT", None, re.compile)
        self.description = description if description is not None else self.default('DESCRIPTION', '')
        self.daily_fee = daily_fee if daily_fee is not None else self.string_amount('DAILY_FEE', '0')
        self.database_query_timeout = (database_query_timeout / 1000.0) if database_query_timeout is not None else \
            (float(self.integer('QUERY_TIMEOUT_MS', 10000)) / 1000.0)

    @classmethod
    def contribute_to_arg_parser(cls, parser):
        super().contribute_to_arg_parser(parser)
        env_daemon_url = cls.default('DAEMON_URL', None)
        parser.add_argument('--daemon_url', required=env_daemon_url is None,
                            help="URL for rpc from lbrycrd or lbcd, "
                                 "<rpcuser>:<rpcpassword>@<lbrycrd rpc ip><lbrycrd rpc port>.",
                            default=env_daemon_url)
        parser.add_argument('--host', type=str, default=cls.default('HOST', 'localhost'),
                            help="Interface for hub server to listen on, use 0.0.0.0 to listen on the external "
                                 "interface. Can be set in env with 'HOST'")
        parser.add_argument('--tcp_port', type=int, default=cls.integer('TCP_PORT', 50001),
                            help="Electrum TCP port to listen on for hub server. Can be set in env with 'TCP_PORT'")
        parser.add_argument('--udp_port', type=int, default=cls.integer('UDP_PORT', 50001),
                            help="'UDP port to listen on for hub server. Can be set in env with 'UDP_PORT'")
        parser.add_argument('--max_sessions', type=int, default=cls.integer('MAX_SESSIONS', 100000),
                            help="Maximum number of electrum clients that can be connected, defaults to 100000.")
        parser.add_argument('--max_send', type=int, default=cls.integer('MAX_SESSIONS', 1000000000000000000),
                            help="Maximum size of a request")
        parser.add_argument('--max_receive', type=int, default=cls.integer('MAX_SESSIONS', 1000000000000000000),
                            help="Maximum size of a response")
        parser.add_argument('--drop_client', type=str, default=cls.default('DROP_CLIENT', None),
                            help="Regex used for blocking clients")
        parser.add_argument('--session_timeout', type=int, default=cls.integer('SESSION_TIMEOUT', 600),
                            help="Session inactivity timeout")
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
        parser.add_argument('--allow_lan_udp', action='store_true',
                            help="Reply to clients on the local network", default=cls.boolean('ALLOW_LAN_UDP', False))
        parser.add_argument('--description', default=cls.default('DESCRIPTION', None), type=str)
        parser.add_argument('--banner_file', default=cls.default('BANNER_FILE', None), type=str)
        parser.add_argument('--country', default=cls.default('COUNTRY', 'US'), type=str)
        parser.add_argument('--payment_address', default=cls.default('PAYMENT_ADDRESS', None), type=str)
        parser.add_argument('--donation_address', default=cls.default('DONATION_ADDRESS', None), type=str)
        parser.add_argument('--daily_fee', default=cls.default('DAILY_FEE', '0'), type=str)
        parser.add_argument('--query_timeout_ms', type=int, default=cls.integer('QUERY_TIMEOUT_MS', 10000),
                            help="Elasticsearch query timeout, in ms. Can be set in env with 'QUERY_TIMEOUT_MS'")
    @classmethod
    def from_arg_parser(cls, args):
        return cls(
            db_dir=args.db_dir, daemon_url=args.daemon_url, host=args.host, elastic_host=args.elastic_host,
            elastic_port=args.elastic_port, max_query_workers=args.max_query_workers, chain=args.chain,
            es_index_prefix=args.es_index_prefix, reorg_limit=args.reorg_limit, tcp_port=args.tcp_port,
            udp_port=args.udp_port, prometheus_port=args.prometheus_port, banner_file=args.banner_file,
            allow_lan_udp=args.allow_lan_udp, cache_all_tx_hashes=args.cache_all_tx_hashes,
            cache_all_claim_txos=args.cache_all_claim_txos, country=args.country, payment_address=args.payment_address,
            donation_address=args.donation_address, max_send=args.max_send, max_receive=args.max_receive,
            max_sessions=args.max_sessions, session_timeout=args.session_timeout,
            drop_client=args.drop_client, description=args.description, daily_fee=args.daily_fee,
            database_query_timeout=args.query_timeout_ms, blocking_channel_ids=args.blocking_channel_ids,
            filtering_channel_ids=args.filtering_channel_ids, elastic_notifier_host=args.elastic_notifier_host,
            elastic_notifier_port=args.elastic_notifier_port, index_address_status=args.index_address_statuses
        )
