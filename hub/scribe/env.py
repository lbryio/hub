from hub.env import Env


class BlockchainEnv(Env):
    def __init__(self, db_dir=None, max_query_workers=None, chain=None, reorg_limit=None,
                 prometheus_port=None, cache_all_tx_hashes=None, cache_all_claim_txos=None,
                 blocking_channel_ids=None, filtering_channel_ids=None,
                 db_max_open_files=64, daemon_url=None, hashX_history_cache_size=None,
                 index_address_status=None, rebuild_address_status_from_height=None,
                 daemon_ca_path=None):
        super().__init__(db_dir, max_query_workers, chain, reorg_limit, prometheus_port, cache_all_tx_hashes,
                         cache_all_claim_txos, blocking_channel_ids, filtering_channel_ids, index_address_status)
        self.db_max_open_files = db_max_open_files
        self.daemon_url = daemon_url if daemon_url is not None else self.required('DAEMON_URL')
        self.hashX_history_cache_size = hashX_history_cache_size if hashX_history_cache_size is not None \
            else self.integer('ADDRESS_HISTORY_CACHE_SIZE', 4096)
        self.rebuild_address_status_from_height = rebuild_address_status_from_height \
            if isinstance(rebuild_address_status_from_height, int) else -1
        self.daemon_ca_path = daemon_ca_path if daemon_ca_path else None

    @classmethod
    def contribute_to_arg_parser(cls, parser):
        super().contribute_to_arg_parser(parser)
        env_daemon_url = cls.default('DAEMON_URL', None)
        parser.add_argument('--daemon_url', required=env_daemon_url is None,
                            help="URL for rpc from lbrycrd or lbcd, "
                                 "<rpcuser>:<rpcpassword>@<lbrycrd rpc ip><lbrycrd rpc port>.",
                            default=env_daemon_url)
        parser.add_argument('--daemon_ca_path', type=str, default='',
                            help='Path to the lbcd ca file, used for lbcd with ssl')
        parser.add_argument('--db_max_open_files', type=int, default=64,
                            help='This setting translates into the max_open_files option given to rocksdb. '
                                 'A higher number will use more memory. Defaults to 64.')
        parser.add_argument('--address_history_cache_size', type=int,
                            default=cls.integer('ADDRESS_HISTORY_CACHE_SIZE', 4096),
                            help="LRU cache size for address histories, used when processing new blocks "
                                 "and when processing mempool updates. Can be set in env with "
                                 "'ADDRESS_HISTORY_CACHE_SIZE'")
        parser.add_argument('--rebuild_address_status_from_height', type=int, default=-1,
                            help="Rebuild address statuses, set to 0 to reindex all address statuses or provide a "
                                 "block height to start reindexing from. Defaults to -1 (off).")

    @classmethod
    def from_arg_parser(cls, args):
        return cls(
            db_dir=args.db_dir, daemon_url=args.daemon_url, db_max_open_files=args.db_max_open_files,
            max_query_workers=args.max_query_workers, chain=args.chain, reorg_limit=args.reorg_limit,
            prometheus_port=args.prometheus_port, cache_all_tx_hashes=args.cache_all_tx_hashes,
            cache_all_claim_txos=args.cache_all_claim_txos, index_address_status=args.index_address_statuses,
            hashX_history_cache_size=args.address_history_cache_size,
            rebuild_address_status_from_height=args.rebuild_address_status_from_height,
            daemon_ca_path=args.daemon_ca_path
        )
