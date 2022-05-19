from hub.env import Env


class ElasticEnv(Env):
    def __init__(self, db_dir=None, max_query_workers=None, chain=None, reorg_limit=None, prometheus_port=None,
                 cache_all_tx_hashes=None, cache_all_claim_txos=None, elastic_host=None, elastic_port=None,
                 es_index_prefix=None, elastic_notifier_host=None, elastic_notifier_port=None,
                 blocking_channel_ids=None, filtering_channel_ids=None, reindex=False):
        super().__init__(db_dir, max_query_workers, chain, reorg_limit, prometheus_port, cache_all_tx_hashes,
                         cache_all_claim_txos, blocking_channel_ids, filtering_channel_ids)
        self.elastic_host = elastic_host if elastic_host is not None else self.default('ELASTIC_HOST', 'localhost')
        self.elastic_port = elastic_port if elastic_port is not None else self.integer('ELASTIC_PORT', 9200)
        self.elastic_notifier_host = elastic_notifier_host if elastic_notifier_host is not None else self.default(
            'ELASTIC_NOTIFIER_HOST', 'localhost')
        self.elastic_notifier_port = elastic_notifier_port if elastic_notifier_port is not None else self.integer(
            'ELASTIC_NOTIFIER_PORT', 19080)
        self.es_index_prefix = es_index_prefix if es_index_prefix is not None else self.default('ES_INDEX_PREFIX', '')
        # Filtering / Blocking
        self.reindex = reindex if reindex is not None else self.boolean('REINDEX_ES', False)

    @classmethod
    def contribute_to_arg_parser(cls, parser):
        super().contribute_to_arg_parser(parser)
        parser.add_argument('--reindex', default=False, help="Drop and rebuild the elasticsearch index.",
                            action='store_true')
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

    @classmethod
    def from_arg_parser(cls, args):
        return cls(
            db_dir=args.db_dir, elastic_host=args.elastic_host,
            elastic_port=args.elastic_port, max_query_workers=args.max_query_workers, chain=args.chain,
            es_index_prefix=args.es_index_prefix, reorg_limit=args.reorg_limit,
            prometheus_port=args.prometheus_port, cache_all_tx_hashes=args.cache_all_tx_hashes,
            cache_all_claim_txos=args.cache_all_claim_txos, blocking_channel_ids=args.blocking_channel_ids,
            filtering_channel_ids=args.filtering_channel_ids, elastic_notifier_host=args.elastic_notifier_host,
            elastic_notifier_port=args.elastic_notifier_port
        )
