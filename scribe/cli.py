import logging
import traceback
import argparse
from scribe.env import Env
from scribe.blockchain.service import BlockchainProcessorService
from scribe.hub.service import HubServerService
from scribe.elasticsearch.service import ElasticSyncService


def get_arg_parser(name):
    parser = argparse.ArgumentParser(
        prog=name
    )
    Env.contribute_to_arg_parser(parser)
    return parser


def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-4s %(name)s:%(lineno)d: %(message)s")
    logging.getLogger('aiohttp').setLevel(logging.WARNING)
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)


def run_writer_forever():
    setup_logging()
    args = get_arg_parser('scribe').parse_args()
    try:
        block_processor = BlockchainProcessorService(Env.from_arg_parser(args))
        block_processor.run()
    except Exception:
        traceback.print_exc()
        logging.critical('scribe terminated abnormally')
    else:
        logging.info('scribe terminated normally')


def run_server_forever():
    setup_logging()
    args = get_arg_parser('scribe-hub').parse_args()

    try:
        server = HubServerService(Env.from_arg_parser(args))
        server.run()
    except Exception:
        traceback.print_exc()
        logging.critical('hub terminated abnormally')
    else:
        logging.info('hub terminated normally')


def run_es_sync_forever():
    setup_logging()
    parser = get_arg_parser('scribe-elastic-sync')
    parser.add_argument('--reindex', type=bool, default=False)
    args = parser.parse_args()

    try:
        server = ElasticSyncService(Env.from_arg_parser(args))
        server.run(args.reindex)
    except Exception:
        traceback.print_exc()
        logging.critical('es sync terminated abnormally')
    else:
        logging.info('es sync terminated normally')
