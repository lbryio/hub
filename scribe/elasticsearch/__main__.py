import os
import logging
import traceback
import argparse
from scribe.env import Env
from scribe.common import setup_logging
from scribe.elasticsearch.service import ElasticSyncService


def main():
    parser = argparse.ArgumentParser(
        prog='scribe-elastic-sync'
    )
    Env.contribute_common_settings_to_arg_parser(parser)
    Env.contribute_elastic_sync_settings_to_arg_parser(parser)
    args = parser.parse_args()

    try:
        env = Env.from_arg_parser(args)
        setup_logging(os.path.join(env.db_dir, 'scribe-elastic-sync.log'))
        server = ElasticSyncService(env)
        server.run(args.reindex)
    except Exception:
        traceback.print_exc()
        logging.critical('es sync terminated abnormally')
    else:
        logging.info('es sync terminated normally')


if __name__ == "__main__":
    main()
