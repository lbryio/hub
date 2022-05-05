import os
import logging
import traceback
import argparse
from scribe.common import setup_logging
from scribe.blockchain.env import BlockchainEnv
from scribe.blockchain.service import BlockchainProcessorService


def main():
    parser = argparse.ArgumentParser(
        prog='scribe'
    )
    BlockchainEnv.contribute_to_arg_parser(parser)
    args = parser.parse_args()

    try:
        env = BlockchainEnv.from_arg_parser(args)
        setup_logging(os.path.join(env.db_dir, 'scribe.log'))
        block_processor = BlockchainProcessorService(env)
        block_processor.run()
    except Exception:
        traceback.print_exc()
        logging.critical('scribe terminated abnormally')
    else:
        logging.info('scribe terminated normally')


if __name__ == "__main__":
    main()
