import logging
import traceback
import argparse
from scribe.env import Env
from scribe.common import setup_logging
from scribe.blockchain.service import BlockchainProcessorService


def main():
    setup_logging()
    parser = argparse.ArgumentParser(
        prog='scribe'
    )
    Env.contribute_to_arg_parser(parser)
    args = parser.parse_args()
    try:
        block_processor = BlockchainProcessorService(Env.from_arg_parser(args))
        block_processor.run()
    except Exception:
        traceback.print_exc()
        logging.critical('scribe terminated abnormally')
    else:
        logging.info('scribe terminated normally')


if __name__ == "__main__":
    main()
