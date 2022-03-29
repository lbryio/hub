import logging
import traceback
import argparse
from scribe.env import Env
from scribe.common import setup_logging
from scribe.hub.service import HubServerService


def main():
    setup_logging()
    parser = argparse.ArgumentParser(
        prog='scribe-hub'
    )
    Env.contribute_to_arg_parser(parser)
    args = parser.parse_args()

    try:

        server = HubServerService(Env.from_arg_parser(args))
        server.run()
    except Exception:
        traceback.print_exc()
        logging.critical('hub terminated abnormally')
    else:
        logging.info('hub terminated normally')


if __name__ == "__main__":
    main()
