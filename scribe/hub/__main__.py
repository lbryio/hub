import os
import logging
import traceback
import argparse
from scribe.common import setup_logging
from scribe.hub.env import ServerEnv
from scribe.hub.service import HubServerService


def main():
    parser = argparse.ArgumentParser(
        prog='scribe-hub'
    )
    ServerEnv.contribute_to_arg_parser(parser)
    args = parser.parse_args()
    try:
        env = ServerEnv.from_arg_parser(args)
        setup_logging(os.path.join(env.db_dir, 'scribe-hub.log'))
        server = HubServerService(env)
        server.run()
    except Exception:
        traceback.print_exc()
        logging.critical('hub terminated abnormally')
    else:
        logging.info('hub terminated normally')


if __name__ == "__main__":
    main()
