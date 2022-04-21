import os
import logging
import traceback
import argparse
from scribe.env import Env
from scribe.common import setup_logging
from scribe.hub.service import HubServerService


def main():
    parser = argparse.ArgumentParser(
        prog='scribe-hub'
    )
    Env.contribute_common_settings_to_arg_parser(parser)
    Env.contribute_server_settings_to_arg_parser(parser)
    args = parser.parse_args()

    try:
        env = Env.from_arg_parser(args)
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
