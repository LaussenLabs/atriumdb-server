import tsc_gen
import logging
from config import config


def main():
    # set up logging
    log_level = {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING, "error": logging.ERROR,
                 "critical": logging.CRITICAL}
    logging.basicConfig(level=log_level[config.loglevel.lower()])
    _LOGGER = logging.getLogger(__name__)
    _LOGGER.info("TSC generator started")
    tsc_gen.run_tsc_generator()


if __name__ == "__main__":
    main()
