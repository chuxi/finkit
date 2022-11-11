import logging

logger = logging.getLogger(__name__)


class MySleeper:
    def sleep(self):
        logger.info("now i am sleeping...")


def static_sleep(k1, k2: str=None):
    logger.info("static sleep... k1: %s, k2: %s", k1, k2)


def static_sleep_v2(k1, **kwargs):
    logger.info("static sleep v2, k1: %s, kwargs: %s", k1, kwargs)
