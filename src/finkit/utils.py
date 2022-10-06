import os.path
import logging.config

import yaml


LOGGING_FILE= "./logging.yml"


def logging_config(file: str = LOGGING_FILE):
    if os.path.exists(file):
        try:
            with open(file, "rt") as f:
                config = yaml.safe_load(f)
                logging.config.dictConfig(config)
        except Exception as e:
            logging.basicConfig(level=logging.WARN)

