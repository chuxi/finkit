import csv
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


def save(file: str, header: [], data: []):
    # save as csv file
    with open(file, "w", newline="") as f:
        writer = csv.DictWriter(f, header)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    logging.info("stored csv file: %s", file)

