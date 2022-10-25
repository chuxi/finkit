import datetime
import logging
from datetime import datetime

from .huatai_option_crawler import HuaTaiOptionCrawler


def main(args):
    logging.info("collector: source %s, output directory %s", args.source, args.directory)
    date = None
    if args.date is not None:
        date = datetime.fromisoformat(args.date).date()

    match args.source:
        case "huatai-option":
            worker = HuaTaiOptionCrawler(args.directory, date,
                tz=args.timezone, overwrite=args.overwrite)
            worker.crawl()
        case _:
            raise ValueError("not a valid data source")
