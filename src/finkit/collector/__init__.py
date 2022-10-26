import datetime
import logging
from datetime import datetime, timezone, timedelta

from .huatai_option_crawler import HuaTaiOptionCrawler


def main(args):
    logging.info("collector: source %s, output directory %s", args.source, args.directory)
    mydate = None
    if args.date is not None:
        mydate = datetime.fromisoformat(args.date).date()
    else:
        mydate = datetime.now(tz=timezone(timedelta(hours=args.timezone))).date()
    match args.source:
        case "huatai-option":
            worker = HuaTaiOptionCrawler(args.directory, mydate,
                tz=args.timezone, overwrite=args.overwrite)
            worker.crawl()
        case _:
            raise ValueError("not a valid data source")
