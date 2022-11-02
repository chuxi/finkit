import datetime
import logging
import os
from datetime import datetime, timezone, timedelta

from .huatai_option_crawler import HuaTaiOptionCrawler
from .shfe_daily_trading_volume import ShfeDailyTradingVolumeCrawler
from .dce_daily_trading_volume import DceDailyTradingVolumeCrawler
from .czce_daily_trading_volume import CzceDailyTradingVolumeCrawler
from .cffex_daily_trading_volume import CffexDailyTradingVolumeCrawler

from .shfe_daily_stock import ShfeDailyStockCrawler


def main(args):
    logging.info("collector: source %s, output directory %s", args.source, args.directory)
    mydate = None
    if args.date is not None:
        mydate = datetime.fromisoformat(args.date).date()
    else:
        mydate = datetime.now(tz=timezone(timedelta(hours=args.timezone))).date()
    if not os.path.exists(args.directory):
        os.makedirs(args.directory)
    match args.source:
        case "huatai-option":
            worker = HuaTaiOptionCrawler(args.directory, mydate,
                tz=args.timezone, overwrite=args.overwrite)
            worker.crawl()
        case "daily-volume":
            ShfeDailyTradingVolumeCrawler(args.directory, mydate, overwrite=args.overwrite).crawl()
            DceDailyTradingVolumeCrawler(args.directory, mydate, overwrite=args.overwrite).crawl()
            CzceDailyTradingVolumeCrawler(args.directory, mydate, overwrite=args.overwrite).crawl()
            CffexDailyTradingVolumeCrawler(args.directory, mydate, overwrite=args.overwrite).crawl()
        case "daily-stock":
            ShfeDailyStockCrawler(args.directory, mydate, overwrite=args.overwrite).crawl()
        case _:
            raise ValueError("not a valid data source")
