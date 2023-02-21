import datetime
import importlib
import logging
import os
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from threading import Event

from crontab import CronTab

from .collector_context import CollectorContext
from .cffex_collector import CffexCollector
from .cffex_daily_trading_volume import CffexDailyTradingVolumeCrawler
from .czce_collector import CzceCollector
from .czce_daily_trading_volume import CzceDailyTradingVolumeCrawler
from .dce_collector import DceCollector
from .dce_daily_trading_volume import DceDailyTradingVolumeCrawler
from .huatai_option_crawler import HuaTaiOptionCrawler
from .shfe_collector import ShfeCollector
from .shfe_daily_stock import ShfeDailyStockCrawler
from .shfe_daily_trading_volume import ShfeDailyTradingVolumeCrawler

logger = logging.getLogger(__name__)


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
        case "shfe":
            cc = ShfeCollector(args.directory, mydate, overwrite=args.overwrite)
            cc.crawl_daily_stock()
            cc.crawl_daily_volume()
        case "dce":
            cc = DceCollector(args.directory, mydate, overwrite=args.overwrite)
            cc.crawl_daily_stock()
            cc.crawl_daily_volume()
        case "czce":
            cc = CzceCollector(args.directory, mydate, overwrite=args.overwrite)
            cc.crawl_daily_stock()
            cc.crawl_daily_volume()
        case "cffex":
            cc = CffexCollector(args.directory, mydate, overwrite=args.overwrite)
            cc.crawl_daily_volume()
        case _:
            raise ValueError("not a valid data source")


def source(args):
    logger.info("running source collecting jobs with config: %s", args.config)
    context = CollectorContext(args.config)
    if hasattr(args, "date") and args.date is not None:
        mydate = datetime.fromisoformat(args.date).date()
        if hasattr(args, "job_name") and args.job_name is None:
            raise ValueError("missing job_name to run single job")
        target = args.job_name
        jobs = [x for x in context.jobs if target == x["name"]]
        if len(jobs) != 1:
            raise ValueError("non job_name found for: " + target)
        job = jobs[0]
        args = job["args"] if "args" in job else {}
        args["mydate"] = mydate
        args["context"] = context
        logger.info("start job: %s, args: %s", job["name"], args)
        func_parts = job["func"].split(".")
        _exec_method(func_parts, args)
    else:
        executor = ThreadPoolExecutor(len(context.jobs))
        events = []
        for job in context.jobs:
            event = Event()
            executor.submit(run, job, context, event)
            events.append(event)
        def exit_handler(sig, frame):
            logger.info("ctrl + c, existing...")
            for ev in events:
                ev.set()
            executor.shutdown(cancel_futures=True)
            sys.exit(0)

        signal.signal(signal.SIGINT, exit_handler)
        while True:
            time.sleep(1)


def run(job: dict, context: CollectorContext, event: Event):
    func_parts = job["func"].split(".")
    args = job["args"] if "args" in job else {}
    args["context"] = context
    cron = CronTab(job["cron"])
    while not event.is_set():
        wait_time = cron.next(now=datetime.now(context.timezone))
        logger.info("%s next job will start after %.2f hours", job["func"], wait_time / 3600)
        event.wait(wait_time)
        if not event.is_set():
            try:
                args["mydate"] = datetime.now(tz=context.timezone).date()
                _exec_method(func_parts, args)
            except BaseException as e:
                logger.error("failed to run job: %s", job["func"], e)
        else:
            logger.info("job %s existed", job["func"])


def _exec_method(func_parts: list, args: dict = None):
    try:
        mod = importlib.import_module("finkit.collector." + ".".join(func_parts[:-1]))
        method = getattr(mod, func_parts[-1])
        if args:
            method(**args)
        else:
            method()
    except ModuleNotFoundError as e:
        try:
            mod = importlib.import_module("finkit.collector." + ".".join(func_parts[:-2]))
            clz = getattr(mod, func_parts[-2])
            if args:
                obj = clz(**args)
            else:
                obj = clz()
            method = getattr(obj, func_parts[-1])
            method()
        except ModuleNotFoundError as e:
            logger.error("module not found for func: %s", func_parts)
            raise e

