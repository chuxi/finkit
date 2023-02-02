import importlib
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta, datetime, timezone
import signal
from threading import Event

import yaml
from crontab import CronTab

logger = logging.getLogger(__name__)


def cron(args):
    logger.info("parsing cron job from config file: %s", args.cron_file)
    jobs = []
    with open(args.cron_file, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
        if "timezone" not in config:
            tz = timezone(timedelta(hours=8))
        else:
            tz = timezone(timedelta(hours=config["timezone"]))
        for job in config["jobs"]:
            if "func" not in job or "cron" not in job:
                raise ValueError("missing func or cron in job: " + job)
            job["timezone"] = tz if "timezone" not in job else\
                timezone(timedelta(hours=job["timezone"]))
            job["cron"] = CronTab(job["cron"])
            jobs.append(job)

    num_threads = len(jobs)
    executor = ThreadPoolExecutor(num_threads)
    events = []
    for job in jobs:
        event = Event()
        executor.submit(run, job, event)
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


def run(job: dict, event: Event):
    func_parts = job["func"].split(".")
    args = None if "args" not in job else job["args"]
    while not event.is_set():
        wait_time = job["cron"].next(now=datetime.now(job["timezone"]))
        logger.info("%s next job will start after %.2f hours", job["func"], wait_time / 3600)
        event.wait(wait_time)
        if not event.is_set():
            try:
                _exec_method(func_parts, args)
            except BaseException as e:
                logger.error("failed to run job: %s", job["func"], e)
        else:
            logger.info("job %s existed", job["func"])


def _exec_method(func_parts: list, args: dict = None):
    try:
        mod = importlib.import_module("finkit." + ".".join(func_parts[:-1]))
        method = getattr(mod, func_parts[-1])
        if args:
            method(**args)
        else:
            method()
    except ModuleNotFoundError as e:
        try:
            mod = importlib.import_module("finkit." + ".".join(func_parts[:-2]))
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

