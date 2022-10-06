import argparse
import logging

from .collector import main as collector
from . import utils

utils.logging_config()


def main():
    parser = argparse.ArgumentParser(description="finance tools kit", prog="finkit")
    parser.add_argument("--version", action="version", version="%(prog)s 0.0.1")

    subparsers = parser.add_subparsers()
    collector_parser = subparsers.add_parser("collector", help="data collector for sources")
    collector_parser.add_argument("--source", "-s", action="store", required=True,
                                  help="specify the data source to fetch, e.g. huatai_option")
    collector_parser.add_argument("--directory", "-d", action="store", default="/tmp",
                                  help="the directory to store output file")
    collector_parser.add_argument("--date", action="store", default=None,
                                  help="the data source date")
    collector_parser.add_argument("--overwrite", action="store_true", default=False,
                                  help="whether to overwrite the stored data file")
    collector_parser.add_argument("--timezone", "-z", action="store", default=8, type=int,
                                  help="specify the timezone info")
    collector_parser.set_defaults(func=collector)
    args = parser.parse_args()

    logging.info("finkit %s, %s, %s", args.source, args.directory, args.overwrite)
    args.func(args)


if __name__ == '__main__':
    main()
