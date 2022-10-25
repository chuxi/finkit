import argparse
import logging

from .collector import main as collector
from .algorithm import option
from . import utils

utils.logging_config()


def main():
    parser = argparse.ArgumentParser(description="finance tools kit", prog="finkit")
    parser.add_argument("--version", action="version", version="%(prog)s 0.0.1")

    subparsers = parser.add_subparsers(title="actions")

    collector_parser = subparsers.add_parser("collector",
                                             help="data collector for sources")
    collector_parser.add_argument("--source", "-s", action="store", required=True,
                                  help="specify the data source to fetch, e.g. huatai_option")
    collector_parser.add_argument("--directory", "-d", action="store", default="./data",
                                  help="the directory to store output file")
    collector_parser.add_argument("--date", action="store", default=None,
                                  help="the data source date")
    collector_parser.add_argument("--overwrite", action="store_true", default=False,
                                  help="whether to overwrite the stored data file")
    collector_parser.add_argument("--timezone", "-z", action="store", default=8, type=int,
                                  help="specify the timezone info")
    collector_parser.set_defaults(func=collector)

    option_parser = subparsers.add_parser("option",
                                          help="option algorithms of futures, options and other derivatives")
    option_parser.add_argument("--source", "-s", action="store", required=True,
                               help="specify the data source to fetch, e.g. huatai_option")
    option_parser.add_argument("--directory", "-d", action="store", default="./data",
                               help="the directory to store output file")
    option_parser.add_argument("--date", action="store", default=None,
                               help="the data source date")
    option_parser.add_argument("--contract", action="store", required=True,
                               help="the contract code, au2211.shex, ag2211.shex etc.")
    option_parser.add_argument("--strike-price", action="store", required=True,
                               help="strike price for the option, 399.5,396.3")
    option_parser.add_argument("--strike-date", action="store", default=None,
                               help="the end expiry dates of option, 2022-11-15,2022-11-16")
    option_parser.add_argument("--rate", action="store", default=0.03, type=float,
                               help="the short risk free rate annually")
    option_parser.add_argument("--dividend-rate", action="store", default=0.03, type=float,
                               help="the dividend rate for stock options, 0 for non-stock options")
    option_parser.add_argument("--method", action="store", default="bsm",
                               help="the option price algorithm method")
    option_parser.set_defaults(func=option)

    args = parser.parse_args()

    logging.info("finkit %s %s", args.func, args.source)
    args.func(args)


if __name__ == '__main__':
    main()
