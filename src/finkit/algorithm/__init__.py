# define algorithms for the kit
import logging
from datetime import datetime, timedelta, timezone

from .huatai_option_algo import HuataiOptionAlgo

logger = logging.getLogger(__name__)


def option(args):
    logger.info("algorithm: source %s, output directory %s", args.source, args.directory)
    # 检查参数
    mydate = None
    if args.date is not None:
        mydate = datetime.fromisoformat(args.date).date()
    else:
        mydate = datetime.now(tz=timezone(timedelta(hours=args.timezone))).date()
    if args.contract is None:
        raise ValueError("option contract is None")
    if args.strike_date is None:
        raise ValueError("no strike date, 2022-11-20,2022-11,21 etc.")
    strike_date = args.strike_date.split(",")
    if args.strike_price is None:
        raise ValueError("no strike price, 399.2,410.5 etc.")
    strike_price = [float(x) for x in args.strike_price.split(",")]
    if args.rate is None:
        args.rate = 0.03
    if args.dividend_rate is None:
        args.dividend_rate = 0

    df = None
    match args.source:
        case "huatai-option":
            algo = HuataiOptionAlgo(args.contract,
                                    strike_date,
                                    strike_price,
                                    args.rate,
                                    args.dividend_rate,
                                    mydate=mydate,
                                    storage_directory=args.directory)
            df = algo.calc()
        case _:
            raise ValueError("not a valid data source")
    logger.info("%s %s %s option calculation result: \n%s", args.source, args.contract, mydate, df)
    filename = args.directory + "/{}-{}-{}.csv".format(args.source, args.contract, mydate)
    df.to_csv(filename, float_format='%.3f', index=False)
