import logging
from datetime import date

from .collector_context import CollectorContext
from .future_source import get_daily_vol_rank, get_daily_kline
import sqlalchemy as sal
import chinese_calendar as ccal

logger = logging.getLogger(__name__)


def daily_vol_rank(context: CollectorContext, mydate: date):
    if not ccal.is_workday(mydate):
        logger.info("%s is not workday, can not download daily vol rank", mydate)
        return
    df = get_daily_vol_rank(mydate)
    if df is None:
        logger.warning("daily vol rank dataset is not ready for %s", mydate)
        return
    engine = context.get_engine()
    with engine.connect() as conn:
        conn.execute(sal.text("delete from future_daily_vol_rank where date = :mydate"), {"mydate": mydate})
        conn.commit()
    df.to_sql(name="future_daily_vol_rank", con=engine, if_exists="append", index=False)
    logger.info("downloaded daily vol rank data: %s", mydate.isoformat())
    logger.debug("daily vol rank data sample: \n%s", df.to_string(max_rows=100))


def daily_kline(context: CollectorContext, mydate: date):
    if not ccal.is_workday(mydate):
        logger.info("%s is not workday, can not download daily vol rank", mydate)
        return
    logger.info("start to get daily kline: %s", mydate.isoformat())
    df = get_daily_kline(mydate)
    if df is not None:
        engine = context.get_engine()
        with engine.connect() as conn:
            conn.execute(sal.text("delete from future_daily_kline where date = :mydate"), {"mydate": mydate})
            conn.commit()
        df.to_sql(name="future_daily_kline", con=engine, if_exists="append", index=False)
        logger.info("downloaded daily kline data: %s", mydate.isoformat())
        logger.debug("daily kline data sample: \n%s", df.to_string(max_rows=100))
