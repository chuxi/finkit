import logging
from datetime import date

from .collector_context import CollectorContext
from .future_source import get_daily_vol_rank
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
    logger.debug("daily vol rank data sample: \n%s", df.to_string(max_rows=100))
