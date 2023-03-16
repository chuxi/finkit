import pytest

from finkit import utils
from datetime import date
from pyecharts.charts import Line
import pyecharts.options as opts
import pandas as pd
import numpy as np
import sqlalchemy as sal
import toml

utils.logging_config("../logging.yml")


def test_pandas_transform():
    engine = _get_engine()
    df = pd.read_sql_query("""select date, symbol, [close] from future_daily_kline 
        where (symbol like 'RB%01' or symbol like 'RB%05' or symbol like 'RB%10') and
         date >= '2021-01-01' and date < '2022-10-01' order by date""", con=engine)

    def month_diff_func(x):
        xdf = x.sort_values(by="symbol", ascending=True)
        # close_price = np.mean(x['close'])
        diff1 = xdf.iloc[0]['close'] - xdf.iloc[1]['close']
        diff2 = xdf.iloc[1]['close'] - xdf.iloc[2]['close']
        return pd.Series([diff1, diff2], index=["diff1", "diff2"])

    result = df.groupby(['date']).apply(month_diff_func).reset_index()
    print(result.to_string())


def _get_engine():
    config = toml.load("../config.toml")
    database: dict = config.get("database")
    url = sal.engine.URL.create(database.get("driver"), database.get("username"),
                                database.get("password"), database.get("host"),
                                database.get("port"), database.get("database"))
    return sal.create_engine(url)
