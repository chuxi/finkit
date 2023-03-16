import pytest

from finkit import utils
from finkit.collector import account_source
from datetime import date, timedelta
import pandas as pd

utils.logging_config("../logging.yml")


def test_get_account_daily():
    start_date = date(2023, 2, 10)
    end_date = date(2023, 2, 22)
    accounts = []
    positions = []
    while start_date <= end_date:
        df1, df2 = account_source.get_account_daily(mydate=start_date)
        accounts.append(df1)
        positions.append(df2)
        start_date = start_date + timedelta(days=1)
    accounts_df = pd.concat(accounts, axis=0)
    positions_df = pd.concat(positions, axis=0)

    print(accounts_df.to_string())
    print(positions_df.to_string())
