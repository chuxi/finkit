import pandas as pd


OPTION_DATAFRAME_INDEX = [ "strike_date", "last_price", "strike_price", "time_delta",
                           "ask_sigma", "bid_sigma", "call_ask_price", "call_bid_price",
                           "put_ask_price", "put_bid_price" ]


class Algorithm:

    def calc(self) -> pd.DataFrame:
        pass

