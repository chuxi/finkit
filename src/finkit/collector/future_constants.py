import pandas as pd
import os

DCE_DAILY_VOLUME_URL = "http://www.dce.com.cn/publicweb/quotesdata/exportMemberDealPosiQuotesBatchData.html"
CZCE_DAILY_VOLUME_URL = "http://www.czce.com.cn/cn/DFSStaticFiles/Future/{}/{}/FutureDataHolding.txt"
SHFE_DAILY_VOLUME_URL = "https://www.shfe.com.cn/data/dailydata/kx/pm{}.dat"
CFFEX_DAILY_VOLUME_URL = "http://www.cffex.com.cn/sj/ccpm/{}/{}_1.csv"
CFFEX_FUTURE_SYMBOLS = ["IF", "IC", "IM", "IH", "TS", "TF", "T"]
DAILY_VOLUME_HEADER = ["date", "symbol", "rank",
                       "member_name", "trading_vol", "trading_vol_chg",
                       "long_member_name", "long_vol", "long_vol_chg",
                       "short_member_name", "short_vol", "short_vol_chg"]


def read_variety_info():
    module_dir = os.path.dirname(os.path.dirname(__file__))
    variety_file = os.path.join(module_dir, "variety_info.csv")
    df = pd.read_csv(variety_file, index_col='variety')
    return df
