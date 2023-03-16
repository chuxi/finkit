"""
statistic all account profit
"""
import logging
import os
import pandas as pd
from datetime import datetime, date

ACCOUNT_DAILY_PROFIT_HEADER = [
    "account", "account_name", "date",
    "start_balance", "end_balance", "available_balance",
    "pnl", "market_pnl", "fee", "margin", "total_value"
]

ACCOUNT_DAILY_POSITION_HEADER = [
    "date", "symbol", "account", "delivery_date", "long_vol", "long_avg_price",
    "short_vol", "short_avg_price", "pre_settle", "settle", "market_pnl", "margin"
]

logger = logging.getLogger(__name__)


def parse_account_daily(mydate: date,
                        data_dir: str = "./data"):
    """
    iterate all the files in data_dir
    :param mydate: 待处理数据日期
    :param date_dir: list the files under data dir
    :return: account
    """
    mydate_str = mydate.strftime("%Y%m%d")
    datetime.now().date()
    file_dir = os.path.join(data_dir, mydate_str)
    accounts = []
    positions = []
    if not os.path.exists(file_dir):
        logger.debug("file dir not exists: %s", file_dir)
        return accounts, positions
    files = [file for file in os.listdir(file_dir) if file.endswith(".txt")]
    files = [os.path.join(file_dir, file) for file in files]
    for file in files:
        mode = 0
        account = {"date": mydate}
        accounts.append(account)
        with open(file, encoding="gb2312") as fp:
            for rl in fp.readlines():
                line = rl.strip("\r\n ")
                if line.startswith("交易核算单"):
                    mode = 1
                elif line.startswith("资金状况"):
                    mode = 2
                elif line.startswith("成交记录"):
                    mode = 3
                elif line.startswith("持仓汇总"):
                    mode = 4
                else:
                    if mode == 1:
                        if line.startswith("客户号"):
                            parts = [s for s in line.split(" ") if s]
                            account["account"] = parts[0][4:].strip()
                            account["account_name"] = parts[1][5:].strip()
                    elif mode == 2:
                        if line.startswith("期初结存"):
                            parts = [s for s in line.split("  ") if s]
                            account["start_balance"] = float(parts[1])
                        elif line.startswith("出 入 金"):
                            parts = [s for s in line.split("  ") if s]
                            account["end_balance"] = float(parts[3])
                            account["available_balance"] = float(parts[5])
                        elif line.startswith("平仓盈亏"):
                            parts = [s for s in line.split("  ") if s]
                            account["pnl"] = float(parts[1])
                        elif line.startswith("持仓盯市盈亏"):
                            parts = [s for s in line.split("  ") if s]
                            account["market_pnl"] = float(parts[1])
                        elif line.startswith("手 续 费"):
                            parts = [s for s in line.split("  ") if s]
                            account["fee"] = float(parts[1])
                            account["margin"] = float(parts[3])
                        elif line.startswith("市值权益"):
                            parts = [s for s in line.split("  ") if s]
                            account["total_value"] = float(parts[1])
                    elif mode == 4:
                        if line.startswith("|"):
                            parts = [s.strip() for s in line.split("|") if s]
                            if len(parts) == 11 and parts[0][0].isascii():
                                positions.append({
                                    "symbol": parts[0],
                                    "account": account["account"],
                                    "date": mydate,
                                    "delivery_date": datetime.strptime(parts[1], '%Y%m%d'),
                                    "long_vol": int(parts[2]),
                                    "long_avg_price": float(parts[3].replace(",", "")),
                                    "short_vol": int(parts[4]),
                                    "short_avg_price": float(parts[5].replace(",", "")),
                                    "pre_settle": float(parts[6].replace(",", "")),
                                    "settle": float(parts[7].replace(",", "")),
                                    "market_pnl": float(parts[8]),
                                    "margin": float(parts[9]),
                                })
    return accounts, positions


def get_account_daily(mydate: date,
                      data_dir: str = "./data"):
    accounts_data, positions_data = parse_account_daily(mydate, data_dir)
    account_df = pd.DataFrame.from_records(accounts_data, columns=ACCOUNT_DAILY_PROFIT_HEADER)
    position_df = pd.DataFrame.from_records(positions_data, columns=ACCOUNT_DAILY_POSITION_HEADER)
    return account_df, position_df

