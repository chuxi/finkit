import json
import logging
import urllib
import zipfile
from functools import reduce

import pandas as pd
import akshare as ak
from datetime import date, datetime
from io import BytesIO, StringIO
from urllib import request, parse
from urllib.error import HTTPError
from .future_constants import *
from .. import utils

logger = logging.getLogger(__name__)


def get_dce_daily_vol_rank(mydate: date):
    form_data = {
        "year": mydate.year,
        "month": mydate.month - 1,
        "day": mydate.day,
        "batchExportFlag": "batch"
    }
    try:
        with request.urlopen(DCE_DAILY_VOLUME_URL,
                data=urllib.parse.urlencode(form_data).encode()) as fp, \
                zipfile.ZipFile(BytesIO(fp.read())) as zfile:
            trading_volumes = []
            for filename in zfile.namelist():
                instrument = filename.split("_")[1]
                volumes = []
                # total_volumes = []
                # if instrument != "i2301":
                #     continue
                # logger.info("processing instrument: %s", instrument)
                for line in zfile.read(filename).splitlines():
                    parts = line.decode("utf-8").split()
                    if len(parts) >= 3:
                        if parts[0].isdigit():
                            volumes.append({
                                "rank": int(parts[0]),
                                "part_name": parts[1],
                                "volume": int(parts[2].replace(",", "")),
                                "change": int(parts[3].replace(",", ""))
                            })
                batch_size = int(len(volumes) / 3)
                if batch_size * 3 == len(volumes):
                    for i in range(batch_size):
                        trading_volumes.append({
                            "date": mydate,
                            "symbol": instrument,
                            "rank": volumes[i]["rank"],
                            "member_name": volumes[i]["part_name"],
                            "trading_vol": volumes[i]["volume"],
                            "trading_vol_chg": volumes[i]["change"],
                            "long_member_name": volumes[i + batch_size]["part_name"],
                            "long_vol": volumes[i + batch_size]["volume"],
                            "long_vol_chg": volumes[i + batch_size]["change"],
                            "short_member_name": volumes[i + batch_size * 2]["part_name"],
                            "short_vol": volumes[i + batch_size * 2]["volume"],
                            "short_vol_chg": volumes[i + batch_size * 2]["change"]
                        })
                else:
                    logger.error("dce daily volume rank process failed for %s", filename)
            return trading_volumes
    except zipfile.BadZipFile as e:
        logger.error("dce: failed to parse zip file")
    except Exception as e:
        logger.error("failed to download dce daily volume", e)


def get_czce_daily_vol_rank(mydate: date):
    url = CZCE_DAILY_VOLUME_URL.format(mydate.year, mydate.strftime("%Y%m%d"))

    def extract_num(e: str) -> int:
        s = e.lstrip("|")
        if s == "-":
            return 0
        else:
            return int(s.replace(",", ""))

    try:
        with request.urlopen(url) as fp:
            instrument = ""
            volumes = []
            for line in fp:
                parts = line.decode("utf-8").split()
                if len(parts) > 1:
                    if parts[0].isdigit():
                        volumes.append({
                            "date": mydate,
                            "symbol": instrument,
                            "rank": parts[0],
                            "member_name": parts[1].lstrip("|"),
                            "trading_vol": extract_num(parts[2]),
                            "trading_vol_chg": extract_num(parts[3]),
                            "long_member_name": parts[4].lstrip("|"),
                            "long_vol": extract_num(parts[5]),
                            "long_vol_chg": extract_num(parts[6]),
                            "short_member_name": parts[7].lstrip("|"),
                            "short_vol": extract_num(parts[8]),
                            "short_vol_chg": extract_num(parts[9])
                        })
                    elif parts[0].startswith("品种：") or parts[0].startswith("合约："):
                        instrument = parts[0].split("：")[1]
            return volumes
    except HTTPError as e:
        logger.error("czce failed to download, code: %s", e.code)
    except Exception as e:
        logger.error("czce daily volume rank failed", e)


def get_shfe_daily_vol_rank(mydate: date):
    url = SHFE_DAILY_VOLUME_URL.format(mydate.strftime("%Y%m%d"))
    try:
        with request.urlopen(url) as f:
            body = f.read()
            data = json.loads(body.decode("utf-8"))["o_cursor"]
            if data is None or not isinstance(data, list):
                logger.error("failed to get data, response: %s", body)
                return
            volume_data = []
            for el in data:
                if el["RANK"] == 999 or el["RANK"] == 0 or el["RANK"] == -1:
                    continue
                volume_data.append({
                    "date": mydate,
                    "symbol": el["INSTRUMENTID"].strip(),
                    "rank": el["RANK"],
                    "member_name": el["PARTICIPANTABBR1"].strip(),
                    # "part_id_1": el["PARTICIPANTID1"].strip(),
                    "trading_vol": el["CJ1"],
                    "trading_vol_chg": el["CJ1_CHG"],
                    "long_member_name": el["PARTICIPANTABBR2"].strip(),
                    # "part_id_2": el["PARTICIPANTID2"].strip(),
                    "long_vol": el["CJ2"],
                    "long_vol_chg": el["CJ2_CHG"],
                    "short_member_name": el["PARTICIPANTABBR3"].strip(),
                    # "part_id_3": el["PARTICIPANTID3"].strip(),
                    "short_vol": el["CJ3"],
                    "short_vol_chg": el["CJ3_CHG"]
                })
            return volume_data
    except HTTPError as e:
        if e.code == 404:
            logger.warning("current shfe daily volume not available, download later...")
        else:
            logger.error("failed to download shfe daily volume rank", e)


def get_cffex_daily_vol_rank(mydate: date):
    try:
        volumes = []
        date_string = mydate.strftime("%Y%m%d")
        for symbol in CFFEX_FUTURE_SYMBOLS:
            url = CFFEX_DAILY_VOLUME_URL.format(mydate.strftime("%Y%m/%d"), symbol)
            f = request.urlopen(url)
            for line in f.read().decode("gb18030").splitlines():
                if not line.startswith(date_string):
                    continue
                parts = line.split(",")
                if len(parts) == 12:
                    volumes.append({
                        "date": mydate,
                        "symbol": parts[1].strip(),
                        "rank": parts[2],
                        "member_name": parts[3].strip(),
                        "trading_vol": parts[4],
                        "trading_vol_chg": parts[5],
                        "long_member_name": parts[6].strip(),
                        "long_vol": parts[7],
                        "long_vol_chg": parts[8],
                        "short_member_name": parts[9].strip(),
                        "short_vol": parts[10],
                        "short_vol_chg": parts[11]})
        if len(volumes) > 0:
            return volumes
    except Exception as e:
        logger.error("failed to get cffex daily trading volume", e)


def get_daily_vol_rank(mydate: date):
    ds1 = get_cffex_daily_vol_rank(mydate)
    ds2 = get_czce_daily_vol_rank(mydate)
    ds3 = get_dce_daily_vol_rank(mydate)
    ds4 = get_shfe_daily_vol_rank(mydate)

    if ds1 is None or ds2 is None or ds3 is None or ds4 is None:
        return

    dataset = ds1 + ds2 + ds3 + ds4
    return pd.DataFrame.from_records(data=dataset, columns=DAILY_VOLUME_HEADER)


def get_daily_kline(mydate: date):
    funcs = [ak.get_shfe_daily, ak.get_dce_daily,
             ak.get_czce_daily, ak.get_gfex_daily,
             ak.get_cffex_daily]
    vars_df = read_variety_info()["unit"]
    dfs = []
    for f in funcs:
        try:
            tmp = f(mydate.strftime("%Y%m%d"))
            if tmp is None:
                logger.error("%s %s daily kline is not ready, return", mydate.isoformat(), f.__name__)
                return
            dfs.append(tmp)
        except Exception as e:
            logger.error("failed to get daily kline for date: %s, error: %s", mydate.isoformat(), e)
            return None

    # 2020-1-1前shfe交易量双边计算，统一成单边
    if mydate < date(2020, 1, 1):
        shfe_df = dfs[0]
        shfe_df[['volume', 'open_interest']] = shfe_df[['volume', 'open_interest']] / 2

    df = pd.concat(dfs, ignore_index=True)
    df = df[~df['symbol'].str.contains('-')]
    df = df[~df['symbol'].str.startswith('SC_TAS')]
    df = df.drop(['turnover'], axis=1)

    df['symbol'] = df['symbol'].astype(str)
    df['variety'] = df['variety'].astype(str).str.upper()
    df['date'] = df['date'].astype(str)
    df['date'] = pd.to_datetime(df['date'])
    df['open'] = pd.to_numeric(df['open'], errors='coerce')
    df['high'] = pd.to_numeric(df['high'], errors='coerce')
    df['low'] = pd.to_numeric(df['low'], errors='coerce')
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    df['settle'] = pd.to_numeric(df['settle'], errors='coerce')
    # df['turnover'] = pd.to_numeric(df['turnover'], errors='coerce')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce', downcast='integer')
    df['open_interest'] = pd.to_numeric(df['open_interest'], errors='coerce', downcast='integer')
    df['volume'].fillna(0, inplace=True)
    df = pd.merge(df, vars_df, how="left", left_on=["variety"], right_index=True)
    df['turnover'] = df['settle'] * df['volume'] * df['unit']
    df.drop(columns="unit", inplace=True)
    return df
