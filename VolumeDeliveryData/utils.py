"""
# @ Author: Meet Patel
# @ Create Time: 2025-10-24 09:44:25
# @ Modified by: Meet Patel
# @ Modified time: 2025-10-24 13:33:50
# @ Description:
"""

import os
import requests
import pandas as pd
import logging
from nseconnect import Nse
from datetime import datetime
import datetime as dt
from nselib.libutil import (
    validate_date_param,
    cleaning_nse_symbol,
    derive_from_and_to_date,
)
from nselib.constants import (
    price_volume_and_deliverable_position_data_columns,
    dd_mm_yyyy,
)


logger = logging.getLogger(__name__)

default_header = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
}

header = {
    "referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Sec-Fetch-User": "?1",
    "Accept": "ext/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-Mode": "navigate",
    "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
}


index_dict = {
    "nifty50": "NIFTY 50",
    "nifty_next_50": "NIFTY NEXT 50",
    "nifty100": "NIFTY 100",
    "nifty200": "NIFTY 200",
    "nifty500": "NIFTY 500",
    "nifty_midcap_50": "NIFTY MIDCAP 50",
    "nifty_midcap_100": "NIFTY MIDCAP 100",
    "nifty_midcap_150": "NIFTY MIDCAP 150",
    "nifty_smallcap_50": "NIFTY SMLCAP 50",
    "nifty_smallcap_100": "NIFTY SMLCAP 100",
    "nifty_smallcap_250": "NIFTY SMLCAP 250",
    "nifty_mid_smallcap_400": "NIFTY MIDSML 400",
    "nifty_microcap_250": "NIFTY MICROCAP250",
}


def get_list_of_nse_stocks():
    nse = Nse()
    list_of_stocks = nse.get_stock_codes()
    logger.info(f"Fetched {len(list_of_stocks)} stocks from NSE.")
    return list_of_stocks


def get_nifty50_stocks():
    nse = Nse()
    nifty50_stocks = nse.get_stocks_in_index(index="NIFTY 50")
    return nifty50_stocks


def get_nifty_index_stocks(index_str):
    nse = Nse()
    if index_str not in index_dict:
        raise RuntimeError("Invalid index name")
    index_name = index_dict[index_str]
    try:
        index_stocks = nse.get_stocks_in_index(index=index_name)
        return index_stocks
    except Exception as e:
        raise RuntimeError(f"Unable to fetch data due to exception: {e}.")


def nse_urlfetch(url, origin_url="http://nseindia.com"):
    r_session = requests.session()
    nse_live = r_session.get(origin_url, headers=default_header)
    cookies = nse_live.cookies
    return r_session.get(url, headers=header, cookies=cookies)


def get_price_volume_and_deliverable_position_data(
    symbol: str, from_date: str, to_date: str, tmp_file: str
):
    origin_url = "https://nsewebsite-staging.nseindia.com/report-detail/eq_security"
    url = (
        "https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData?"
    )
    payload = f"from={from_date}&to={to_date}&symbol={symbol}&type=priceVolumeDeliverable&series=ALL&csv=true"
    try:
        data_text = nse_urlfetch(url + payload, origin_url=origin_url).text
        data_text = data_text.replace("\x82", "").replace("â¹", "In Rs")
        with open(tmp_file, "w") as f:
            f.write(data_text)
        f.close()
    except Exception as e:
        raise RuntimeError(f" Resource not available. Exception: {e}")
    data_df = pd.read_csv(tmp_file)
    os.remove(tmp_file)
    data_df.columns = [name.replace(" ", "") for name in data_df.columns]
    return data_df


def price_volume_and_deliverable_position_data(
    symbol: str,
    from_date: str = None,
    to_date: str = None,
    period: str = None,
    tmp_file: str = "temp.csv",
):
    """
    get Security wise price volume & Deliverable position data set. use get_nse_symbols() to get all symbols
    :param symbol: symbol eg: 'SBIN'
    :param from_date: '17-03-2022' ('dd-mm-YYYY')
    :param to_date: '17-06-2023' ('dd-mm-YYYY')
    :param period: use one {'1D': last day data,'1W': for last 7 days data,
                            '1M': from last month same date, '6M': last 6 month data, '1Y': from last year same date)
    :return: pandas.DataFrame
    :raise ValueError if the parameter input is not proper
    """
    validate_date_param(from_date, to_date, period)
    symbol = cleaning_nse_symbol(symbol=symbol)
    from_date, to_date = derive_from_and_to_date(
        from_date=from_date, to_date=to_date, period=period
    )
    nse_df = pd.DataFrame(columns=price_volume_and_deliverable_position_data_columns)
    from_date = datetime.strptime(from_date, dd_mm_yyyy)
    to_date = datetime.strptime(to_date, dd_mm_yyyy)
    load_days = (to_date - from_date).days
    while load_days > 0:
        if load_days > 365:
            end_date = (from_date + dt.timedelta(364)).strftime(dd_mm_yyyy)
            start_date = from_date.strftime(dd_mm_yyyy)
        else:
            end_date = to_date.strftime(dd_mm_yyyy)
            start_date = from_date.strftime(dd_mm_yyyy)
        data_df = get_price_volume_and_deliverable_position_data(
            symbol=symbol, from_date=start_date, to_date=end_date, tmp_file=tmp_file
        )
        from_date = from_date + dt.timedelta(365)
        load_days = (to_date - from_date).days
        if not data_df.empty and not data_df.isna().all().all():
            nse_df = pd.concat([nse_df, data_df], ignore_index=True)

    nse_df["TotalTradedQuantity"] = pd.to_numeric(
        nse_df["TotalTradedQuantity"].str.replace(",", ""), errors="coerce"
    )
    nse_df["TurnoverInRs"] = pd.to_numeric(
        nse_df["TurnoverInRs"].str.replace(",", ""), errors="coerce"
    )
    nse_df["No.ofTrades"] = pd.to_numeric(
        nse_df["No.ofTrades"].str.replace(",", ""), errors="coerce"
    )
    nse_df["DeliverableQty"] = pd.to_numeric(
        nse_df["DeliverableQty"].str.replace(",", ""), errors="coerce"
    )
    return nse_df


def get_volume_deliverable_data(symbol, start_date_str, end_date_str):
    def convert_to_float(data, column):
        if data[column].dtype == "object":
            data = data[~data[column].astype(str).str.contains("-", na=False)]
            data = data.copy()
            data[column] = data[column].astype(str).str.replace(",", "").astype(float)
        return data

    tmp_file = f"{symbol}_temp.csv"
    data = price_volume_and_deliverable_position_data(
        symbol, start_date_str, end_date_str, tmp_file=tmp_file
    )
    data["Symbol"] = data["Symbol"].astype(str)
    data["Series"] = data["Series"].astype(str)
    data = convert_to_float(data, "PrevClose")
    data = convert_to_float(data, "OpenPrice")
    data = convert_to_float(data, "HighPrice")
    data = convert_to_float(data, "LowPrice")
    data = convert_to_float(data, "LastPrice")
    data = convert_to_float(data, "ClosePrice")
    data = convert_to_float(data, "AveragePrice")
    data = convert_to_float(data, "%DlyQttoTradedQty")
    data["DeliverableQty"] = data["DeliverableQty"].astype("int64")
    data["Date"] = pd.to_datetime(data["Date"], format="%d-%b-%Y")
    return data
