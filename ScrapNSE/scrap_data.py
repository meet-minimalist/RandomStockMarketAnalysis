# This script downloads the 1 day interval data for all the stocks in NSE NIFTY 50 index.

from nseconnect import Nse
import yfinance as yf
import pandas as pd
from tqdm import tqdm
import os

index = "NIFTY 50"
start_date = "2015-01-01"
end_date = "2025-01-01"
interval = "1d"

output_dir = f"./{index.replace(' ', '_')}_{start_date.replace('-', '_')}_{end_date.replace('-', '_')}"
os.makedirs(output_dir, exist_ok=True)

nse = Nse()
stocks = nse.get_stock_codes()

# print(nse.get_index_list())

stock_list = nse.get_stocks_in_index(index=index)

print("Total stocks to parse: ", len(stock_list))

tickers = []
for stock in stock_list:
    tickers.append(stock + ".NS")  # NSE listed stocks end with .NS

data = yf.download(
    tickers,
    start=start_date,
    end=end_date,
    threads=4,
    group_by="ticker",
    interval=interval,
)

for ticker in tqdm(tickers):
    df = data[ticker].copy()
    csv_path = os.path.join(output_dir, f"{ticker.replace('.NS', '')}_data.csv")
    df.to_csv(csv_path)
    print(f"Saved: {ticker} → {csv_path}")
