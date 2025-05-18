#!/usr/bin/env python3
"""
NSE Historical Data Downloader

This script downloads daily OHLC (Open-High-Low-Close) data for:
- A specific stock (if provided) OR
- All stocks in the NSE NIFTY 50 index
from Yahoo Finance. The data includes volume and adjusted close prices.

Features:
- Downloads data for a user-specified date range
- Supports single stock or all index stocks download
- Configurable download threads for parallel processing
- Customizable output directory naming
- Saves each stock's data as a separate CSV file
- Progress tracking with tqdm

Usage examples:
    # Download all NIFTY 50 stocks with 8 threads
    python nse_ohlc_downloader.py --start 2015-01-01 --end 2025-01-01 --threads 8

    # Download specific stock with default threads (4)
    python nse_ohlc_downloader.py --stock RELIANCE --start 2015-01-01 --end 2025-01-01

Dependencies:
    nseconnect, yfinance, pandas, tqdm
"""

import argparse
from nseconnect import Nse
import yfinance as yf
import pandas as pd
from tqdm import tqdm
import os


def parse_arguments():
    """Parse command line arguments for the script."""
    parser = argparse.ArgumentParser(
        description="Download NIFTY 50 stock historical data"
    )
    parser.add_argument(
        "--stock", type=str, help="Specific stock symbol to download (e.g. RELIANCE)"
    )
    parser.add_argument(
        "--start",
        type=str,
        default="2015-01-01",
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end", type=str, default="2025-01-01", help="End date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--interval",
        type=str,
        default="1d",
        choices=["1d", "1wk", "1mo"],  # TODO: Add more choices based on yfinance
        help="Data interval (daily, weekly, monthly)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="Number of threads for parallel downloads (default: 4)",
    )
    parser.add_argument(
        "--output", type=str, help="Output directory path (default: auto-generated)"
    )
    return parser.parse_args()


def main():
    args = parse_arguments()

    index = "NIFTY 50"

    # Set up output directory
    if args.output:
        output_dir = args.output
    else:
        if args.stock:
            output_dir = f"./{args.stock}_{args.start.replace('-', '_')}_{args.end.replace('-', '_')}"
        else:
            output_dir = f"./{index.replace(' ', '_')}_{args.start.replace('-', '_')}_{args.end.replace('-', '_')}"

    os.makedirs(output_dir, exist_ok=True)

    # Initialize NSE connection
    nse = Nse()

    if args.stock:
        # Download single stock
        ticker = f"{args.stock}.NS"
        tickers = [ticker]
        print(f"Downloading data for single stock: {args.stock}")
        print(f"Date range: {args.start} to {args.end}")
    else:
        # Download all NIFTY 50 stocks
        stock_list = nse.get_stocks_in_index(index=index)
        print(f"Downloading data for all {len(stock_list)} stocks in {index}")
        print(f"Date range: {args.start} to {args.end}")

        # Prepare ticker symbols for Yahoo Finance (NSE stocks end with .NS)
        tickers = [f"{stock}.NS" for stock in stock_list]

    # Download historical data using yfinance
    print(f"Using {args.threads} threads for download...")
    data = yf.download(
        tickers,
        start=args.start,
        end=args.end,
        threads=args.threads,
        group_by="ticker",
        interval=args.interval,
    )

    # Save data to CSV files
    print(f"\nSaving data to {output_dir}...")
    for ticker in tqdm(tickers, desc="Processing"):
        try:
            df = data[ticker].copy()
            csv_path = os.path.join(output_dir, f"{ticker.replace('.NS', '')}_data.csv")
            df.to_csv(csv_path)
        except KeyError:
            print(f"\nWarning: No data available for {ticker}")

    print("\nDownload complete!")
    if args.stock:
        print(f"Data saved for {args.stock} at: {os.path.abspath(output_dir)}")
    else:
        print(f"Data saved for {len(tickers)} stocks at: {os.path.abspath(output_dir)}")


if __name__ == "__main__":
    main()
