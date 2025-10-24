'''
 # @ Author: Meet Patel
 # @ Create Time: 2025-10-24 09:36:23
 # @ Modified by: Meet Patel
 # @ Modified time: 2025-10-24 18:41:15
 # @ Description:
 '''

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from fetch_vol_delivery_data import NSEDataDownloader, logger
import datetime
from datetime import datetime, timedelta


class BulkNSEDownloader:
    def __init__(self, base_dir="nse_data"):
        self.downloader = NSEDataDownloader(base_dir)
        self.default_start_year = 2000

    def download_all_symbols(self, symbols=None, max_workers=3):
        """
        Bulk download with comprehensive error handling and recovery
        """
        if symbols is None:
            symbols = self.get_nifty_500_symbols()

        logger.info(f"Starting download for {len(symbols)} symbols")

        # Download symbols in parallel
        successful, failed = self.parallel_download(symbols, max_workers)

        logger.info(
            f"Download completed: {len(successful)} successful, {len(failed)} failed"
        )

        # Generate detailed report
        self.generate_download_report(successful, failed)

        return successful, failed

    def update_all_symbols(self, symbols=None, max_workers=3):
        """
        Update data for all symbols - only download missing data
        Efficiently updates each symbol by downloading only the most recent data
        """
        if symbols is None:
            symbols = self.get_nifty_500_symbols()

        logger.info(f"Starting update for {len(symbols)} symbols")

        updated_symbols = []
        failed_symbols = []
        no_update_needed = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {
                executor.submit(self._update_single_symbol, symbol): symbol
                for symbol in symbols
            }

            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    if result["updated"]:
                        updated_symbols.append(symbol)
                        logger.info(
                            f"✅ Updated {symbol} with {result['new_records']} new records"
                        )
                    else:
                        if result["error"]:
                            failed_symbols.append(symbol)
                            logger.error(
                                f"❌ Failed to update {symbol}: {result['error']}"
                            )
                        else:
                            no_update_needed.append(symbol)
                            logger.info(f"ℹ️ {symbol} already up to date")

                except Exception as e:
                    failed_symbols.append(symbol)
                    logger.error(f"❌ Unexpected error updating {symbol}: {e}")

                # Rate limiting
                time.sleep(2)

        self._generate_update_report(updated_symbols, failed_symbols, no_update_needed)
        return updated_symbols, failed_symbols, no_update_needed

    def _update_single_symbol(self, symbol):
        """
        Update data for a single symbol - only download missing data
        """
        try:
            # Get the latest date we have in CSV files
            latest_date = self._get_latest_date_in_csv(symbol)
            current_date = datetime.now().date()

            # If no data exists, we need to download everything
            if latest_date is None:
                logger.info(
                    f"No existing data found for {symbol}, downloading complete history"
                )
                data = self.downloader.download_symbol_data(symbol)
                return {
                    "updated": True,
                    "new_records": len(data) if data else 0,
                    "error": None,
                }

            # If data is already up to date (within 1 day)
            if latest_date >= current_date - timedelta(days=1):
                return {"updated": False, "new_records": 0, "error": None}

            # Calculate start date for update (day after latest date we have)
            start_date = latest_date + timedelta(days=1)
            end_date = current_date - timedelta(days=1)  # Yesterday

            # Skip if start date is in future
            if start_date > end_date:
                return {"updated": False, "new_records": 0, "error": None}

            logger.info(f"Updating {symbol} from {start_date} to {end_date}")

            # Download only the missing data
            new_data = self.downloader.update_symbol_data(
                symbol,
                datetime.combine(start_date, datetime.min.time()),
                datetime.combine(end_date, datetime.min.time()),
            )

        except Exception as e:
            return {"updated": False, "new_records": 0, "error": str(e)}

    def _get_latest_date_in_csv(self, symbol):
        """
        Find the latest date we have in CSV files for a symbol
        """
        symbol_dir = self.downloader.get_symbol_directory(symbol)
        csv_files = list(symbol_dir.glob("*.csv"))

        if not csv_files:
            return None

        latest_date = None

        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                if not df.empty and "mTIMESTAMP" in df.columns:
                    df["mTIMESTAMP"] = pd.to_datetime(df["mTIMESTAMP"])
                    file_latest_date = df["mTIMESTAMP"].max().date()

                    if latest_date is None or file_latest_date > latest_date:
                        latest_date = file_latest_date
            except Exception as e:
                logger.warning(f"Error reading {csv_file}: {e}")
                continue

        return latest_date

    def _generate_update_report(self, updated, failed, no_update):
        """Generate a detailed update report"""
        report_path = self.downloader.base_dir / "update_report.txt"

        with open(report_path, "w") as f:
            f.write(f"NSE Data Update Report - {datetime.now()}\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Updated symbols: {len(updated)}\n")
            f.write(f"Failed symbols: {len(failed)}\n")
            f.write(f"Already up-to-date: {len(no_update)}\n\n")

            f.write("Updated symbols:\n")
            for symbol in sorted(updated):
                f.write(f"  - {symbol}\n")

            f.write("\nFailed symbols:\n")
            for symbol in sorted(failed):
                f.write(f"  - {symbol}\n")

            f.write("\nAlready up-to-date symbols:\n")
            for symbol in sorted(no_update):
                f.write(f"  - {symbol}\n")

        logger.info(f"Update report saved to {report_path}")

    def parallel_download(self, symbols, max_workers, delay=2):
        """Download phase with threading"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        successful = []
        failed = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {
                executor.submit(
                    self.downloader.download_symbol_data,
                    symbol,
                    self.default_start_year,
                ): symbol
                for symbol in symbols
            }

            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    data = future.result()
                    if not data.empty:
                        successful.append(symbol)
                        logger.info(f"✅ {symbol}: {len(data)} records")
                    else:
                        failed.append(symbol)
                        logger.warning(f"⚠️ No data for {symbol}")
                except Exception as e:
                    failed.append(symbol)
                    logger.error(f"❌ Failed {symbol}: {e}")

                time.sleep(delay)

        return successful, failed

    def generate_download_report(self, successful, failed):
        """Generate a detailed download report"""
        report_path = self.downloader.base_dir / "download_report.txt"

        with open(report_path, "w") as f:
            f.write(f"NSE Data Download Report - {datetime.now()}\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Successful downloads: {len(successful)}\n")
            f.write(f"Failed downloads: {len(failed)}\n\n")

            f.write("Successful symbols:\n")
            for symbol in sorted(successful):
                f.write(f"  - {symbol}\n")

            f.write("\nFailed symbols:\n")
            for symbol in sorted(failed):
                f.write(f"  - {symbol}\n")

        logger.info(f"Download report saved to {report_path}")


if __name__ == "__main__":
    from utils import get_nifty50_stocks

    nifty50_symbols = get_nifty50_stocks()
    bulk_downloader = BulkNSEDownloader()
    successful, failed = bulk_downloader.download_all_symbols(
        nifty50_symbols, max_workers=10
    )

    # bulk_downloader = BulkNSEDownloader()
    # updated_symbols = bulk_downloader.update_all_symbols(nifty50_symbols, max_workers=3)
    # print(f"Updated {len(updated_symbols)} symbols")
