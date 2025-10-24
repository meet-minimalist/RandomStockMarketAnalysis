"""
# @ Author: Meet Patel
# @ Create Time: 2025-10-24 09:29:42
# @ Modified by: Meet Patel
# @ Modified time: 2025-10-24 14:01:43
# @ Description:
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import logging
from nseconnect.ua import Session
from utils import get_volume_deliverable_data

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class NSEDataDownloader:
    def __init__(self, base_dir="nse_data"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        self.session = Session()

    def make_request(self, url, params=None, operation_name=""):
        """
        Make HTTP request with payload.
        """
        try:
            req = requests.Request("GET", url, params=params)
            prepared = req.prepare()
            response = self.session.fetch(prepared.url)
            response.raise_for_status()

            # Check if response contains valid data
            data = response.json()
            if data is None:
                raise ValueError(f"Empty response data for {operation_name}")

            return data

        except Exception as e:
            logger.exception(f"Exception occured: {e}")

        return None

    def download_symbol_data(self, symbol, start_year=None, end_year=None):
        """
        Robust version of download with comprehensive error handling and recovery
        """
        # Get listing date for intelligent downloading
        listing_date = self.get_listing_date(symbol)

        if start_year is None:
            if listing_date:
                start_date_str = listing_date.strftime("%d-%m-%Y")
                logger.info(
                    f"Using listing date {listing_date} as start date for {symbol}."
                )
            else:
                start_date_str = datetime(2000, 1, 1).strftime(
                    "%d-%m-%Y"
                )  # Very early start as fallback
                logger.warning(f"Using fallback start year {start_year} for {symbol}.")
        else:
            start_date_str = datetime(start_year, 1, 1).strftime(
                "%d-%m-%Y"
            )  # Very early start as fallback

        if end_year is None:
            end_date_str = datetime.now().strftime("%d-%m-%Y")
        else:
            end_date_str = datetime(end_year, 12, 31).strftime("%d-%m-%Y")

        # Check if we already have complete data for this year
        if self.is_data_complete(symbol):
            logger.info(f"Using cached complete data for {symbol}.")
            all_data = self.load_from_csv(symbol)
        else:
            logger.warning(
                f"Using start date as {start_date_str} and end date as {end_date_str} for {symbol}."
            )
            all_data = get_volume_deliverable_data(symbol, start_date_str, end_date_str)
            self.save_to_csv(symbol, all_data)
        return all_data

    def is_data_complete(self, symbol):
        """
        Check if we already have complete data for a year
        Basic check - you can enhance this with more sophisticated logic
        """
        csv_path = self.get_csv_file_path(symbol)

        if not csv_path.exists():
            return False

        try:
            df = self.read_csv(csv_path)
            latest_date = df["Date"].max()
            # If we have data from yesterday or today, consider it complete
            if latest_date.date() >= (datetime.now().date() - timedelta(days=2)):
                return True
            return False

        except Exception as e:
            logger.warning(f"Error checking data completeness for {symbol}: {e}")
            return False

    def get_listing_date(self, symbol):
        """Get listing date with retry logic"""
        url = "https://www.nseindia.com/api/equity-meta-info"
        params = {"symbol": symbol}

        try:
            data = self.make_request(url, params, f"listing_date_{symbol}")
            if data and "listingDate" in data:
                return datetime.strptime(data["listingDate"], "%Y-%m-%d").date()
        except Exception as e:
            logger.error(f"Failed to get listing date for {symbol}: {e}")

        return None

    def get_csv_file_path(self, symbol):
        """Get CSV file path for a symbol and year"""
        return self.base_dir / f"{symbol}.csv"

    def save_to_csv(self, symbol: str, data: pd.DataFrame):
        """Save data to CSV file"""
        if data.empty:
            return False

        csv_path = self.get_csv_file_path(symbol)

        try:
            # Save to CSV
            data.to_csv(csv_path, index=False)
            logger.info(f"Saved {len(data)} records for {symbol} to {csv_path}")
            return True

        except Exception as e:
            logger.error(f"Error saving CSV for {symbol}: {e}")
            return False

    def read_csv(self, csv_path):
        df = pd.read_csv(csv_path)
        df["Date"] = pd.to_datetime(df["Date"])
        return df

    def load_from_csv(self, symbol):
        """Load data from CSV file if exists"""
        csv_path = self.get_csv_file_path(symbol)

        if csv_path.exists():
            try:
                df = self.read_csv(csv_path)
                logger.info(f"Loaded {len(df)} records for {symbol} from CSV")
                return df
            except Exception as e:
                logger.error(f"Error loading CSV for {symbol}: {e}")

        return pd.DataFrame()

    def update_symbol_data(self, symbol):
        """
        Update data for a symbol - download only missing data
        """
        current_year = datetime.now().year
        latest_date_in_csv = None

        # Find the latest date we have in CSV
        csv_path = self.get_csv_file_path(symbol)

        if csv_path.exists():
            df = self.read_csv(csv_path)
            latest_date_in_csv = df["Date"].max()

        # If no data exists or data is old, download from beginning of current year
        if latest_date_in_csv is None or latest_date_in_csv.year < current_year:
            start_date = datetime(current_year, 1, 1)
        else:
            start_date = latest_date_in_csv + timedelta(days=1)

        end_date = datetime.now() - timedelta(days=1)

        if start_date <= end_date:
            start_date_str = start_date.strftime("%d-%m-%Y")
            end_date_str = end_date.strftime("%d-%m-%Y")
            logger.info(f"Updating {symbol} from {start_date_str} to {end_date_str}")
            new_data = get_volume_deliverable_data(symbol, start_date_str, end_date_str)

            if not new_data.empty:
                # Append to existing CSV or create new
                existing_data = self.load_from_csv(symbol)
                all_data = (
                    pd.concat([existing_data, new_data])
                    .drop_duplicates(subset=["Date"])
                    .sort_values(by="Date")
                )
                self.save_to_csv(symbol, all_data)
                return new_data

        logger.info(f"{symbol} is already up to date")
        return []


if __name__ == "__main__":
    # Example 1: Download data for a single symbol
    downloader = NSEDataDownloader()

    # Download complete history for CUPID
    cupid_data = downloader.download_symbol_data("NESTLEIND", 2000, 2025)
    print(f"Downloaded {len(cupid_data)} records for CUPID")

    # downloader.update_symbol_data('CUPID')
    # print(f"Updated records for CUPID")
