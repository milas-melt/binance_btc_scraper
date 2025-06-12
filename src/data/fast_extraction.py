import logging
import tempfile
import requests
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import time
import urllib3
import concurrent.futures
import threading
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from tqdm import tqdm

from src.config import BASE_URL, RAW_DATA_DIR, LOG_FILE, MAX_WORKERS, INTERVAL

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ======================== LOGGING SETUP ======================== #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ======================== HELPER CLASSES ======================== #


class Counter:
    """Thread-safe counter for tracking processed downloads."""

    def __init__(self):
        self.value = 0
        self.lock = threading.Lock()

    def increment(self):
        with self.lock:
            self.value += 1


# ======================== DATA DOWNLOAD ======================== #


def download_with_retries(
    url: str, max_retries: int = 5, backoff_factor: int = 2
) -> Optional[requests.Response]:
    """Download a file with retries and exponential backoff."""
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, verify=False, timeout=10)
            response.raise_for_status()
            return response
        except (
            requests.exceptions.RequestException,
            requests.exceptions.ConnectionError,
        ) as e:
            wait_time = backoff_factor**attempt
            log.warning(f"Attempt {attempt}/{max_retries} failed for {url}: {e}")
            if attempt < max_retries:
                log.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                log.error(f"Max retries reached for {url}")
    return None


def download_and_extract_data(date: datetime) -> Optional[pd.DataFrame]:
    """Download and extract data for a specific date."""
    filename = f"BTCUSDT-{INTERVAL}-{date:%Y-%m-%d}.zip"
    url = f"{BASE_URL}{filename}"

    columns = [
        "trading_datetime",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "ignore",
    ]

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = Path(temp_dir) / filename

            # Download with retry logic
            response = download_with_retries(url)
            if not response:
                log.warning(f"Failed to download after retries: {url}")
                return None

            with open(zip_path, "wb") as f:
                f.write(response.content)

            # Extract and read the CSV
            with zipfile.ZipFile(zip_path) as zip_ref:
                csv_name = zip_ref.namelist()[0]
                with zip_ref.open(csv_name) as csv_file:
                    df = pd.read_csv(
                        csv_file, header=None, names=columns, na_filter=False
                    )

                    # Convert timestamps
                    df["trading_datetime"] = pd.to_datetime(
                        df["trading_datetime"], unit="ms"
                    )
                    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

                    # Convert numeric columns
                    numeric_cols = [
                        "open_price",
                        "high_price",
                        "low_price",
                        "close_price",
                        "volume",
                        "quote_asset_volume",
                        "number_of_trades",
                        "taker_buy_base_volume",
                        "taker_buy_quote_volume",
                    ]
                    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)

                    df = df.drop("ignore", axis=1)
                    df["date"] = date.date()

                    return df

    except Exception as e:
        log.warning(f"Failed to process data for {date:%Y-%m-%d}: {str(e)}")
        return None


# ======================== DATA MANAGEMENT ======================== #


def get_binance_data(
    start_date: str = "2017-12-17", end_date: str = None
) -> pd.DataFrame:
    """Download and combine Binance data between start_date and end_date."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()

    log.info(f"Downloading data from {start:%Y-%m-%d} to {end:%Y-%m-%d}")

    dates = [start + timedelta(days=x) for x in range((end - start).days + 1)]
    processed_count, error_count, all_data = Counter(), 0, []

    start_time = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(download_and_extract_data, date): date for date in dates
        }

        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                df = future.result()
                if df is not None:
                    all_data.append(df)
                    processed_count.increment()
                else:
                    error_count += 1
            except Exception as e:
                error_count += 1
                log.error(f"Error processing {futures[future]}: {e}")

            if i % 10 == 0 or i == len(futures):
                elapsed_time = time.time() - start_time
                avg_rate = i / elapsed_time if elapsed_time > 0 else 0
                remaining = len(futures) - i
                eta = remaining / avg_rate if avg_rate > 0 else 0
                log.info(
                    f"[{i}/{len(futures)}] Downloaded {processed_count.value} files "
                    f"| Errors: {error_count} "
                    f"| Avg Rate: {avg_rate:.2f} files/sec "
                    f"| ETA: {eta:.2f} sec"
                )

    total_time = time.time() - start_time
    log.info(
        f"Download Complete | Total files: {len(futures)} | "
        f"Successful: {processed_count.value} | Errors: {error_count} | "
        f"Time Elapsed: {total_time:.2f} seconds"
    )

    if not all_data:
        log.warning("No data was downloaded.")
        return pd.DataFrame()

    final_df = pd.concat(all_data, ignore_index=True)
    log.info(f"Final dataset size: {len(final_df)} rows")

    return final_df
