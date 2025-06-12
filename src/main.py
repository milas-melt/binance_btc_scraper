from pathlib import Path
import pandas as pd

from src.data.fast_extraction import get_binance_data
from src.data.btc_data_preprocessing import preprocess_btc_data
from src.config import (
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
    SYMBOL,
)


def save_data(df: pd.DataFrame, output_file: Path):
    """Save data to CSV."""
    df.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")
    return output_file


def process_binance_data():
    """Download, save, and preprocess Binance BTCUSDT data."""
    raw_btc_file = RAW_DATA_DIR / f"{SYMBOL}_raw.csv"
    processed_btc_file = PROCESSED_DATA_DIR / "BTCUSDT_Preprocessed.csv"

    # Download and save raw Binance data
    df_btc = get_binance_data(start_date="2017-12-17", end_date="2024-12-06")
    save_data(df_btc, raw_btc_file)

    # Preprocess the Binance data
    preprocess_btc_data(raw_btc_file, processed_btc_file)

def main():
    # Process Binance BTCUSDT data
    process_binance_data()


if __name__ == "__main__":
    main()
