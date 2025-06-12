import pandas as pd
from pathlib import Path
from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR


def preprocess_btc_data(input_file: Path, output_file: Path):
    """Preprocess BTCUSDT raw data and save the cleaned version."""

    # Read the raw CSV data
    btcusdt_df = pd.read_csv(input_file)

    # Convert timestamp columns to datetime
    for col in ["trading_datetime", "close_time"]:
        if pd.api.types.is_numeric_dtype(btcusdt_df[col]):
            btcusdt_df[col] = pd.to_datetime(btcusdt_df[col], unit="ms")
        else:
            btcusdt_df[col] = pd.to_datetime(btcusdt_df[col])

    # Rename price columns to lowercase
    btcusdt_df.rename(
        columns={
            "open_price": "open",
            "high_price": "high",
            "low_price": "low",
            "close_price": "close",
        },
        inplace=True,
    )

    # Add 'asset_name' column
    btcusdt_df["asset_name"] = "BTCUSDT"

    # Set 'trading_datetime' as the index
    btcusdt_df.set_index("trading_datetime", inplace=True)
    btcusdt_df.index.name = "timestamp"

    # Keep only necessary columns
    btcusdt_df = btcusdt_df[["asset_name", "open", "high", "low", "close", "volume"]]

    # Sort by index (timestamp)
    btcusdt_df.sort_index(inplace=True)

    # Save the preprocessed data
    btcusdt_df.to_csv(output_file, index=True)
    print(f"Preprocessed data saved to: {output_file}")
    print(f"Data shape: {btcusdt_df.shape}")


if __name__ == "__main__":
    # Define input and output paths
    input_csv = RAW_DATA_DIR / "BTCUSDT_raw.csv"
    output_csv = PROCESSED_DATA_DIR / "BTCUSDT_Preprocessed.csv"

    # Run preprocessing
    preprocess_btc_data(input_csv, output_csv)
