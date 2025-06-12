# Binance BTC Scraper

This repository provides a small utility for downloading and preprocessing historical BTC/USDT candlestick data from Binance. The scripts fetch all 30‑minute klines in a given date range, save them locally and output a cleaned CSV ready for analysis.

## Features

-   Multithreaded download of zipped daily kline files
-   Automatic retry logic with exponential backoff on failed requests
-   CSV preprocessing that converts timestamps, renames columns and sorts data
-   Logging to both console and rotating log files

## Installation

1. Clone the repository
2. Install the required packages:

```bash
pip install -r requirements.txt
```

## Usage

Run the main module to download and process the data:

```bash
python -m src.main
```

By default this downloads BTC/USDT data from `2017-12-17` up to `2024-12-06` and writes the results to `data/processed/BTCUSDT_Preprocessed.csv`. Raw CSVs and logs are stored in `data/raw` and `data/logs` respectively.

Configuration
Key parameters can be adjusted in `src/config.py`:

-   `SYMBOL` – trading pair symbol (default `BTCUSDT`)

-   `INTERVAL` – kline interval (default `30m`)

-   `BASE_URL` – location of the zipped kline files

-   `MAX_WORKERS` – number of download threads

All data directories are created automatically on first run.

## Repository Structure

```bash
.
├── requirements.txt        # Python dependencies
├── src/
│   ├── config.py           # Paths and settings
│   ├── main.py             # Entry point for downloading and preprocessing
│   └── data/
│       ├── fast_extraction.py       # Multithreaded downloader
│       └── btc_data_preprocessing.py # Data cleaning utilities
```

## Contributing

Improvements are welcome! Potential areas include adding tests, extending preprocessing steps or packaging the tool as a CLI. Feel free to open issues or pull requests.
