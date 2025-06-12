from pathlib import Path
from datetime import datetime



SYMBOL = "BTCUSDT"
INTERVAL = "30m"
BASE_URL = f"https://data.binance.vision/data/spot/daily/klines/{SYMBOL}/{INTERVAL}/"

# ======================== BASE PATHS ======================== #

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
LOG_DIR = DATA_DIR / "logs"
RESULTS_DIR = DATA_DIR / "results"

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ======================== SCRIPT SETTINGS ======================== #

MAX_WORKERS = 50
DATE_FORMAT = "%Y%m%d"

# Add LOG_FILE definition
SCRIPT_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOG_DIR / f"{SYMBOL}_data_extraction_{SCRIPT_TIMESTAMP}.log"
