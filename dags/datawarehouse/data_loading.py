import json
from datetime import date
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def _latest_data_file():
    files = sorted(Path("./data").glob("YT_data_*.json"))
    if not files:
        raise FileNotFoundError("No data/YT_data_*.json files found.")
    return files[-1]


def load_data(file_path=None):
    file_path = Path(file_path) if file_path else Path(f"./data/YT_data_{date.today()}.json")

    if not file_path.exists() and file_path.name == f"YT_data_{date.today()}.json":
        latest_file = _latest_data_file()
        logger.warning("Today's data file %s was not found; using latest file %s", file_path, latest_file)
        file_path = latest_file

    try:
        logger.info("Processing file: %s", file_path)
        with open(file_path, "r", encoding="utf-8") as raw_data:
            data = json.load(raw_data) #加载数据
        return data
    
    except FileNotFoundError:
        logger.error(f"File not found:{file_path}")
        raise

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {file_path}")
        raise
