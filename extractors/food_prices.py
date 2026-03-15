import pandas as pd
from pathlib import Path
from utils.logger import get_logger

log = get_logger("extractor.food_prices")

COLUMN_MAP = {
    "date":        "report_date",
    "admin1":      "admin1",
    "admin2":      "admin2",
    "market":      "market",
    "market_id":   "market_id",
    "latitude":    "latitude",
    "longitude":   "longitude",
    "category":    "category",
    "commodity":   "commodity",
    "commodity_id":"commodity_id",
    "unit":        "unit",
    "priceflag":   "price_flag",
    "pricetype":   "price_type",
    "currency":    "currency",
    "price":       "price",
    "usdprice":    "usd_price",
}


def extract(file_path: str | Path) -> pd.DataFrame:
    log.info(f"Reading food prices: {file_path}")
    df = pd.read_csv(file_path, parse_dates=["date"], low_memory=False)

    log.info(f"Raw rows: {len(df):,}")

    # Rename columns
    df = df.rename(columns=COLUMN_MAP)

    # Keep only mapped columns
    df = df[[c for c in COLUMN_MAP.values() if c in df.columns]]

    # Clean
    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.date
    df["price"]       = pd.to_numeric(df["price"],     errors="coerce")
    df["usd_price"]   = pd.to_numeric(df["usd_price"], errors="coerce")
    df["latitude"]    = pd.to_numeric(df["latitude"],  errors="coerce")
    df["longitude"]   = pd.to_numeric(df["longitude"], errors="coerce")

    # Drop rows with no price or date
    before = len(df)
    df = df.dropna(subset=["report_date", "price"])
    dropped = before - len(df)
    if dropped:
        log.warning(f"Dropped {dropped:,} rows with null date or price")

    log.info(f"Clean rows: {len(df):,}  |  date range: {df['report_date'].min()} → {df['report_date'].max()}")
    log.info(f"Markets: {df['market'].nunique()}  |  Commodities: {df['commodity'].nunique()}")

    return df
