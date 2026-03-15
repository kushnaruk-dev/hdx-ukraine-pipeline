import pandas as pd
from pathlib import Path
from utils.logger import get_logger

log = get_logger("extractor.hunger_map")

COLUMN_MAP = {
    "countrycode":    "country_code",
    "countryname":    "country_name",
    "adminone":       "admin_one",
    "adminlevel":     "admin_level",
    "date":           "report_date",
    "datatype":       "data_type",
    "indicator name": "indicator",
    "population":     "population",
    "prevalence":     "prevalence",
}


def extract(file_path: str | Path) -> pd.DataFrame:
    log.info(f"Reading hunger map: {file_path}")
    df = pd.read_csv(file_path, low_memory=False)

    log.info(f"Raw rows: {len(df):,}")

    df = df.rename(columns=COLUMN_MAP)
    df = df[[c for c in COLUMN_MAP.values() if c in df.columns]]

    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.date
    df["prevalence"]  = pd.to_numeric(df["prevalence"],  errors="coerce")
    df["population"]  = pd.to_numeric(df["population"],  errors="coerce").astype("Int64")

    df = df.dropna(subset=["report_date", "prevalence"])

    log.info(f"Clean rows: {len(df):,}")
    log.info(f"Indicators: {df['indicator'].unique().tolist()}")
    log.info(f"Date range: {df['report_date'].min()} → {df['report_date'].max()}")

    # Latest national FCS prevalence
    national = df[(df["admin_level"] == "national") & (df["indicator"] == "fcs")]
    if not national.empty:
        latest = national.sort_values("report_date").iloc[-1]
        log.info(f"Latest national FCS prevalence: {latest['prevalence']:.1%}  ({latest['report_date']})")

    return df
