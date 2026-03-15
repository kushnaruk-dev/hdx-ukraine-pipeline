import pandas as pd
from pathlib import Path
from utils.logger import get_logger

log = get_logger("extractor.fts_funding")

COLUMN_MAP = {
    "countryCode":    "country_code",
    "id":             "plan_id",
    "name":           "plan_name",
    "code":           "plan_code",
    "startDate":      "start_date",
    "endDate":        "end_date",
    "year":           "report_year",
    "clusterCode":    "cluster_code",
    "cluster":        "cluster",
    "requirements":   "requirements",
    "funding":        "funding",
    "percentFunded":  "pct_funded",
}


def extract(file_path: str | Path) -> pd.DataFrame:
    log.info(f"Reading FTS funding: {file_path}")
    df = pd.read_csv(file_path, low_memory=False)

    log.info(f"Raw rows: {len(df):,}")

    df = df.rename(columns=COLUMN_MAP)
    df = df[[c for c in COLUMN_MAP.values() if c in df.columns]]

    # Parse dates
    for col in ("start_date", "end_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # Numeric
    for col in ("requirements", "funding", "pct_funded"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["cluster"])

    log.info(f"Clean rows: {len(df):,}")
    log.info(f"Years: {sorted(df['report_year'].dropna().unique().tolist())}")
    log.info(f"Clusters: {df['cluster'].nunique()}")

    # Funding gap summary
    if "requirements" in df.columns and "funding" in df.columns:
        total_req = df["requirements"].sum()
        total_fun = df["funding"].sum()
        gap       = total_req - total_fun
        log.info(f"Total requirements: ${total_req:,.0f}  |  Funded: ${total_fun:,.0f}  |  Gap: ${gap:,.0f}")

    return df
