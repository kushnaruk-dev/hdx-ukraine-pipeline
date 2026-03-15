import pandas as pd
from pathlib import Path
from utils.logger import get_logger

log = get_logger("extractor.five_w")

COLUMN_MAP = {
    "Oblast":                                   "oblast",
    "Pcode":                                    "pcode",
    "Camp Coordination & Camp Management":      "camp_management",
    "Education":                                "education",
    "Food Security & Livelihoods":              "food_security_livelihoods",
    "Health":                                   "health",
    "Protection: Child Protection":             "protection_child",
    "Protection: Gender Based Violence":        "protection_gbv",
    "Protection: Mine Action":                  "protection_mine_action",
    "Protection: Protection":                   "protection_general",
    "Shelter & Non-Food Items":                 "shelter_nfi",
    "Water, Sanitation and Hygiene":            "wash",
    "Multipurpose Cash Assistance":             "cash_assistance",
    "Inter-Cluster":                            "inter_cluster",
}


def extract(file_path: str | Path, report_year: int = 2025) -> pd.DataFrame:
    log.info(f"Reading 5W data: {file_path}")

    xl = pd.ExcelFile(file_path)
    log.info(f"Sheets found: {xl.sheet_names}")

    # People Reached sheet is always sheet 0
    df = pd.read_excel(file_path, sheet_name="People Reached by Oblast")

    log.info(f"Raw rows: {len(df):,}")

    # Rename
    df = df.rename(columns=COLUMN_MAP)

    # Keep only mapped columns that exist
    keep = [v for v in COLUMN_MAP.values() if v in df.columns]
    df   = df[keep].copy()

    # Numeric coercion on all cluster columns
    cluster_cols = [c for c in keep if c not in ("oblast", "pcode")]
    for col in cluster_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop completely empty rows
    before = len(df)
    df = df.dropna(subset=["oblast"])
    if before - len(df):
        log.warning(f"Dropped {before - len(df)} rows with no Oblast")

    # Tag year
    df["report_year"] = report_year

    log.info(f"Clean rows: {len(df):,}  |  Oblasts: {df['oblast'].nunique()}")

    # Quick summary
    if "health" in df.columns:
        log.info(f"Total people reached (Health): {df['health'].sum():,.0f}")
    if "inter_cluster" in df.columns:
        log.info(f"Total people reached (Inter-Cluster): {df['inter_cluster'].sum():,.0f}")

    return df
