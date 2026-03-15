"""
HDX Ukraine — Humanitarian Data Pipeline
=========================================
Orchestrates extraction from all HDX sources and loads into MS SQL Server.

Usage:
    python pipeline.py                    # run all sources
    python pipeline.py --source food      # run single source
    python pipeline.py --dry-run          # extract only, skip DB write
    python pipeline.py --validate         # test DB connection and exit
"""

import os
import sys
import argparse
import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

from utils.logger      import get_logger
from db.connection     import get_engine, load_dataframe, run_sql, test_connection
from extractors        import food_prices, five_w, fts_funding, hunger_map

log = get_logger("pipeline")

# ── Default local data directory ──────────────────────────────────────────
DATA_DIR = Path(os.getenv("LOCAL_DATA_DIR", "./data/raw"))

# ── Source definitions ────────────────────────────────────────────────────
# Each entry: key → (extractor_fn, filename, target_table, extra_kwargs)
SOURCES = {
    "food": (
        food_prices.extract,
        "wfp_food_prices_ukr.csv",
        "food_prices",
        {},
    ),
    "five_w": (
        five_w.extract,
        "ukraine-5w-2025-january-december-2025-12-31.xlsx",
        "five_w",
        {"report_year": 2025},
    ),
    "funding": (
        fts_funding.extract,
        "fts_requirements_funding_cluster_ukr.csv",
        "fts_funding",
        {},
    ),
    "hunger": (
        hunger_map.extract,
        "wfp-hungermap-data-for-ukr-long.csv",
        "hunger_map",
        {},
    ),
}


def log_run(engine, source: str, table: str, rows: int, status: str, error: str = None):
    """Write a run record to dbo.pipeline_runs."""
    try:
        run_sql(engine, f"""
            INSERT INTO dbo.pipeline_runs (source, target_table, rows_loaded, status, error_msg)
            VALUES ('{source}', '{table}', {rows}, '{status}', {f"'{error}'" if error else 'NULL'})
        """)
    except Exception as e:
        log.warning(f"Could not write run log: {e}")


def run_source(key: str, engine, dry_run: bool = False) -> bool:
    extractor_fn, filename, table, kwargs = SOURCES[key]
    file_path = DATA_DIR / filename

    if not file_path.exists():
        log.error(f"File not found: {file_path}")
        log.error(f"  → Place your HDX files in: {DATA_DIR.resolve()}")
        return False

    log.info(f"{'─'*55}")
    log.info(f"SOURCE: {key.upper()}  →  [{table}]")

    try:
        df = extractor_fn(file_path, **kwargs)

        if dry_run:
            log.info(f"DRY RUN — skipping DB write. Shape: {df.shape}")
            return True

        rows = load_dataframe(df, table, engine, if_exists="replace")
        log_run(engine, key, table, rows, "SUCCESS")
        return True

    except Exception as e:
        log.error(f"FAILED [{key}]: {e}")
        if not dry_run:
            try:
                log_run(engine, key, table, 0, "FAILED", str(e)[:500])
            except Exception:
                pass
        return False


def main():
    parser = argparse.ArgumentParser(description="HDX Ukraine Pipeline")
    parser.add_argument("--source",   choices=list(SOURCES.keys()), help="Run a single source")
    parser.add_argument("--dry-run",  action="store_true", help="Extract only, skip DB write")
    parser.add_argument("--validate", action="store_true", help="Test DB connection and exit")
    args = parser.parse_args()

    log.info("=" * 55)
    log.info("  HDX Ukraine Humanitarian Data Pipeline")
    log.info(f"  Started: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

    # ── Validate mode ─────────────────────────────────────────
    if args.validate:
        ok = test_connection()
        sys.exit(0 if ok else 1)

    # ── Connect ───────────────────────────────────────────────
    if not args.dry_run:
        if not test_connection():
            log.error("Aborting — cannot reach database. Check .env settings.")
            sys.exit(1)
        engine = get_engine()
    else:
        engine = None
        log.info("DRY RUN mode — no DB writes")

    # ── Run sources ───────────────────────────────────────────
    targets = [args.source] if args.source else list(SOURCES.keys())
    results = {}

    for key in targets:
        results[key] = run_source(key, engine, dry_run=args.dry_run)

    # ── Summary ───────────────────────────────────────────────
    log.info("=" * 55)
    log.info("  PIPELINE SUMMARY")
    log.info("=" * 55)
    for key, ok in results.items():
        status = "✓  SUCCESS" if ok else "✗  FAILED "
        log.info(f"  {status}  {key}")

    failed = [k for k, ok in results.items() if not ok]
    if failed:
        log.error(f"Pipeline finished with {len(failed)} failure(s)")
        sys.exit(1)
    else:
        log.info("All sources loaded successfully")


if __name__ == "__main__":
    main()
