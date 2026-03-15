import os
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
from utils.logger import get_logger

load_dotenv()
log = get_logger("db")


def get_connection_string() -> str:
    server   = os.getenv("DB_SERVER", "localhost")
    database = os.getenv("DB_NAME",   "hdx_ukraine")
    driver   = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
    user     = os.getenv("DB_USER",   "")
    password = os.getenv("DB_PASSWORD", "")

    if user and password:
        conn = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
        )
    else:
        # Windows Authentication — works seamlessly on local SSMS setup
        conn = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
        )
    return conn


def get_engine():
    conn_str = get_connection_string()
    quoted   = quote_plus(conn_str)
    engine   = create_engine(f"mssql+pyodbc:///?odbc_connect={quoted}", fast_executemany=True)
    return engine


def load_dataframe(df: pd.DataFrame, table: str, engine, if_exists: str = "replace") -> int:
    """
    Write a DataFrame to SQL Server.
    Returns row count written.
    if_exists: 'replace' drops+recreates, 'append' adds rows, 'fail' raises on existing.
    """
    log.info(f"Writing {len(df):,} rows → [{table}]  (mode: {if_exists})")
    df.to_sql(
        name=table,
        con=engine,
        if_exists=if_exists,
        index=False,
        chunksize=1000,
    )
    log.info(f"✓  [{table}] written successfully")
    return len(df)


def run_sql(engine, statement: str):
    """Execute a raw SQL statement (DDL, stored procs, etc.)"""
    with engine.begin() as conn:
        conn.execute(text(statement))


def test_connection() -> bool:
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("Database connection OK")
        return True
    except Exception as e:
        log.error(f"Database connection FAILED: {e}")
        return False
