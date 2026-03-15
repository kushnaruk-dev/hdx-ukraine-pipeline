import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()


def _build_conn_string() -> str:
    """
    Connection priority:
    1. Streamlit secrets (cloud deployment / local .streamlit/secrets.toml)
    2. .env file (local dev fallback)
    """
    try:
        db      = st.secrets["database"]
        server  = db["server"]
        database= db["database"]
        driver  = db["driver"]
        user    = db.get("user", "")
        password= db.get("password", "")
    except (KeyError, FileNotFoundError):
        server  = os.getenv("DB_SERVER",  ".\\SQLEXPRESS")
        database= os.getenv("DB_NAME",    "hdx_ukraine")
        driver  = os.getenv("DB_DRIVER",  "ODBC Driver 17 for SQL Server")
        user    = os.getenv("DB_USER",    "")
        password= os.getenv("DB_PASSWORD","")

    if user and password:
        conn = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};PWD={password};"
        )
    else:
        conn = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
        )
    return conn


@st.cache_resource
def get_engine():
    conn_str = _build_conn_string()
    quoted   = quote_plus(conn_str)
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quoted}",
        fast_executemany=True
    )


@st.cache_data(ttl=3600)
def query(_engine, sql: str) -> pd.DataFrame:
    """Cached query — refreshes every hour."""
    with _engine.connect() as conn:
        return pd.read_sql(text(sql), conn)
