import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()


def _get_params():
    try:
        db       = st.secrets["database"]
        server   = db["server"].split(",")[0]  # strip port if present
        database = db["database"]
        user     = db["user"]
        password = db["password"]
    except (KeyError, FileNotFoundError):
        server   = os.getenv("DB_SERVER", "localhost").split(",")[0]
        database = os.getenv("DB_NAME",   "hdx_ukraine")
        user     = os.getenv("DB_USER",   "")
        password = os.getenv("DB_PASSWORD","")
    return server, database, user, password


@st.cache_resource
def get_engine():
    server, database, user, password = _get_params()
    # pymssql — no ODBC driver needed, works on Linux out of the box
    engine = create_engine(
        f"mssql+pymssql://{user}:{password}@{server}/{database}"
    )
    return engine


@st.cache_data(ttl=3600)
def query(_engine, sql: str) -> pd.DataFrame:
    with _engine.connect() as conn:
        return pd.read_sql(text(sql), conn)
