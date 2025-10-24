# app.py
import yfinance as yf
import pandas as pd
from supabase import create_client, Client
from datetime import datetime
from dotenv import load_dotenv
import os

# Tick data settings
TICKERS = ["MNQ=F", "MES=F", "MGC=F"]
INTERVAL = "1m"
PERIOD = "1d"

# Supabase credentials (loaded from env in GitHub Actions)
load_dotenv()
API_URL = os.getenv("DB_URL")
API_KEY = os.getenv("API_KEY")

supabase_c: Client = create_client(API_URL, API_KEY)

def fetch_data():
    data = yf.download(
        TICKERS,
        interval=INTERVAL,
        period=PERIOD,
        group_by="ticker",
        progress=False
    )

    # This creates a clean long format DataFrame:
    df = (
        data.stack(level=0)
            .rename_axis(["time", "ticker"])
            .reset_index()
    )

    df = df.rename(columns={
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    })

    # Remove timezone
    df["time"] = df["time"].dt.tz_localize(None)

    return df

def insert_supabase(df):
    records = df.rename(columns={
        "Datetime": "time",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    })

    records["time"] = pd.to_datetime(records["time"]).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    records = records[["time","close", "volume", "ticker"]].to_dict(orient="records")

    supabase_c.table("bars_one_minute").upsert(records).execute()  # upsert prevents duplicates

if __name__ == "__main__":
    df = fetch_data()
    insert_supabase(df)
    print("✅ Uploaded to Supabase")