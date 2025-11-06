# app.py
from fastapi import FastAPI, Query, HTTPException
from typing import Optional
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware

import pandas as pd

app = FastAPI(title="Analytics API (POC)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can restrict this to specific domains in production
    allow_credentials=True,
    allow_methods=["*"],  # or specify: ["GET", "POST", "PUT", "DELETE"]
    allow_headers=["*"],  # or specify: ["Content-Type", "Authorization"]
)

# Load CSV once at startup
df = pd.read_csv("./sample_data_218111.csv", sep = ";")
print(df.head())
# Parse datetime columns if needed
if "transaction_time" in df.columns:
    df["transaction_time"] = pd.to_datetime(df["transaction_time"])
elif "date" in df.columns:
    df["date"] = pd.to_datetime(df["date"])


@app.get("/analytics/summary")
def summary(
    terminal_id: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Dashboard summary: total transactions, favorite operation, peak hour."""
    data = df.copy()

    # --- Combine year, month, day, hour into datetime ---
    if {"year", "month", "day", "hour"}.issubset(data.columns):
        data["datetime"] = pd.to_datetime(data[["year", "month", "day", "hour"]])
    else:
        raise HTTPException(status_code=400, detail="CSV missing required date columns.")

    # --- Apply filters ---
    if terminal_id:
        data = data[data["terminal_id"] == int(terminal_id)]

    if start and end:
        start_dt = pd.to_datetime(start)
        end_dt = pd.to_datetime(end)
        data = data[(data["datetime"] >= start_dt) & (data["datetime"] <= end_dt)]

    if data.empty:
        raise HTTPException(status_code=404, detail="No data found for the filters provided.")

    # --- Current summary ---
    total_txns = len(data)
    fav_op = data["operation"].value_counts().idxmax() if "operation" in data.columns else None
    peak_hour = int(data["hour"].mode()[0]) if not data["hour"].empty else None

    # --- Growth Calculation ---
    txn_growth = hour_growth = None
    if start and end:
        period_days = (end_dt - start_dt).days
        prev_start = start_dt - pd.Timedelta(days=period_days)
        prev_end = start_dt - pd.Timedelta(days=1)

        prev_data = df.copy()
        if {"year", "month", "day", "hour"}.issubset(prev_data.columns):
            prev_data["datetime"] = pd.to_datetime(prev_data[["year", "month", "day", "hour"]])
        prev_data = prev_data[(prev_data["datetime"] >= prev_start) & (prev_data["datetime"] <= prev_end)]

        if not prev_data.empty:
            prev_txns = len(prev_data)
            prev_peak_hour = int(prev_data["hour"].mode()[0]) if not prev_data["hour"].empty else None

            txn_growth = ((total_txns - prev_txns) / prev_txns * 100) if prev_txns > 0 else None
            if prev_peak_hour and prev_peak_hour > 0:
                hour_growth = ((peak_hour - prev_peak_hour) / prev_peak_hour * 100)

    # --- Final structured response ---
    summary = {
        "total_transactions": {
            "value": int(total_txns),
            "growth": txn_growth
        },
        "favorite_operation": fav_op,
        "peak_hour": {
            "value": int(peak_hour) if peak_hour is not None else None,
            "growth": hour_growth
        }
    }

    return summary

@app.get("/analytics/group-by")
def group_by(
    dimension: str = Query(..., description="Group by one of: channel / operation / entity"),
    terminal_id: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    """Group transactions by a specific dimension (channel, operation, or entity)."""
    allowed = ["channel", "operation", "entity"]
    if dimension not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid dimension '{dimension}'. Choose from {allowed}."
        )

    data = df.copy()

    # If no datetime column, build one from year/month/day
    if {"year", "month", "day"}.issubset(data.columns):
        data["date"] = pd.to_datetime(
            data[["year", "month", "day"]].astype(str).agg("-".join, axis=1),
            errors="coerce"
        )

    # Filter by terminal_id if provided
    if terminal_id:
        data = data[data["terminal_id"].astype(str) == str(terminal_id)]

    # Filter by date range if provided
    if start and end:
        try:
            start_dt = pd.to_datetime(start)
            end_dt = pd.to_datetime(end)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

        if "date" in data.columns:
            data = data[(data["date"] >= start_dt) & (data["date"] <= end_dt)]

    if data.empty:
        raise HTTPException(status_code=404, detail="No data found for the filters provided.")

    # Validate the dimension column
    if dimension not in data.columns:
        raise HTTPException(status_code=400, detail=f"'{dimension}' column not found in data.")

    # Aggregate
    result = (
        data.groupby(dimension, dropna=False)
        .agg(transactions=("cant_trx", "sum"), total_amount=("transaction_amount", "sum"))
        .reset_index()
        .sort_values("transactions", ascending=False)
    )

    result[dimension] = result[dimension].fillna("Unknown")

    return result.to_dict(orient="records")
@app.get("/analytics/timeseries")
def timeseries(
    terminal_id: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    """Return transaction count and total amount per day."""
    data = df.copy()

    # --- Combine year, month, day, hour into datetime ---
    if {"year", "month", "day", "hour"}.issubset(data.columns):
        data["datetime"] = pd.to_datetime(data[["year", "month", "day", "hour"]])
    else:
        raise HTTPException(status_code=400, detail="CSV missing required date columns.")

    # --- Apply filters ---
    if terminal_id:
        data = data[data["terminal_id"] == int(terminal_id)]

    if start and end:
        start_dt = pd.to_datetime(start)
        end_dt = pd.to_datetime(end)
        data = data[(data["datetime"] >= start_dt) & (data["datetime"] <= end_dt)]

    if data.empty:
        raise HTTPException(status_code=404, detail="No data found for the filters provided.")

    # --- Extract only date (drop hour) ---
    data["date"] = data["datetime"].dt.date

    # --- Group by date ---
    result = (
        data.groupby("date")
        .agg(
            transactions=("cant_trx", "sum") if "cant_trx" in data.columns else ("datetime", "count"),
            total_amount=("transaction_amount", "sum") if "transaction_amount" in data.columns else None
        )
        .reset_index()
        .sort_values("date")
    )

    # --- Clean NaN columns (if total_amount not available) ---
    result = result.dropna(axis=1, how="all")

    return result.to_dict(orient="records")
