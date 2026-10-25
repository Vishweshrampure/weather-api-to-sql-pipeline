"""
Weather API -> Python (pandas) -> SQL pipeline with data quality checks.

Steps: Extract (REST API) -> Transform (pandas) -> Data Quality checks
       -> Load (SQL table) -> Reconcile (row counts) -> Log.

By default it loads into a local SQLite file. To load into SQL Server,
set the DB_URL environment variable (see README).
"""
import logging
import os
import uuid
from datetime import datetime, timezone

import pandas as pd
import requests
from sqlalchemy import DateTime, bindparam, create_engine, inspect, text

# ---------------------------------------------------------------- config
API_URL = "https://api.open-meteo.com/v1/forecast"
API_PARAMS = {
    "latitude": 18.52,          # Pune
    "longitude": 73.86,
    "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
    "past_days": 7,
    "forecast_days": 1,
    "timezone": "Asia/Kolkata",
}
DB_URL = os.getenv("DB_URL", "sqlite:///weather.db")
TARGET_TABLE = "weather_hourly"

# Allowed ranges used by the validity check
RANGES = {
    "temperature_c": (-10, 55),
    "humidity_pct": (0, 100),
    "precipitation_mm": (0, 500),
    "wind_kmh": (0, 250),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)
log = logging.getLogger("pipeline")


# --------------------------------------------------------------- extract
def extract() -> dict:
    """Call the API and return the raw JSON."""
    log.info("Extracting data from API")
    response = requests.get(API_URL, params=API_PARAMS, timeout=30)
    response.raise_for_status()
    return response.json()


# ------------------------------------------------------------- transform
def transform(raw: dict) -> pd.DataFrame:
    """Turn the API JSON into a clean, typed DataFrame."""
    hourly = raw["hourly"]
    df = pd.DataFrame(hourly).rename(
        columns={
            "time": "time",
            "temperature_2m": "temperature_c",
            "relative_humidity_2m": "humidity_pct",
            "precipitation": "precipitation_mm",
            "wind_speed_10m": "wind_kmh",
        }
    )
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    for col in RANGES:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["ingested_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    log.info("Transformed %d rows", len(df))
    return df


# --------------------------------------------------------- data quality
def run_quality_checks(df: pd.DataFrame):
    """
    Returns (clean_df, rejected_df, results).
    Critical failures (empty data, null/duplicate timestamps) stop the load.
    Out-of-range rows are quarantined into the rejected table instead of loaded.
    """
    results = []

    def record(name, passed, details, critical=False):
        status = "PASS" if passed else ("FAIL" if critical else "WARN")
        results.append({"check_name": name, "status": status, "details": details})
        log.info("DQ check %-22s %s | %s", name, status, details)

    # 1. Not empty
    record("row_count_not_empty", len(df) > 0, f"{len(df)} rows", critical=True)
    if len(df) == 0:
        return df, df.iloc[0:0], results

    # 2. Null timestamps
    null_time = int(df["time"].isna().sum())
    record("no_null_timestamps", null_time == 0, f"{null_time} null timestamps", critical=True)

    # 3. Duplicate timestamps
    dupes = int(df["time"].duplicated().sum())
    record("no_duplicate_timestamps", dupes == 0, f"{dupes} duplicate timestamps", critical=True)

    # 4. Missing values in measurement columns (warning only)
    for col in RANGES:
        nulls = int(df[col].isna().sum())
        record(f"nulls_in_{col}", nulls == 0, f"{nulls} nulls")

    # 5. Range / validity check -> quarantine bad rows
    bad_mask = pd.Series(False, index=df.index)
    for col, (low, high) in RANGES.items():
        out_of_range = df[col].notna() & ((df[col] < low) | (df[col] > high))
        record(f"range_{col}", not out_of_range.any(),
               f"{int(out_of_range.sum())} rows outside {low}..{high}")
        bad_mask |= out_of_range

    # 6. Freshness: newest record should be recent
    if df["time"].notna().any():
        newest = df["time"].max()
        age_days = (pd.Timestamp.now() - newest).days
        record("freshness", age_days <= 2, f"newest record {newest}, {age_days} days old")

    critical_failed = any(r["status"] == "FAIL" for r in results)
    rejected = df[bad_mask].copy()
    clean = df[~bad_mask].copy()
    if critical_failed:
        return df.iloc[0:0], rejected, results
    return clean, rejected, results


# ------------------------------------------------------------------ load
def window_sql(prefix: str):
    """SQL for a time window; typed bind parameters keep datetime formats consistent."""
    return text(f"{prefix} WHERE time BETWEEN :lo AND :hi").bindparams(
        bindparam("lo", type_=DateTime), bindparam("hi", type_=DateTime)
    )


def load(engine, clean: pd.DataFrame, rejected: pd.DataFrame, results: list, run_id: str):
    """Idempotent load: delete the overlapping time window, then insert."""
    if not clean.empty:
        lo = clean["time"].min().to_pydatetime()
        hi = clean["time"].max().to_pydatetime()
        if inspect(engine).has_table(TARGET_TABLE):
            with engine.begin() as conn:
                conn.execute(window_sql(f"DELETE FROM {TARGET_TABLE}"), {"lo": lo, "hi": hi})
        else:
            log.info("Target table does not exist yet - it will be created")
        clean.to_sql(TARGET_TABLE, engine, if_exists="append", index=False)

        # Reconciliation: rows we meant to load == rows now in the table window
        with engine.connect() as conn:
            loaded = conn.execute(
                window_sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}"), {"lo": lo, "hi": hi}
            ).scalar()
        ok = loaded == len(clean)
        results.append({"check_name": "row_count_reconciliation",
                        "status": "PASS" if ok else "FAIL",
                        "details": f"expected {len(clean)}, found {loaded}"})
        log.info("Reconciliation expected=%d found=%d", len(clean), loaded)

    if not rejected.empty:
        rejected.assign(run_id=run_id).to_sql(
            f"{TARGET_TABLE}_rejected", engine, if_exists="append", index=False)

    log_df = pd.DataFrame(results)
    log_df["run_id"] = run_id
    log_df["run_ts"] = datetime.now(timezone.utc).replace(tzinfo=None)
    log_df.to_sql("dq_results", engine, if_exists="append", index=False)


# ------------------------------------------------------------------ main
def main():
    run_id = uuid.uuid4().hex[:8]
    log.info("=== Pipeline run %s started ===", run_id)
    engine = create_engine(DB_URL)

    raw = extract()
    df = transform(raw)
    clean, rejected, results = run_quality_checks(df)

    if any(r["status"] == "FAIL" for r in results):
        log.error("Critical data quality failure - nothing loaded")
    load(engine, clean, rejected, results, run_id)
    log.info("=== Run %s finished: %d loaded, %d rejected ===", run_id, len(clean), len(rejected))


if __name__ == "__main__":
    main()
