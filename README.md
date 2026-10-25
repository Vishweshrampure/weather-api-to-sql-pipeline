# Weather API to SQL Pipeline with Data Quality Checks

A small, end-to-end ETL pipeline in Python. It pulls hourly weather data for Pune from a free public REST API (Open-Meteo, no API key needed), cleans it with pandas, runs automated data quality checks, and loads it into a SQL database.

## Pipeline flow

```
Open-Meteo REST API  ->  Extract  ->  Transform (pandas)  ->  Data Quality Checks  ->  Load (SQL)  ->  Reconcile  ->  Log
                                                                  |
                                                                  +--> bad rows go to a rejected (quarantine) table
```

## What it does

| Step | Details |
|------|---------|
| Extract | Calls the API with `requests` (last 7 days + today, hourly) |
| Transform | Renames columns, converts types, adds an `ingested_at` timestamp |
| Data quality | Empty data, null timestamps, duplicate timestamps, nulls in measures, value range checks, freshness |
| Quarantine | Rows outside valid ranges go to `weather_hourly_rejected` instead of the main table |
| Fail-fast | Critical failures (empty data, null or duplicate timestamps) stop the load |
| Load | Idempotent: deletes the overlapping time window then inserts, so re-running never creates duplicates |
| Reconcile | After loading, compares expected rows vs rows found in the table |
| Audit | Every check result is stored in the `dq_results` table with a run id; logs go to `pipeline.log` |

## Tech stack

Python, pandas, SQLAlchemy, requests, pytest, SQLite (default) or SQL Server.

## How to run

```bash
# 1. Install
pip install -r requirements.txt

# 2. Run the pipeline (loads into a local SQLite file weather.db)
python pipeline.py

# 3. Run the tests
python -m pytest -q
```

### Load into SQL Server instead of SQLite

1. Install the ODBC Driver 17 or 18 for SQL Server, and `pip install pyodbc`.
2. Create an empty database, for example `WeatherDB`.
3. Set the connection string and run:

```bash
# Windows (PowerShell), Windows authentication
$env:DB_URL = "mssql+pyodbc://@localhost/WeatherDB?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
python pipeline.py
```

## Check the results (SQL)

```sql
SELECT TOP 10 * FROM weather_hourly ORDER BY time DESC;       -- SQLite: use LIMIT 10 instead of TOP 10
SELECT check_name, status, details FROM dq_results ORDER BY run_ts DESC;
SELECT * FROM weather_hourly_rejected;
```

## Project structure

```
pipeline.py          main ETL script
tests/test_pipeline.py   unit tests for transform and data quality checks
requirements.txt
```

## Screenshots

Add a screenshot of your `dq_results` table and your terminal run here.

## What I learned

- Building a repeatable ETL pipeline and making loads idempotent
- Data quality checks, quarantining bad records and reconciliation
- Writing unit tests for data transformations
