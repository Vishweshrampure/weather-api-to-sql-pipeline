import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import pipeline


def make_raw(n=24, bad_temp=None, dup=False):
    times = pd.date_range("2026-10-20", periods=n, freq="h").strftime("%Y-%m-%dT%H:%M").tolist()
    temps = [25.0] * n
    if bad_temp is not None:
        temps[3] = bad_temp
    if dup:
        times[5] = times[4]
    return {"hourly": {
        "time": times,
        "temperature_2m": temps,
        "relative_humidity_2m": [60] * n,
        "precipitation": [0.0] * n,
        "wind_speed_10m": [10.0] * n,
    }}


def test_transform_types():
    df = pipeline.transform(make_raw())
    assert len(df) == 24
    assert str(df["time"].dtype).startswith("datetime64")
    assert df["temperature_c"].dtype.kind == "f"


def test_clean_data_passes():
    clean, rejected, results = pipeline.run_quality_checks(pipeline.transform(make_raw()))
    assert len(clean) == 24 and rejected.empty
    assert not any(r["status"] == "FAIL" for r in results)


def test_out_of_range_rows_are_quarantined():
    clean, rejected, _ = pipeline.run_quality_checks(pipeline.transform(make_raw(bad_temp=999)))
    assert len(rejected) == 1 and len(clean) == 23


def test_duplicate_timestamps_stop_the_load():
    clean, _, results = pipeline.run_quality_checks(pipeline.transform(make_raw(dup=True)))
    assert clean.empty
    assert any(r["check_name"] == "no_duplicate_timestamps" and r["status"] == "FAIL" for r in results)
