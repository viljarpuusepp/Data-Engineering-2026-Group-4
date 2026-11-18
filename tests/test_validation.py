import json
from datetime import date

import pytest

from citibike_pipeline.validation import validate_trip_csv, validate_weather_payload

CSV_HEADER = (
    "ride_id,rideable_type,started_at,ended_at,start_station_name,start_station_id,"
    "end_station_name,end_station_id,start_lat,start_lng,end_lat,end_lng,member_casual\n"
)
CSV_ROW = (
    "trip-1,classic_bike,2026-08-01T10:00:00,2026-08-01T10:15:00,A,A1,B,B1,"
    "40.7,-74.0,40.8,-74.1,member\n"
)


def test_duplicate_trip_ids_are_rejected(tmp_path):
    csv_path = tmp_path / "trips.csv"
    csv_path.write_text(CSV_HEADER + CSV_ROW + CSV_ROW, encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate"):
        validate_trip_csv(csv_path)


def test_empty_csv_is_rejected(tmp_path):
    csv_path = tmp_path / "trips.csv"
    csv_path.write_text(CSV_HEADER, encoding="utf-8")
    with pytest.raises(ValueError, match="no data"):
        validate_trip_csv(csv_path)


def test_incomplete_weather_payload_is_rejected(tmp_path):
    payload_path = tmp_path / "weather.json"
    payload_path.write_text(
        json.dumps({"hourly": {"time": ["2026-08-01T00:00"]}}), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="cover every"):
        validate_weather_payload(payload_path, date(2026, 8, 1), date(2026, 8, 1))
