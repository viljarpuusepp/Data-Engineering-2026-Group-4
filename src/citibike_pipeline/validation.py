from __future__ import annotations

import csv
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from citibike_pipeline.settings import REQUIRED_TRIP_COLUMNS


def _parse_trip_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def validate_trip_csv(path: str | Path) -> dict[str, str | int]:
    csv_path = Path(path)
    if not csv_path.is_file() or csv_path.stat().st_size == 0:
        raise ValueError("CitiBike CSV is missing or empty")
    with csv_path.open(newline="", encoding="utf-8-sig") as file_handle:
        reader = csv.DictReader(file_handle)
        if reader.fieldnames is None or not set(REQUIRED_TRIP_COLUMNS).issubset(reader.fieldnames):
            raise ValueError("CitiBike CSV header does not match the expected schema")
        ride_ids: set[str] = set()
        min_date: date | None = None
        max_date: date | None = None
        row_count = 0
        for row in reader:
            row_count += 1
            if not row["ride_id"] or not row["started_at"] or not row["ended_at"]:
                raise ValueError("CitiBike CSV contains null required trip fields")
            if row["ride_id"] in ride_ids:
                raise ValueError("CitiBike CSV contains duplicate ride_id values")
            ride_ids.add(row["ride_id"])
            started_at = _parse_trip_timestamp(row["started_at"])
            ended_at = _parse_trip_timestamp(row["ended_at"])
            if ended_at < started_at:
                raise ValueError("CitiBike CSV contains a negative trip duration")
            trip_date = started_at.date()
            min_date = trip_date if min_date is None or trip_date < min_date else min_date
            max_date = trip_date if max_date is None or trip_date > max_date else max_date
    if row_count == 0 or min_date is None or max_date is None:
        raise ValueError("CitiBike CSV has no data rows")
    return {
        "row_count": row_count,
        "start_date": min_date.isoformat(),
        "end_date": max_date.isoformat(),
    }

def expected_local_hours(start_date: date, end_date: date) -> int:
    """Hours from start_date 00:00 to the end of end_date, in New York local time."""
    zone = ZoneInfo("America/New_York")
    start = datetime(start_date.year, start_date.month, start_date.day, tzinfo=zone)
    end = datetime(end_date.year, end_date.month, end_date.day, tzinfo=zone) + timedelta(days=1)
    return int((end.astimezone(UTC) - start.astimezone(UTC)).total_seconds() // 3600)

def validate_weather_payload(path: str | Path, start_date: date, end_date: date) -> dict[str, int]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict) or not isinstance(hourly.get("time"), list):
        raise ValueError("Open-Meteo response has no hourly time series")
    expected_hours = expected_local_hours(start_date, end_date)
    if len(hourly["time"]) != expected_hours:
        raise ValueError("Open-Meteo response does not cover every requested hour")
    for field, values in hourly.items():
        if isinstance(values, list) and len(values) != expected_hours:
            raise ValueError(f"Open-Meteo field {field} has an incomplete hourly series")
    return {"row_count": expected_hours}

