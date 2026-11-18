from __future__ import annotations

import csv
import json
import os
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

from citibike_pipeline.ingestion import utc_now

TRIP_COLUMNS = [
    "ride_id", "rideable_type", "started_at", "ended_at", "start_station_name",
    "start_station_id", "end_station_name", "end_station_id", "start_lat", "start_lng",
    "end_lat", "end_lng", "member_casual", "source_month", "source_url",
    "payload_sha256", "ingested_at",
]
WEATHER_COLUMNS = [
    "observation_hour", "temperature_2m", "apparent_temperature", "precipitation", "rain",
    "snowfall", "snow_depth", "wind_speed_10m", "weather_code", "source_month",
    "source_url", "payload_sha256", "ingested_at",
]


def _clickhouse_client():
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "pipeline"),
    )


def _nullable_float(value: str | None) -> float | None:
    return float(value) if value not in (None, "") else None


def _nullable_int(value: Any) -> int | None:
    return int(value) if value not in (None, "") else None


def _chunks(rows: Iterator[list[Any]], size: int = 10_000) -> Iterator[list[list[Any]]]:
    batch: list[list[Any]] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _trip_rows(
    path: Path, month: str, url: str, checksum: str, ingested_at: datetime
) -> Iterator[list[Any]]:
    with path.open(newline="", encoding="utf-8-sig") as file_handle:
        for row in csv.DictReader(file_handle):
            yield [
                row["ride_id"], row["rideable_type"] or None, row["started_at"], row["ended_at"],
                row["start_station_name"] or None, row["start_station_id"] or None,
                row["end_station_name"] or None, row["end_station_id"] or None,
                _nullable_float(row["start_lat"]), _nullable_float(row["start_lng"]),
                _nullable_float(row["end_lat"]), _nullable_float(row["end_lng"]),
                row["member_casual"] or None, month, url, checksum, ingested_at,
            ]


def load_trip_csv(path: str, month: str, url: str, checksum: str) -> None:
    client = _clickhouse_client()
    now = utc_now()
    for batch in _chunks(_trip_rows(Path(path), month, url, checksum, now)):
        client.insert("bronze.citibike_trip_raw", batch, column_names=TRIP_COLUMNS)


def load_weather_json(path: str, month: str, url: str, checksum: str) -> None:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    hourly = payload["hourly"]
    now = utc_now()
    rows = []
    for index, observation_hour in enumerate(hourly["time"]):
        rows.append([
            observation_hour, _nullable_float(hourly.get("temperature_2m", [None])[index]),
            _nullable_float(hourly.get("apparent_temperature", [None])[index]),
            _nullable_float(hourly.get("precipitation", [None])[index]),
            _nullable_float(hourly.get("rain", [None])[index]),
            _nullable_float(hourly.get("snowfall", [None])[index]),
            _nullable_float(hourly.get("snow_depth", [None])[index]),
            _nullable_float(hourly.get("wind_speed_10m", [None])[index]),
            _nullable_int(hourly.get("weather_code", [None])[index]),
            month,
            url,
            checksum,
            now,
        ])
    client = _clickhouse_client()
    for batch in _chunks(iter(rows)):
        client.insert("bronze.open_meteo_hourly_raw", batch, column_names=WEATHER_COLUMNS)


def compact_bronze_month(month: str) -> None:
    """Synchronously merge replacement versions so a rerun leaves no visible Bronze duplicates."""
    client = _clickhouse_client()
    for table in ("citibike_trip_raw", "open_meteo_hourly_raw"):
        client.command(f"OPTIMIZE TABLE bronze.{table} PARTITION '{month}' FINAL")

