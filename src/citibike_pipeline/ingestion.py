from __future__ import annotations

import hashlib
import json
import re
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

import requests

from citibike_pipeline.settings import (
    CITIBIKE_BASE_URL,
    JERSEY_CITY_LATITUDE,
    JERSEY_CITY_LONGITUDE,
    OPEN_METEO_URL,
    RAW_ROOT,
)

MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def parse_month(value: str) -> str:
    if not isinstance(value, str) or not MONTH_RE.fullmatch(value):
        raise ValueError("month must use YYYY-MM format")
    return value


def citibike_archive_url(month: str) -> str:
    return f"{CITIBIKE_BASE_URL}/JC-{parse_month(month).replace('-', '')}-citibike-tripdata.csv.zip"


def month_directory(month: str) -> Path:
    return RAW_ROOT / "citibike" / parse_month(month)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for block in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def download_citibike_archive(month: str) -> dict[str, str]:
    directory = month_directory(month)
    directory.mkdir(parents=True, exist_ok=True)
    archive_path = directory / "citibike.zip"
    url = citibike_archive_url(month)
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    archive_path.write_bytes(response.content)

    with zipfile.ZipFile(archive_path) as archive:
        csv_members = [
            member.filename
            for member in archive.infolist()
            if member.filename.lower().endswith(".csv")
            and not member.is_dir()
            and "__MACOSX" not in Path(member.filename).parts
            and not Path(member.filename).name.startswith("._")
        ]
        if len(csv_members) != 1:
            raise ValueError("CitiBike archive must contain exactly one CSV file")
        csv_path = directory / "citibike.csv"
        with archive.open(csv_members[0]) as source, csv_path.open("wb") as destination:
            destination.write(source.read())
    return {"path": str(csv_path), "url": url, "sha256": sha256_file(archive_path)}


def weather_request_url(start_date: date, end_date: date) -> str:
    parameters = {
        "latitude": JERSEY_CITY_LATITUDE,
        "longitude": JERSEY_CITY_LONGITUDE,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "timezone": "UTC",
        "hourly": (
            "temperature_2m,apparent_temperature,precipitation,rain,snowfall,"
            "snow_depth,wind_speed_10m,weather_code"
        ),
    }
    prepared = requests.Request("GET", OPEN_METEO_URL, params=parameters).prepare()
    return prepared.url


def download_weather(month: str, start_date: date, end_date: date) -> dict[str, str]:
    directory = month_directory(month)
    directory.mkdir(parents=True, exist_ok=True)
    payload_path = directory / "open_meteo.json"
    url = weather_request_url(start_date, end_date)
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    payload_path.write_text(json.dumps(response.json()), encoding="utf-8")
    return {"path": str(payload_path), "url": url, "sha256": sha256_file(payload_path)}


def utc_now() -> datetime:
    return datetime.now(UTC)

