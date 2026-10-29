from datetime import date
from pathlib import Path

import pytest

from citibike_pipeline.ingestion import (
    citibike_archive_url,
    parse_month,
    sha256_file,
    weather_request_url,
)


def test_month_requires_year_month_format():
    assert parse_month("2026-08") == "2026-08"
    with pytest.raises(ValueError):
        parse_month("2026/08")


def test_citibike_url_is_deterministic():
    assert citibike_archive_url("2026-08").endswith("JC-202608-citibike-tripdata.csv.zip")


def test_weather_url_covers_the_requested_date_range():
    url = weather_request_url(date(2026, 8, 1), date(2026, 8, 31))
    assert "start_date=2026-08-01" in url
    assert "end_date=2026-08-31" in url


def test_checksum_changes_when_file_contents_change(tmp_path):
    payload = Path(tmp_path / "payload.txt")
    payload.write_text("first", encoding="utf-8")
    first_checksum = sha256_file(payload)
    payload.write_text("second", encoding="utf-8")
    assert sha256_file(payload) != first_checksum

