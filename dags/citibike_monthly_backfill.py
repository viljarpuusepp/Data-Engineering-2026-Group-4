"""Manual, idempotent monthly CitiBike and Open-Meteo backfill DAG."""
from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

import pendulum
from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow/src")

from citibike_pipeline.ingestion import download_citibike_archive, download_weather, parse_month
from citibike_pipeline.loader import compact_bronze_month, load_trip_csv, load_weather_json
from citibike_pipeline.validation import validate_trip_csv, validate_weather_payload


@dag(
    dag_id="citibike_monthly_backfill",
    description="Download, validate, load, and model one Jersey City CitiBike month.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["citibike", "bronze", "dbt"],
)
def citibike_monthly_backfill():
    @task
    def validate_month(requested: str) -> str:
        return parse_month(requested)

    @task
    def download_trip(month: str) -> dict[str, str]:
        return download_citibike_archive(month)

    @task
    def validate_trip(artifact: dict[str, str]) -> dict[str, str | int]:
        return {**artifact, **validate_trip_csv(artifact["path"])}

    @task
    def download_open_meteo(month: str, trip: dict[str, str | int]) -> dict[str, str]:
        start_date = date.fromisoformat(str(trip["start_date"]))
        end_date = date.fromisoformat(str(trip["end_date"]))
        return download_weather(month, start_date, end_date)

    @task
    def validate_weather(
        artifact: dict[str, str], trip: dict[str, str | int]
    ) -> dict[str, str | int]:
        start_date = date.fromisoformat(str(trip["start_date"]))
        end_date = date.fromisoformat(str(trip["end_date"]))
        return {**artifact, **validate_weather_payload(artifact["path"], start_date, end_date)}

    @task
    def load_bronze(month: str, trip: dict[str, str | int], weather: dict[str, str | int]) -> None:
        load_trip_csv(str(trip["path"]), month, str(trip["url"]), str(trip["sha256"]))
        load_weather_json(str(weather["path"]), month, str(weather["url"]), str(weather["sha256"]))
        compact_bronze_month(month)

    @task
    def build_dbt() -> None:
        subprocess.run(
            [
                "dbt",
                "build",
                "--project-dir",
                "/opt/airflow/dbt",
                "--profiles-dir",
                "/opt/airflow/dbt",
            ],
            check=True,
            cwd=Path("/opt/airflow/dbt"),
        )

    month = validate_month("{{ dag_run.conf.get('month', logical_date.strftime('%Y-%m')) }}")
    trip = download_trip(month)
    valid_trip = validate_trip(trip)
    weather = download_open_meteo(month, valid_trip)
    valid_weather = validate_weather(weather, valid_trip)
    bronze_load = load_bronze(month, valid_trip, valid_weather)
    dbt_build = build_dbt()
    bronze_load >> dbt_build


citibike_monthly_backfill()
