
from __future__ import annotations

import calendar
import csv
import json
import os
import shutil
import sqlite3
import tempfile
import zipfile

from datetime import datetime
from pathlib import Path

import pendulum
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.sensors.python import PythonSensor


# ============================================================
# CONFIGURATION
# ============================================================

# These are paths INSIDE the Airflow Docker containers.
RAW_DIR = Path("/opt/airflow/data/raw")
CITIBIKE_DIR = RAW_DIR / "citibike"
WEATHER_DIR = RAW_DIR / "weather"

CITIBIKE_URL = "https://s3.amazonaws.com/tripdata"
WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"

TIMEZONE = pendulum.timezone("Europe/Tallinn")

CITIBIKE_COLUMNS = {
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual",
}

REQUIRED_COLUMNS = [
    "ride_id",
    "started_at",
    "ended_at",
]

WEATHER_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "snowfall",
    "snow_depth",
    "apparent_temperature",
    "wind_speed_10m",
    "weather_code",
]


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_month_dates(month):
    """Return first and last date of a YYYY-MM month."""

    year, month_number = map(int, month.split("-"))

    last_day = calendar.monthrange(year, month_number)[1]

    return (
        f"{month}-01",
        f"{month}-{last_day:02d}",
    )


def get_archive_url(month):
    """Build official NYC Citi Bike archive URL."""

    filename = f"{month.replace('-', '')}-citibike-tripdata.zip"

    return f"{CITIBIKE_URL}/{filename}"


def get_month_csvs(month):
    """Find only CSV files belonging to the requested month."""

    prefix = f"{month.replace('-', '')}-citibike-tripdata"

    return sorted(CITIBIKE_DIR.glob(f"{prefix}*.csv"))


def get_source_metadata(url):
    """
    Check the remote file without downloading it.

    ETag, Last-Modified and Content-Length help identify
    whether the source archive has changed.
    """

    response = requests.head(
        url,
        allow_redirects=True,
        timeout=(20, 60),
    )

    response.raise_for_status()

    return {
        "etag": response.headers.get("ETag"),
        "last_modified": response.headers.get("Last-Modified"),
        "content_length": response.headers.get("Content-Length"),
    }


def metadata_unchanged(old, new):
    """Compare available source metadata."""

    fields = [
        key
        for key in ("etag", "last_modified", "content_length")
        if new.get(key) is not None
    ]

    if not fields:
        return False

    return all(old.get(key) == new.get(key) for key in fields)


def read_manifest(path):
    """Read ingestion metadata if it exists."""

    try:
        with open(path, encoding="utf-8") as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError):
        return None


def write_manifest(path, data):
    """Write manifest atomically."""

    temporary = path.with_suffix(".json.part")

    with open(temporary, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    os.replace(temporary, path)


def file_signatures(paths):
    """Record local CSV names and sizes."""

    return [
        {
            "name": path.name,
            "size": path.stat().st_size,
        }
        for path in sorted(paths)
    ]


# ============================================================
# CITI BIKE VALIDATION
# ============================================================

def validate_and_count_csvs(csv_files):
    """
    Validate CSV structure and count records.

    Checks:
    - Files exist and are not empty
    - Expected columns exist
    - Essential fields are not NULL/empty
    - No duplicate ride IDs across files

    SQLite is used temporarily to avoid keeping millions
    of ride IDs in Python memory.
    """

    if not csv_files:
        raise ValueError("No Citi Bike CSV files found.")

    temporary_db = tempfile.NamedTemporaryFile(
        suffix=".sqlite",
        delete=False,
    )

    db_path = temporary_db.name
    temporary_db.close()

    connection = None

    total_rows = 0
    missing_values = 0

    try:
        connection = sqlite3.connect(db_path)

        connection.execute(
            """
            CREATE TABLE ride_ids (
                ride_id TEXT PRIMARY KEY
            ) WITHOUT ROWID
            """
        )

        for csv_path in sorted(csv_files):

            if not csv_path.exists() or csv_path.stat().st_size == 0:
                raise ValueError(
                    f"Missing or empty CSV: {csv_path}"
                )

            batch = []

            print(f"Validating {csv_path.name}")

            with open(
                csv_path,
                "r",
                newline="",
                encoding="utf-8-sig",
            ) as file:

                reader = csv.DictReader(file)

                if not reader.fieldnames:
                    raise ValueError(
                        f"CSV header missing: {csv_path.name}"
                    )

                missing_columns = (
                    CITIBIKE_COLUMNS - set(reader.fieldnames)
                )

                if missing_columns:
                    raise ValueError(
                        f"Missing columns in {csv_path.name}: "
                        f"{sorted(missing_columns)}"
                    )

                for row in reader:

                    total_rows += 1

                    if any(
                        not (row.get(column) or "").strip()
                        for column in REQUIRED_COLUMNS
                    ):
                        missing_values += 1
                        continue

                    batch.append((row["ride_id"].strip(),))

                    if len(batch) >= 10000:

                        connection.executemany(
                            "INSERT OR IGNORE INTO ride_ids VALUES (?)",
                            batch,
                        )

                        batch.clear()

                if batch:
                    connection.executemany(
                        "INSERT OR IGNORE INTO ride_ids VALUES (?)",
                        batch,
                    )

        unique_ids = connection.execute(
            "SELECT COUNT(*) FROM ride_ids"
        ).fetchone()[0]

        duplicates = total_rows - missing_values - unique_ids

        print(f"Total rows: {total_rows:,}")
        print(f"Unique ride IDs: {unique_ids:,}")
        print(f"Missing essential values: {missing_values:,}")
        print(f"Duplicate IDs: {duplicates:,}")

        if total_rows == 0:
            raise ValueError("Citi Bike data is empty.")

        if missing_values > 0:
            raise ValueError(
                f"Found {missing_values} rows with missing required values."
            )

        if duplicates > 0:
            raise ValueError(
                f"Found {duplicates} duplicate ride IDs."
            )

        return total_rows

    finally:

        if connection is not None:
            connection.close()

        if os.path.exists(db_path):
            os.remove(db_path)


# ============================================================
# WEATHER VALIDATION
# ============================================================

def weather_is_valid(path, start_date, end_date):

    if not path.exists() or path.stat().st_size == 0:
        return False

    try:

        with open(path, encoding="utf-8") as file:
            data = json.load(file)

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])

        if not times:
            return False

        if not times[0].startswith(start_date):
            return False

        if not times[-1].startswith(end_date):
            return False

        for variable in WEATHER_VARIABLES:

            if len(hourly.get(variable, [])) != len(times):
                return False

        return True

    except (OSError, json.JSONDecodeError, TypeError, AttributeError):
        return False


# ============================================================
# DOWNLOAD HELPERS
# ============================================================

def download_archive(url, destination):
    """Download ZIP to a temporary file and validate it."""

    temporary = destination.with_suffix(".zip.part")

    try:

        with requests.get(
            url,
            stream=True,
            timeout=(30, 300),
        ) as response:

            response.raise_for_status()

            with open(temporary, "wb") as output:

                for chunk in response.iter_content(
                    chunk_size=1024 * 1024
                ):

                    if chunk:
                        output.write(chunk)

        if temporary.stat().st_size == 0:
            raise ValueError("Downloaded ZIP is empty.")

        with zipfile.ZipFile(temporary) as archive:

            bad_file = archive.testzip()

            if bad_file:
                raise ValueError(
                    f"Corrupted ZIP member: {bad_file}"
                )

        os.replace(temporary, destination)

    finally:

        if temporary.exists():
            temporary.unlink()


def extract_to_staging(zip_path, staging_dir):
    """Extract CSV files safely into a temporary directory."""

    extracted_files = []

    with zipfile.ZipFile(zip_path) as archive:

        members = [
            item
            for item in archive.infolist()
            if not item.is_dir()
            and item.filename.lower().endswith(".csv")
        ]

        if not members:
            raise ValueError("No CSV files found inside ZIP.")

        for member in members:

            # Use basename only to avoid extracting arbitrary paths.
            filename = Path(member.filename).name

            destination = staging_dir / filename

            with archive.open(member) as source:

                with open(destination, "wb") as output:
                    shutil.copyfileobj(source, output)

            extracted_files.append(destination)

    return extracted_files


# ============================================================
# DAG
# ============================================================

with DAG(

    dag_id="citibike_weather_monthly_ingestion",

    description=(
        "Wait for previous month's Citi Bike data, "
        "download and validate it, then download matching weather."
    ),

    start_date=pendulum.datetime(
        2026, 9, 5, 6,
        tz=TIMEZONE,
    ),

    # Run on the 5th day of every month at 06:00 Tallinn time.
    schedule="0 6 5 * *",

    catchup=False,

    max_active_runs=1,

    params={
        "target_month": Param(
            "",
            type="string",
            pattern=r"^$|^\d{4}-(0[1-9]|1[0-2])$",
            description=(
                "Optional month in YYYY-MM format. "
                "Leave empty to use the previous month."
            ),
        )
    },

    tags=[
        "citibike",
        "weather",
        "ingestion",
    ],

) as dag:

    # --------------------------------------------------------
    # TASK 1: Determine target month
    # --------------------------------------------------------

    @task
    def determine_month():

        context = get_current_context()

        dag_run = context["dag_run"]

        manual_month = (
            (dag_run.conf or {}).get("target_month")
            if dag_run else None
        )

        requested_month = (
            manual_month
            or context["params"].get("target_month")
        )

        if requested_month:

            datetime.strptime(requested_month, "%Y-%m")

            print(f"Using requested month: {requested_month}")

            return requested_month

        # For a manual run, use today's date.
        # For a scheduled run, use the scheduled interval end.
        if dag_run and str(dag_run.run_id).startswith("manual__"):

            reference_date = pendulum.now(TIMEZONE)

        else:

            reference_date = (
                context["data_interval_end"]
                .in_timezone(TIMEZONE)
            )

        target_month = (
            reference_date
            .subtract(months=1)
            .format("YYYY-MM")
        )

        print(f"Target month: {target_month}")

        return target_month

    # --------------------------------------------------------
    # TASK 2: Wait for Citi Bike publication
    # --------------------------------------------------------

    def check_citibike_availability(month):

        url = get_archive_url(month)

        print(f"Checking archive: {url}")

        response = requests.head(
            url,
            allow_redirects=True,
            timeout=(20, 60),
        )

        if response.status_code == 404:

            print(
                f"Citi Bike data for {month} is not available yet."
            )

            return False

        response.raise_for_status()

        print("Citi Bike archive is available.")

        return True

    month = determine_month()

    wait_for_citibike = PythonSensor(

        task_id="wait_for_citibike_archive",

        python_callable=check_citibike_availability,

        op_kwargs={
            "month": month,
        },

        # Release the worker while waiting.
        mode="reschedule",

        # Check again every 24 hours.
        poke_interval=24 * 60 * 60,

        # Stop after 60 days.
        timeout=60 * 24 * 60 * 60,

        soft_fail=False,

        do_xcom_push=False,

    )

    month >> wait_for_citibike

    # --------------------------------------------------------
    # TASK 3: Download or reuse Citi Bike data
    # --------------------------------------------------------

    @task
    def ingest_citibike(month):

        CITIBIKE_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        url = get_archive_url(month)

        zip_path = (
            CITIBIKE_DIR
            / f"{month.replace('-', '')}-citibike-tripdata.zip"
        )

        manifest_path = (
            CITIBIKE_DIR
            / f"{month.replace('-', '')}_manifest.json"
        )

        # Check current source metadata without downloading.
        remote_metadata = get_source_metadata(url)

        local_csvs = get_month_csvs(month)

        manifest = read_manifest(manifest_path)

        # ----------------------------------------------------
        # Fast rerun: unchanged source and validated local data
        # ----------------------------------------------------

        if manifest and local_csvs:

            if (
                manifest.get("month") == month
                and metadata_unchanged(
                    manifest.get("source", {}),
                    remote_metadata,
                )
                and manifest.get("files") == file_signatures(local_csvs)
            ):

                # Check local rows and data quality again.
                local_count = validate_and_count_csvs(local_csvs)

                if local_count == manifest.get("trip_rows"):

                    print(
                        "Citi Bike source and local data are unchanged. "
                        "Skipping download."
                    )

                    return {
                        "month": month,
                        "trip_rows": local_count,
                        "csv_files": [str(p) for p in local_csvs],
                        "action": "skipped_unchanged",
                    }

        # ----------------------------------------------------
        # Obtain source ZIP
        # ----------------------------------------------------

        # If a manifest exists but source metadata changed,
        # download a fresh archive. Otherwise reuse the ZIP
        # already downloaded manually, if present.
        source_changed = (
            manifest is not None
            and not metadata_unchanged(
                manifest.get("source", {}),
                remote_metadata,
            )
        )

        if source_changed or not zip_path.exists():

            print("Downloading current Citi Bike archive.")

            download_archive(url, zip_path)

        else:

            print("Using existing local Citi Bike ZIP.")

        staging_dir = Path(
            tempfile.mkdtemp(
                prefix=f"citibike_{month}_",
                dir=CITIBIKE_DIR,
            )
        )

        try:

            staged_csvs = extract_to_staging(
                zip_path,
                staging_dir,
            )

            # Count and validate source archive records.
            source_count = validate_and_count_csvs(
                staged_csvs
            )

            # Count and validate local records.
            local_count = None

            if local_csvs:

                try:

                    local_count = validate_and_count_csvs(
                        local_csvs
                    )

                except (OSError, ValueError, csv.Error) as error:

                    print(
                        f"Existing CSV data failed validation: {error}"
                    )

            # ------------------------------------------------
            # Compare source and local data
            # ------------------------------------------------

            if local_count == source_count:

                print(
                    f"Row counts match: {source_count:,}. "
                    "Keeping existing CSV files."
                )

                final_csvs = local_csvs

                action = "skipped_same_row_count"

            else:

                print(
                    f"Local rows: {local_count}. "
                    f"Source rows: {source_count:,}."
                )

                print("Replacing local CSV files.")

                # Only replace this month's files.
                for old_file in local_csvs:
                    old_file.unlink()

                final_csvs = []

                for staged_file in staged_csvs:

                    destination = (
                        CITIBIKE_DIR / staged_file.name
                    )

                    os.replace(
                        staged_file,
                        destination,
                    )

                    final_csvs.append(destination)

                action = "downloaded_or_replaced"

            # Save source information for future reruns.
            write_manifest(
                manifest_path,
                {
                    "month": month,
                    "source_url": url,
                    "source": remote_metadata,
                    "trip_rows": source_count,
                    "files": file_signatures(final_csvs),
                    "action": action,
                    "checked_at_utc": (
                        pendulum.now("UTC").to_iso8601_string()
                    ),
                },
            )

            return {
                "month": month,
                "trip_rows": source_count,
                "csv_files": [str(p) for p in final_csvs],
                "action": action,
            }

        finally:

            shutil.rmtree(
                staging_dir,
                ignore_errors=True,
            )

    bike_result = ingest_citibike(month)

    wait_for_citibike >> bike_result

    # --------------------------------------------------------
    # TASK 4: Download or reuse matching weather data
    # --------------------------------------------------------

    @task
    def ingest_weather(month, bike_result):

        # This task cannot execute until Citi Bike succeeds.
        if bike_result["month"] != month:

            raise ValueError(
                "Citi Bike and weather months do not match."
            )

        start_date, end_date = get_month_dates(month)

        WEATHER_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_path = (
            WEATHER_DIR / f"nyc_weather_{month}.json"
        )

        temporary_path = (
            WEATHER_DIR / f"nyc_weather_{month}.json.part"
        )

        # Reuse valid weather JSON.
        if weather_is_valid(
            output_path,
            start_date,
            end_date,
        ):

            with open(output_path, encoding="utf-8") as file:
                existing = json.load(file)

            print("Existing weather JSON is valid. Skipping download.")

            return {
                "month": month,
                "weather_file": str(output_path),
                "weather_hours": len(existing["hourly"]["time"]),
                "action": "skipped_existing",
            }

        # Request matching NYC weather observations.
        parameters = {

            "latitude": 40.7128,

            "longitude": -74.0060,

            "start_date": start_date,

            "end_date": end_date,

            "hourly": ",".join(WEATHER_VARIABLES),

            "wind_speed_unit": "kmh",

            "timezone": "America/New_York",
        }

        print(f"Downloading weather for {month}.")

        response = requests.get(
            WEATHER_URL,
            params=parameters,
            timeout=(30, 180),
        )

        response.raise_for_status()

        data = response.json()

        hourly = data.get("hourly", {})

        times = hourly.get("time", [])

        if not times:
            raise ValueError(
                "Open-Meteo returned no hourly observations."
            )

        if not times[0].startswith(start_date):
            raise ValueError("Weather start date is incorrect.")

        if not times[-1].startswith(end_date):
            raise ValueError("Weather end date is incorrect.")

        for variable in WEATHER_VARIABLES:

            if len(hourly.get(variable, [])) != len(times):

                raise ValueError(
                    f"Incorrect number of values for {variable}."
                )

        with open(
            temporary_path,
            "w",
            encoding="utf-8",
        ) as file:

            json.dump(data, file, indent=2)

        if not weather_is_valid(
            temporary_path,
            start_date,
            end_date,
        ):

            raise ValueError(
                "Downloaded weather JSON failed validation."
            )

        os.replace(
            temporary_path,
            output_path,
        )

        print(f"Weather saved: {output_path}")

        return {
            "month": month,
            "weather_file": str(output_path),
            "weather_hours": len(times),
            "action": "downloaded",
        }

    weather_result = ingest_weather(
        month,
        bike_result,
    )

    # --------------------------------------------------------
    # TASK 5: Final report
    # --------------------------------------------------------

    @task
    def report_results(bike_result, weather_result):

        print("===================================")
        print("MONTHLY RAW INGESTION COMPLETED")
        print("===================================")

        print(f"Month: {bike_result['month']}")

        print(
            f"Citi Bike trips: {bike_result['trip_rows']:,}"
        )

        print(
            f"Citi Bike action: {bike_result['action']}"
        )

        print(
            f"Weather observations: "
            f"{weather_result['weather_hours']}"
        )

        print(
            f"Weather action: {weather_result['action']}"
        )

    report_results(
        bike_result,
        weather_result,
    )
