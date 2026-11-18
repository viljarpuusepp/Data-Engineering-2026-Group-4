from pathlib import Path

RAW_ROOT = Path("/opt/airflow/data/raw")
CITIBIKE_BASE_URL = "https://s3.amazonaws.com/tripdata"
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
JERSEY_CITY_LATITUDE = 40.7178
JERSEY_CITY_LONGITUDE = -74.0431
REQUIRED_TRIP_COLUMNS = (
    "ride_id", "rideable_type", "started_at", "ended_at", "start_station_name",
    "start_station_id", "end_station_name", "end_station_id", "start_lat",
    "start_lng", "end_lat", "end_lng", "member_casual",
)

