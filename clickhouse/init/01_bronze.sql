CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS bronze.citibike_trip_raw
(
    ride_id String,
    rideable_type Nullable(String),
    started_at Nullable(DateTime64(0, 'UTC')),
    ended_at Nullable(DateTime64(0, 'UTC')),
    start_station_name Nullable(String),
    start_station_id Nullable(String),
    end_station_name Nullable(String),
    end_station_id Nullable(String),
    start_lat Nullable(Float64),
    start_lng Nullable(Float64),
    end_lat Nullable(Float64),
    end_lng Nullable(Float64),
    member_casual Nullable(String),
    source_month String,
    source_url String,
    payload_sha256 FixedString(64),
    ingested_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY source_month
ORDER BY ride_id;

CREATE TABLE IF NOT EXISTS bronze.open_meteo_hourly_raw
(
    observation_hour DateTime64(0, 'UTC'),
    temperature_2m Nullable(Float64),
    apparent_temperature Nullable(Float64),
    precipitation Nullable(Float64),
    rain Nullable(Float64),
    snowfall Nullable(Float64),
    snow_depth Nullable(Float64),
    wind_speed_10m Nullable(Float64),
    weather_code Nullable(Int16),
    source_month String,
    source_url String,
    payload_sha256 FixedString(64),
    ingested_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY source_month
ORDER BY observation_hour;

