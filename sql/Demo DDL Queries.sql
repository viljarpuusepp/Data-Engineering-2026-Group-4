CREATE TABLE dim_date (
    date_key        INTEGER PRIMARY KEY,
    date_ddmmyyyy   DATE NOT NULL,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    weekday         VARCHAR(20) NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    is_workday      BOOLEAN NOT NULL,
    is_holiday      BOOLEAN NOT NULL
);

CREATE TABLE dim_time (
    time_key        INTEGER PRIMARY KEY,
    ride_start_hour INTEGER NOT NULL,
    is_rush_hour    BOOLEAN NOT NULL
);

CREATE TABLE dim_station (
    station_key     INTEGER PRIMARY KEY,
    station_id      VARCHAR(100),
    station_name    VARCHAR(255),
    lat             DOUBLE PRECISION,
    lng             DOUBLE PRECISION,
    capacity        INTEGER
);

CREATE TABLE dim_weather (
    weather_key      INTEGER PRIMARY KEY,
    start_hour       TIMESTAMP NOT NULL,
    temp_c           DOUBLE PRECISION,
    apparent_temp_c  DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    rain_mm          DOUBLE PRECISION,
    snowfall_cm      DOUBLE PRECISION,
    snow_depth_m     DOUBLE PRECISION,
    wind_speed_kmh   DOUBLE PRECISION,
    weather_code     VARCHAR(50),
    weather_category VARCHAR(50),
    is_wet           BOOLEAN
);

CREATE TABLE dim_rider (
    rider_key       INTEGER PRIMARY KEY,
    rideable_type   VARCHAR(50),
    member_casual   VARCHAR(50)
);

CREATE TABLE fact_trip (
    ride_id           VARCHAR(50) PRIMARY KEY,
    date_key          INTEGER NOT NULL,
    time_key          INTEGER NOT NULL,
    start_station_key INTEGER NOT NULL,
    end_station_key   INTEGER NOT NULL,
    weather_key       INTEGER,
    rider_key         INTEGER NOT NULL,
    started_at        TIMESTAMP NOT NULL,
    ended_at          TIMESTAMP NOT NULL,
    trip_duration_sec INTEGER,

    CONSTRAINT fk_trip_date
        FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key),

    CONSTRAINT fk_trip_time
        FOREIGN KEY (time_key)
        REFERENCES dim_time(time_key),

    CONSTRAINT fk_trip_start_station
        FOREIGN KEY (start_station_key)
        REFERENCES dim_station(station_key),

    CONSTRAINT fk_trip_end_station
        FOREIGN KEY (end_station_key)
        REFERENCES dim_station(station_key),

    CONSTRAINT fk_trip_weather
        FOREIGN KEY (weather_key)
        REFERENCES dim_weather(weather_key),

    CONSTRAINT fk_trip_rider
        FOREIGN KEY (rider_key)
        REFERENCES dim_rider(rider_key)
);

# Adding indexes
CREATE INDEX idx_fact_trip_date
    ON fact_trip(date_key);

CREATE INDEX idx_fact_trip_time
    ON fact_trip(time_key);

CREATE INDEX idx_fact_trip_start_station
    ON fact_trip(start_station_key);

CREATE INDEX idx_fact_trip_end_station
    ON fact_trip(end_station_key);

CREATE INDEX idx_fact_trip_weather
    ON fact_trip(weather_key);

CREATE INDEX idx_fact_trip_rider
    ON fact_trip(rider_key);
