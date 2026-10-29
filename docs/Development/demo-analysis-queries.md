# Demo Analytics Queries

Run these in the [ClickHouse Play UI](http://localhost:8123/play) or `clickhouse-client` after a successful monthly backfill. The Play UI runs one statement at a time and may reset to the `default` database, so every table below is explicitly qualified with `analytics.`.

These queries preserve the Project 1 analysis questions but target the implemented three-dimension Gold schema. Unlike the older P1 SQL, `fact_trip` contains `trip_date` and `trip_start_hour`; there are no `dim_date` or `dim_time` tables. Open-Meteo fields use names such as `temperature_2m`, `precipitation`, and `wind_speed_10m`.

## 1. Five most popular morning-rush starting stations

```sql
SELECT
    station.station_name,
    count(fact.ride_id) AS rush_hour_trips
FROM analytics.fact_trip AS fact
JOIN analytics.dim_station AS station
    ON fact.start_station_key = station.station_key
WHERE fact.trip_start_hour IN (7, 8)
GROUP BY station.station_name
ORDER BY rush_hour_trips DESC
LIMIT 5;
```

## 2. Weekend versus weekday rental volume

```sql
SELECT
    toDayOfWeek(trip_date) IN (6, 7) AS is_weekend,
    uniqExact(trip_date) AS days,
    count() AS trips_in_total,
    round(count() / uniqExact(trip_date), 1) AS average_trips_per_day,
    round(avg(trip_duration_sec / 60), 1) AS average_ride_time_minutes
FROM analytics.fact_trip
GROUP BY is_weekend
ORDER BY is_weekend;
```

## 3. Are casual riders more weather-sensitive than members?

This normalises each segment's ride count by the number of observed wet or dry weather hours.

```sql
WITH weather_hours AS (
    SELECT
        countIf(is_wet = 0) AS dry_hours,
        countIf(is_wet = 1) AS wet_hours
    FROM analytics.dim_weather
),
rider_activity AS (
    SELECT
        rider.member_casual,
        countIf(weather.is_wet = 0) / weather_hours.dry_hours AS dry_rides_per_hour,
        countIf(weather.is_wet = 1) / weather_hours.wet_hours AS wet_rides_per_hour
    FROM analytics.fact_trip AS fact
    JOIN analytics.dim_weather AS weather
        ON fact.weather_key = weather.weather_key
    JOIN analytics.dim_rider AS rider
        ON fact.rider_key = rider.rider_key
    CROSS JOIN weather_hours
    GROUP BY rider.member_casual, weather_hours.dry_hours, weather_hours.wet_hours
)
SELECT
    member_casual,
    round(dry_rides_per_hour, 1) AS dry_rides_per_hour,
    round(wet_rides_per_hour, 1) AS wet_rides_per_hour,
    round(100 * (wet_rides_per_hour / dry_rides_per_hour - 1), 1) AS pct_change_when_wet
FROM rider_activity
ORDER BY pct_change_when_wet;
```

## 4. Temperature and precipitation effect on hourly rides

The CTE is an in-query helper table. It includes weather hours with zero rides without creating a persistent table.

```sql
WITH hourly_rides AS (
    SELECT
        weather.weather_key,
        weather.observation_hour,
        weather.temperature_2m,
        weather.precipitation,
        weather.rain,
        weather.snowfall,
        weather.wind_speed_10m,
        weather.is_wet,
        coalesce(trips.rides, 0) AS rides
    FROM analytics.dim_weather AS weather
    LEFT JOIN (
        SELECT weather_key, count() AS rides
        FROM analytics.fact_trip
        WHERE weather_key IS NOT NULL
        GROUP BY weather_key
    ) AS trips
        ON weather.weather_key = trips.weather_key
)
SELECT
    floor(temperature_2m / 5) * 5 AS temp_bucket_c,
    multiIf(
        precipitation = 0, '1 dry',
        precipitation < 2.5, '2 light',
        '3 heavy'
    ) AS precipitation_band,
    count() AS hours_observed,
    round(avg(rides), 1) AS average_rides_per_hour
FROM hourly_rides
GROUP BY temp_bucket_c, precipitation_band
ORDER BY temp_bucket_c, precipitation_band;
```

## 5. Which weather condition reduces riding the most?

```sql
WITH hourly_rides AS (
    SELECT
        weather.temperature_2m,
        weather.rain,
        weather.snowfall,
        weather.wind_speed_10m,
        coalesce(trips.rides, 0) AS rides
    FROM analytics.dim_weather AS weather
    LEFT JOIN (
        SELECT weather_key, count() AS rides
        FROM analytics.fact_trip
        WHERE weather_key IS NOT NULL
        GROUP BY weather_key
    ) AS trips
        ON weather.weather_key = trips.weather_key
),
baselines AS (
    SELECT avgIf(rides, rain = 0 AND snowfall = 0 AND wind_speed_10m < 20
        AND temperature_2m >= 10 AND temperature_2m < 25) AS fair_rides_per_hour
    FROM hourly_rides
)
SELECT
    round(fair_rides_per_hour, 1) AS fair_rides_per_hour,
    round(100 * (avgIf(rides, rain > 0) / fair_rides_per_hour - 1), 1) AS rain_pct_change,
    round(100 * (avgIf(rides, snowfall > 0) / fair_rides_per_hour - 1), 1) AS snow_pct_change,
    round(100 * (avgIf(rides, wind_speed_10m >= 20) / fair_rides_per_hour - 1), 1) AS wind_pct_change,
    round(100 * (avgIf(rides, temperature_2m < 10) / fair_rides_per_hour - 1), 1) AS cold_pct_change,
    round(100 * (avgIf(rides, temperature_2m >= 25) / fair_rides_per_hour - 1), 1) AS heat_pct_change
FROM hourly_rides
CROSS JOIN baselines;
```

## 6. Do riders shift to e-bikes in wet weather?

```sql
SELECT
    weather.is_wet,
    count() AS total_trips,
    countIf(rider.rideable_type = 'electric_bike') AS ebike_trips,
    round(100 * countIf(rider.rideable_type = 'electric_bike') / count(), 1) AS ebike_share_pct
FROM analytics.fact_trip AS fact
JOIN analytics.dim_weather AS weather
    ON fact.weather_key = weather.weather_key
JOIN analytics.dim_rider AS rider
    ON fact.rider_key = rider.rider_key
GROUP BY weather.is_wet
ORDER BY weather.is_wet;
```

## Interpretation notes

- Weather analyses include only trips that matched an hourly weather observation; the weather-hour CTEs explicitly preserve zero-trip weather hours for rate comparisons.
- Results are descriptive, not causal: seasonality, weekday, station coverage, and source-data gaps can affect them.
- The data is monthly and local. Do not compare counts across months without first checking the number of covered days and weather hours.

