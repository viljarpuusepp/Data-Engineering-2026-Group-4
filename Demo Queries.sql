-- What are the top 5 most popular starting stations during the morning rush hour (7 AM - 9 AM)?
SELECT
    ds.station_name,
    count(ft.ride_id) as rush_hour_trips
FROM fact_trip ft
JOIN dim_station ds 
ON ft.start_station_key = ds.station_key
JOIN dim_time dt 
ON ft.time_key = dt.time_key
WHERE ride_start_hour IN (7,8)
GROUP BY ds.station_name
ORDER BY rush_hour_trips DESC
LIMIT 5;


-- How does the total rental volume differ between weekends and weekdays?
SELECT
    dd.is_weekend,
    uniqExact(ft.date_key) as days,
    count(ft.ride_id) as trips_in_total,
    round(count(ft.ride_id)/uniqExact(ft.date_key)) as average_trips_per_day,    
    round(avg(ft.trip_duration_sec/60),1) as average_ride_time_minutes
FROM fact_trip ft
JOIN dim_date dd 
ON ft.date_key = dd.date_key
GROUP BY dd.is_weekend
ORDER BY dd.is_weekend;


-- Help table to include hours with 0 rides
CREATE TABLE hourly_rides AS
SELECT
    dw.weather_key,
    dw.start_hour,
    dw.temp_c,
    dw.precipitation_mm,
    dw.rain_mm,
    dw.snowfall_cm,
    dw.wind_speed_kmh,
    dw.is_wet,
    coalesce(t.ride, 0) as rides
FROM dim_weather dw
LEFT JOIN (
    SELECT
    weather_key,
    count(ride_id) as rides
    FROM fact_trip
    GROUP BY weather_key
) t ON t.weather_key = dw.weather_key;


-- How do temperature and precipitation affect the number of rides per hour?
SELECT
    floor(temp_c / 5) * 5 as temp_bucket_c,
    multiIf(precipitation_mm = 0, '1 dry',
            precipitation_mm < 2.5, '2 light',
            '3 heavy') as precipitation,
    count() as hours_observed,
    round(avg(rides)) as avg_rides_per_hour
FROM hourly_rides
GROUP BY temp_bucket_c, precipitation
ORDER BY temp_bucket_c, precipitation;


-- Which weather condition reduces riding the most: rain, snow, wind, cold or heat?
SELECT
    round(avgIf(rides, rain_mm = 0 AND snowfall_cm = 0 AND wind_speed_kmh < 20 AND temp_c >= 10 AND temp_c < 25)) as fair_rides_per_hour,
    round(100 * (avgIf(rides, rain_mm > 0) / fair_rides_per_hour - 1), 1) as rain_pct_change,
    round(100 * (avgIf(rides, snowfall_cm > 0) / fair_rides_per_hour - 1), 1) as snow_pct_change,
    round(100 * (avgIf(rides, wind_speed_kmh >= 20) / fair_rides_per_hour - 1), 1) as wind_pct_change,
    round(100 * (avgIf(rides, temp_c < 10) / fair_rides_per_hour - 1), 1) as cold_pct_change,
    round(100 * (avgIf(rides, temp_c >= 25) / fair_rides_per_hour - 1), 1) as heat_pct_change
FROM hourly_rides;


-- Are casual riders more weather-sensitive than members?
SELECT
    dr.member_casual,
    round(countIf(dw.is_wet = 0) / (SELECT countIf(is_wet = 0) FROM dim_weather), 1) as dry_rides_per_hour,
    round(countIf(dw.is_wet = 1) / (SELECT countIf(is_wet = 1) FROM dim_weather), 1) as wet_rides_per_hour,
    round(100 * (wet_rides_per_hour / dry_rides_per_hour - 1), 1) as pct_change_when_wet
FROM fact_trip ft
JOIN dim_weather dw 
ON ft.weather_key = dw.weather_key
JOIN dim_rider dr 
ON ft.rider_key = dr.rider_key
GROUP BY dr.member_casual
ORDER BY pct_change_when_wet;


-- Do riders shift to e-bikes in bad weather?
SELECT
    dw.is_wet,
    count(ft.ride_id) as total_trips,
    countIf(dr.rideable_type = 'electric_bike') as ebike_trips,
    round(100 * countIf(dr.rideable_type = 'electric_bike') / count(ft.ride_id), 1) as ebike_share_pct
FROM fact_trip ft
JOIN dim_weather dw 
ON ft.weather_key = dw.weather_key
JOIN dim_rider dr 
ON ft.rider_key = dr.rider_key
GROUP BY dw.is_wet
ORDER BY dw.is_wet;
