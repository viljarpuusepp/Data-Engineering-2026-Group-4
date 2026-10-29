{{ config(materialized='table', engine='MergeTree()', partition_by='toYYYYMM(trip_date)', order_by='(trip_date, ride_id)') }}

select
    trips.ride_id,
    toDate(trips.started_at) as trip_date,
    toHour(trips.started_at) as trip_start_hour,
    cityHash64(trips.start_station_id) as start_station_key,
    cityHash64(trips.end_station_id) as end_station_key,
    cityHash64(concat(trips.rideable_type, '|', trips.member_casual)) as rider_key,
    weather.weather_key,
    trips.started_at,
    trips.ended_at,
    trips.trip_duration_sec,
    trips.source_month
from {{ ref('stg_trips') }} as trips
left join {{ ref('dim_weather') }} as weather
    on weather.observation_hour = toStartOfHour(trips.started_at)

