{{ config(materialized='table', engine='MergeTree()', partition_by='toYYYYMM(trip_date)', order_by='(trip_date, ride_id)') }}

-- Grain: one row per completed CitiBike ride (ride_id).
-- Station and rider keys are the same deterministic cityHash64 hashes used in the
-- dimensions, so no join is needed to look them up. The depends_on hints below make
-- dbt build the dimensions first and show the star schema in the lineage graph.
-- depends_on: {{ ref('dim_station') }}
-- depends_on: {{ ref('dim_rider') }}

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

