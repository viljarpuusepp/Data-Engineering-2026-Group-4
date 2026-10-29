{{ config(materialized='view') }}

with ranked as (
    select
        *,
        row_number() over (partition by ride_id order by ingested_at desc) as latest_row
    from {{ source('bronze', 'citibike_trip_raw') }}
),
cleaned as (
    select
        ride_id,
        assumeNotNull(rideable_type) as rideable_type,
        assumeNotNull(started_at) as started_at,
        assumeNotNull(ended_at) as ended_at,
        coalesce(start_station_name, 'Unknown') as start_station_name,
        assumeNotNull(start_station_id) as start_station_id,
        coalesce(end_station_name, 'Unknown') as end_station_name,
        assumeNotNull(end_station_id) as end_station_id,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        assumeNotNull(member_casual) as member_casual,
        source_month,
        dateDiff('second', started_at, ended_at) as trip_duration_sec
    from ranked
    where latest_row = 1
      and started_at is not null
      and ended_at is not null
      and ended_at >= started_at
      and dateDiff('second', started_at, ended_at) <= 86400
      and start_station_id is not null
      and end_station_id is not null
      and rideable_type is not null
      and member_casual is not null
)
select * from cleaned

