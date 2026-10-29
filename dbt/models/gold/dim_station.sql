{{ config(materialized='table', engine='MergeTree()', order_by='station_key') }}

with stations as (
    select start_station_id as station_id, start_station_name as station_name, start_lat as latitude, start_lng as longitude
    from {{ ref('stg_trips') }}
    union all
    select end_station_id as station_id, end_station_name as station_name, end_lat as latitude, end_lng as longitude
    from {{ ref('stg_trips') }}
)
select
    cityHash64(station_id) as station_key,
    station_id,
    anyLast(station_name) as station_name,
    avg(latitude) as latitude,
    avg(longitude) as longitude
from stations
group by station_id

