{{ config(materialized='view') }}

with ranked as (
    select
        *,
        row_number() over (partition by observation_hour order by ingested_at desc) as latest_row
    from {{ source('bronze', 'open_meteo_hourly_raw') }}
)
select
    observation_hour,
    temperature_2m,
    apparent_temperature,
    precipitation,
    rain,
    snowfall,
    snow_depth,
    wind_speed_10m,
    weather_code,
    source_month
from ranked
where latest_row = 1

