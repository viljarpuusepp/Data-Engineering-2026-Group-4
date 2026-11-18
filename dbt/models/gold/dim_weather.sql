{{ config(materialized='table', engine='MergeTree()', order_by='weather_key') }}

select
    toUInt64(formatDateTime(observation_hour, '%Y%m%d%H')) as weather_key,
    observation_hour,
    temperature_2m,
    apparent_temperature,
    precipitation,
    rain,
    snowfall,
    snow_depth,
    wind_speed_10m,
    weather_code,
    (coalesce(precipitation, 0) > 0 or coalesce(rain, 0) > 0 or coalesce(snowfall, 0) > 0) as is_wet
from {{ ref('stg_weather') }}

