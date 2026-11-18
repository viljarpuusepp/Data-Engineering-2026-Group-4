-- One-time setup, if the http extension is available
CREATE EXTENSION IF NOT EXISTS http;


WITH api_response AS (
    SELECT content::jsonb AS data
    FROM http_get(
        'https://archive-api.open-meteo.com/v1/archive'
        || '?latitude=40.7128'
        || '&longitude=74.006'
        || '&start_date=2026-10-04'
        || '&end_date=2026-10-04'
        || '&hourly=temperature_2m,relative_humidity_2m,precipitation,rain,snowfall,snow_depth,apparent_temperature'
    )
),

hourly_data AS (
    SELECT
        t.ordinality,

        t.value::timestamp AS start_hour,

        (a.data->'hourly'->'temperature_2m'
            ->>(t.ordinality - 1))::double precision AS temp_c,

        (a.data->'hourly'->'apparent_temperature'
            ->>(t.ordinality - 1))::double precision AS apparent_temp_c,

        (a.data->'hourly'->'precipitation'
            ->>(t.ordinality - 1))::double precision AS precipitation_mm,

        (a.data->'hourly'->'rain'
            ->>(t.ordinality - 1))::double precision AS rain_mm,

        (a.data->'hourly'->'snowfall'
            ->>(t.ordinality - 1))::double precision AS snowfall_cm,

        (a.data->'hourly'->'snow_depth'
            ->>(t.ordinality - 1))::double precision AS snow_depth_m

    FROM api_response a

    CROSS JOIN LATERAL
        jsonb_array_elements_text(
            a.data->'hourly'->'time'
        ) WITH ORDINALITY AS t(value, ordinality)
)

INSERT INTO dim_weather (
    weather_key,
    start_hour,
    temp_c,
    apparent_temp_c,
    precipitation_mm,
    rain_mm,
    snowfall_cm,
    snow_depth_m,
    wind_speed_kmh,
    weather_code,
    weather_category,
    is_wet
)

SELECT
    -- Example key format YYYYMMDDHH
    to_char(start_hour, 'YYYYMMDDHH')::integer AS weather_key,

    start_hour,
    temp_c,
    apparent_temp_c,
    precipitation_mm,
    rain_mm,
    snowfall_cm,
    snow_depth_m,

    NULL AS wind_speed_kmh,
    NULL AS weather_code,

    CASE
        WHEN snowfall_cm > 0 THEN 'Snowy'
        WHEN rain_mm > 0 THEN 'Rainy'
        WHEN precipitation_mm > 0 THEN 'Wet'
        ELSE 'Clear'
    END AS weather_category,

    CASE
        WHEN precipitation_mm > 0
          OR rain_mm > 0
          OR snowfall_cm > 0
        THEN TRUE
        ELSE FALSE
    END AS is_wet

FROM hourly_data;
