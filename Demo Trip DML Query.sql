/* ============================================================
   CITI BIKE TRIP DATA ETL
   ------------------------------------------------------------
   Purpose:
   1. Load raw trip CSV into staging_trip
   2. Populate dim_date
   3. Populate dim_time
   4. Populate dim_station
   5. Populate dim_rider
   6. Populate fact_trip
   7. Match trips to hourly weather data

   Source example:
   JC-202608-citibike-tripdata.csv

   Assumptions:
   - PostgreSQL
   - fact_trip.ride_id is VARCHAR
   - dim_weather already contains hourly weather data
   - dim_weather.start_hour is rounded to full hours
   ============================================================ */


/* ============================================================
   1. CREATE RAW STAGING TABLE
   ------------------------------------------------------------
   This table mirrors the Citi Bike CSV structure.
   It is intentionally not normalized.
   ============================================================ */

DROP TABLE IF EXISTS staging_trip;

CREATE TABLE staging_trip (
    ride_id            VARCHAR(50),
    rideable_type      VARCHAR(50),
    started_at         TIMESTAMP,
    ended_at           TIMESTAMP,
    start_station_name VARCHAR(255),
    start_station_id   VARCHAR(100),
    end_station_name   VARCHAR(255),
    end_station_id     VARCHAR(100),
    start_lat          DOUBLE PRECISION,
    start_lng          DOUBLE PRECISION,
    end_lat            DOUBLE PRECISION,
    end_lng            DOUBLE PRECISION,
    member_casual      VARCHAR(50)
);


/* ============================================================
   2. LOAD CSV INTO staging_trip
   ------------------------------------------------------------
   Run this command from psql.

   Change the file path to the actual extracted CSV file.

   Example:
   \copy staging_trip
   FROM 'C:/data/JC-202608-citibike-tripdata.csv'
   WITH (
       FORMAT CSV,
       HEADER TRUE,
       DELIMITER ',',
       QUOTE '"'
   );

   NOTE:
   \copy is a psql command rather than normal SQL.
   ============================================================ */


/* ============================================================
   3. POPULATE DATE DIMENSION
   ------------------------------------------------------------
   date_key format:
       YYYYMMDD

   Example:
       2026-08-23 -> 20260823

   is_holiday is set to FALSE in this demo.
   A separate holiday calendar should be used in production.
   ============================================================ */

INSERT INTO dim_date (
    date_key,
    date_ddmmyyyy,
    year,
    month,
    weekday,
    is_weekend,
    is_workday,
    is_holiday
)
SELECT DISTINCT
    TO_CHAR(started_at::date, 'YYYYMMDD')::INTEGER AS date_key,

    started_at::date AS date_ddmmyyyy,

    EXTRACT(YEAR FROM started_at)::INTEGER AS year,

    EXTRACT(MONTH FROM started_at)::INTEGER AS month,

    TO_CHAR(started_at, 'FMDay') AS weekday,

    CASE
        WHEN EXTRACT(ISODOW FROM started_at) IN (6, 7)
        THEN TRUE
        ELSE FALSE
    END AS is_weekend,

    CASE
        WHEN EXTRACT(ISODOW FROM started_at) BETWEEN 1 AND 5
        THEN TRUE
        ELSE FALSE
    END AS is_workday,

    FALSE AS is_holiday

FROM staging_trip

WHERE started_at IS NOT NULL

ON CONFLICT (date_key) DO NOTHING;


/* ============================================================
   4. POPULATE TIME DIMENSION
   ------------------------------------------------------------
   time_key is simply the hour:
       0 ... 23

   Rush hours in this demo:
       Morning:   07:00-09:59
       Afternoon: 16:00-18:59
   ============================================================ */

INSERT INTO dim_time (
    time_key,
    ride_start_hour,
    is_rush_hour
)
SELECT DISTINCT
    EXTRACT(HOUR FROM started_at)::INTEGER AS time_key,

    EXTRACT(HOUR FROM started_at)::INTEGER AS ride_start_hour,

    CASE
        WHEN EXTRACT(HOUR FROM started_at) BETWEEN 7 AND 9
          OR EXTRACT(HOUR FROM started_at) BETWEEN 16 AND 18
        THEN TRUE
        ELSE FALSE
    END AS is_rush_hour

FROM staging_trip

WHERE started_at IS NOT NULL

ON CONFLICT (time_key) DO NOTHING;


/* ============================================================
   5. POPULATE STATION DIMENSION
   ------------------------------------------------------------
   Start and end stations are combined into one station list.

   station_key:
       Surrogate key used by fact_trip.

   station_id:
       Citi Bike's original station identifier, e.g. HB303.

   AVG(lat) / AVG(lng):
       The source data may contain slightly different coordinates
       for the same station, so average coordinates are used.

   capacity:
       Not present in Citi Bike trip data, therefore NULL.
   ============================================================ */

WITH all_stations AS (

    /* Start stations */
    SELECT
        start_station_id   AS station_id,
        start_station_name AS station_name,
        start_lat          AS lat,
        start_lng          AS lng
    FROM staging_trip
    WHERE start_station_id IS NOT NULL

    UNION ALL

    /* End stations */
    SELECT
        end_station_id     AS station_id,
        end_station_name   AS station_name,
        end_lat            AS lat,
        end_lng            AS lng
    FROM staging_trip
    WHERE end_station_id IS NOT NULL

),

new_stations AS (

    SELECT
        station_id,
        MAX(station_name) AS station_name,
        AVG(lat)          AS lat,
        AVG(lng)          AS lng
    FROM all_stations

    GROUP BY station_id

)

INSERT INTO dim_station (
    station_key,
    station_id,
    station_name,
    lat,
    lng,
    capacity
)
SELECT
    ROW_NUMBER() OVER (
        ORDER BY ns.station_id
    )
    +
    COALESCE(
        (
            SELECT MAX(station_key)
            FROM dim_station
        ),
        0
    ) AS station_key,

    ns.station_id,
    ns.station_name,
    ns.lat,
    ns.lng,

    NULL AS capacity

FROM new_stations ns

WHERE NOT EXISTS (
    SELECT 1
    FROM dim_station d
    WHERE d.station_id = ns.station_id
);


/* ============================================================
   6. POPULATE RIDER DIMENSION
   ------------------------------------------------------------
   dim_rider represents the unique combination of:

       rideable_type
       member_casual

   Examples:

       electric_bike + member
       electric_bike + casual
       classic_bike  + member
       classic_bike  + casual
   ============================================================ */

WITH rider_combinations AS (

    SELECT DISTINCT
        rideable_type,
        member_casual

    FROM staging_trip

    WHERE rideable_type IS NOT NULL
      AND member_casual IS NOT NULL

)

INSERT INTO dim_rider (
    rider_key,
    rideable_type,
    member_casual
)
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            r.rideable_type,
            r.member_casual
    )
    +
    COALESCE(
        (
            SELECT MAX(rider_key)
            FROM dim_rider
        ),
        0
    ) AS rider_key,

    r.rideable_type,
    r.member_casual

FROM rider_combinations r

WHERE NOT EXISTS (
    SELECT 1
    FROM dim_rider d

    WHERE d.rideable_type = r.rideable_type
      AND d.member_casual = r.member_casual
);


/* ============================================================
   7. LOAD FACT TABLE
   ------------------------------------------------------------
   Each staging trip is converted into one fact_trip row.

   Dimension mappings:

   date_key:
       derived from started_at

   time_key:
       hour from started_at

   start_station_key:
       looked up using start_station_id

   end_station_key:
       looked up using end_station_id

   rider_key:
       looked up using:
           rideable_type + member_casual

   weather_key:
       looked up using the trip start hour

   trip_duration_sec:
       ended_at - started_at in seconds

   Weather matching example:

       trip started:
           2026-08-23 07:43:32

       weather record:
           2026-08-23 07:00:00
   ============================================================ */

INSERT INTO fact_trip (
    ride_id,
    date_key,
    time_key,
    start_station_key,
    end_station_key,
    weather_key,
    rider_key,
    started_at,
    ended_at,
    trip_duration_sec
)

SELECT
    /* Original Citi Bike ride identifier */
    s.ride_id,


    /* Date surrogate key: YYYYMMDD */
    TO_CHAR(
        s.started_at::date,
        'YYYYMMDD'
    )::INTEGER AS date_key,


    /* Hour of day: 0-23 */
    EXTRACT(
        HOUR FROM s.started_at
    )::INTEGER AS time_key,


    /* Resolve starting station */
    start_station.station_key
        AS start_station_key,


    /* Resolve destination station */
    end_station.station_key
        AS end_station_key,


    /* Resolve weather observation for trip start hour */
    weather.weather_key,


    /* Resolve rider/bike combination */
    rider.rider_key,


    /* Original timestamps */
    s.started_at,
    s.ended_at,


    /* Calculate trip duration in seconds */
    EXTRACT(
        EPOCH FROM (
            s.ended_at - s.started_at
        )
    )::INTEGER AS trip_duration_sec


FROM staging_trip s


/* ------------------------------------------------------------
   Start station dimension
   ------------------------------------------------------------ */
JOIN dim_station start_station
    ON start_station.station_id =
       s.start_station_id


/* ------------------------------------------------------------
   End station dimension
   ------------------------------------------------------------ */
JOIN dim_station end_station
    ON end_station.station_id =
       s.end_station_id


/* ------------------------------------------------------------
   Rider dimension

   Unique rider dimension record is determined by:
       rideable_type
       +
       member_casual
   ------------------------------------------------------------ */
JOIN dim_rider rider
    ON rider.rideable_type =
       s.rideable_type

   AND rider.member_casual =
       s.member_casual


/* ------------------------------------------------------------
   Weather dimension

   DATE_TRUNC converts:

       2026-08-23 07:43:32
   to
       2026-08-23 07:00:00

   so the trip can be matched with hourly weather data.
   ------------------------------------------------------------ */
LEFT JOIN dim_weather weather
    ON weather.start_hour =
       DATE_TRUNC(
           'hour',
           s.started_at
       )


/* Ignore invalid source rows */
WHERE s.ride_id IS NOT NULL
  AND s.started_at IS NOT NULL
  AND s.ended_at IS NOT NULL


/* Do not insert the same ride twice */
ON CONFLICT (ride_id) DO NOTHING;


/* ============================================================
   8. VERIFY LOADED FACT DATA
   ============================================================ */

SELECT
    f.ride_id,
    f.started_at,
    f.ended_at,
    f.trip_duration_sec,

    d.date_ddmmyyyy,
    t.ride_start_hour,

    ss.station_name AS start_station,
    es.station_name AS end_station,

    r.rideable_type,
    r.member_casual,

    w.temp_c,
    w.precipitation_mm,
    w.weather_category

FROM fact_trip f

JOIN dim_date d
    ON d.date_key = f.date_key

JOIN dim_time t
    ON t.time_key = f.time_key

JOIN dim_station ss
    ON ss.station_key = f.start_station_key

JOIN dim_station es
    ON es.station_key = f.end_station_key

JOIN dim_rider r
    ON r.rider_key = f.rider_key

LEFT JOIN dim_weather w
    ON w.weather_key = f.weather_key

ORDER BY f.started_at

LIMIT 100;


/* ============================================================
   9. OPTIONAL LOAD SUMMARY
   ============================================================ */

SELECT
    COUNT(*) AS number_of_trips,

    MIN(started_at) AS first_trip,

    MAX(started_at) AS last_trip,

    ROUND(
        AVG(trip_duration_sec)
    ) AS avg_trip_duration_sec,

    COUNT(weather_key) AS trips_with_weather,

    COUNT(*) - COUNT(weather_key)
        AS trips_without_weather

FROM fact_trip;
