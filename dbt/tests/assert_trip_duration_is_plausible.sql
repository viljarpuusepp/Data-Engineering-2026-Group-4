select *
from {{ ref('fact_trip') }}
where trip_duration_sec < 0 or trip_duration_sec > 86400

