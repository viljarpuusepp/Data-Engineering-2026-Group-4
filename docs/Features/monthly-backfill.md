# Monthly Backfill

## Behavior

An operator triggers `citibike_monthly_backfill` with `{"month":"YYYY-MM"}`. When omitted, Airflow uses the run's logical month. The pipeline downloads the matching `JC-YYYYMM-citibike-tripdata.csv.zip`, extracts the only CSV, determines its real trip date range, then fetches the matching hourly Jersey City Open-Meteo history.

## Rules and failure behavior

- The month must match `YYYY-MM`.
- A trip CSV must be non-empty, have all expected fields, contain required trip values, contain no duplicate ride IDs, and have no negative duration. Silver excludes trips longer than 24 hours.
- Weather must contain a complete hourly series for the requested trip-date range.
- Validation failure prevents all Bronze writes for that run.
- A rerun writes newer Bronze versions, then compacts the affected ClickHouse partition so Bronze, Silver, and Gold each expose one record per natural key.

## Definition of done

A month is successful when all Airflow tasks complete, `dbt build` passes, and one fact row exists for every valid unique Bronze ride ID.

