# Status

## Current state

The local Docker, Airflow, ClickHouse, and dbt implementation is running and verified for a manual August 2026 Jersey City backfill. Weather is requested in America/New_York local time to match CitiBike trip timestamps (see DECISIONS.md). Bronze contains 111,227 trip rows with 111,227 unique ride IDs. Silver excludes 350 rides with a missing start or end station; 43 of those are also longer than 24 hours. `fact_trip` contains 110,877 rows, with no placeholder stations and no trips missing a weather match.

## Next actions

1. Record the excluded rows and their reasons. Run this query to confirm the counts:

   ```sql
   SELECT
       countIf(start_station_id IS NULL OR end_station_id IS NULL) AS missing_station,
       countIf(dateDiff('second', started_at, ended_at) > 86400) AS over_24h
   FROM bronze.citibike_trip_raw
   WHERE source_month = '2026-08';
   ```

   Result: 350 missing station, 43 over 24 hours. All 43 long rides are among the 350 missing-station rides.
2. Trigger a different historical month to extend the dataset.
3. Add hosted CI only if the course or project scope later requires it.

## Maturity

Build/test/automation maturity: locally verified, including `pytest` and `ruff`. CI and production operation: not configured.