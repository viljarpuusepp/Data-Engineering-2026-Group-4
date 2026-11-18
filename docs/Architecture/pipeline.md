# Pipeline Architecture

## End-to-end data flow

```mermaid
flowchart LR
  operator[Operator<br/>month: YYYY-MM] --> airflow[Airflow DAG]
  citibike[CitiBike ZIP<br/>JC-YYYYMM archive] --> download[Download and extract]
  airflow --> download
  download --> trip_check[Trip CSV quality checks]
  trip_check --> range[Derive actual trip-date range]
  range --> weather[Open-Meteo hourly API]
  weather --> weather_check[Weather payload quality checks]
  trip_check --> bronze_trips[(Bronze<br/>citibike_trip_raw)]
  weather_check --> bronze_weather[(Bronze<br/>open_meteo_hourly_raw)]
  bronze_trips --> compact[ClickHouse partition compaction]
  bronze_weather --> compact
  compact --> silver[dbt Silver<br/>deduplicated typed views]
  silver --> stations[dim_station]
  silver --> riders[dim_rider]
  silver --> weather_dim[dim_weather]
  stations --> fact[fact_trip]
  riders --> fact
  weather_dim --> fact
```

## Airflow orchestration flow

```mermaid
flowchart TD
  validate_month[validate_month] --> download_trip[download_trip]
  download_trip --> validate_trip[validate_trip]
  validate_trip --> download_open_meteo[download_open_meteo]
  download_open_meteo --> validate_weather[validate_weather]
  validate_weather --> load_bronze[load_bronze<br/>insert and compact]
  load_bronze --> build_dbt[build_dbt<br/>models and tests]

  validate_trip -. failure blocks writes .-> stop1[Fail DAG run]
  validate_weather -. failure blocks writes .-> stop2[Fail DAG run]
  build_dbt -. test failure .-> stop3[Fail DAG run]
```

## Idempotent rerun flow

```mermaid
flowchart LR
  run1[Run month 2026-08] --> insert1[Insert Bronze versions]
  rerun[Repeat month 2026-08] --> insert2[Insert newer versions<br/>same natural keys]
  insert1 --> compact[OPTIMIZE ... PARTITION month FINAL]
  insert2 --> compact
  compact --> canonical[One physical Bronze row per ride_id<br/>and observation_hour]
  canonical --> silver[Silver latest-record selection]
  silver --> gold[Gold fact_trip unique by ride_id]
```

Airflow writes deterministic local raw artifacts by month, validates each payload before any Bronze load, and invokes dbt only after both Bronze datasets load. ClickHouse persists raw records in `bronze`; dbt owns Silver views and Gold tables in `analytics`. The Compose environment creates a local `pipeline` ClickHouse user with no password; it is not a credential for any external service.

The fact table is partitioned by month and ordered by trip date/ride ID. Dimensions use stable hashes of their natural identifiers. A weather join is optional so valid trips survive an unavailable matching observation.

