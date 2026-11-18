# Decisions

## 2026-10-29 — Three-dimension Gold schema

Use `dim_station`, `dim_weather`, and `dim_rider` with `fact_trip`. Date and hour stay as typed fact attributes. This supersedes the five-dimension P1 demonstration schema for the runnable implementation.

## 2026-10-29 — Idempotent raw ingestion

Bronze tables use `ReplacingMergeTree(ingested_at)` and natural keys (`ride_id` and `observation_hour`). Silver models select the latest record per natural key, so repeated monthly DAG runs converge even before ClickHouse background merges finish.

## 2026-10-29 — Manual month backfills

The DAG has no schedule and accepts `month` in `YYYY-MM` format. This matches CitiBike monthly archive publication and keeps external-download execution under operator control.

## 2026-11-08 — Weather in local time

Open-Meteo weather is requested with `timezone=America/New_York`, the same local wall-clock time as CitiBike `started_at` values. Bronze stores both as wall-clock values, so `toStartOfHour(started_at)` matches `observation_hour`. Requesting UTC weather shifted each ride's weather by four to five hours.

The weather validator counts expected hours in New York local time, so DST change days (23 or 25 hours) are accepted.