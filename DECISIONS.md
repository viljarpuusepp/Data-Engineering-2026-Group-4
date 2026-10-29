# Decisions

## 2026-10-29 — Three-dimension Gold schema

Use `dim_station`, `dim_weather`, and `dim_rider` with `fact_trip`. Date and hour stay as typed fact attributes. This supersedes the five-dimension P1 demonstration schema for the runnable implementation.

## 2026-10-29 — Idempotent raw ingestion

Bronze tables use `ReplacingMergeTree(ingested_at)` and natural keys (`ride_id` and `observation_hour`). Silver models select the latest record per natural key, so repeated monthly DAG runs converge even before ClickHouse background merges finish.

## 2026-10-29 — Manual month backfills

The DAG has no schedule and accepts `month` in `YYYY-MM` format. This matches CitiBike monthly archive publication and keeps external-download execution under operator control.

