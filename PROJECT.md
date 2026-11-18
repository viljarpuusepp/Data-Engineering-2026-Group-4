# CitiBike Analytics Pipeline

## Purpose

This repository contains a local, Docker-based learning pipeline that ingests monthly Jersey City CitiBike trips and hourly Open-Meteo observations, retains canonical Bronze records in ClickHouse, and exposes a three-dimension analytics star schema through dbt.

## Boundary and source of truth

The repository root is the logical project boundary. Docker Compose is the single deployable local environment; it includes ClickHouse, Airflow, and Airflow's PostgreSQL metadata service. This repository is Git-backed, but no CI, deployment target, or production automation is configured.

Source-of-truth order: executable code/configuration, database DDL, dbt definitions, feature/operations docs, then planning records. The original P1 SQL files are historical design inputs and are not executable pipeline sources.

## Project map

- `dags/` — Airflow orchestration entry points.
- `src/citibike_pipeline/` — deterministic download, validation, and load helpers.
- `clickhouse/init/` — ClickHouse bootstrap DDL.
- `dbt/` — Silver and Gold transformations, tests, and documentation.
- `docs/` — MCAF feature, architecture, testing, development, and operations guidance.
- `data/` — ignored local raw payloads and Airflow logs.

