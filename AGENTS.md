# Agent Instructions

## Purpose

This repository implements a local CitiBike-to-ClickHouse analytics pipeline. Follow the MCAF principles in `docs/Home.md`: document meaningful behavior before implementation, keep code and operational context together, and prove changes with automated checks.

## Required workflow

1. Read `PROJECT.md`, `STATUS.md`, and relevant documents under `docs/` before substantial work.
2. Preserve the original P1 SQL files unless the task explicitly changes them.
3. Add or update tests with every behavior change. Use real Docker services for integration verification; do not mock ClickHouse.
4. Record durable architectural choices in `DECISIONS.md` and update `STATUS.md` at the end of substantial work.
5. Never commit raw source data, credentials, Docker volumes, or generated dbt output.

## Canonical commands

Run all commands from the repository root.

- `docker compose up -d --build` — build and start ClickHouse and Airflow.
- `docker compose down` — stop services while preserving named volumes.
- `python -m pytest` — run unit tests (install `requirements-dev.txt` first).
- `ruff check .` — static analysis.
- `docker compose exec airflow-scheduler dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt` — build and test dbt models.
- `docker compose exec airflow-scheduler airflow dags trigger citibike_monthly_backfill --conf '{"month":"2026-08"}'` — trigger a monthly backfill.
- `docker compose config` — validate Compose configuration.

## Definition of done

A change is complete only when relevant documentation, tests, and static analysis are updated; the focused checks pass; and container/integration verification is run when Docker is available. Do not claim hosted CI or production operation: this project is local-only.

