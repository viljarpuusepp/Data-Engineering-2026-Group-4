# Data-Engineering-2026-Group-4

This repository contains both the original Project 1 modelling artefacts and a runnable local
CitiBike analytics pipeline. The pipeline uses Docker, Airflow, ClickHouse, and dbt to ingest a
manually selected Jersey City CitiBike month and matching Open-Meteo hourly weather data.

## Run the pipeline

Follow [the local setup guide](docs/Development/local-setup.md). In short:

```powershell
docker compose up -d --build
docker compose exec airflow-scheduler airflow dags trigger citibike_monthly_backfill --conf '{"month":"2026-08"}'
```

The Airflow UI is available at `http://localhost:8081` by default. The complete project map, operational
rules, and verification commands are in [PROJECT.md](PROJECT.md), [AGENTS.md](AGENTS.md), and
[docs/Home.md](docs/Home.md).

## Original Project 1 artefacts

The remaining SQL files document the original P1 star-schema design. They are retained as course
inputs and historical reference; the Docker pipeline implements the newer three-dimension Gold
schema described in the project documentation.

1. NYC CitiBike Trip Data
Source: Open data portal (https://s3.amazonaws.com/tripdata/index.html)

2. Open-Meteo Historical Weather (API)
Source: API (https://archive-api.open-meteo.com)
