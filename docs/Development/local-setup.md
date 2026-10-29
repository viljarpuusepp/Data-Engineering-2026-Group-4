# Local Setup

## Prerequisites

Docker Desktop with Compose v2, Python 3.12 for local unit tests, and outbound access to CitiBike and Open-Meteo. Copy `.env.example` to `.env` only when changing the default Airflow login.

## Start and verify

```powershell
docker compose up -d --build
docker compose ps
docker compose exec airflow-scheduler airflow dags list | Select-String citibike_monthly_backfill
docker compose exec airflow-scheduler airflow dags trigger citibike_monthly_backfill --conf '{"month":"2026-08"}'
```

Open Airflow at `http://localhost:8081` by default (override `AIRFLOW_WEB_PORT` in `.env`). Use `docker compose logs -f airflow-scheduler` to investigate a run. The first boot initializes Airflow's metadata database and ClickHouse Bronze databases.

## Explore ClickHouse

The locally exposed ClickHouse connection is:

| Setting | Value |
| --- | --- |
| Host | `localhost` |
| HTTP port | `8123` |
| Native port | `9000` |
| User | `pipeline` |
| Password | None (local Docker environment only) |
| Gold database | `analytics` |
| Bronze database | `bronze` |

Open an interactive SQL client from the repository root:

```powershell
docker compose exec clickhouse clickhouse-client --user pipeline --database analytics
```

For a desktop SQL client, use `jdbc:clickhouse://localhost:8123/analytics`, user `pipeline`, and an empty password. Useful exploration queries:

```sql
SHOW DATABASES;
SHOW TABLES FROM analytics;
SHOW TABLES FROM bronze;

SELECT count() FROM analytics.fact_trip;
SELECT * FROM analytics.fact_trip LIMIT 10;
SELECT * FROM analytics.dim_station LIMIT 10;
```

Run local code checks after installing development dependencies:

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest
ruff check .
```

