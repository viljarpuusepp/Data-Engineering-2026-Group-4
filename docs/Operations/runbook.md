# Operations Runbook

This is a local-only workflow. An operator manually triggers a month; there is no automatic schedule, cloud deployment, or hosted CI.

Before a run, confirm the expected CitiBike archive exists and the Docker services are healthy. On a download or validation failure, correct the parameter or wait for the external source; do not manually insert partial data. Raw payloads remain under ignored `data/raw/` for diagnosis and safe reruns.

To stop services without deleting data, run `docker compose down`. Removing named volumes is destructive and requires deliberate operator approval because it erases Airflow metadata and ClickHouse data.

