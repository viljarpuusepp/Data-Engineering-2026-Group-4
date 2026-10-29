# Status

## Current state

The local Docker, Airflow, ClickHouse, and dbt implementation is running and verified for a manual August 2026 Jersey City backfill. Airflow run `manual__2026-10-29T08:35:19+00:00` completed successfully after downloading, validating, loading, transforming, compacting Bronze, and testing the public source data. After repeated August reruns, Bronze contains 111,227 rows with 111,227 unique ride IDs; Gold contains 111,184 rows with 111,184 unique ride IDs.

## Next actions

1. Review the Airflow run and Gold tables in the local UI/database.
2. Trigger a different historical month to extend the dataset.
3. Add hosted CI only if the course/project scope later requires it.

## Maturity

Build/test/automation maturity: locally verified. CI and production operation: not configured.

