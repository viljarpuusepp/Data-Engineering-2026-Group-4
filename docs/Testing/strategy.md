# Test Strategy

Unit tests cover month parsing, deterministic external URLs, payload validation, duplicate detection, empty input, and incomplete weather responses. They do not call external services.

Integration verification uses the Compose ClickHouse container and a real publicly available month. Required checks are: initial load counts, rerun count stability by natural key, dbt tests, and rejected invalid input before Bronze writes. ClickHouse is never mocked in integration checks.

`dbt build` is the transformation acceptance gate: it builds all models and runs YAML plus singular SQL tests.

