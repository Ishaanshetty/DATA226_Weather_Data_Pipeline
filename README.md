# DATA226_Weather_Data_Pipeline

# NY vs LA: Which Coast Has Better Weather? ☀️🌧️

A scheduled, idempotent data pipeline that pulls daily weather for New York and Los Angeles, transforms it into comparative metrics, and visualizes the results on a dashboard — built to settle the age-old coastal weather debate with data.

## Overview

This project pulls current and forecast weather data from the [Open-Meteo API](https://api.open-meteo.com/v1/forecast) for two cities on a schedule, lands the raw data in a warehouse, transforms it into derived metrics with dbt, and surfaces those metrics in a BI dashboard.

**Architecture:** Open-Meteo → Airflow ETL DAG → raw warehouse table → Airflow-triggered dbt DAG → staging/mart models → BI dashboard.

The pipeline is split into two DAGs so orchestration and transformation stay cleanly separated, while still guaranteeing correct run order (dbt never transforms a table that hasn't finished loading).

## Tech Stack

| Component | Choice |
|---|---|
| Data source | Open-Meteo Forecast API |
| Orchestration | Airflow |
| Warehouse | Snowflake |
| Transformation | dbt |
| BI tool | Preset |
| Version control | GitHub |

## Metrics

All derived metrics are computed in dbt from the raw daily readings (temperature, precipitation, wind, UV, sunshine/daylight duration, etc.). Highlights include:

- **Preference-based head-to-head win rate** — scores each city daily against user-selected thresholds and counts the days each city "wins"
- **Weather whiplash index** — rolling 7-day average of day-over-day mean temperature swings
- **Predictability score** — coefficient of variation of daily temperature over a window
- **Dry spell length / comfort streak length** — longest consecutive runs of dry or "comfortable" days (gaps-and-islands pattern)
- **Layer-up day flag**, **feels-like gap**, **rain intensity**, **sunshine ratio**, **weekend ruined rate**, **forecast miss rate**, **forecast drift**, **UV danger days**

See `dbt/models/marts/fct_weather_metrics.sql` for full definitions.

## Repository Structure

```
repo/
├── dags/
│   ├── weather_etl_dag.py     # pulls Open-Meteo data, loads raw table
│   └── weather_dbt_dag.py     # runs dbt after ETL DAG succeeds
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_weather.sql
│   │   │   └── stg_weather.yml
│   │   └── marts/
│   │       ├── fct_weather_metrics.sql
│   │       └── fct_weather_metrics.yml
│   ├── snapshots/
│   │   └── raw_weather_snapshot.sql
│   ├── dbt_project.yml
│   └── packages.yml
├── README.md
└── requirements.txt
```

No scratch notebooks, unused sample files, or committed credentials live in this repo.

## Data Model

Raw and mart tables are keyed on `(city, date)`, one row per city per day. Key columns include daily max/min/mean temperature, apparent temperature, precipitation sum/hours/probability, wind speed, sunshine/daylight duration, UV index, and WMO weather code (decoded via a dbt seed table). See Section 5 of the [requirements doc](./docs/Prelim_Weather_Lab_BRD.pdf) for full column-level types, units, and constraints.

## Setup (for a new teammate / fresh machine)

Cloning the repo gets you the DAG and dbt code, but a few things are intentionally **not** in Git (credentials, and anything stored in this project's local Airflow/Postgres instance). Set these up once per machine:

### 1. Get the private key
You need a Snowflake key pair registered to the account/user this project uses. Either:
- Get a copy of the team's `rsa_key.p8` from a teammate (out of band — Slack/email, never Git), or
- Generate your own key pair and have someone with `ACCOUNTADMIN` register your public key with `ALTER USER ... SET RSA_PUBLIC_KEY = '...'`.

Place the private key at `keys/rsa_key.p8` in the project root (this folder is gitignored).

### 2. Create `.env`
In the project root, create a file named `.env` (gitignored) with:
```
AIRFLOW_UID=50000
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=<passphrase for rsa_key.p8, if it has one>
```

### 3. Start Docker
```bash
docker compose up airflow-init
docker compose up -d
```
Wait for the containers to finish installing packages (`docker compose exec airflow which dbt` should return a path once ready), then open the Airflow UI at `http://localhost:8081` (user/pass: `airflow` / `airflow`).

### 4. Create Airflow Connections
In the UI, go to **Admin → Connections → +** and add:

| Connection Id | Connection Type | Fields |
|---|---|---|
| `open_meteo_api` | HTTP | Host: `api.open-meteo.com`, Schema: `https` |
| `weather_warehouse` | Snowflake | Login, Account, Schema, Role, and an **Extra** JSON of: `{"private_key_file": "/opt/airflow/keys/rsa_key.p8", "private_key_file_pwd": "<same passphrase as .env>", "warehouse": "<warehouse>", "database": "<database>", "role": "<role>"}` |

### 5. Create the Airflow Variable
In the UI, go to **Admin → Variables → +**:

| Key | Value |
|---|---|
| `weather_cities` | `[{"name": "New York", "lat": 40.7128, "lon": -74.0060}, {"name": "Los Angeles", "lat": 34.0522, "lon": -118.2437}]` |

### 6. Run the pipeline
Trigger `weather_etl_dag` in the UI once to load raw data, then run dbt:
```bash
docker compose exec airflow bash -c "cd /opt/airflow/dbt && dbt deps && dbt run && dbt test && dbt snapshot"
```

## Running the Pipeline

1. Trigger `weather_etl_dag` in the Airflow UI — fetches and loads raw weather data into `RAW.WEATHER_DAILY`.
2. Trigger `weather_dbt_dag` — runs `dbt deps`, `dbt run`, `dbt test`, `dbt snapshot` in sequence, rebuilding `fct_weather_metrics` and the raw snapshot.
3. The BI dashboard reads from `fct_weather_metrics`; refresh the dashboard after step 2 to see updated numbers.

Re-running the ETL DAG for the same day/hour updates existing rows rather than duplicating them (idempotent upsert with rollback-and-retry on failure).

## Dashboard

Built in Preset on top of `fct_weather_metrics`, split across two rows of charts comparing New York and Los Angeles:

- **Average Comfort Days** (LA vs. NYC) — single-value cards
- **Average Temp by City** — line chart, LA vs. NY over time
- **Temperature Anomalies** — bar chart, NY vs. LA, lower is better
- **Rolling 7-Day Windspeed** — bar chart, NY vs. LA
- **Feels Like Temp vs. Actual Temp** — area chart, temperature delta over time
- **Weekly Avg Sunshine Ratio** — bar chart, NYC vs. LA

Preset connects to Snowflake over a SQLAlchemy URI with username/password auth, since Preset's basic connection form doesn't support the key-pair auth used by Airflow and dbt.

## Future Work

- Add more cities via a fully config-driven list
- Data quality alerting (e.g. Slack notification on dbt test failure)
- Backfill historical weather via Open-Meteo's archive API for a longer anomaly baseline
- Forecast-accuracy metric comparing forecasted vs. observed values
- Migrate from Postgres to a cloud warehouse for larger-scale, multi-city expansion
