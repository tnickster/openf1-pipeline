# OpenF1 Pipeline

An end-to-end data engineering pipeline that ingests live F1 telemetry from the OpenF1 API, transforms it through a medallion architecture in BigQuery, and serves a race replay visualisation via Streamlit.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Orchestration                            │
│                                                                 │
│   calendar_dag          race_weekend_sensor     ingestion_dag   │
│   (monthly)        ──►  (daily sensor)     ──►  (triggered)    │
│   Fetch F1 calendar     Checks if race          Fetch 8 API    │
│   Store to GCS          weekend ended           endpoints       │
└────────────────────────────────┬────────────────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │      Google Cloud        │
                    │                         │
                    │  GCS (raw JSON files)   │
                    │          │              │
                    │  BigQuery raw tables    │
                    │  (partitioned by        │
                    │   meeting_key)          │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │       dbt Core          │
                    │                         │
                    │  staging  (views)       │
                    │     │                   │
                    │  intermediate (views)   │
                    │     │                   │
                    │  marts (tables)         │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │     Streamlit App       │
                    │                         │
                    │  Race replay            │
                    │  Live standings         │
                    │  Driver telemetry       │
                    └─────────────────────────┘
```

## Tech Stack

| Layer | Tool | Version |
|---|---|---|
| Orchestration | Apache Airflow | 3.1.8 |
| Executor | CeleryExecutor + Redis | |
| Containerisation | Docker + Docker Compose | |
| Object storage | Google Cloud Storage | |
| Data warehouse | BigQuery | |
| Transformation | dbt Core | 1.11.7 |
| Serving | Streamlit Community Cloud | |
| Source API | OpenF1 REST API | |

## How It Works

### Ingestion

Three Airflow DAGs handle the full ingestion lifecycle.

`calendar_dag` runs on the first of each month. It fetches the F1 race calendar from the OpenF1 `/meetings` endpoint, filters out testing sessions, and stores the schedule as newline-delimited JSON to GCS. This is the source of truth for the sensor.

`race_weekend_sensor` runs daily. It reads the calendar from GCS and checks whether yesterday's date matches any race `date_end`. If it does, it triggers the ingestion DAG with the corresponding `meeting_key` via `TriggerDagRunOperator`. No manual intervention required after a race weekend.

`ingestion_dag` is triggered with a `meeting_key`. It fetches eight OpenF1 endpoints: sessions, drivers, laps, location, starting_grid, meetings, car_data, and stints. Location and car_data are paginated per driver number to avoid 422 errors from the API. Each endpoint is uploaded to GCS as newline-delimited JSON and loaded into BigQuery raw tables using `WRITE_TRUNCATE` scoped to the partition, so re-running a race overwrites only that race's data.

### Storage

Raw BigQuery tables are range-partitioned by `meeting_key` (range 1000–2000, interval 1). This means queries that filter by `meeting_key` scan only the relevant partition, and re-ingesting a race does not touch other races' data. `ALLOW_FIELD_ADDITION` on load jobs means new columns from the API are added automatically without dropping tables.

GCS stores raw JSON files at:
```
raw/meetings={meeting_key}/session={session_key}/{endpoint}.json
```

### Transformation

dbt Core runs in a Python 3.12 virtual environment against the BigQuery adapter. The medallion layers are:

**Staging** (views): light cleaning and renaming of raw tables. One model per source table, no business logic.

```
stg_openf1__drivers
stg_openf1__laps
stg_openf1__location
stg_openf1__starting_grid
stg_openf1__car_data
```

**Intermediate** (views): enrich with driver context. One design decision worth noting: `starting_grid` uses the qualifying `session_key`, not the race `session_key`, so the intermediate model joins on `meeting_key + driver_number` only to avoid a broken join.

```
int_location__enriched
int_laps__enriched
int_starting_grid__enriched
```

**Marts** (tables) : final serving layer, materialised as tables for query performance.

```
dim_drivers           one row per driver, attributes and team info
fct_race_replay       x/y position per driver per timestamp
fct_laps              lap timing data enriched with driver info
fct_starting_grid     qualifying positions enriched with driver info
fct_car_telemetry     rpm, speed, gear, throttle, brake, drs per driver per timestamp
```

### Data Quality

All mart models have dbt tests for `not_null` and `unique` constraints on primary keys. `fct_car_telemetry` has additional domain-specific custom tests that validate telemetry values against known physical limits.

```
brake_range               brake values must be between 0 and 105
throttle_range            throttle values must be between 0 and 110
rpm_range                 rpm values must be between 0 and 15000
speed_range               speed values must be between 0 and 410 km/h
valid_drs                 drs values must match known OpenF1 states
brake_throttle_combination  flags sustained simultaneous extreme brake and throttle
```

The thresholds are intentionally permissive of known sensor behaviour : for example OpenF1 encodes full braking as 104 rather than 100, so the brake ceiling is set to 105 rather than 100 to avoid false positives while still catching genuinely corrupt values.

### Serving

The Streamlit app queries BigQuery directly using a read-only service account. Only meetings with corresponding location data in `fct_race_replay` appear in the race selector, which automatically excludes cancelled or incomplete races regardless of what the pipeline ingested.

Animation runs entirely in JavaScript via `requestAnimationFrame` at 60fps. Plotly frames were abandoned due to 500MB message size limits and jerky animation. Binary search interpolation between position samples produces smooth car movement between data points. Race data is sampled every 5th row per driver in BigQuery before being sent to the browser.

## Project Structure

```
openf1-pipeline/
├── dags/
│   ├── calendar_dag.py
│   ├── race_weekend_sensor.py
│   └── ingestion_dag.py
├── openf1_dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── macros/
│   │   ├── generate_schema_name.sql
│   │   └── car_telemetry_tests.sql
│   └── dbt_project.yml
├── schemas.py
├── docker-compose.yml
└── .gitignore
```

## Setup

### Prerequisites

Docker Desktop, a GCP project with BigQuery and GCS enabled, and a service account with the following roles: `BigQuery Data Editor`, `BigQuery Job User`, `Storage Object Admin`.

### 1. Clone the repo

```bash
git clone https://github.com/tnickster/openf1-pipeline.git
cd openf1-pipeline
```

### 2. Configure environment

```bash
cp .env.example .env
```

Required variables:

```
AIRFLOW__CORE__FERNET_KEY=<your fernet key>
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account.json
GCS_BUCKET=your-gcs-bucket-name
BQ_PROJECT=your-gcp-project-id
```

### 3. Start Airflow

```bash
docker compose up -d
```

Airflow will be available at `http://localhost:8080`. Default credentials are `airflow / airflow`.

### 4. Run dbt

```bash
cd openf1_dbt
python -m venv venv
venv\Scripts\activate
pip install dbt-bigquery==1.11.7
dbt deps
dbt run
dbt test
```

### 5. Trigger a backfill (optional)

```bash
airflow dags trigger ingestion_dag --conf '{"meeting_key": 1281}'
```

## Known Limitations

**Location coordinate precision**: OpenF1's `/location` endpoint returns x/y coordinates that approximate track position but lack lateral placement precision. Cars on the inside and outside of a corner appear at the same point. This is an upstream API limitation.

**Local deployment**: Airflow runs on Docker Desktop on a local Windows machine. Migration to Oracle Cloud Free Tier is planned once the core pipeline is stable.

**Real-time playback speed**: the race replay runs at 1:1 speed. Configurable playback speed is on the roadmap.

**Cancelled race handling**: the pipeline ingests metadata for all scheduled races including cancelled ones. The Streamlit app filters these out by checking for the presence of location data, but the raw and staging layers will still contain partial data for cancelled events.

## Related

[Streamlit app repo](https://github.com/tnickster/streamlit-f1-app) [Live app](#): [OpenF1 API docs](https://openf1.org)
