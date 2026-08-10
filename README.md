# Germany Grid Pipeline V2
 
An end-to-end data pipeline for German energy data, built with Apache Airflow, PySpark, dbt, and DuckDB — fully containerized with Docker.
 
## What It Does
 
Ingests hourly energy data from the SMARD API and daily weather data from Open-Meteo, transforms it through a Bronze → Silver → Gold medallion architecture, and produces analytical tables ready for querying.
 
## Tech Stack
 
- **Apache Airflow 3.0.2** — orchestration and scheduling
- **PySpark** — Bronze to Silver transformation
- **dbt + DuckDB** — Silver to Gold SQL transformations with tests
- **Docker + docker-compose** — full local containerization
- **SMARD API** — German energy data (prices, solar, wind, hydro, nuclear, etc.)
- **Open-Meteo API** — historical weather data (temperature, wind speed, solar radiation)
## Architecture
 
```
SMARD API      →  Bronze (raw JSON)    →  Silver (clean parquet)  →  Gold (dbt/DuckDB)
Open-Meteo API →  Bronze (parquet)     ──────────────────────────→  Gold (dbt/DuckDB)
```
 
### Gold Models
 
| Model | Description |
|---|---|
| `gold_daily_prices` | Daily average energy prices (EUR/MWh) |
| `gold_total_consumption` | Total daily energy consumption (MWh) |
| `gold_total_generation` | Total daily generation across all sources (MWh) |
| `gold_prices_weather` | Daily prices joined with weather variables |
 
## DAGs
 
**`smard_pipeline`** — runs daily at midnight
- Fetches energy data for 10 energy types from SMARD
- Transforms raw JSON to clean parquet with PySpark
- Triggers dbt Gold models after Silver is ready
**`weather_pipeline`** — runs daily at 01:00
- Fetches daily weather for Germany (Berlin coordinates)
- Triggers `gold_prices_weather` dbt model
## Running Locally
 
### Prerequisites
- Docker Desktop
- Python 3.12+
- dbt-duckdb (`pip install dbt-duckdb`)
### Setup
 
1. Clone the repo:
```bash
git clone https://github.com/mohamedkhalifa9/germany-grid-pipelineV2.git
cd germany-grid-pipelineV2
```
 
2. Create a `.env` file:
```
AIRFLOW_UID=501
```
 
3. Create required directories:
```bash
mkdir -p spark_data/raw-data spark_data/silver-data spark_data/gold spark_jobs
```
 
4. Build and start:
```bash
docker compose build
docker compose up airflow-init
docker compose up -d
```
 
5. Access Airflow UI at `http://localhost:8080` (admin/admin)
6. Trigger `smard_pipeline` and `weather_pipeline` manually to start ingestion.
## Project Structure
 
```
airflow-pipeline/
├── dags/
│   ├── smard_pipeline.py       # Main energy pipeline DAG
│   └── weather_pipeline.py     # Weather data DAG
├── energy_dbt/
│   ├── models/                 # dbt Gold models (SQL)
│   ├── profiles.yml            # DuckDB connection config
│   └── dbt_project.yml
├── spark_jobs/
│   └── transform_silver.py     # PySpark transformation script
├── Dockerfile                  # Custom Airflow image
└── docker-compose.yaml
```
 
## Data Sources
 
- **SMARD**: German energy market data — [smard.de](https://www.smard.de)
- **Open-Meteo**: Free weather API — [open-meteo.com](https://open-meteo.com)
