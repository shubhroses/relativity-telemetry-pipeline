# Rocket Engine Telemetry Data Pipeline

A comprehensive data engineering project demonstrating skills in SQL, Redshift, dbt, Data Modeling, Python, and Airbyte.

## Project Overview

This project simulates a rocket engine telemetry data pipeline that:
- Generates synthetic telemetry data with realistic anomalies
- Cleans and validates incoming data using Python
- Loads data into Redshift using Airbyte
- Transforms data into a star schema using dbt
- Implements advanced SQL analytics and anomaly detection

## Tech Stack

- **Python**: Data generation, cleaning, and validation
- **Airbyte**: Data ingestion and loading
- **Redshift**: Data warehouse
- **dbt**: Data transformation and modeling
- **SQL**: Analytics and anomaly detection

## Project Structure

```
relativity-telemetry-pipeline/
├── README.md
├── python/                    # Python scripts for data generation and cleaning
├── dbt/                      # dbt project for data transformation
├── airbyte/                  # Airbyte connector configurations
└── sql/                      # Standalone SQL queries and analysis
```

## Quick Start

1. **Generate telemetry data**: `python python/generate_telemetry.py`
2. **Clean and validate**: `python python/ingest_and_clean.py`
3. **Load to Redshift**: Configure and run Airbyte sync
4. **Transform with dbt**: `dbt run && dbt test`
5. **Analyze data**: Execute SQL queries in `sql/` directory

## Data Model

**Star Schema:**
- `dim_engine`: Engine dimension table
- `fact_telemetry`: Main fact table with telemetry readings

**Key Metrics:**
- Chamber pressure
- Fuel flow rate
- Engine temperature
- Real-time anomaly detection
