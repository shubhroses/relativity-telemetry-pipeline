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
├── requirements.txt              # Python dependencies
├── .gitignore                   # Protect sensitive data
├── python/                      # Python scripts for data generation and cleaning
│   ├── generate_telemetry.py   # Synthetic data generator with anomalies
│   ├── ingest_and_clean.py     # Data validation and cleaning pipeline
│   └── test_redshift_connection.py  # Redshift connection tester
├── config/                      # Configuration files
│   └── redshift_connection_template.json  # Template for DB credentials
├── docs/                        # Documentation
│   └── redshift_setup_guide.md  # Step-by-step AWS setup guide
├── dbt/                         # dbt project for data transformation
├── airbyte/                     # Airbyte connector configurations
├── data/                        # Generated data files and outputs
└── sql/                         # Standalone SQL queries and analysis
```

## Quick Start

### Prerequisites
```bash
# Install Python dependencies
pip install -r requirements.txt
```

### Setup Process

1. **Set up AWS Redshift**: Follow guide in `docs/redshift_setup_guide.md`
2. **Configure connection**: Copy `config/redshift_connection_template.json` to `config/redshift_connection.json` and fill in your details
3. **Test connection**: `python python/test_redshift_connection.py`
4. **Generate telemetry data**: `python python/generate_telemetry.py`
5. **Clean and validate**: `python python/ingest_and_clean.py`
6. **Load to Redshift**: Configure and run Airbyte sync
7. **Transform with dbt**: `dbt run && dbt test`
8. **Analyze data**: Execute SQL queries in `sql/` directory

## Data Model

**Star Schema:**
- `dim_engine`: Engine dimension table
- `fact_telemetry`: Main fact table with telemetry readings

**Database**: AWS Redshift Serverless (`dev` database)

**Key Metrics:**
- Chamber pressure
- Fuel flow rate
- Engine temperature
- Real-time anomaly detection
