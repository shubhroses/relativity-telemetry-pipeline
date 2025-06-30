# Airbyte Setup Guide

## Overview
Airbyte will load cleaned telemetry data from CSV files into Redshift's `telemetry_raw` schema.

## Option 1: Airbyte Cloud (Recommended for Demo)

### Prerequisites
- Airbyte Cloud account: https://cloud.airbyte.com
- Our CSV data in `data/telemetry_clean.csv`
- Redshift connection details

### Steps
1. **Sign up** for Airbyte Cloud (free tier available)
2. **Create workspace** for telemetry project
3. **Configure source**: File (CSV)
4. **Configure destination**: Redshift
5. **Set up connection** with sync settings

## Option 2: Airbyte OSS (Local)

### Prerequisites
- Docker and Docker Compose installed
- 8GB+ RAM available

### Installation
```bash
# Clone Airbyte OSS
git clone https://github.com/airbytehq/airbyte.git
cd airbyte

# Start Airbyte locally
./run-ab-platform.sh
```

Access at: http://localhost:8000

## Configuration Details

### Source Configuration (File/CSV)
```yaml
connector_type: File
format: CSV
url: /data/telemetry_clean.csv
provider:
  storage: local
```

### Destination Configuration (Redshift)
```yaml
connector_type: Redshift
host: telemetry-demo.484907493448.us-east-2.redshift-serverless.amazonaws.com
port: 5439
database: dev
schema: telemetry_raw
username: admin
password: [from config]
ssl_mode: require
```

### Sync Configuration
```yaml
sync_mode: full_refresh_overwrite
frequency: daily
destination_namespace: telemetry_raw
```

## Data Schema Mapping

Our CSV columns will map to Redshift as:
```sql
CREATE TABLE telemetry_raw.telemetry_data (
    timestamp TIMESTAMP,
    engine_id VARCHAR(20),
    chamber_pressure DECIMAL(10,2),
    fuel_flow DECIMAL(10,2),
    temperature DECIMAL(10,2),
    _airbyte_ab_id VARCHAR(36),
    _airbyte_emitted_at TIMESTAMP,
    _airbyte_normalized_at TIMESTAMP
);
```

## Testing the Pipeline

1. **Generate fresh data**: `python python/generate_telemetry.py 100 | python python/ingest_and_clean.py`
2. **Trigger sync** in Airbyte UI
3. **Verify data** in Redshift: `SELECT COUNT(*) FROM telemetry_raw.telemetry_data;`

## Troubleshooting

- **Connection issues**: Verify Redshift security group allows Airbyte IPs
- **Schema errors**: Check CSV column names match expected format
- **Permission errors**: Ensure Redshift user has CREATE/INSERT permissions 