/*
    Core fact table for telemetry readings
    
    This model:
    - Transforms staging data into star schema fact table
    - Looks up dimension keys from dimension tables
    - Applies business logic and calculations
*/

{{ config(
    materialized='table',
    schema='core',
    post_hook=['ANALYZE {{ this }}']
) }}

WITH staging_readings AS (
    SELECT * FROM {{ ref('stg_telemetry_readings') }}
),

engine_dim AS (
    SELECT 
        engine_key,
        engine_id
    FROM telemetry_clean.dim_engines
    WHERE is_current = TRUE
),

fact_data AS (
    SELECT 
        -- Foreign keys (dimension references)
        COALESCE(e.engine_key, -1) AS engine_key,
        
        -- Generate simple time_key from timestamp (YYYYMMDDHH format - reduced precision)
        CAST(
            EXTRACT(YEAR FROM s.reading_timestamp) * 1000000 +
            EXTRACT(MONTH FROM s.reading_timestamp) * 10000 +
            EXTRACT(DAY FROM s.reading_timestamp) * 100 +
            EXTRACT(HOUR FROM s.reading_timestamp)
            AS BIGINT
        ) AS time_key,
        
        -- Degenerate dimensions
        s._airbyte_raw_id AS airbyte_raw_id,
        
        -- Add reading timestamp for temporal analysis
        s.reading_timestamp,
        s.engine_id,
        
        -- Facts/measures
        s.chamber_pressure_psi,
        s.fuel_flow_kg_per_sec,
        s.temperature_fahrenheit,
        s.fuel_efficiency_ratio,
        s.performance_score,
        
        -- Data quality flags
        s.is_anomaly,
        s.anomaly_type,
        
        -- Health status classification
        CASE 
            WHEN s.performance_score >= 80 THEN 'EXCELLENT'
            WHEN s.performance_score >= 60 THEN 'GOOD'
            WHEN s.performance_score >= 40 THEN 'FAIR'
            WHEN s.performance_score >= 20 THEN 'POOR'
            ELSE 'CRITICAL'
        END AS health_status,
        
        -- ETL metadata
        s.source_system,
        s.processed_at AS created_at,
        s.processed_at AS updated_at
        
    FROM staging_readings s
    LEFT JOIN engine_dim e 
        ON s.engine_id = e.engine_id
)

SELECT 
    -- Generate surrogate key using row_number
    ROW_NUMBER() OVER (ORDER BY time_key, engine_key) AS reading_key,
    engine_key,
    time_key,
    airbyte_raw_id,
    reading_timestamp,
    engine_id,
    chamber_pressure_psi,
    fuel_flow_kg_per_sec,
    temperature_fahrenheit,
    fuel_efficiency_ratio,
    performance_score,
    is_anomaly,
    anomaly_type,
    health_status,
    source_system,
    created_at,
    updated_at
FROM fact_data 