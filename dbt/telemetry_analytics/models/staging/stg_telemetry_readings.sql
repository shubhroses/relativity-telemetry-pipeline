/*
    Staging model for raw telemetry readings from Airbyte
    
    This model:
    - Cleans and standardizes raw Airbyte data
    - Adds data quality flags
    - Prepares data for star schema transformation
*/

{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source_data AS (
    SELECT 
        -- Airbyte metadata
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_generation_id,
        
        -- Business data
        engine_id,
        timestamp::timestamp AS reading_timestamp,
        chamber_pressure::numeric(8,3) AS chamber_pressure_psi,
        fuel_flow::numeric(8,3) AS fuel_flow_kg_per_sec,
        temperature::numeric(8,3) AS temperature_fahrenheit
        
    FROM {{ source('telemetry_raw', 'telemetry_data') }}
),

cleaned_data AS (
    SELECT 
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_generation_id,
        
        -- Standardized business fields
        UPPER(TRIM(engine_id)) AS engine_id,
        reading_timestamp,
        chamber_pressure_psi,
        fuel_flow_kg_per_sec,
        temperature_fahrenheit,
        
        -- Calculated metrics
        CASE 
            WHEN chamber_pressure_psi > 0 
            THEN fuel_flow_kg_per_sec / chamber_pressure_psi 
            ELSE NULL 
        END AS fuel_efficiency_ratio,
        
        -- Composite performance score (weighted average)
        (
            CASE WHEN chamber_pressure_psi BETWEEN 150 AND 300 THEN 0.4 ELSE 0.0 END +
            CASE WHEN fuel_flow_kg_per_sec BETWEEN 50 AND 150 THEN 0.3 ELSE 0.0 END +
            CASE WHEN temperature_fahrenheit BETWEEN 2000 AND 3500 THEN 0.3 ELSE 0.0 END
        ) * 100 AS performance_score,
        
        -- Data quality flags
        CASE 
            WHEN chamber_pressure_psi < 100 OR chamber_pressure_psi > 350 THEN TRUE
            WHEN fuel_flow_kg_per_sec < 25 OR fuel_flow_kg_per_sec > 200 THEN TRUE
            WHEN temperature_fahrenheit < 1500 OR temperature_fahrenheit > 4000 THEN TRUE
            ELSE FALSE
        END AS is_anomaly,
        
        -- Anomaly type classification
        CASE 
            WHEN chamber_pressure_psi < 100 THEN 'LOW_PRESSURE'
            WHEN chamber_pressure_psi > 350 THEN 'HIGH_PRESSURE'
            WHEN fuel_flow_kg_per_sec < 25 THEN 'LOW_FUEL_FLOW'
            WHEN fuel_flow_kg_per_sec > 200 THEN 'HIGH_FUEL_FLOW'
            WHEN temperature_fahrenheit < 1500 THEN 'LOW_TEMPERATURE'
            WHEN temperature_fahrenheit > 4000 THEN 'HIGH_TEMPERATURE'
            ELSE NULL
        END AS anomaly_type,
        
        -- ETL metadata
        'AIRBYTE' AS source_system,
        GETDATE() AS processed_at
        
    FROM source_data
    WHERE engine_id IS NOT NULL
      AND reading_timestamp IS NOT NULL
      AND chamber_pressure_psi IS NOT NULL
      AND fuel_flow_kg_per_sec IS NOT NULL
      AND temperature_fahrenheit IS NOT NULL
)

SELECT * FROM cleaned_data 