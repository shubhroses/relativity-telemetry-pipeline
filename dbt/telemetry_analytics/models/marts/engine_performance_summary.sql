/*
    Engine Performance Summary Mart
    
    This model provides:
    - Aggregated performance metrics by engine
    - Anomaly rates and trends
    - Key performance indicators for operational dashboards
*/

{{ config(
    materialized='table',
    schema='marts'
) }}

WITH fact_readings AS (
    SELECT * FROM {{ ref('fact_telemetry_readings') }}
),

engine_dims AS (
    SELECT 
        engine_key,
        engine_id,
        engine_name,
        engine_type,
        manufacturer,
        max_chamber_pressure_psi,
        max_fuel_flow_kg_per_sec,
        max_temperature_fahrenheit,
        installation_date,
        operational_status
    FROM telemetry_clean.dim_engines
    WHERE is_current = TRUE
),

time_dims AS (
    SELECT 
        time_key,
        date_actual,
        operational_window
    FROM telemetry_clean.dim_time
),

engine_metrics AS (
    SELECT 
        e.engine_id,
        e.engine_name,
        e.engine_type,
        e.manufacturer,
        e.operational_status,
        e.installation_date,
        
        -- Performance metrics
        COUNT(*) AS total_readings,
        AVG(f.chamber_pressure_psi) AS avg_chamber_pressure_psi,
        AVG(f.fuel_flow_kg_per_sec) AS avg_fuel_flow_kg_per_sec,
        AVG(f.temperature_fahrenheit) AS avg_temperature_fahrenheit,
        AVG(f.fuel_efficiency_ratio) AS avg_fuel_efficiency_ratio,
        AVG(f.performance_score) AS avg_performance_score,
        
        -- Statistical measures
        STDDEV(f.chamber_pressure_psi) AS stddev_chamber_pressure,
        STDDEV(f.fuel_flow_kg_per_sec) AS stddev_fuel_flow,
        STDDEV(f.temperature_fahrenheit) AS stddev_temperature,
        
        -- Min/Max values
        MIN(f.chamber_pressure_psi) AS min_chamber_pressure_psi,
        MAX(f.chamber_pressure_psi) AS max_chamber_pressure_psi,
        MIN(f.fuel_flow_kg_per_sec) AS min_fuel_flow_kg_per_sec,
        MAX(f.fuel_flow_kg_per_sec) AS max_fuel_flow_kg_per_sec,
        MIN(f.temperature_fahrenheit) AS min_temperature_fahrenheit,
        MAX(f.temperature_fahrenheit) AS max_temperature_fahrenheit,
        
        -- Anomaly analysis
        COUNT(CASE WHEN f.is_anomaly THEN 1 END) AS anomaly_count,
        ROUND(
            COUNT(CASE WHEN f.is_anomaly THEN 1 END) * 100.0 / COUNT(*), 
            2
        ) AS anomaly_rate_percent,
        
        -- Specific anomaly types
        COUNT(CASE WHEN f.anomaly_type LIKE '%PRESSURE%' THEN 1 END) AS pressure_anomalies,
        COUNT(CASE WHEN f.anomaly_type LIKE '%FUEL%' THEN 1 END) AS fuel_anomalies,
        COUNT(CASE WHEN f.anomaly_type LIKE '%TEMPERATURE%' THEN 1 END) AS temperature_anomalies,
        
        -- Operational efficiency vs design limits
        ROUND(
            AVG(f.chamber_pressure_psi) * 100.0 / e.max_chamber_pressure_psi, 
            2
        ) AS pressure_utilization_percent,
        ROUND(
            AVG(f.fuel_flow_kg_per_sec) * 100.0 / e.max_fuel_flow_kg_per_sec, 
            2
        ) AS fuel_flow_utilization_percent,
        ROUND(
            AVG(f.temperature_fahrenheit) * 100.0 / e.max_temperature_fahrenheit, 
            2
        ) AS temperature_utilization_percent,
        
        -- Time range of data
        MIN(t.date_actual) AS earliest_reading_date,
        MAX(t.date_actual) AS latest_reading_date,
        
        -- Operational window analysis
        COUNT(CASE WHEN t.operational_window = 'BURN' THEN 1 END) AS burn_phase_readings,
        AVG(CASE WHEN t.operational_window = 'BURN' THEN f.performance_score END) AS burn_phase_avg_performance,
        
        -- Data quality indicators
        COUNT(CASE WHEN f.airbyte_raw_id IS NOT NULL THEN 1 END) AS records_with_source_id,
        MIN(f.created_at) AS first_processed_at,
        MAX(f.created_at) AS last_processed_at
        
    FROM fact_readings f
    INNER JOIN engine_dims e ON f.engine_key = e.engine_key
    LEFT JOIN time_dims t ON f.time_key = t.time_key
    GROUP BY 
        e.engine_id,
        e.engine_name,
        e.engine_type,
        e.manufacturer,
        e.operational_status,
        e.installation_date,
        e.max_chamber_pressure_psi,
        e.max_fuel_flow_kg_per_sec,
        e.max_temperature_fahrenheit
),

ranked_engines AS (
    SELECT 
        *,
        -- Performance ranking
        RANK() OVER (ORDER BY avg_performance_score DESC) AS performance_rank,
        RANK() OVER (ORDER BY anomaly_rate_percent ASC) AS reliability_rank,
        RANK() OVER (ORDER BY avg_fuel_efficiency_ratio DESC) AS efficiency_rank,
        
        -- Health status based on anomaly rate
        CASE 
            WHEN anomaly_rate_percent <= 5 THEN 'EXCELLENT'
            WHEN anomaly_rate_percent <= 15 THEN 'GOOD'
            WHEN anomaly_rate_percent <= 30 THEN 'FAIR'
            ELSE 'NEEDS_ATTENTION'
        END AS health_status,
        
        -- Current timestamp for reporting
        GETDATE() AS report_generated_at
        
    FROM engine_metrics
)

SELECT * FROM ranked_engines
ORDER BY performance_rank 