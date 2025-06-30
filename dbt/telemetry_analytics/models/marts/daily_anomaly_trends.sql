/*
    Daily Anomaly Trends Mart
    
    This model provides:
    - Daily aggregation of anomalies by type
    - Trend analysis for operational monitoring
    - Time-based patterns and seasonality detection
*/

{{ config(
    materialized='table',
    schema='marts'
) }}

WITH fact_readings AS (
    SELECT * FROM {{ ref('fact_telemetry_readings') }}
),

daily_metrics AS (
    SELECT 
        DATE(reading_timestamp) AS date_actual,
        EXTRACT(DOW FROM reading_timestamp) AS day_of_week,
        CASE WHEN EXTRACT(DOW FROM reading_timestamp) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        
        -- Overall metrics
        COUNT(*) AS total_readings,
        COUNT(CASE WHEN is_anomaly THEN 1 END) AS total_anomalies,
        ROUND(
            COUNT(CASE WHEN is_anomaly THEN 1 END) * 100.0 / COUNT(*), 
            2
        ) AS daily_anomaly_rate_percent,
        
        -- Anomaly type breakdown
        COUNT(CASE WHEN anomaly_type = 'LOW_PRESSURE' THEN 1 END) AS low_pressure_anomalies,
        COUNT(CASE WHEN anomaly_type = 'HIGH_PRESSURE' THEN 1 END) AS high_pressure_anomalies,
        COUNT(CASE WHEN anomaly_type = 'LOW_FUEL_FLOW' THEN 1 END) AS low_fuel_flow_anomalies,
        COUNT(CASE WHEN anomaly_type = 'HIGH_FUEL_FLOW' THEN 1 END) AS high_fuel_flow_anomalies,
        COUNT(CASE WHEN anomaly_type = 'LOW_TEMPERATURE' THEN 1 END) AS low_temperature_anomalies,
        COUNT(CASE WHEN anomaly_type = 'HIGH_TEMPERATURE' THEN 1 END) AS high_temperature_anomalies,
        
        -- Performance metrics
        AVG(performance_score) AS avg_daily_performance_score,
        MIN(performance_score) AS min_daily_performance_score,
        MAX(performance_score) AS max_daily_performance_score,
        STDDEV(performance_score) AS stddev_daily_performance,
        
        -- Engine distribution
        COUNT(DISTINCT engine_id) AS active_engines,
        
        -- Health status distribution
        COUNT(CASE WHEN health_status = 'EXCELLENT' THEN 1 END) AS excellent_readings,
        COUNT(CASE WHEN health_status = 'GOOD' THEN 1 END) AS good_readings,
        COUNT(CASE WHEN health_status = 'FAIR' THEN 1 END) AS fair_readings,
        COUNT(CASE WHEN health_status = 'POOR' THEN 1 END) AS poor_readings,
        COUNT(CASE WHEN health_status = 'CRITICAL' THEN 1 END) AS critical_readings,
        
        -- Data freshness
        MIN(created_at) AS earliest_processing_time,
        MAX(created_at) AS latest_processing_time
        
    FROM fact_readings
    GROUP BY 
        DATE(reading_timestamp),
        EXTRACT(DOW FROM reading_timestamp)
),

trend_analysis AS (
    SELECT 
        *,
        -- Moving averages for trend detection
        AVG(daily_anomaly_rate_percent) OVER (
            ORDER BY date_actual 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS seven_day_avg_anomaly_rate,
        
        AVG(avg_daily_performance_score) OVER (
            ORDER BY date_actual 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS seven_day_avg_performance,
        
        -- Day-over-day changes
        LAG(daily_anomaly_rate_percent, 1) OVER (ORDER BY date_actual) AS prev_day_anomaly_rate,
        LAG(avg_daily_performance_score, 1) OVER (ORDER BY date_actual) AS prev_day_performance,
        
        -- Week-over-week comparison
        LAG(daily_anomaly_rate_percent, 7) OVER (ORDER BY date_actual) AS week_ago_anomaly_rate,
        LAG(avg_daily_performance_score, 7) OVER (ORDER BY date_actual) AS week_ago_performance,
        
        -- Ranking for identifying worst/best days
        RANK() OVER (ORDER BY daily_anomaly_rate_percent DESC) AS worst_anomaly_day_rank,
        RANK() OVER (ORDER BY avg_daily_performance_score DESC) AS best_performance_day_rank
        
    FROM daily_metrics
),

final_analysis AS (
    SELECT 
        *,
        -- Calculated changes
        ROUND(daily_anomaly_rate_percent - prev_day_anomaly_rate, 2) AS day_over_day_anomaly_change,
        ROUND(avg_daily_performance_score - prev_day_performance, 2) AS day_over_day_performance_change,
        ROUND(daily_anomaly_rate_percent - week_ago_anomaly_rate, 2) AS week_over_week_anomaly_change,
        ROUND(avg_daily_performance_score - week_ago_performance, 2) AS week_over_week_performance_change,
        
        -- Trend indicators
        CASE 
            WHEN daily_anomaly_rate_percent > seven_day_avg_anomaly_rate * 1.2 THEN 'DETERIORATING'
            WHEN daily_anomaly_rate_percent < seven_day_avg_anomaly_rate * 0.8 THEN 'IMPROVING'
            ELSE 'STABLE'
        END AS anomaly_trend,
        
        CASE 
            WHEN avg_daily_performance_score > seven_day_avg_performance * 1.1 THEN 'IMPROVING'
            WHEN avg_daily_performance_score < seven_day_avg_performance * 0.9 THEN 'DETERIORATING'
            ELSE 'STABLE'
        END AS performance_trend,
        
        -- Alert flags
        CASE 
            WHEN daily_anomaly_rate_percent > 25 THEN 'HIGH_ANOMALY_ALERT'
            WHEN critical_readings > 0 THEN 'CRITICAL_HEALTH_ALERT'
            WHEN day_over_day_anomaly_change > 10 THEN 'RAPID_DETERIORATION_ALERT'
            ELSE NULL
        END AS alert_flag,
        
        -- Report metadata
        GETDATE() AS report_generated_at
        
    FROM trend_analysis
)

SELECT * FROM final_analysis
ORDER BY date_actual DESC 