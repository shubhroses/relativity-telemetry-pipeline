#!/usr/bin/env python3
"""
Simple Star Schema Creation - Individual Table Approach
Purpose: Create star schema tables one by one to avoid transaction issues
"""

import psycopg2
import json
import logging
from datetime import datetime, timedelta
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_redshift_config():
    with open('config/redshift_connection.json', 'r') as f:
        return json.load(f)

def get_connection(config):
    return psycopg2.connect(
        host=config['host'],
        port=config['port'],
        database=config['database'],
        user=config['username'],
        password=config['password'],
        sslmode='require'
    )

def create_fact_table(config):
    """Create fact_telemetry_readings table"""
    logger.info("🏗️ Creating fact_telemetry_readings...")
    
    ddl = """
    CREATE TABLE IF NOT EXISTS telemetry_clean.fact_telemetry_readings (
        reading_key BIGINT IDENTITY(1,1) PRIMARY KEY,
        engine_key INTEGER NOT NULL,
        time_key INTEGER NOT NULL,
        airbyte_raw_id VARCHAR(50),
        chamber_pressure_psi DECIMAL(8,3) NOT NULL,
        fuel_flow_kg_per_sec DECIMAL(8,3) NOT NULL, 
        temperature_fahrenheit DECIMAL(8,3) NOT NULL,
        fuel_efficiency_ratio DECIMAL(8,6),
        performance_score DECIMAL(8,3),
        is_anomaly BOOLEAN DEFAULT FALSE,
        anomaly_type VARCHAR(50),
        source_system VARCHAR(20) DEFAULT 'AIRBYTE',
        created_at TIMESTAMP DEFAULT GETDATE(),
        updated_at TIMESTAMP DEFAULT GETDATE()
    )
    DISTSTYLE KEY 
    DISTKEY (engine_key)
    SORTKEY (time_key, engine_key);
    """
    
    conn = get_connection(config)
    try:
        cursor = conn.cursor()
        cursor.execute(ddl)
        conn.commit()
        logger.info("✅ fact_telemetry_readings created")
    except Exception as e:
        logger.warning(f"⚠️ {e}")
    finally:
        conn.close()

def create_dim_engines(config):
    """Create dim_engines table"""
    logger.info("🚀 Creating dim_engines...")
    
    ddl = """
    CREATE TABLE IF NOT EXISTS telemetry_clean.dim_engines (
        engine_key INTEGER IDENTITY(1,1) PRIMARY KEY,
        engine_id VARCHAR(10) NOT NULL UNIQUE,
        engine_name VARCHAR(100),
        engine_type VARCHAR(50) DEFAULT 'Rocket Engine',
        manufacturer VARCHAR(100) DEFAULT 'Relativity Space',
        max_chamber_pressure_psi INTEGER DEFAULT 350,
        max_fuel_flow_kg_per_sec INTEGER DEFAULT 200,
        max_temperature_fahrenheit INTEGER DEFAULT 4000,
        installation_date DATE,
        last_maintenance_date DATE,
        operational_status VARCHAR(20) DEFAULT 'ACTIVE',
        effective_from_date DATE DEFAULT CURRENT_DATE,
        effective_to_date DATE DEFAULT '2999-12-31',
        is_current BOOLEAN DEFAULT TRUE,
        source_system VARCHAR(20) DEFAULT 'MANUAL',
        created_at TIMESTAMP DEFAULT GETDATE(),
        updated_at TIMESTAMP DEFAULT GETDATE()
    )
    DISTSTYLE ALL
    SORTKEY (engine_id);
    """
    
    conn = get_connection(config)
    try:
        cursor = conn.cursor()
        cursor.execute(ddl)
        conn.commit()
        logger.info("✅ dim_engines created")
    except Exception as e:
        logger.warning(f"⚠️ {e}")
    finally:
        conn.close()

def create_dim_time(config):
    """Create dim_time table"""
    logger.info("🕐 Creating dim_time...")
    
    ddl = """
    CREATE TABLE IF NOT EXISTS telemetry_clean.dim_time (
        time_key INTEGER PRIMARY KEY,
        full_timestamp TIMESTAMP NOT NULL,
        date_actual DATE NOT NULL,
        year_number INTEGER NOT NULL,
        quarter_number INTEGER NOT NULL,
        month_number INTEGER NOT NULL,
        month_name VARCHAR(10) NOT NULL,
        week_number INTEGER NOT NULL,
        day_of_year INTEGER NOT NULL,
        day_of_month INTEGER NOT NULL,
        day_of_week INTEGER NOT NULL,
        day_name VARCHAR(10) NOT NULL,
        hour_24 INTEGER NOT NULL,
        hour_12 INTEGER NOT NULL,
        minute_number INTEGER NOT NULL,
        second_number INTEGER NOT NULL,
        am_pm VARCHAR(2) NOT NULL,
        is_weekend BOOLEAN NOT NULL,
        is_business_day BOOLEAN NOT NULL,
        mission_day INTEGER,
        test_phase VARCHAR(50),
        operational_window VARCHAR(50),
        created_at TIMESTAMP DEFAULT GETDATE()
    )
    DISTSTYLE ALL
    SORTKEY (time_key);
    """
    
    conn = get_connection(config)
    try:
        cursor = conn.cursor()
        cursor.execute(ddl)
        conn.commit()
        logger.info("✅ dim_time created")
    except Exception as e:
        logger.warning(f"⚠️ {e}")
    finally:
        conn.close()

def create_dim_metrics(config):
    """Create dim_telemetry_metrics table"""
    logger.info("📊 Creating dim_telemetry_metrics...")
    
    ddl = """
    CREATE TABLE IF NOT EXISTS telemetry_clean.dim_telemetry_metrics (
        metric_key INTEGER IDENTITY(1,1) PRIMARY KEY,
        metric_name VARCHAR(50) NOT NULL UNIQUE,
        metric_category VARCHAR(30) NOT NULL,
        metric_description TEXT,
        unit_of_measure VARCHAR(20) NOT NULL,
        normal_min_value DECIMAL(8,3),
        normal_max_value DECIMAL(8,3),
        critical_min_value DECIMAL(8,3),
        critical_max_value DECIMAL(8,3),
        precision_digits INTEGER DEFAULT 3,
        data_quality_rules TEXT,
        created_at TIMESTAMP DEFAULT GETDATE(),
        updated_at TIMESTAMP DEFAULT GETDATE()
    )
    DISTSTYLE ALL
    SORTKEY (metric_name);
    """
    
    conn = get_connection(config)
    try:
        cursor = conn.cursor()
        cursor.execute(ddl)
        conn.commit()
        logger.info("✅ dim_telemetry_metrics created")
    except Exception as e:
        logger.warning(f"⚠️ {e}")
    finally:
        conn.close()

def populate_engines(config):
    """Populate engines dimension"""
    logger.info("🚀 Populating dim_engines...")
    
    engines_data = [
        ('ENG-001', 'Aeon Engine Alpha', 'Rocket Engine', 'Relativity Space', 325, 180, 3800),
        ('ENG-002', 'Aeon Engine Beta', 'Rocket Engine', 'Relativity Space', 330, 185, 3850),
        ('ENG-003', 'Aeon Engine Gamma', 'Rocket Engine', 'Relativity Space', 320, 175, 3750),
        ('ENG-004', 'Aeon Engine Delta', 'Rocket Engine', 'Relativity Space', 335, 190, 3900),
        ('ENG-005', 'Aeon Engine Epsilon', 'Rocket Engine', 'Relativity Space', 340, 195, 3950),
    ]
    
    conn = get_connection(config)
    try:
        cursor = conn.cursor()
        
        # Clear existing data first
        cursor.execute("DELETE FROM telemetry_clean.dim_engines WHERE source_system = 'MANUAL'")
        
        insert_sql = """
        INSERT INTO telemetry_clean.dim_engines 
        (engine_id, engine_name, engine_type, manufacturer, 
         max_chamber_pressure_psi, max_fuel_flow_kg_per_sec, max_temperature_fahrenheit,
         installation_date, operational_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        base_date = datetime(2024, 1, 15)
        for i, (engine_id, name, type_, mfr, max_pressure, max_flow, max_temp) in enumerate(engines_data):
            install_date = base_date + timedelta(days=i*30)
            cursor.execute(insert_sql, (
                engine_id, name, type_, mfr, max_pressure, max_flow, max_temp,
                install_date.date(), 'ACTIVE'
            ))
        
        conn.commit()
        logger.info(f"✅ Inserted {len(engines_data)} engines")
        
    except Exception as e:
        logger.error(f"❌ Error populating engines: {e}")
    finally:
        conn.close()

def populate_metrics(config):
    """Populate metrics dimension"""
    logger.info("📊 Populating dim_telemetry_metrics...")
    
    metrics_data = [
        ('chamber_pressure', 'PRESSURE', 'Chamber pressure measurement', 'PSI', 150, 300, 100, 350),
        ('fuel_flow', 'FLOW', 'Fuel flow rate measurement', 'kg/s', 50, 150, 25, 200),
        ('temperature', 'TEMPERATURE', 'Engine temperature measurement', '°F', 2000, 3500, 1500, 4000),
    ]
    
    conn = get_connection(config)
    try:
        cursor = conn.cursor()
        
        # Clear existing data first
        cursor.execute("DELETE FROM telemetry_clean.dim_telemetry_metrics")
        
        insert_sql = """
        INSERT INTO telemetry_clean.dim_telemetry_metrics 
        (metric_name, metric_category, metric_description, unit_of_measure,
         normal_min_value, normal_max_value, critical_min_value, critical_max_value)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for metric_data in metrics_data:
            cursor.execute(insert_sql, metric_data)
        
        conn.commit()
        logger.info(f"✅ Inserted {len(metrics_data)} metrics")
        
    except Exception as e:
        logger.error(f"❌ Error populating metrics: {e}")
    finally:
        conn.close()

def verify_schema(config):
    """Verify the schema was created"""
    logger.info("🔍 Verifying star schema...")
    
    conn = get_connection(config)
    try:
        cursor = conn.cursor()
        
        tables = ['fact_telemetry_readings', 'dim_engines', 'dim_time', 'dim_telemetry_metrics']
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM telemetry_clean.{table}")
            count = cursor.fetchone()[0]
            logger.info(f"✅ {table}: {count:,} rows")
        
        # Show sample engine data
        cursor.execute("SELECT engine_id, engine_name FROM telemetry_clean.dim_engines LIMIT 3")
        engines = cursor.fetchall()
        logger.info("📋 Sample engines:")
        for engine in engines:
            logger.info(f"   🚀 {engine[0]} - {engine[1]}")
        
    except Exception as e:
        logger.error(f"❌ Verification error: {e}")
    finally:
        conn.close()

def main():
    logger.info("🌟 Starting Simple Star Schema Creation...")
    
    config = load_redshift_config()
    
    # Create tables individually
    create_fact_table(config)
    create_dim_engines(config) 
    create_dim_time(config)
    create_dim_metrics(config)
    
    # Populate dimensions
    populate_engines(config)
    populate_metrics(config)
    
    # Verify
    verify_schema(config)
    
    logger.info("🎉 Star Schema Ready!")

if __name__ == "__main__":
    main() 