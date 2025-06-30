#!/usr/bin/env python3
"""
Star Schema Creation and Reference Data Population
Purpose: Create analytics-ready star schema tables in Redshift
Author: Data Engineering Demonstration Project
"""

import psycopg2
import json
import logging
from datetime import datetime, timedelta
import sys
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_redshift_config():
    """Load Redshift connection configuration"""
    try:
        with open('config/redshift_connection.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error("Redshift configuration file not found!")
        sys.exit(1)

def create_star_schema_tables(cursor):
    """Create all star schema tables"""
    
    logger.info("🏗️ Creating star schema tables...")
    
    # Read the star schema DDL
    with open('sql/star_schema_design.sql', 'r') as f:
        ddl_content = f.read()
    
    # Split the DDL into individual statements
    statements = []
    current_statement = []
    
    for line in ddl_content.split('\n'):
        # Skip comments and empty lines for execution
        if line.strip().startswith('--') or line.strip().startswith('/*') or line.strip() == '':
            continue
        if line.strip().endswith('*/'):
            continue
            
        current_statement.append(line)
        
        # Check if statement is complete (ends with semicolon)
        if line.strip().endswith(';'):
            statement = '\n'.join(current_statement).strip()
            if statement and not statement.startswith('COMMENT'):
                statements.append(statement)
            current_statement = []
    
    # Execute each DDL statement
    for i, statement in enumerate(statements, 1):
        try:
            logger.info(f"📋 Executing DDL statement {i}/{len(statements)}...")
            cursor.execute(statement)
            logger.info("✅ Success")
        except Exception as e:
            logger.warning(f"⚠️ Statement {i} failed (may already exist): {e}")
            continue
    
    logger.info("🎉 Star schema tables creation complete!")

def populate_dim_engines(cursor):
    """Populate the engines dimension with reference data"""
    
    logger.info("🚀 Populating dim_engines...")
    
    engines_data = [
        ('ENG-001', 'Aeon Engine Alpha', 'Rocket Engine', 'Relativity Space', 325, 180, 3800),
        ('ENG-002', 'Aeon Engine Beta', 'Rocket Engine', 'Relativity Space', 330, 185, 3850),
        ('ENG-003', 'Aeon Engine Gamma', 'Rocket Engine', 'Relativity Space', 320, 175, 3750),
        ('ENG-004', 'Aeon Engine Delta', 'Rocket Engine', 'Relativity Space', 335, 190, 3900),
        ('ENG-005', 'Aeon Engine Epsilon', 'Rocket Engine', 'Relativity Space', 340, 195, 3950),
    ]
    
    insert_sql = """
    INSERT INTO telemetry_clean.dim_engines 
    (engine_id, engine_name, engine_type, manufacturer, 
     max_chamber_pressure_psi, max_fuel_flow_kg_per_sec, max_temperature_fahrenheit,
     installation_date, operational_status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Add installation dates (simulate realistic data)
    base_date = datetime(2024, 1, 15)
    
    for i, (engine_id, name, type_, mfr, max_pressure, max_flow, max_temp) in enumerate(engines_data):
        install_date = base_date + timedelta(days=i*30)  # Installed monthly
        
        try:
            cursor.execute(insert_sql, (
                engine_id, name, type_, mfr, max_pressure, max_flow, max_temp,
                install_date.date(), 'ACTIVE'
            ))
            logger.info(f"✅ Added engine: {engine_id}")
        except Exception as e:
            logger.warning(f"⚠️ Engine {engine_id} may already exist: {e}")

def populate_dim_telemetry_metrics(cursor):
    """Populate the telemetry metrics dimension"""
    
    logger.info("📊 Populating dim_telemetry_metrics...")
    
    metrics_data = [
        ('chamber_pressure', 'PRESSURE', 'Chamber pressure measurement', 'PSI', 150, 300, 100, 350),
        ('fuel_flow', 'FLOW', 'Fuel flow rate measurement', 'kg/s', 50, 150, 25, 200),
        ('temperature', 'TEMPERATURE', 'Engine temperature measurement', '°F', 2000, 3500, 1500, 4000),
    ]
    
    insert_sql = """
    INSERT INTO telemetry_clean.dim_telemetry_metrics 
    (metric_name, metric_category, metric_description, unit_of_measure,
     normal_min_value, normal_max_value, critical_min_value, critical_max_value)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    for metric_data in metrics_data:
        try:
            cursor.execute(insert_sql, metric_data)
            logger.info(f"✅ Added metric: {metric_data[0]}")
        except Exception as e:
            logger.warning(f"⚠️ Metric {metric_data[0]} may already exist: {e}")

def populate_dim_time(cursor):
    """Populate time dimension with recent dates"""
    
    logger.info("🕐 Populating dim_time...")
    
    # Generate time dimension for past 30 days and next 7 days
    start_date = datetime.now() - timedelta(days=30)
    end_date = datetime.now() + timedelta(days=7)
    
    current_date = start_date
    time_key = 1
    
    insert_sql = """
    INSERT INTO telemetry_clean.dim_time 
    (time_key, full_timestamp, date_actual, year_number, quarter_number, month_number, 
     month_name, week_number, day_of_year, day_of_month, day_of_week, day_name,
     hour_24, hour_12, minute_number, second_number, am_pm, is_weekend, is_business_day,
     operational_window)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Sample operational windows for aerospace context
    operational_windows = ['PRE_FLIGHT', 'IGNITION', 'BURN', 'SHUTDOWN', 'POST_FLIGHT']
    
    batch_size = 100
    batch_data = []
    
    while current_date <= end_date:
        # Generate hourly records for more granular time dimension
        for hour in range(0, 24, 1):  # Every hour
            for minute in [0, 30]:  # Every 30 minutes
                timestamp = current_date.replace(hour=hour, minute=minute, second=0)
                
                # Extract time components
                year = timestamp.year
                quarter = (timestamp.month - 1) // 3 + 1
                month = timestamp.month
                month_name = timestamp.strftime('%B')[:10]
                week = timestamp.isocalendar()[1]
                day_of_year = timestamp.timetuple().tm_yday
                day_of_month = timestamp.day
                day_of_week = timestamp.weekday() + 1  # 1=Monday, 7=Sunday
                day_name = timestamp.strftime('%A')[:10]
                
                hour_12 = hour if hour <= 12 else hour - 12
                if hour_12 == 0:
                    hour_12 = 12
                am_pm = 'AM' if hour < 12 else 'PM'
                
                is_weekend = day_of_week in [6, 7]  # Saturday, Sunday
                is_business_day = not is_weekend
                
                # Assign operational window based on hour (simulate test phases)
                if 6 <= hour <= 8:
                    op_window = 'PRE_FLIGHT'
                elif 9 <= hour <= 11:
                    op_window = 'IGNITION'
                elif 12 <= hour <= 16:
                    op_window = 'BURN'
                elif 17 <= hour <= 19:
                    op_window = 'SHUTDOWN'
                else:
                    op_window = 'POST_FLIGHT'
                
                batch_data.append((
                    time_key, timestamp, timestamp.date(), year, quarter, month,
                    month_name, week, day_of_year, day_of_month, day_of_week, day_name,
                    hour, hour_12, minute, 0, am_pm, is_weekend, is_business_day, op_window
                ))
                
                time_key += 1
                
                # Execute batch when it reaches batch_size
                if len(batch_data) >= batch_size:
                    try:
                        cursor.executemany(insert_sql, batch_data)
                        logger.info(f"✅ Inserted {len(batch_data)} time dimension records")
                        batch_data = []
                    except Exception as e:
                        logger.warning(f"⚠️ Batch insert failed: {e}")
                        batch_data = []
        
        current_date += timedelta(days=1)
    
    # Insert remaining records
    if batch_data:
        try:
            cursor.executemany(insert_sql, batch_data)
            logger.info(f"✅ Inserted final {len(batch_data)} time dimension records")
        except Exception as e:
            logger.warning(f"⚠️ Final batch insert failed: {e}")

def verify_star_schema(cursor):
    """Verify the star schema was created successfully"""
    
    logger.info("🔍 Verifying star schema...")
    
    tables_to_check = [
        'fact_telemetry_readings',
        'dim_engines', 
        'dim_time',
        'dim_telemetry_metrics',
        'bridge_engine_metrics'
    ]
    
    for table in tables_to_check:
        try:
            cursor.execute(f"""
                SELECT COUNT(*) as row_count 
                FROM telemetry_clean.{table}
            """)
            
            count = cursor.fetchone()[0]
            logger.info(f"✅ {table}: {count:,} rows")
            
        except Exception as e:
            logger.error(f"❌ Error checking {table}: {e}")
    
    # Show sample data from dimensions
    logger.info("\n📋 Sample dimension data:")
    
    # Engines sample
    cursor.execute("SELECT engine_id, engine_name FROM telemetry_clean.dim_engines LIMIT 3")
    engines = cursor.fetchall()
    for engine in engines:
        logger.info(f"   🚀 Engine: {engine[0]} - {engine[1]}")
    
    # Metrics sample  
    cursor.execute("SELECT metric_name, metric_category, unit_of_measure FROM telemetry_clean.dim_telemetry_metrics")
    metrics = cursor.fetchall()
    for metric in metrics:
        logger.info(f"   📊 Metric: {metric[0]} ({metric[1]}) - {metric[2]}")

def main():
    """Main execution function"""
    
    logger.info("🌟 Starting Star Schema Creation Process...")
    
    # Load configuration
    config = load_redshift_config()
    
    try:
        # Connect to Redshift
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['username'],
            password=config['password'],
            sslmode='require'
        )
        
        cursor = conn.cursor()
        logger.info("✅ Connected to Redshift")
        
        # Create star schema tables
        create_star_schema_tables(cursor)
        conn.commit()
        
        # Populate dimensions with reference data
        populate_dim_engines(cursor)
        conn.commit()
        
        populate_dim_telemetry_metrics(cursor)
        conn.commit()
        
        populate_dim_time(cursor)
        conn.commit()
        
        # Verify everything was created
        verify_star_schema(cursor)
        
        logger.info("🎉 Star Schema Creation Complete!")
        logger.info("🚀 Ready for dbt transformations!")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        sys.exit(1)
        
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("✅ Database connection closed")

if __name__ == "__main__":
    main() 