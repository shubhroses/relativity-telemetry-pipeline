#!/usr/bin/env python3
"""
Verify Airbyte Sync Results

Checks that data was successfully loaded from CSV to Redshift via Airbyte.
"""

import json
import psycopg2
import sys
from pathlib import Path
from typing import Dict, Any


def load_config() -> Dict[str, Any]:
    """Load Redshift connection configuration"""
    config_path = Path("config/redshift_connection.json")
    
    if not config_path.exists():
        print("❌ Redshift config not found!")
        sys.exit(1)
    
    with open(config_path, 'r') as f:
        return json.load(f)


def verify_airbyte_sync():
    """Verify Airbyte sync results in Redshift"""
    config = load_config()
    
    try:
        print("🔍 Verifying Airbyte sync results...")
        print("=" * 50)
        
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["username"],
            password=config["password"],
            sslmode=config.get("ssl_mode", "require")
        )
        
        cursor = conn.cursor()
        
        # Check if Airbyte created the table
        schema = config["schemas"]["raw"]
        cursor.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}' 
            AND table_name LIKE '%telemetry%'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        print(f"📋 Tables in {schema} schema:")
        if tables:
            for table in tables:
                print(f"   - {table[0]}")
        else:
            print("   ❌ No telemetry tables found")
            cursor.close()
            conn.close()
            return
        
        # Find the main data table (Airbyte may add prefixes/suffixes)
        data_table = None
        for table in tables:
            if 'telemetry' in table[0].lower() and 'data' in table[0].lower():
                data_table = f"{schema}.{table[0]}"
                break
        
        if not data_table:
            # Try common Airbyte patterns
            possible_names = [
                f"{schema}.telemetry_data",
                f"{schema}._airbyte_raw_telemetry_data", 
                f"{schema}.telemetry_clean"
            ]
            
            for table_name in possible_names:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name} LIMIT 1")
                    data_table = table_name
                    break
                except:
                    continue
        
        if not data_table:
            print("❌ Could not find telemetry data table")
            cursor.close()
            conn.close()
            return
            
        print(f"\n📊 Analyzing data in: {data_table}")
        
        # Count total records
        cursor.execute(f"SELECT COUNT(*) FROM {data_table}")
        total_count = cursor.fetchone()[0]
        print(f"   Total records: {total_count}")
        
        # Check data types and columns
        cursor.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = '{schema}' 
            AND table_name = '{data_table.split('.')[-1]}'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print(f"\n📋 Table schema:")
        for col_name, col_type in columns:
            print(f"   {col_name}: {col_type}")
        
        # Sample data
        cursor.execute(f"SELECT * FROM {data_table} LIMIT 5")
        sample_data = cursor.fetchall()
        
        if sample_data:
            print(f"\n📊 Sample data (first 5 rows):")
            col_names = [desc[0] for desc in cursor.description]
            
            # Print header
            print("   " + " | ".join(f"{col[:15]:<15}" for col in col_names))
            print("   " + "-" * (16 * len(col_names)))
            
            # Print sample rows
            for i, row in enumerate(sample_data, 1):
                row_str = " | ".join(f"{str(val)[:15]:<15}" for val in row)
                print(f"   {row_str}")
        
        # Check for Airbyte metadata
        airbyte_cols = [col for col, _ in columns if col.startswith('_airbyte')]
        if airbyte_cols:
            print(f"\n✅ Airbyte metadata columns found: {len(airbyte_cols)}")
            for col in airbyte_cols:
                print(f"   - {col}")
        
        # Data quality checks
        print(f"\n🔍 Data Quality Checks:")
        
        # Check for null values in key columns
        key_columns = ['timestamp', 'engine_id', 'chamber_pressure', 'fuel_flow', 'temperature']
        for col in key_columns:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {data_table} WHERE {col} IS NULL")
                null_count = cursor.fetchone()[0]
                if null_count > 0:
                    print(f"   ⚠️  {col}: {null_count} null values")
                else:
                    print(f"   ✅ {col}: No null values")
            except:
                print(f"   ❓ {col}: Column not found or check failed")
        
        # Check timestamp range
        try:
            cursor.execute(f"""
                SELECT 
                    MIN(timestamp) as earliest,
                    MAX(timestamp) as latest
                FROM {data_table}
                WHERE timestamp IS NOT NULL
            """)
            time_range = cursor.fetchone()
            if time_range and time_range[0]:
                print(f"   📅 Time range: {time_range[0]} to {time_range[1]}")
        except:
            print("   ❓ Could not analyze timestamp range")
        
        cursor.close()
        conn.close()
        
        print(f"\n🎉 Airbyte sync verification complete!")
        print(f"✅ Found {total_count} records in Redshift")
        
        if total_count > 0:
            print("\n🚀 Next steps:")
            print("1. Set up dbt transformations")
            print("2. Create star schema models")
            print("3. Build analytics queries")
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    verify_airbyte_sync() 