#!/usr/bin/env python3
"""
Airbyte Helper Script

Utilities for preparing data and configuring Airbyte connections.
"""

import json
import csv
import os
import shutil
from pathlib import Path
from typing import Dict, Any
import psycopg2


def load_config() -> Dict[str, Any]:
    """Load Redshift connection configuration"""
    config_path = Path("config/redshift_connection.json")
    
    if not config_path.exists():
        print("❌ Redshift config not found! Run setup first.")
        return {}
    
    with open(config_path, 'r') as f:
        return json.load(f)


def prepare_sample_data(num_records: int = 100) -> str:
    """Generate fresh sample data for Airbyte ingestion"""
    print(f"🔄 Generating {num_records} sample telemetry records...")
    
    # Generate data using our existing pipeline
    os.system(f"python python/generate_telemetry.py {num_records} | python python/ingest_and_clean.py")
    
    csv_path = "data/telemetry_clean.csv"
    if Path(csv_path).exists():
        print(f"✅ Sample data ready: {csv_path}")
        
        # Show sample of data
        with open(csv_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            sample_rows = [next(reader) for _ in range(3)]
            
        print(f"📊 Sample data preview:")
        print(f"   Headers: {', '.join(header)}")
        for i, row in enumerate(sample_rows, 1):
            print(f"   Row {i}: {', '.join(row)}")
            
        return csv_path
    else:
        print("❌ Failed to generate sample data")
        return ""


def validate_csv_format(csv_path: str) -> bool:
    """Validate CSV format for Airbyte ingestion"""
    try:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            
            expected_headers = ['timestamp', 'engine_id', 'chamber_pressure', 'fuel_flow', 'temperature']
            
            print(f"🔍 Validating CSV format...")
            print(f"   Expected headers: {expected_headers}")
            print(f"   Actual headers: {list(headers)}")
            
            missing_headers = set(expected_headers) - set(headers)
            if missing_headers:
                print(f"❌ Missing headers: {missing_headers}")
                return False
                
            # Check first few rows for data validity
            row_count = 0
            for row in reader:
                row_count += 1
                if row_count > 5:  # Check first 5 rows
                    break
                    
                # Validate required fields are not empty
                for header in expected_headers:
                    if not row.get(header, '').strip():
                        print(f"❌ Empty value found in row {row_count}, column {header}")
                        return False
                        
            print(f"✅ CSV format valid! Found {row_count} sample rows")
            return True
            
    except Exception as e:
        print(f"❌ CSV validation failed: {e}")
        return False


def test_redshift_destination() -> bool:
    """Test Redshift connection for Airbyte destination"""
    config = load_config()
    if not config:
        return False
        
    try:
        print("🔍 Testing Redshift destination readiness...")
        
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["username"],
            password=config["password"],
            sslmode=config.get("ssl_mode", "require")
        )
        
        cursor = conn.cursor()
        
        # Check if target schema exists
        schema = config["schemas"]["raw"]
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = %s
        """, (schema,))
        
        if not cursor.fetchone():
            print(f"❌ Target schema '{schema}' not found")
            return False
            
        # Test table creation permissions
        test_table = f"{schema}.airbyte_test_table"
        cursor.execute(f"DROP TABLE IF EXISTS {test_table}")
        cursor.execute(f"""
            CREATE TABLE {test_table} (
                id INTEGER,
                test_data VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute(f"INSERT INTO {test_table} (id, test_data) VALUES (1, 'Airbyte destination test')")
        cursor.execute(f"SELECT COUNT(*) FROM {test_table}")
        count = cursor.fetchone()[0]
        
        cursor.execute(f"DROP TABLE {test_table}")
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Redshift destination ready! Schema: {schema}")
        print(f"   Connection successful, table operations working")
        return True
        
    except Exception as e:
        print(f"❌ Redshift destination test failed: {e}")
        return False


def create_airbyte_configs() -> Dict[str, str]:
    """Create customized Airbyte configuration files"""
    config = load_config()
    
    # Update destination config with actual password
    dest_config_path = Path("airbyte/destination_config.json")
    if dest_config_path.exists():
        with open(dest_config_path, 'r') as f:
            dest_config = json.load(f)
            
        # Replace placeholder password
        dest_config["connectionConfiguration"]["password"] = config["password"]
        
        # Save updated config
        with open(dest_config_path, 'w') as f:
            json.dump(dest_config, f, indent=2)
            
        print("✅ Updated Airbyte destination config with actual credentials")
    
    return {
        "source_config": "airbyte/source_config.json",
        "destination_config": "airbyte/destination_config.json", 
        "connection_config": "airbyte/connection_config.json"
    }


def print_next_steps():
    """Print next steps for Airbyte setup"""
    print("\n🚀 Airbyte Setup Next Steps:")
    print("=" * 50)
    print("1. Choose your Airbyte option:")
    print("   • Airbyte Cloud: https://cloud.airbyte.com (recommended)")
    print("   • Local OSS: Follow docs/airbyte_setup_guide.md")
    print()
    print("2. Create source connection:")
    print("   • Type: File (CSV)")
    print("   • Upload: data/telemetry_clean.csv")
    print("   • Format: CSV with headers")
    print()
    print("3. Create destination connection:")
    print("   • Type: Redshift")
    print("   • Use credentials from airbyte/destination_config.json")
    print()
    print("4. Set up sync:")
    print("   • Mode: Full refresh + Overwrite")
    print("   • Schedule: Manual (for demo)")
    print("   • Target: telemetry_raw schema")
    print()
    print("5. Test sync and verify data in Redshift")


def main():
    """Main function for command line usage"""
    print("🔧 Airbyte Helper - Telemetry Pipeline Setup")
    print("=" * 50)
    
    # Step 1: Prepare sample data
    csv_path = prepare_sample_data(50)
    if not csv_path:
        return
        
    # Step 2: Validate CSV format
    if not validate_csv_format(csv_path):
        return
        
    # Step 3: Test Redshift destination
    if not test_redshift_destination():
        return
        
    # Step 4: Update Airbyte configs
    configs = create_airbyte_configs()
    
    # Step 5: Print next steps
    print_next_steps()


if __name__ == "__main__":
    main() 