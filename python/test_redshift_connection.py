#!/usr/bin/env python3
"""
Test Redshift Connection

Tests connection to AWS Redshift Serverless and validates schema setup.
Run this after setting up your Redshift connection details.
"""

import json
import psycopg2
import sys
from pathlib import Path
from typing import Dict, Any


def load_connection_config() -> Dict[str, Any]:
    """Load Redshift connection configuration"""
    config_path = Path("config/redshift_connection.json")
    
    if not config_path.exists():
        print("❌ Connection config not found!")
        print("Please copy config/redshift_connection_template.json to config/redshift_connection.json")
        print("and fill in your actual Redshift connection details.")
        sys.exit(1)
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Validate required fields
    required_fields = ["host", "port", "database", "username", "password"]
    missing_fields = [field for field in required_fields if not config.get(field)]
    
    if missing_fields:
        print(f"❌ Missing required fields in config: {missing_fields}")
        sys.exit(1)
    
    if "YOUR_PASSWORD_HERE" in config.get("password", ""):
        print("❌ Please update the password in config/redshift_connection.json")
        sys.exit(1)
    
    return config


def test_connection(config: Dict[str, Any]) -> bool:
    """Test basic connection to Redshift"""
    try:
        print("🔗 Testing Redshift connection...")
        
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["username"],
            password=config["password"],
            sslmode=config.get("ssl_mode", "require")
        )
        
        cursor = conn.cursor()
        
        # Test basic query
        cursor.execute("SELECT current_user, current_database(), version();")
        result = cursor.fetchone()
        
        print(f"✅ Connected successfully!")
        print(f"   User: {result[0]}")
        print(f"   Database: {result[1]}")
        print(f"   Version: {result[2][:50]}...")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


def test_schemas(config: Dict[str, Any]) -> bool:
    """Test that required schemas exist"""
    try:
        print("\n📋 Testing schema setup...")
        
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["username"],
            password=config["password"],
            sslmode=config.get("ssl_mode", "require")
        )
        
        cursor = conn.cursor()
        
        # Check for required schemas
        required_schemas = [
            config["schemas"]["raw"],
            config["schemas"]["clean"]
        ]
        
        for schema in required_schemas:
            cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = %s
            """, (schema,))
            
            result = cursor.fetchone()
            if result:
                print(f"✅ Schema '{schema}' exists")
            else:
                print(f"❌ Schema '{schema}' not found")
                print(f"   Run: CREATE SCHEMA {schema};")
                cursor.close()
                conn.close()
                return False
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Schema test failed: {e}")
        return False


def create_test_table(config: Dict[str, Any]) -> bool:
    """Create a simple test table to verify write permissions"""
    try:
        print("\n🔧 Testing table creation permissions...")
        
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["username"],
            password=config["password"],
            sslmode=config.get("ssl_mode", "require")
        )
        
        cursor = conn.cursor()
        
        # Create test table in raw schema
        raw_schema = config["schemas"]["raw"]
        
        cursor.execute(f"""
            DROP TABLE IF EXISTS {raw_schema}.connection_test;
        """)
        
        cursor.execute(f"""
            CREATE TABLE {raw_schema}.connection_test (
                test_id INTEGER,
                test_message VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Insert test data
        cursor.execute(f"""
            INSERT INTO {raw_schema}.connection_test (test_id, test_message)
            VALUES (1, 'Redshift connection successful!');
        """)
        
        # Query test data
        cursor.execute(f"""
            SELECT * FROM {raw_schema}.connection_test;
        """)
        
        result = cursor.fetchone()
        
        # Clean up
        cursor.execute(f"""
            DROP TABLE {raw_schema}.connection_test;
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Table operations successful!")
        print(f"   Test record: {result}")
        
        return True
        
    except Exception as e:
        print(f"❌ Table test failed: {e}")
        return False


def main():
    """Main test function"""
    print("🚀 Redshift Connection Test")
    print("=" * 40)
    
    # Load config
    config = load_connection_config()
    
    # Run tests
    tests = [
        ("Connection", lambda: test_connection(config)),
        ("Schemas", lambda: test_schemas(config)),
        ("Table Operations", lambda: create_test_table(config))
    ]
    
    all_passed = True
    
    for test_name, test_func in tests:
        passed = test_func()
        if not passed:
            all_passed = False
            break
    
    print("\n" + "=" * 40)
    if all_passed:
        print("🎉 All tests passed! Redshift is ready for the pipeline.")
        print("\nNext steps:")
        print("1. Configure Airbyte connector")
        print("2. Set up dbt profiles")
        print("3. Run the telemetry pipeline")
    else:
        print("❌ Some tests failed. Please fix the issues before proceeding.")
        sys.exit(1)


if __name__ == "__main__":
    main() 