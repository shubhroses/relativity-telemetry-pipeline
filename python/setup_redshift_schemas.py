#!/usr/bin/env python3
"""
Setup Redshift Schemas

Creates the required schemas for the telemetry pipeline.
"""

import json
import psycopg2
import sys
from pathlib import Path


def load_connection_config():
    """Load Redshift connection configuration"""
    config_path = Path("config/redshift_connection.json")
    
    if not config_path.exists():
        print("❌ Connection config not found!")
        sys.exit(1)
    
    with open(config_path, 'r') as f:
        return json.load(f)


def setup_schemas():
    """Create required schemas"""
    config = load_connection_config()
    
    try:
        print("🔧 Setting up Redshift schemas...")
        
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["username"],
            password=config["password"],
            sslmode=config.get("ssl_mode", "require")
        )
        
        cursor = conn.cursor()
        
        # Create schemas
        schemas_to_create = [
            config["schemas"]["raw"],
            config["schemas"]["clean"]
        ]
        
        for schema in schemas_to_create:
            print(f"📋 Creating schema: {schema}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            
        # Verify schemas were created
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN %s
            ORDER BY schema_name;
        """, (tuple(schemas_to_create),))
        
        created_schemas = [row[0] for row in cursor.fetchall()]
        
        print("\n✅ Schemas created successfully:")
        for schema in created_schemas:
            print(f"   - {schema}")
        
        # Grant permissions (optional, but good practice)
        for schema in schemas_to_create:
            cursor.execute(f"GRANT ALL ON SCHEMA {schema} TO admin;")
            
        conn.commit()
        cursor.close()
        conn.close()
        
        print("\n🎉 Schema setup complete!")
        print("\nNext steps:")
        print("1. Test connection: python python/test_redshift_connection.py")
        print("2. Set up dbt project")
        print("3. Configure Airbyte")
        
    except Exception as e:
        print(f"❌ Schema setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    setup_schemas() 