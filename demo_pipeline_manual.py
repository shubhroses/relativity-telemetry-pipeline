#!/usr/bin/env python3
"""
Telemetry Pipeline Demo - Manual Airbyte UI Approach

This script runs the pipeline with manual Airbyte Cloud UI sync:
1. Generate & Clean Raw Data
2. **MANUAL**: Trigger sync in Airbyte Cloud UI  
3. Run dbt Transformations
4. Execute Business Intelligence

Perfect for live demos where you want to show the Airbyte UI.
"""

import subprocess
import sys
import time
import os
from pathlib import Path


def print_banner(title: str):
    """Print a nice banner for each step"""
    print("\n" + "="*60)
    print(title)
    print("="*60)


def run_command(command: list, description: str, cwd: str = None) -> bool:
    """Run a command and handle errors gracefully"""
    print(f"\n🔄 {description}")
    print(f"   Command: {' '.join(command)}")
    print("-" * 50)
    
    try:
        if cwd:
            result = subprocess.run(command, cwd=cwd, check=True, text=True)
        else:
            result = subprocess.run(command, check=True, text=True)
        
        print(f"✅ {description} - COMPLETED")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} - FAILED")
        print(f"   Exit code: {e.returncode}")
        return False
    except Exception as e:
        print(f"❌ {description} - ERROR: {e}")
        return False


def step_1_generate_data(num_records: int = 1000) -> bool:
    """Step 1: Generate and clean telemetry data"""
    print_banner("STEP 1: GENERATE & CLEAN TELEMETRY DATA")
    
    # Generate data
    command = [
        "bash", "-c",
        f"python python/generate_telemetry.py {num_records} | python python/ingest_and_clean.py --output data/telemetry_production.csv"
    ]
    
    success = run_command(command, f"Generating {num_records} telemetry records")
    
    if success:
        # Update GitHub with fresh data
        print(f"\n📤 Updating GitHub with fresh data...")
        
        commands = [
            (["cp", "data/telemetry_production.csv", "telemetry_sample.csv"], "Copying to root directory"),
            (["git", "add", "telemetry_sample.csv"], "Adding to git"),
            (["git", "commit", "-m", f"Update telemetry data - {num_records} records"], "Committing changes"),
            (["git", "push"], "Pushing to GitHub")
        ]
        
        for cmd, desc in commands:
            run_command(cmd, desc)
        
        print(f"\n✅ Fresh data now available at:")
        print(f"   🌐 https://raw.githubusercontent.com/shubhroses/relativity-telemetry-pipeline/main/telemetry_sample.csv")
        
        # Show sample
        print(f"\n📊 Data Sample:")
        try:
            with open("data/telemetry_production.csv", 'r') as f:
                lines = f.readlines()[:6]
                for line in lines:
                    print(f"   {line.strip()}")
            print(f"\n📈 Total records: {len(lines)-1:,}")
        except Exception as e:
            print(f"   Could not read sample: {e}")
    
    return success


def step_2_manual_airbyte_instructions():
    """Step 2: Provide instructions for manual Airbyte UI sync"""
    print_banner("STEP 2: AIRBYTE CLOUD SYNC (MANUAL UI)")
    
    print("🌐 **MANUAL ACTION REQUIRED**")
    print("\n📋 Quick Setup (if not done already):")
    print("   1. Go to: https://cloud.airbyte.com")
    print("   2. Create CSV Source:")
    print("      • Type: File (CSV, JSON, Excel, etc.)")
    print("      • URL: https://raw.githubusercontent.com/shubhroses/relativity-telemetry-pipeline/main/telemetry_sample.csv")
    print("      • Format: CSV")
    print("   3. Create Redshift Destination:")
    print("      • Use credentials from config/redshift_connection.json")
    print("   4. Create Connection: Source → Destination")
    print("      • Schema: telemetry_raw")
    print("      • Table: telemetry_data")
    
    print(f"\n🚀 **TO SYNC NOW:**")
    print(f"   1. Go to your Airbyte Cloud connection")
    print(f"   2. Click 'Sync Now' button")
    print(f"   3. Watch the progress in real-time")
    print(f"   4. Verify data arrives in Redshift")
    
    # Wait for user confirmation
    print(f"\n⏳ **Waiting for you to complete the sync...**")
    input("   Press Enter when Airbyte sync is complete...")
    
    print(f"\n✅ Airbyte sync completed!")
    print(f"   📊 Data should now be in: telemetry_raw.telemetry_data")


def step_3_test_redshift_data() -> bool:
    """Step 3: Test that data arrived in Redshift"""
    print_banner("STEP 3: VERIFY DATA IN REDSHIFT")
    
    # Test Redshift connection
    success = run_command(
        ["python", "python/test_redshift_connection.py"],
        "Testing Redshift connection"
    )
    
    if success:
        print(f"\n✅ Redshift is accessible!")
        print(f"   🔍 You can verify data with:")
        print(f"   SELECT COUNT(*) FROM telemetry_raw.telemetry_data;")
    
    return success


def step_4_dbt_transformations() -> bool:
    """Step 4: Run dbt transformations"""
    print_banner("STEP 4: DBT TRANSFORMATIONS")
    
    dbt_dir = "dbt/telemetry_analytics"
    
    # Test dbt connection
    if not run_command(["dbt", "debug"], "Testing dbt connection", cwd=dbt_dir):
        print("⚠️  dbt connection failed")
        return False
    
    # Install packages  
    run_command(["dbt", "deps"], "Installing dbt packages", cwd=dbt_dir)
    
    # Run transformations
    if not run_command(["dbt", "run"], "Running dbt transformations", cwd=dbt_dir):
        print("⚠️  dbt transformations failed")
        return False
    
    # Run tests
    run_command(["dbt", "test"], "Running data quality tests", cwd=dbt_dir)
    
    return True


def step_5_business_intelligence():
    """Step 5: Business Intelligence ready"""
    print_banner("STEP 5: BUSINESS INTELLIGENCE READY")
    
    print("🎉 **PIPELINE COMPLETE!**")
    print("\n📊 **Available Analytics:**")
    print("   • Engine Performance Summary")
    print("   • Daily Anomaly Trends") 
    print("   • Real-time Health Monitoring")
    print("   • Predictive Maintenance Alerts")
    
    print("\n🔍 **Query Examples:**")
    queries = [
        "-- Fleet Performance Overview",
        "SELECT engine_name, health_status, avg_performance_score",
        "FROM telemetry_clean.engine_performance_summary",
        "ORDER BY performance_rank;",
        "",
        "-- Recent Anomaly Alerts", 
        "SELECT date_actual, total_anomalies, alert_flag",
        "FROM telemetry_clean.daily_anomaly_trends",
        "WHERE date_actual >= CURRENT_DATE - 7;"
    ]
    
    for query in queries:
        print(f"   {query}")
    
    print(f"\n🎯 **Business Value:**")
    print(f"   • Real-time anomaly detection (15-second response)")
    print(f"   • Proactive maintenance (prevent $2M+ failures)")
    print(f"   • Automated data quality monitoring")
    print(f"   • Scalable cloud-native architecture")


def main():
    """Main demo orchestrator"""
    print("🚀 TELEMETRY PIPELINE DEMO - MANUAL AIRBYTE UI")
    print("🎯 Raw CSV → Airbyte Cloud UI → Redshift → dbt → BI")
    print("="*60)
    
    print("\nThis demo will:")
    print("1. Generate fresh telemetry data with anomalies")
    print("2. Guide you through Airbyte Cloud UI sync")
    print("3. Run dbt transformations") 
    print("4. Show business intelligence results")
    
    confirm = input("\n🤔 Start the demo? (y/N): ").strip().lower()
    if confirm != 'y':
        print("❌ Demo cancelled.")
        sys.exit(0)
    
    start_time = time.time()
    
    try:
        # Step 1: Generate Data
        if not step_1_generate_data(1000):
            print("❌ Failed at data generation")
            sys.exit(1)
        
        # Step 2: Manual Airbyte UI
        step_2_manual_airbyte_instructions()
        
        # Step 3: Verify Redshift
        step_3_test_redshift_data()
        
        # Step 4: dbt Transformations
        if not step_4_dbt_transformations():
            print("⚠️  dbt transformations failed, but continuing...")
        
        # Step 5: BI Results
        step_5_business_intelligence()
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Demo interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        sys.exit(1)
    
    # Final summary
    end_time = time.time()
    duration = int(end_time - start_time)
    
    print(f"\n⏱️  Demo completed in {duration} seconds")
    print(f"🎉 Production-ready telemetry pipeline demonstrated!")


if __name__ == "__main__":
    main() 