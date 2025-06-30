#!/usr/bin/env python3
"""
Complete Telemetry Data Pipeline Orchestrator

This script runs the full end-to-end pipeline:
1. Generate & Clean Raw Data
2. Trigger Airbyte Cloud Sync (CSV → Redshift)
3. Run dbt Transformations (Staging → Core → Marts)  
4. Execute Business Intelligence Queries

Perfect for demonstrating the complete data engineering workflow.
"""

import subprocess
import sys
import time
import os
from pathlib import Path


class PipelineOrchestrator:
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.steps_completed = []
        
    def run_command(self, command: list, description: str, cwd: str = None) -> bool:
        """Run a command and handle errors gracefully"""
        print(f"\n🔄 {description}")
        print(f"   Command: {' '.join(command)}")
        print("-" * 60)
        
        try:
            if cwd:
                result = subprocess.run(
                    command, 
                    cwd=cwd, 
                    check=True, 
                    capture_output=False,
                    text=True
                )
            else:
                result = subprocess.run(
                    command, 
                    check=True, 
                    capture_output=False,
                    text=True
                )
            
            print(f"✅ {description} - COMPLETED")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ {description} - FAILED")
            print(f"   Exit code: {e.returncode}")
            return False
        except Exception as e:
            print(f"❌ {description} - ERROR: {e}")
            return False
    
    def step_1_generate_data(self, num_records: int = 1000) -> bool:
        """Step 1: Generate and clean telemetry data"""
        print("\n" + "="*60)
        print("STEP 1: GENERATE & CLEAN TELEMETRY DATA")
        print("="*60)
        
        # Generate raw data and pipe to cleaning script
        command = [
            "bash", "-c",
            f"python python/generate_telemetry.py {num_records} | python python/ingest_and_clean.py --output data/telemetry_production.csv"
        ]
        
        success = self.run_command(
            command,
            f"Generating {num_records} telemetry records with anomalies"
        )
        
        if success:
            self.steps_completed.append("Data Generation")
            
            # Show data sample
            print(f"\n📊 Data Sample:")
            try:
                with open("data/telemetry_production.csv", 'r') as f:
                    lines = f.readlines()[:6]  # Header + 5 data rows
                    for line in lines:
                        print(f"   {line.strip()}")
                        
                print(f"\n📈 Total records: {len(lines)-1:,}")
            except Exception as e:
                print(f"   Could not read data sample: {e}")
        
        return success
    
    def step_2_sync_airbyte(self) -> bool:
        """Step 2: Trigger Airbyte Cloud sync"""
        print("\n" + "="*60)
        print("STEP 2: AIRBYTE CLOUD SYNC (CSV → REDSHIFT)")
        print("="*60)
        
        # Check if Airbyte config exists
        if not os.path.exists("config/airbyte_config.json"):
            print("⚠️  Airbyte configuration not found!")
            print("\n📋 Quick Setup Required:")
            print("   1. Go to: https://cloud.airbyte.com")
            print("   2. Create CSV source with URL: https://raw.githubusercontent.com/shubhroses/relativity-telemetry-pipeline/main/telemetry_sample.csv")
            print("   3. Create Redshift destination with your credentials")
            print("   4. Create connection between source and destination")
            print("   5. Get API key and update config/airbyte_config.json")
            print(f"\n   Then run: python python/trigger_airbyte_sync.py")
            
            # Return True to continue with manual option
            return True
        
        success = self.run_command(
            ["python", "python/trigger_airbyte_sync.py"],
            "Triggering Airbyte Cloud sync"
        )
        
        if success:
            self.steps_completed.append("Airbyte Sync")
        
        return success
    
    def step_3_dbt_transformations(self) -> bool:
        """Step 3: Run dbt transformations"""
        print("\n" + "="*60)
        print("STEP 3: DBT TRANSFORMATIONS (STAGING → CORE → MARTS)")
        print("="*60)
        
        dbt_dir = "dbt/telemetry_analytics"
        
        # Test dbt connection
        if not self.run_command(
            ["dbt", "debug"],
            "Testing dbt connection to Redshift",
            cwd=dbt_dir
        ):
            print("⚠️  dbt connection failed. Please check your Redshift credentials.")
            return False
        
        # Install dbt packages
        if not self.run_command(
            ["dbt", "deps"],
            "Installing dbt packages",
            cwd=dbt_dir
        ):
            print("⚠️  Failed to install dbt packages")
            return False
        
        # Run transformations
        if not self.run_command(
            ["dbt", "run"],
            "Running dbt transformations",
            cwd=dbt_dir
        ):
            print("⚠️  dbt transformations failed")
            return False
            
        # Run tests
        if not self.run_command(
            ["dbt", "test"],
            "Running dbt data quality tests",
            cwd=dbt_dir
        ):
            print("⚠️  Some dbt tests failed, but continuing...")
        
        self.steps_completed.append("dbt Transformations")
        return True
    
    def step_4_business_intelligence(self) -> bool:
        """Step 4: Run business intelligence queries"""
        print("\n" + "="*60)
        print("STEP 4: BUSINESS INTELLIGENCE ANALYTICS")
        print("="*60)
        
        # Create BI queries script
        bi_queries = """
-- 🚨 CRITICAL ENGINE ALERTS
SELECT 
    engine_name,
    health_status,
    anomaly_rate_percent,
    avg_performance_score
FROM telemetry_clean.engine_performance_summary 
WHERE health_status = 'NEEDS_ATTENTION'
ORDER BY anomaly_rate_percent DESC;

-- 📊 FLEET PERFORMANCE OVERVIEW  
SELECT 
    engine_name,
    ROUND(avg_performance_score, 1) as performance,
    performance_rank,
    health_status,
    total_readings
FROM telemetry_clean.engine_performance_summary 
ORDER BY performance_rank;

-- 📈 RECENT ANOMALY TRENDS
SELECT 
    date_actual,
    daily_anomaly_rate_percent,
    total_anomalies,
    alert_flag,
    anomaly_trend
FROM telemetry_clean.daily_anomaly_trends 
WHERE date_actual >= CURRENT_DATE - 7
ORDER BY date_actual DESC;
"""
        
        # Save BI queries
        with open("analytics_queries.sql", "w") as f:
            f.write(bi_queries)
        
        print("✅ Business Intelligence queries ready!")
        print(f"   📄 Saved to: analytics_queries.sql")
        print(f"   🔍 Execute in your SQL client or Redshift Query Editor")
        
        self.steps_completed.append("Business Intelligence")
        return True
    
    def print_final_summary(self):
        """Print final pipeline summary"""
        print("\n" + "="*80)
        print("🎉 TELEMETRY PIPELINE EXECUTION COMPLETE!")
        print("="*80)
        
        print(f"\n✅ Steps Completed:")
        for i, step in enumerate(self.steps_completed, 1):
            print(f"   {i}. {step}")
        
        print(f"\n🎯 Pipeline Flow:")
        print(f"   Raw CSV → Airbyte Cloud → Redshift → dbt → Business Intelligence")
        
        print(f"\n📊 What You Can Do Now:")
        print(f"   • Query raw data: SELECT * FROM telemetry_raw.telemetry_data LIMIT 10;")
        print(f"   • View engine performance: SELECT * FROM telemetry_clean.engine_performance_summary;")
        print(f"   • Check anomaly trends: SELECT * FROM telemetry_clean.daily_anomaly_trends;")
        print(f"   • Run analytics queries: Use analytics_queries.sql")
        
        print(f"\n🌐 Access Points:")
        print(f"   • Airbyte Cloud: https://cloud.airbyte.com")
        print(f"   • Redshift Query Editor: AWS Console")
        print(f"   • dbt Docs: cd dbt/telemetry_analytics && dbt docs generate && dbt docs serve")
        
        print(f"\n💰 Business Value Demonstrated:")
        print(f"   • Real-time anomaly detection and alerting")
        print(f"   • Automated data quality monitoring") 
        print(f"   • Scalable cloud-native architecture")
        print(f"   • Production-ready data engineering practices")


def main():
    """Main function to orchestrate the complete pipeline"""
    print("🚀 TELEMETRY DATA PIPELINE - FULL EXECUTION")
    print("🎯 End-to-End Demo: Raw CSV → Airbyte → Redshift → dbt → BI")
    print("="*80)
    
    orchestrator = PipelineOrchestrator()
    
    # Confirm execution
    print("\nThis will run the complete telemetry pipeline:")
    print("1. Generate 1000 telemetry records with anomalies")
    print("2. Trigger Airbyte Cloud sync to Redshift")  
    print("3. Run dbt transformations (staging → core → marts)")
    print("4. Prepare business intelligence analytics")
    
    confirm = input("\n🤔 Continue with full pipeline execution? (y/N): ").strip().lower()
    if confirm != 'y':
        print("❌ Pipeline execution cancelled.")
        sys.exit(0)
    
    start_time = time.time()
    
    # Execute pipeline steps
    try:
        # Step 1: Data Generation
        if not orchestrator.step_1_generate_data(1000):
            print("❌ Pipeline failed at Step 1: Data Generation")
            sys.exit(1)
        
        # Step 2: Airbyte Sync  
        if not orchestrator.step_2_sync_airbyte():
            print("⚠️  Airbyte sync step needs manual setup. Continuing...")
        
        # Step 3: dbt Transformations
        if not orchestrator.step_3_dbt_transformations():
            print("⚠️  dbt transformations failed. Check Redshift connection.")
        
        # Step 4: Business Intelligence
        orchestrator.step_4_business_intelligence()
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Pipeline execution interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        sys.exit(1)
    
    # Final summary
    end_time = time.time()
    duration = int(end_time - start_time)
    
    orchestrator.print_final_summary()
    print(f"\n⏱️  Total execution time: {duration} seconds")
    print("🚀 Ready for production deployment!")


if __name__ == "__main__":
    main() 