#!/usr/bin/env python3
"""
Airbyte Cloud Pipeline Trigger

This script triggers Airbyte Cloud syncs programmatically and monitors their progress.
Perfect for automating the telemetry data pipeline from local development.
"""

import json
import time
import requests
import sys
from pathlib import Path
from typing import Dict, Any, Optional


class AirbyteCloudTrigger:
    def __init__(self, config_path: str = "config/airbyte_config.json"):
        self.config = self.load_config(config_path)
        self.session = requests.Session()
        
        # Set up authentication
        self.session.headers.update({
            "Authorization": f"Bearer {self.config['api_key']}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
        
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load Airbyte Cloud API configuration"""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                
            # Validate required fields
            required_fields = ['api_key', 'workspace_id', 'connection_id', 'base_url']
            missing_fields = [field for field in required_fields if not config.get(field) or config[field] == f"YOUR_{field.upper()}_HERE"]
            
            if missing_fields:
                print(f"❌ Missing configuration fields: {missing_fields}")
                print(f"Please update {config_path} with your Airbyte Cloud credentials.")
                sys.exit(1)
                
            return config
            
        except FileNotFoundError:
            print(f"❌ Configuration file not found: {config_path}")
            print("Please create the configuration file with your Airbyte Cloud credentials.")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in configuration file: {e}")
            sys.exit(1)
    
    def trigger_sync(self) -> Optional[str]:
        """Trigger a manual sync and return the job ID"""
        url = f"{self.config['base_url']}/jobs"
        
        payload = {
            "jobType": "sync",
            "connectionId": self.config['connection_id']
        }
        
        try:
            print(f"🚀 Triggering Airbyte sync for connection: {self.config['connection_id']}")
            response = self.session.post(url, json=payload)
            response.raise_for_status()
            
            job_data = response.json()
            job_id = job_data.get('jobId')
            
            print(f"✅ Sync triggered successfully!")
            print(f"   Job ID: {job_id}")
            print(f"   Job Type: {job_data.get('jobType')}")
            print(f"   Status: {job_data.get('status')}")
            
            return job_id
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to trigger sync: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"   Response: {e.response.text}")
            return None
    
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get the status of a specific job"""
        url = f"{self.config['base_url']}/jobs/{job_id}"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to get job status: {e}")
            return {}
    
    def monitor_sync(self, job_id: str, max_wait_minutes: int = 10) -> bool:
        """Monitor sync progress until completion"""
        if not job_id:
            return False
            
        print(f"\n📊 Monitoring sync progress...")
        print(f"   Job ID: {job_id}")
        print(f"   Max wait time: {max_wait_minutes} minutes")
        print("-" * 50)
        
        start_time = time.time()
        max_wait_seconds = max_wait_minutes * 60
        
        while True:
            job_status = self.get_job_status(job_id)
            
            if not job_status:
                print("❌ Could not retrieve job status")
                return False
                
            status = job_status.get('status', 'unknown')
            elapsed = int(time.time() - start_time)
            
            print(f"⏱️  [{elapsed:03d}s] Status: {status}")
            
            if status == 'succeeded':
                print(f"\n🎉 Sync completed successfully!")
                self.print_sync_summary(job_status)
                return True
                
            elif status == 'failed':
                print(f"\n❌ Sync failed!")
                print(f"   Error: {job_status.get('failureReason', 'Unknown error')}")
                return False
                
            elif status in ['cancelled', 'incomplete']:
                print(f"\n⚠️  Sync {status}")
                return False
                
            elif status in ['pending', 'running']:
                # Still in progress
                if elapsed >= max_wait_seconds:
                    print(f"\n⏰ Timeout reached ({max_wait_minutes} minutes)")
                    print(f"   Sync is still running. Check Airbyte Cloud UI for updates.")
                    return False
                    
                time.sleep(10)  # Wait 10 seconds before checking again
                
            else:
                print(f"   Unknown status: {status}")
                time.sleep(5)
    
    def print_sync_summary(self, job_status: Dict[str, Any]) -> None:
        """Print a summary of the sync results"""
        print("\n📈 Sync Summary:")
        print("-" * 30)
        
        # Basic job info
        print(f"   Job ID: {job_status.get('jobId')}")
        print(f"   Status: {job_status.get('status')}")
        print(f"   Started: {job_status.get('startTime', 'N/A')}")
        print(f"   Ended: {job_status.get('endTime', 'N/A')}")
        
        # Stream stats if available
        streams = job_status.get('streams', [])
        if streams:
            print(f"\n📊 Stream Statistics:")
            for stream in streams:
                stream_name = stream.get('streamName', 'Unknown')
                records_synced = stream.get('recordsEmitted', 0)
                bytes_synced = stream.get('bytesEmitted', 0)
                print(f"   • {stream_name}: {records_synced:,} records, {bytes_synced:,} bytes")
        
        print(f"\n🎯 Next Steps:")
        print(f"   • Data is now in Redshift schema: telemetry_raw")
        print(f"   • Run dbt transformations: cd dbt/telemetry_analytics && dbt run")
        print(f"   • View data: SELECT COUNT(*) FROM telemetry_raw.telemetry_data;")
    
    def test_connection(self) -> bool:
        """Test the API connection and permissions"""
        url = f"{self.config['base_url']}/workspaces/{self.config['workspace_id']}"
        
        try:
            print("🔍 Testing Airbyte Cloud API connection...")
            response = self.session.get(url)
            response.raise_for_status()
            
            workspace_data = response.json()
            print(f"✅ Connected successfully!")
            print(f"   Workspace: {workspace_data.get('name', 'N/A')}")
            print(f"   Workspace ID: {self.config['workspace_id']}")
            
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Connection test failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"   Response: {e.response.text}")
            return False


def main():
    """Main function to trigger and monitor Airbyte sync"""
    print("🚀 Telemetry Pipeline - Airbyte Cloud Trigger")
    print("=" * 50)
    
    # Initialize trigger
    trigger = AirbyteCloudTrigger()
    
    # Test connection first
    if not trigger.test_connection():
        print("\n💡 Setup Instructions:")
        print("   1. Get API key from: https://cloud.airbyte.com/settings/account")
        print("   2. Update config/airbyte_config.json with your credentials")
        print("   3. Create source/destination/connection in Airbyte Cloud")
        sys.exit(1)
    
    # Trigger the sync
    job_id = trigger.trigger_sync()
    
    if job_id:
        # Monitor progress
        success = trigger.monitor_sync(job_id, max_wait_minutes=10)
        
        if success:
            print(f"\n🎉 Pipeline step 2/4 complete!")
            print(f"   ✅ Raw CSV → Airbyte → Redshift ✅")
            print(f"   🔄 Next: Run dbt transformations")
            sys.exit(0)
        else:
            print(f"\n❌ Sync did not complete successfully")
            sys.exit(1)
    else:
        print(f"\n❌ Failed to trigger sync")
        sys.exit(1)


if __name__ == "__main__":
    main() 