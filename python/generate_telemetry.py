#!/usr/bin/env python3
"""
Rocket Engine Telemetry Data Generator

Generates synthetic telemetry data with realistic anomalies:
- Missing fields (simulating sensor failures)
- Duplicate records (simulating transmission errors)
- Out-of-range values (simulating sensor malfunctions)
"""

import json
import random
import time
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, Optional


class TelemetryGenerator:
    def __init__(self):
        self.engine_ids = ["ENG-001", "ENG-002", "ENG-003", "ENG-004", "ENG-005"]
        self.start_time = datetime.now()
        self.last_records = {}  # For introducing duplicates
        
    def generate_normal_reading(self, engine_id: str, timestamp: datetime) -> Dict[str, Any]:
        """Generate a normal telemetry reading"""
        return {
            "timestamp": timestamp.isoformat(),
            "engine_id": engine_id,
            "chamber_pressure": round(random.uniform(150.0, 300.0), 2),  # PSI
            "fuel_flow": round(random.uniform(50.0, 150.0), 2),          # kg/s
            "temperature": round(random.uniform(2000.0, 3500.0), 2)      # °F
        }
    
    def introduce_anomalies(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Introduce various anomalies to simulate real-world issues"""
        
        # 5% chance of missing required fields (sensor failure)
        if random.random() < 0.05:
            fields_to_remove = random.sample(
                ["chamber_pressure", "fuel_flow", "temperature"], 
                random.randint(1, 2)
            )
            for field in fields_to_remove:
                record.pop(field, None)
        
        # 3% chance of out-of-range values (sensor malfunction)
        if random.random() < 0.03:
            anomaly_type = random.choice(["negative_pressure", "extreme_temp", "zero_flow"])
            
            if anomaly_type == "negative_pressure" and "chamber_pressure" in record:
                record["chamber_pressure"] = random.uniform(-50.0, -1.0)
            elif anomaly_type == "extreme_temp" and "temperature" in record:
                record["temperature"] = random.choice([
                    random.uniform(-100.0, 0.0),    # Too cold
                    random.uniform(5000.0, 8000.0)  # Too hot
                ])
            elif anomaly_type == "zero_flow" and "fuel_flow" in record:
                record["fuel_flow"] = 0.0
        
        # 2% chance of duplicate record (transmission error)
        if random.random() < 0.02 and record["engine_id"] in self.last_records:
            return self.last_records[record["engine_id"]]
        
        # Store for potential duplication
        self.last_records[record["engine_id"]] = record.copy()
        
        return record
    
    def generate_batch(self, num_records: int = 100) -> None:
        """Generate a batch of telemetry records"""
        for i in range(num_records):
            # Simulate readings every 1-5 seconds
            time_offset = timedelta(seconds=i * random.uniform(1, 5))
            timestamp = self.start_time + time_offset
            
            # Select random engine
            engine_id = random.choice(self.engine_ids)
            
            # Generate normal reading
            record = self.generate_normal_reading(engine_id, timestamp)
            
            # Introduce anomalies
            anomalous_record = self.introduce_anomalies(record)
            
            if anomalous_record:
                print(json.dumps(anomalous_record))
                
            # Small delay to simulate real-time streaming
            time.sleep(0.01)


def main():
    """Main function to generate telemetry data"""
    generator = TelemetryGenerator()
    
    # Default to 100 records, but allow command line override
    num_records = 100
    if len(sys.argv) > 1:
        try:
            num_records = int(sys.argv[1])
        except ValueError:
            print("Error: Invalid number of records. Using default (100).", file=sys.stderr)
    
    print(f"# Generating {num_records} telemetry records...", file=sys.stderr)
    print(f"# Engine IDs: {generator.engine_ids}", file=sys.stderr)
    print(f"# Starting at: {generator.start_time.isoformat()}", file=sys.stderr)
    print("# Expected anomalies: ~5% missing fields, ~3% out-of-range values, ~2% duplicates", file=sys.stderr)
    print("", file=sys.stderr)
    
    generator.generate_batch(num_records)
    
    print(f"# Generated {num_records} records successfully!", file=sys.stderr)


if __name__ == "__main__":
    main() 