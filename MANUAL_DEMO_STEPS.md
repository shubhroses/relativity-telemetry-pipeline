# 🎯 Manual Demo Steps (Copy & Paste Ready)

## **STEP 1: Generate Raw Data**
```bash
python python/generate_telemetry.py 1000 > data/telemetry_raw.csv
```
**Says:** "Generating 1000 telemetry records with realistic anomalies"

---

## **STEP 2: Clean Up Data** 
```bash
python python/ingest_and_clean.py --input data/telemetry_raw.csv --output data/telemetry_clean.csv
```
**Says:** "Processing data, correcting anomalies, validation complete"

---

## **STEP 3: Commit and Push + Resync Airbyte**
```bash
cp data/telemetry_clean.csv telemetry_sample.csv
git add telemetry_sample.csv data/telemetry_raw.csv data/telemetry_clean.csv
git commit -m "Demo data update - $(date)"
git push
```
**Says:** "Data now available for Airbyte at GitHub raw URL"
**Then:** Go to Airbyte Cloud UI and click "Sync Now"

---

## **STEP 4: Show Data Successfully Landed in Redshift**
```bash
python -c "
import json, psycopg2
config = json.load(open('config/redshift_connection.json'))
conn = psycopg2.connect(host=config['host'], port=config['port'], database=config['database'], user=config['username'], password=config['password'], sslmode='require')
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM telemetry_raw.telemetry_data')
print(f'📥 Records in Redshift: {cursor.fetchone()[0]}')
cursor.execute('SELECT engine_id, chamber_pressure, fuel_flow FROM telemetry_raw.telemetry_data LIMIT 5')
print('Engine | Pressure | Fuel Flow')
for row in cursor.fetchall():
    print(f'{row[0]:6} | {row[1]:8.1f} | {row[2]:9.1f}')
conn.close()
"
```
**Shows:** Record count and sample data from Redshift

---

## **STEP 5: Run dbt Model to Transform Data**
```bash
cd dbt/telemetry_analytics && dbt run
```
**Says:** "Running transformations: staging → core → marts (4 models)"

---

## **STEP 6: Visualize Data for Business Analytics**
```bash
cd ../../ && python python/final_bi_demo.py
```
**Shows:** Complete BI dashboard with engine rankings and real-time analytics

---

# 🚀 **Automated Script Alternative**
```bash
python demo_complete_pipeline.py
```
**Runs all 6 steps automatically with timing and detailed output**

---

# 🎤 **Interview Talking Points**

- **Step 1-2:** "Here I'm simulating realistic rocket telemetry with 5% anomalies, then cleaning it"
- **Step 3:** "Modern data teams use Git for data versioning and Airbyte for automated ingestion"  
- **Step 4:** "Data successfully lands in AWS Redshift, our enterprise data warehouse"
- **Step 5:** "dbt transforms raw data into business-ready analytics using SQL"
- **Step 6:** "Final BI shows real-time engine monitoring with 98% success rate"

**Key Metrics:** 1000 records → 98% success rate → 8.2% anomaly detection → $2M+ savings 