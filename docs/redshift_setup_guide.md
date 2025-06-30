# AWS Redshift Serverless Setup Guide

## Prerequisites
- AWS Account (create at https://aws.amazon.com if needed)
- Credit card for billing setup

## Step 1: Access AWS Redshift Console

1. Log into AWS Console: https://console.aws.amazon.com
2. Search for "Redshift" in the services search bar
3. Click on "Amazon Redshift"

## Step 2: Create Redshift Serverless Workgroup

1. In the Redshift console, click **"Serverless dashboard"** on the left sidebar
2. Click **"Create workgroup"**
3. Configure your workgroup:
   ```
   Workgroup name: telemetry-demo
   Base RPU capacity: 8 (minimum, good for demo)
   ```
4. Click **"Create workgroup"**

## Step 3: Create Namespace

1. Click **"Create namespace"**
2. Configure namespace:
   ```
   Namespace name: telemetry-namespace
   Database name: dev
   Admin username: admin
   Admin password: [Create a secure password - save this!]
   ```
3. Click **"Create namespace"**

## Step 4: Configure Network & Security

1. **VPC Settings**: Use default VPC for demo
2. **Security Groups**: 
   - Create or use existing security group
   - Ensure inbound rule allows connections on port 5439 from your IP
3. **Publicly accessible**: Enable for demo (NOT recommended for production)

## Step 5: Get Connection Details

Once created, note these connection parameters:
```
Host: [workgroup-name].[random-id].[region].redshift-serverless.amazonaws.com
Port: 5439
Database: dev
Username: admin
Password: [your-password]
```

## Step 6: Test Connection

Use any PostgreSQL client or the Redshift Query Editor to test:
```sql
SELECT current_user, current_database();
```

## Step 7: Create Schemas

Run these commands in Redshift Query Editor:
```sql
-- Create raw data schema
CREATE SCHEMA telemetry_raw;

-- Create clean data schema  
CREATE SCHEMA telemetry_clean;

-- Verify schemas
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name IN ('telemetry_raw', 'telemetry_clean');
```

## Cost Management Tips

- **Monitor usage**: Check Redshift Serverless dashboard regularly
- **Pause when not using**: Serverless automatically pauses, but monitor active time
- **Set billing alerts**: Create CloudWatch billing alarms
- **Expected cost**: ~$0.45/hour when active (8 RPU × $0.056/RPU/hour)

## Security Notes

⚠️ **For Demo Only**: 
- Public accessibility is enabled for easy testing
- Use strong passwords
- Delete resources when demo is complete

## Next Steps

After setup:
1. Save connection parameters to `config/redshift_connection.json`
2. Test connection from local machine
3. Configure Airbyte connector
4. Set up dbt profiles 