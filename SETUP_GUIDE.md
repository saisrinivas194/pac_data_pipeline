# Setup Guide

Complete setup instructions for PAC data processing pipeline.

## Prerequisites

- Python 3.8 or newer
- Firebase project with Firestore enabled
- (Optional for now) Snowflake account with data warehouse

## Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

This installs:
- pandas (data processing)
- snowflake-connector-python (database access)  
- firebase-admin (cloud storage)
- python-dotenv (configuration)

## Step 2: Setup Environment

```bash
# Copy template
cp ENVIRONMENT_TEMPLATE.txt .env

# Edit .env with your credentials
```

### Required for All Pipelines:
- Firebase project ID
- Firebase private key
- Firebase client email
- Firebase client ID

### Additional for Snowflake:
- Snowflake username and password
- Snowflake account identifier
- Warehouse, database, and schema names

## Step 3: Test Setup

```bash
# Test all connections
python test_snowflake_pipeline.py

# Test specific components
python test_snowflake_pipeline.py env
python test_snowflake_pipeline.py snowflake
python test_snowflake_pipeline.py firebase
```

## Step 4: Run Pipeline

### For CSV Data:
```bash
python pac_data_processor.py
```

### For PAC Snowflake Data:
```bash
python pac_snowflake_pipeline.py
```

## Getting Credentials

### Firebase Setup:
1. Go to Firebase Console
2. Select your project
3. Click Settings (gear icon)
4. Go to Service Accounts tab
5. Click "Generate new private key"
6. Copy values from JSON file to .env

### Snowflake Setup:
1. Log into Snowflake web interface
2. Go to Account > Account Profiles (upper right)
3. Note Account Locator (like "abc123.us-east-1")
4. Note username and password
5. Find warehouse name in Data > Warehouses
6. Find database and schema in Data > Databases

## Troubleshooting

### Environment Issues:
- Make sure .env file exists
- Verify no placeholder values 
- Check file has no extra spaces or quotes

### Firebase Issues:
- Ensure Firestore is enabled (not Datastore)
- Test: python test_firebase.py
- Check service account has write permissions

### Snowflake Issues:
- Verify warehouse is running
- Check user has access to database/schema
- Test: python test_snowflake_pipeline.py snowflake

### Data Issues:
- Check CSV file format
- Verify Snowflake query returns data
- Test: python test_snowflake_pipeline.py data
