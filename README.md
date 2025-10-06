# PAC Data Processing Pipeline

### Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Setup environment
cp ENVIRONMENT_TEMPLATE.txt .env
# Add your credentials to .env file

# Test connections
python test_snowflake_pipeline.py
```

### Usage

**For CSV Data:**
```bash
python pac_data_processor.py
```

**For PAC Snowflake Data:**
```bash
python pac_snowflake_pipeline.py
```

## Features

- Read PAC data from CSV files or Snowflake
- Clean and standardize data automatically  
- Generate comprehensive analysis reports
- Upload data to Firebase Firestore
- Batch processing for large datasets
- Duplicate checking and validation

## File Structure

- `pac_data_processor.py` - CSV to Firebase pipeline
- `snowflake_to_firebase.py` - Simple Snowflake upload
- `snowflake_batch_upload.py` - Large data batch processing
- `check_duplicates.py` - Duplicate detection
- `test_snowflake_pipeline.py` - Connection testing

## Requirements

- Python 3.8+
- CSV files OR Snowflake account
- Firebase Firestore project
- Service account credentials