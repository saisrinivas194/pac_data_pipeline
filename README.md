# Data Pipeline for Firebase

Tools to move data from different sources to Firebase.

## Quick Start

First time setup:
```bash
pip install -r requirements.txt
cp ENVIRONMENT_TEMPLATE.txt .env
# Edit .env with your credentials
```

## Running the Scripts

**For CSV data:**
```bash
python pac_data_processor.py
```

**For Snowflake data:**
```bash
python pac_snowflake_pipeline.py
```

**For Index Align issues:**
```bash
python index_align_to_firebase.py
```

**Test connections first:**
```bash
python test_index_align.py
```

## What it does

- Pulls data from CSVs, Snowflake, or Index Align database
- Connects through SSH to get Index Align data
- Cleans up the data
- Sends everything to Firebase
- Handles duplicates automatically

## Scripts

- `index_align_to_firebase.py` - Main script for Index Align issues
- `test_index_align.py` - Test your connections before running
- `pac_data_processor.py` - CSV to Firebase
- `pac_snowflake_pipeline.py` - Snowflake to Firebase

## What you need

- Python 3.8 or newer
- Firebase project set up
- SSH access to Index Align server
- Database credentials
- See SETUP_GUIDE_INDEX_ALIGN.md for detailed setup