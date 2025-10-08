#!/usr/bin/env python3
"""
PAC SNOWFLAKE TO REALTIME DATABASE TRANSFER
Professional PAC data pipeline for transferring data from Snowflake to Firebase Realtime Database
"""

import os
import pandas as pd
import snowflake.connector
from firebase_admin import credentials, initialize_app, db
import firebase_admin
from dotenv import load_dotenv
import json
from datetime import datetime

# Load environment variables
load_dotenv()

def setup_firebase_realtime():
    """Connect to Firebase Realtime Database"""
    try:
        if not firebase_admin._apps:
            cred_info = {
                "type": "service_account",
                "project_id": os.getenv('FIREBASE_PROJECT_ID'),
                "private_key_id": os.getenv('FIREBASE_PRIVATE_KEY_ID'),
                "private_key": os.getenv('FIREBASE_PRIVATE_KEY').replace('\\n', '\n'),
                "client_email": os.getenv('FIREBASE_CLIENT_EMAIL'),
                "client_id": os.getenv('FIREBASE_CLIENT_ID'),
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://accounts.google.com/o/oauth2/token"
            }
            cred = credentials.Certificate(cred_info)
            initialize_app(cred, {
                'databaseURL': f"https://{os.getenv('FIREBASE_PROJECT_ID')}-default-rtdb.firebaseio.com/"
            })
        
        # Get reference to Realtime Database
        ref = db.reference()
        print("SUCCESS: Realtime Database connection ready")
        return ref
        
    except Exception as e:
        print(f"ERROR: Firebase Realtime Database connection failed: {str(e)}")
        return None

def connect_to_snowflake():
    """Connect to Snowflake database"""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        print("SUCCESS: Snowflake connection established")
        return conn
    except Exception as e:
        print(f"ERROR: Snowflake connection failed: {str(e)}")
        return None

def get_pac_data_from_snowflake(conn, query):
    """Retrieve PAC data from Snowflake"""
    try:
        df = pd.read_sql(query, conn)
        print(f"SUCCESS: Retrieved {len(df)} records from Snowflake")
        return df
    except Exception as e:
        print(f"ERROR: Data retrieval failed: {str(e)}")
        return None

def clean_pac_data(df):
    """Clean and format PAC data for Firebase"""
    try:
        # Clean column names
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        
        # Convert data types
        if 'receipt_amount' in df.columns:
            df['receipt_amount'] = pd.to_numeric(df['receipt_amount'], errors='coerce')
        
        # Handle missing values
        df = df.fillna('')
        
        # Add metadata
        df['upload_timestamp'] = datetime.now().isoformat()
        df['data_source'] = 'snowflake'
        
        print("SUCCESS: Data cleaned and formatted")
        return df
    except Exception as e:
        print(f"ERROR: Data cleaning failed: {str(e)}")
        return None

def check_and_delete_duplicates_realtime(ref, data_key):
    """Check for duplicates in Realtime Database"""
    try:
        print("CHECKING FOR DUPLICATES")
        print("=" * 50)
        
        # Get existing data
        existing_data = ref.child(data_key).get()
        
        if not existing_data:
            print("SUCCESS: No existing data found")
            return True
        
        duplicates_found = 0
        
        # Check for duplicates based on key fields
        for record_id, record_data in existing_data.items():
            if isinstance(record_data, dict):
                # Create unique key for comparison
                key_fields = ['ticker', 'election_cycle', 'committee_name', 'receipt_amount']
                unique_key = '_'.join([str(record_data.get(field, '')) for field in key_fields])
                
                # Check if this key appears multiple times
                count = sum(1 for rid, rdata in existing_data.items() 
                           if isinstance(rdata, dict) and 
                           '_'.join([str(rdata.get(field, '')) for field in key_fields]) == unique_key)
                
                if count > 1:
                    print(f"Found duplicate: {record_id}")
                    ref.child(data_key).child(record_id).delete()
                    duplicates_found += 1
        
        if duplicates_found > 0:
            print(f"SUCCESS: Deleted {duplicates_found} duplicate records")
        else:
            print("SUCCESS: No duplicates found")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Duplicate check failed: {str(e)}")
        return False

def compare_upload_vs_existing_realtime(ref, data_key, new_data):
    """Compare new data with existing data"""
    try:
        print("COMPARING UPLOAD VS EXISTING DATA")
        print("=" * 50)
        
        existing_data = ref.child(data_key).get()
        
        if not existing_data:
            print("No existing data found")
            print(f"Will upload {len(new_data)} new records")
            return True
        
        existing_count = len(existing_data) if existing_data else 0
        new_count = len(new_data)
        
        print(f"Existing records: {existing_count}")
        print(f"New records to upload: {new_count}")
        print(f"Total records after upload: {existing_count + new_count}")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Data comparison failed: {str(e)}")
        return False

def upload_batch_to_realtime(ref, batch_data, batch_number, total_batches):
    """Upload a batch of data to Realtime Database"""
    try:
        print(f"Uploading batch {batch_number}/{total_batches} ({len(batch_data)} records)")
        
        success_count = 0
        
        for index, record in batch_data.iterrows():
            try:
                # Convert record to dictionary
                record_dict = record.to_dict()
                
                # Generate unique key
                record_key = f"pac_{index}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                # Upload to Realtime Database
                ref.child('pac_contributions').child(record_key).set(record_dict)
                success_count += 1
                
            except Exception as e:
                print(f"ERROR: Failed to upload record {index}: {str(e)}")
                continue
        
        print(f"SUCCESS: Uploaded {success_count}/{len(batch_data)} records from batch {batch_number}")
        return success_count
        
    except Exception as e:
        print(f"ERROR: Batch upload failed: {str(e)}")
        return 0

def upload_all_batches_realtime(ref, df, batch_size=1000, dry_run=False):
    """Upload all data in batches to Realtime Database"""
    if dry_run:
        print("DRY RUN MODE - Testing upload")
        print("=" * 50)
        print(f"Would upload {len(df)} records in batches of {batch_size}")
        print("Sample data:")
        print(df.head(3).to_string())
        print("\nWould automatically delete any duplicates found")
        print("Run with real upload to send data.")
        return True
    else:
        print("UPLOADING TO REALTIME DATABASE")
        print("=" * 50)
        
        # Step 1: Compare upload vs existing data
        if not compare_upload_vs_existing_realtime(ref, 'pac_contributions', df):
            print("WARNING: Upload comparison failed, continuing with upload...")
        
        # Step 2: Check and delete duplicates
        if not check_and_delete_duplicates_realtime(ref, 'pac_contributions'):
            print("WARNING: Duplicate check failed, continuing...")
        
        # Step 3: Upload in batches
        print("\nStep 3: Uploading new data...")
        total_records = len(df)
        total_batches = (total_records + batch_size - 1) // batch_size
        
        success_count = 0
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, total_records)
            batch_data = df.iloc[start_idx:end_idx]
            
            batch_success = upload_batch_to_realtime(ref, batch_data, batch_num + 1, total_batches)
            success_count += batch_success
            
            # Small delay between batches
            if batch_num < total_batches - 1:
                import time
                time.sleep(0.1)
        
        print(f"\nSUCCESS: Uploaded {success_count}/{total_records} records across {total_batches} batches")
        print("SUCCESS: Duplicate check and upload completed (Realtime Database)")
        return success_count >= total_records * 0.8

def main():
    """Main function to run the PAC data pipeline"""
    print("PAC SNOWFLAKE TO REALTIME DATABASE TRANSFER")
    print("=" * 60)
    
    # Step 1: Setup connections
    print("\nStep 1: Setting up connections...")
    snowflake_conn = connect_to_snowflake()
    if not snowflake_conn:
        return False
    
    firebase_ref = setup_firebase_realtime()
    if not firebase_ref:
        return False
    
    # Step 2: Get data from Snowflake
    print("\nStep 2: Retrieving data from Snowflake...")
    query = """
    SELECT 
        ticker,
        election_cycle,
        committee_name,
        receipt_amount,
        entity_type_name,
        contribution_date
    FROM pac_contributions
    WHERE receipt_amount > 0
    ORDER BY receipt_amount DESC
    """
    
    df = get_pac_data_from_snowflake(snowflake_conn, query)
    if df is None or df.empty:
        print("ERROR: No data retrieved from Snowflake")
        return False
    
    # Step 3: Clean data
    print("\nStep 3: Cleaning and formatting data...")
    df_clean = clean_pac_data(df)
    if df_clean is None:
        print("ERROR: Data cleaning failed")
        return False
    
    # Step 4: Upload to Firebase
    print("\nStep 4: Uploading to Firebase Realtime Database...")
    
    # Ask user for confirmation
    try:
        user_input = input("\nDo you want to proceed with upload? (y/n): ").lower().strip()
        if user_input == 'y':
            success = upload_all_batches_realtime(firebase_ref, df_clean, dry_run=False)
        else:
            print("Running in dry run mode...")
            success = upload_all_batches_realtime(firebase_ref, df_clean, dry_run=True)
    except EOFError:
        print("Running in dry run mode...")
        success = upload_all_batches_realtime(firebase_ref, df_clean, dry_run=True)
    
    # Step 5: Close connections
    print("\nStep 5: Closing connections...")
    snowflake_conn.close()
    print("SUCCESS: Snowflake connection closed")
    
    if success:
        print("\nSUCCESS: PAC data pipeline completed successfully")
        return True
    else:
        print("\nERROR: PAC data pipeline failed")
        return False

if __name__ == "__main__":
    main()
