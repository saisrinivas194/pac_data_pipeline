import pandas as pd
import snowflake.connector
import firebase_admin
from firebase_admin import credentials, firestore
import os
from dotenv import load_dotenv
from datetime import datetime
import uuid
import math
import json

# Load environment variables
load_dotenv()

def connect_to_snowflake():
    """Connect to Snowflake database"""
    print("OPENING SNOWFLAKE CONNECTION")
    print("=" * 50)
    
    try:
        # Connect to Snowflake using login info
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        
        print("SUCCESS: Connected to Snowflake")
        return conn
        
    except Exception as e:
        print(f"ERROR: Could not connect to Snowflake: {str(e)}")
        return None

def run_snowflake_query(conn, query):
    """Run a query on Snowflake and get results"""
    print("GETTING DATA FROM SNOWFLAKE")
    print("=" * 50)
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Get all results
        results = cursor.fetchall()
        
        # Convert to simple data table
        df = pd.DataFrame(results, columns=columns)
        
        print(f"SUCCESS: Got {len(df)} records from Snowflake")
        print(f"Columns: {list(df.columns)}")


        # Show sample data
        print("Sample data:")
        print(df.head(3))
        
        cursor.close()
        return df
        
    except Exception as e:
        print(f"ERROR: Query failed: {str(e)}")
        return None

def clean_snowflake_data(df):
    """Clean and fix Snowflake data"""
    print("FIXING DATA FROM SNOWFLAKE")
    print("=" * 50)
    
    try:
        # Count records before cleaning
        original_count = len(df)
        
        # Fix column names (make them lowercase)
        df.columns = df.columns.str.lower()
        
        # Fix text fields - remove extra spaces
        text_columns = [col for col in df.columns if df[col].dtype == 'object']
        for col in text_columns:
            df[col] = df[col].astype(str).str.strip()
        
        # Fix number fields - turn into proper numbers
        numeric_columns = []
        money_columns = []
        
        for col in df.columns:
            if 'amount' in df.columns or 'value' in col.lower() or 'price' in col.lower():
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    money_columns.append(col)
                except:
                    pass
            
            elif col.lower() in ['id', 'count', 'number', 'quantity']:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    numeric_columns.append(col)
                except:
                    pass
        
        # Fix date fields
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        for col in date_columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                pass
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Add processing metadata
        df['processed_date'] = datetime.now()
        df['data_source'] = 'snowflake'
        df['firebase_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        print(f"SUCCESS: Fixed {len(df)} records (removed {original_count - len(df)} empty rows)")
        if money_columns:
            print(f"Money fields fixed: {money_columns}")
        if numeric_columns:
            print(f"Number fields fixed: {numeric_columns}")
        if date_columns:
            print(f"Date fields fixed: {date_columns}")
        
        return df
        
    except Exception as e:
        print(f"ERROR: Data fixing failed: {str(e)}")
        return None

def setup_firebase():
    """Connect to Firebase"""
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
                "token_uri": "https://oauth2.googleapis.com/token"
            }
            
            cred = credentials.Certificate(cred_info)
            firebase_admin.initialize_app(cred)
        
        db = firestore.client()
        print("SUCCESS: Firebase connection ready")
        return db
        
    except Exception as e:
        print(f"ERROR: Firebase connection failed: {str(e)}")
        return None

def prepare_for_firebase_batches(df, collection_name, batch_size=1000):
    """Get data ready for Firebase upload in batches"""
    print("GETTING DATA READY FOR FIREBASE BATCHES")
    print("=" * 50)
    
    try:
        all_batches = []
        total_records = len(df)
        
        # Split into batches
        for start_idx in range(0, total_records, batch_size):
            end_idx = min(start_idx + batch_size, total_records)
            batch_df = df.iloc[start_idx:end_idx]
            
            firebase_records = []
            for index, row in batch_df.iterrows():
                record = row.to_dict()
                
                # Convert datetime to string for Firebase
                for key, value in record.items():
                    if key == 'processed_date' and value:
                        record[key] = value.isoformat()
                    elif 'date' in key.lower() and hasattr(value, 'isoformat'):
                        try:
                            record[key] = value.isoformat()
                        except:
                            record[key] = str(value)
                
                firebase_records.append({
                    'collection': collection_name,
                    'document_id': record['firebase_id'],
                    'data': record
                })
            
            all_batches.append(firebase_records)
        
        total_batches = len(all_batches)
        print(f"SUCCESS: Split {total_records} records into {total_batches} batches")
        print(f"Batch size: {batch_size} records per batch")
        
        return all_batches
        
    except Exception as e:
        print(f"ERROR: Getting batches ready failed: {str(e)}")
        return None

def check_and_delete_duplicates(db, collection_name):
    """Check for duplicates and delete them automatically"""
    print(f"\nCHECKING FOR DUPLICATES IN COLLECTION: {collection_name}")
    print("=" * 60)
    
    try:
        duplicates_found = 0
        
        # Get existing records from Firebase
        existing_docs = db.collection(collection_name).stream()
        existing_data = {}
        
        for doc in existing_docs:
            doc_data = doc.to_dict()
            # Create a unique key based on important fields
            key_fields = []
            
            # Build key from available fields
            for field in ['ticker', 'election_cycle', 'committee_name', 'receipt_amount', 'entity_type_name']:
                if field in doc_data and doc_data[field] is not None:
                    key_fields.append(str(doc_data[field]))
            
            key = "_".join(key_fields)
            
            if key in existing_data:
                # Duplicate found - delete it
                print(f"  Found duplicate: {doc.id} (Key: {key})")
                db.collection(collection_name).document(doc.id).delete()
                duplicates_found += 1
            else:
                existing_data[key] = doc.id
        
        if duplicates_found > 0:
            print(f"SUCCESS: Deleted {duplicates_found} duplicate records from {collection_name}")
        else:
            print(f"SUCCESS: No duplicates found in {collection_name}")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Duplicate check failed for {collection_name}: {str(e)}")
        return False

def compare_upload_vs_existing(db, all_batches):
    """Compare what we're uploading vs what's already in Firebase"""
    print("\nCOMPARING UPLOAD DATA VS EXISTING DATA")
    print("=" * 60)
    
    try:
        collections_to_check = set()
        upload_counts = {}
        
        # Count what we're about to upload
        for batch in all_batches:
            for record in batch:
                collection_name = record['collection']
                collections_to_check.add(collection_name)
                upload_counts[collection_name] = upload_counts.get(collection_name, 0) + 1
        
        # Count what's already in Firebase
        existing_counts = {}
        for collection_name in collections_to_check:
            existing_docs = db.collection(collection_name).stream()
            existing_count = sum(1 for _ in existing_docs)
            existing_counts[collection_name] = existing_count
        
        # Show comparison
        print("UPLOAD COMPARISON:")
        for collection_name in collections_to_check:
            upload_count = upload_counts.get(collection_name, 0)
            existing_count = existing_counts.get(collection_name, 0)
            print(f"  {collection_name}:")
            print(f"    Uploading: {upload_count} records")
            print(f"    Existing: {existing_count} records")
            print(f"    Total after upload: {upload_count + existing_count} records")
            print()
        
        return True
        
    except Exception as e:
        print(f"ERROR: Upload comparison failed: {str(e)}")
        return False

def upload_batch_to_firebase(db, batch_records, batch_number, total_batches):
    """Upload one batch to Firebase"""
    print(f"Uploading Batch {batch_number}/{total_batches}")
    
    try:
        # Use batch write for better performance
        batch = db.batch()
        
        for record in batch_records:
            doc_ref = db.collection(record['collection']).document(record['document_id'])
            batch.set(doc_ref, record['data'])
        
        # Commit the batch
        batch.commit()
        
        print(f"   SUCCESS: Batch {batch_number} uploaded ({len(batch_records)} records)")
        return len(batch_records)
        
    except Exception as e:
        print(f"   ERROR: Batch {batch_number} failed: {str(e)}")
        return 0

def upload_all_batches(db, all_batches, dry_run=False):
    """Upload all batches to Firebase"""
    if dry_run:
        print("DRY RUN MODE - Testing upload")
        print("=" * 50)
        total_records = sum(len(batch) for batch in all_batches)
        print(f"Would upload {total_records} records in {len(all_batches)} batches to Firebase")
        print("Sample from first batch:")
        for i, record in enumerate(all_batches[0][:3]):
            print(f"  {i+1}. Collection: {record['collection']}")
            print(f"     Document: {record['document_id']}")
            sample_data = {k: v for k, v in list(record['data'].items())[:5]}
            print(f"     Data: {sample_data}")
        print("\nWould automatically delete any duplicates found")
        print("Run with real upload to send data.")
        return True
    else:
        print("UPLOADING TO FIREBASE (DATASTORE MODE)")
        print("=" * 50)
        
        # Step 1: Compare upload vs existing data
        if not compare_upload_vs_existing(db, all_batches):
            print("WARNING: Upload comparison failed, continuing with upload...")
        
        # Step 2: Check and delete duplicates from all collections
        collections_to_check = set()
        for batch in all_batches:
            for record in batch:
                collections_to_check.add(record['collection'])
        
        for collection_name in collections_to_check:
            if not check_and_delete_duplicates(db, collection_name):
                print(f"WARNING: Duplicate check failed for {collection_name}, continuing...")
        
        # Step 3: Upload all batches
        print("\nStep 3: Uploading new data...")
        success_count = 0
        total_batches = len(all_batches)
        
        for batch_number, batch_records in enumerate(all_batches, 1):
            batch_success = upload_batch_to_firebase(db, batch_records, batch_number, total_batches)
            success_count += batch_success
            
            # Small delay between batches to be nice to Firebase
            if batch_number < total_batches:
                import time
                time.sleep(0.1)
        
        total_records = sum(len(batch) for batch in all_batches)
        print(f"\nSUCCESS: Uploaded {success_count}/{total_records} records across {total_batches} batches")
        print("SUCCESS: Duplicate check and upload completed (Datastore Mode)")
        return success_count >= total_records * 0.8

def main():
    """Main program to transfer PAC data from Snowflake to Firebase"""
    print("PAC SNOWFLAKE TO FIREBASE DATA TRANSFER PROGRAM")
    print("=" * 60)
    print("Reading PAC data from Snowflake and uploading to Firebase...")
    print()
    
    try:
        # Connect to Snowflake
        snowflake_conn = connect_to_snowflake()
        if not snowflake_conn:
            print("Cannot proceed without Snowflake connection")
            return
        
        # Ask user for batch size
        batch_size_input = input("Enter batch size (default 1000): ").strip()
        if batch_size_input:
            batch_size = int(batch_size_input)
        else:
            batch_size = 1000
        
        print(f"Using batch size: {batch_size}")
        
        # Sample query - user needs to replace this
        sample_query = """
        SELECT ticker, election_cycle, committee_name, receipt_amount, 
               committee_type, party_name
        FROM pac_contributions_table
        ORDER BY ticker
        """
        
        print("\nUsing sample query - replace with your actual Snowflake table:")
        print(sample_query)
        
        user_input = input("\nReplace sample query? (y/n): ").lower().strip()
        if user_input == 'y':
            actual_query = input("Enter your Snowflake query: ")
        else:
            actual_query = sample_query
        
        # Run Snowflake query
        df = run_snowflake_query(snowflake_conn, actual_query)
        if df is None:
            print("Cannot proceed without data from Snowflake")
            snowflake_conn.close()
            return
        
        # Clean the data
        cleaned_df = clean_snowflake_data(df)
        if cleaned_df is None:
            print("Cannot proceed without cleaned data")
            snowflake_conn.close()
            return
        
        # Setup Firebase
        firebase_db = setup_firebase()
        if not firebase_db:
            print("Cannot proceed without Firebase connection")
            snowflake_conn.close()
            return
        
        # Prepare batches for Firebase
        collection_name = input("Enter Firebase collection name (e.g., 'snowflake_batch_data'): ").strip()
        if not collection_name:
            collection_name = 'snowflake_batch_data'
        
        all_batches = prepare_for_firebase_batches(cleaned_df, collection_name, batch_size)
        if not all_batches:
            print("Cannot prepare batches for Firebase")
            snowflake_conn.close()
            return
        
        # Dry run first
        upload_all_batches(firebase_db, all_batches, dry_run=True)
        
        # Ask for real upload
        upload_real = input("\nUpload all batches to Firebase for real? (y/n): ").lower().strip()
        if upload_real == 'y':
            success = upload_all_batches(firebase_db, all_batches, dry_run=False)
            if success:
                print("SUCCESS: Real data uploaded to Firebase in batches!")
            else:
                print("WARNING: Some batches failed")
        else:
            print("Skipping real upload")
        
        # Close Snowflake connection
        snowflake_conn.close()
        
        print("\nPROGRAM RUN COMPLETE!")
        
    except Exception as e:
        print(f"ERROR: Program run failed: {str(e)}")
        print("Check your Snowflake and Firebase settings")

if __name__ == "__main__":
    main()
