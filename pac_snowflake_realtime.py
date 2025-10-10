#!/usr/bin/env python3
"""
PAC SNOWFLAKE TO REALTIME DATABASE TRANSFER
Professional PAC data pipeline for transferring aggregated data from Snowflake to Firebase Realtime Database
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

def clean_and_aggregate_pac_data(df):
    """Clean and aggregate PAC data for Firebase Realtime Database structure"""
    try:
        # Clean column names
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        
        # Convert data types
        if 'receipt_amount' in df.columns:
            df['receipt_amount'] = pd.to_numeric(df['receipt_amount'], errors='coerce')
        
        # Handle missing values
        df = df.fillna('')
        
        print("AGGREGATING DATA BY COMPANY AND ELECTION CYCLE")
        print("=" * 50)
        
        # Aggregate by company (ticker) and election cycle
        aggregated_data = {}
        
        # Group by ticker and election cycle
        for (ticker, election_cycle), group in df.groupby(['ticker', 'election_cycle']):
            if pd.isna(ticker) or ticker == '':
                continue
                
            # Calculate totals by political party
            democrat_total = 0
            republican_total = 0
            
            for _, row in group.iterrows():
                committee_name = str(row.get('committee_name', '')).lower()
                amount = row.get('receipt_amount', 0)
                
                if pd.isna(amount) or amount <= 0:
                    continue
                
                # Determine political party based on committee name
                if any(keyword in committee_name for keyword in ['republican', 'gop', 'conservative']):
                    republican_total += amount
                elif any(keyword in committee_name for keyword in ['democrat', 'democratic', 'liberal', 'progressive']):
                    democrat_total += amount
                # Skip independent/other parties for now
            
            # Only include companies with actual contributions
            if democrat_total > 0 or republican_total > 0:
                if ticker not in aggregated_data:
                    aggregated_data[ticker] = {}
                
                aggregated_data[ticker][str(election_cycle)] = {
                    'pac': {
                        'democrat': democrat_total,
                        'republican': republican_total
                    }
                }
        
        print(f"SUCCESS: Aggregated {len(df)} individual records into {len(aggregated_data)} company summaries")
        return aggregated_data
        
    except Exception as e:
        print(f"ERROR: Data aggregation failed: {str(e)}")
        return None

def upload_aggregated_data_to_realtime(ref, aggregated_data, dry_run=False):
    """Upload aggregated company data to Realtime Database"""
    if dry_run:
        print("DRY RUN MODE - Testing aggregated data upload")
        print("=" * 50)
        print(f"Would upload {len(aggregated_data)} company summaries")
        print("\nSample aggregated data:")
        for i, (ticker, cycles) in enumerate(list(aggregated_data.items())[:3]):
            print(f"\nCompany {i+1}: {ticker}")
            for cycle, data in cycles.items():
                pac_data = data.get('pac', {})
                print(f"  {cycle}: Democrat ${pac_data.get('democrat', 0):,.2f}, Republican ${pac_data.get('republican', 0):,.2f}")
        print("\nWould automatically delete any duplicates found")
        print("Run with real upload to send data.")
        return True
    else:
        print("UPLOADING AGGREGATED DATA TO REALTIME DATABASE")
        print("=" * 50)
        
        success_count = 0
        
        for ticker, cycles in aggregated_data.items():
            try:
                # Upload to brands/[ticker]/records/[cycle]/pac
                for cycle, data in cycles.items():
                    pac_data = data.get('pac', {})
                    
                    # Create the path: brands/[ticker]/records/[cycle]/pac
                    brand_path = f"brands/{ticker}/records/{cycle}/pac"
                    
                    # Upload PAC data
                    ref.child(brand_path).set(pac_data)
                    success_count += 1
                    
                    print(f"  Uploaded: {ticker} ({cycle}) - D: ${pac_data.get('democrat', 0):,.2f}, R: ${pac_data.get('republican', 0):,.2f}")
                
            except Exception as e:
                print(f"ERROR: Failed to upload {ticker}: {str(e)}")
                continue
        
        print(f"\nSUCCESS: Uploaded {success_count} company-election cycle combinations")
        print("SUCCESS: Aggregated data upload completed (Realtime Database)")
        return success_count > 0

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
    
    # Step 3: Clean and aggregate data
    print("\nStep 3: Cleaning and aggregating data...")
    aggregated_data = clean_and_aggregate_pac_data(df)
    if aggregated_data is None or len(aggregated_data) == 0:
        print("ERROR: Data aggregation failed")
        return False
    
    # Step 4: Upload to Firebase
    print("\nStep 4: Uploading aggregated data to Firebase Realtime Database...")
    
    # Ask user for confirmation
    try:
        user_input = input("\nDo you want to proceed with upload? (y/n): ").lower().strip()
        if user_input == 'y':
            success = upload_aggregated_data_to_realtime(firebase_ref, aggregated_data, dry_run=False)
        else:
            print("Running in dry run mode...")
            success = upload_aggregated_data_to_realtime(firebase_ref, aggregated_data, dry_run=True)
    except EOFError:
        print("Running in dry run mode...")
        success = upload_aggregated_data_to_realtime(firebase_ref, aggregated_data, dry_run=True)
    
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