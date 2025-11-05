#!/usr/bin/env python3
"""
INDEX ALIGN TO FIREBASE ISSUES TRANSFER
Professional data pipeline for transferring issues data from Index Align database to Firebase
"""

import os
import pymysql
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from firebase_admin import credentials, initialize_app, db
import firebase_admin
from dotenv import load_dotenv
from datetime import datetime
import json
from typing import Dict, Any, Optional

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
        print("SUCCESS: Firebase Realtime Database connection ready")
        return ref
        
    except Exception as e:
        print(f"ERROR: Firebase Realtime Database connection failed: {str(e)}")
        return None

def connect_to_index_align_db():
    """Connect to Index Align database via SSH tunnel"""
    try:
        # SSH tunnel configuration
        ssh_host = os.getenv('INDEX_ALIGN_SSH_HOST')
        ssh_user = os.getenv('INDEX_ALIGN_SSH_USER')
        ssh_port = int(os.getenv('INDEX_ALIGN_SSH_PORT', '22'))
        
        db_host = os.getenv('INDEX_ALIGN_DB_HOST')
        db_port = int(os.getenv('INDEX_ALIGN_DB_PORT', '3306'))
        db_name = os.getenv('INDEX_ALIGN_DB_NAME')
        db_user = os.getenv('INDEX_ALIGN_DB_USER')
        db_password = os.getenv('INDEX_ALIGN_DB_PASSWORD')
        
        print(f"Setting up SSH tunnel to {ssh_host}...")
        
        # Create SSH tunnel
        ssh_key_path = os.getenv('INDEX_ALIGN_SSH_KEY_PATH')
        
        if ssh_key_path and os.path.exists(ssh_key_path):
            ssh_key = open(ssh_key_path).read()
            tunnel = SSHTunnelForwarder(
                (ssh_host, ssh_port),
                ssh_username=ssh_user,
                ssh_pkey=ssh_key,
                remote_bind_address=(db_host, db_port),
                local_bind_address=('127.0.0.1', 0)
            )
        else:
            # Use password authentication
            tunnel = SSHTunnelForwarder(
                (ssh_host, ssh_port),
                ssh_username=ssh_user,
                ssh_password=os.getenv('INDEX_ALIGN_SSH_PASSWORD'),
                remote_bind_address=(db_host, db_port),
                local_bind_address=('127.0.0.1', 0)
            )
        
        tunnel.start()
        print(f"SUCCESS: SSH tunnel established (local port: {tunnel.local_bind_port})")
        
        # Connect to MySQL through tunnel
        conn = pymysql.connect(
            host='127.0.0.1',
            port=tunnel.local_bind_port,
            user=db_user,
            password=db_password,
            database=db_name,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        print("SUCCESS: Index Align database connection established")
        
        return conn, tunnel
        
    except Exception as e:
        print(f"ERROR: Index Align database connection failed: {str(e)}")
        print(f"Make sure SSH credentials are correct and SSH key is set up")
        return None, None

def get_issues_table_structure(conn):
    """Get the structure of the issues table"""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW COLUMNS FROM issues")
            columns = cursor.fetchall()
            print(f"ISSUES TABLE STRUCTURE:")
            print("=" * 50)
            for col in columns:
                print(f"  {col['Field']} ({col['Type']})")
            return [col['Field'] for col in columns]
    except Exception as e:
        print(f"ERROR: Failed to get table structure: {str(e)}")
        return None

def get_issues_from_database(conn):
    """Retrieve issues data from Index Align database"""
    try:
        # First, get table structure
        columns = get_issues_table_structure(conn)
        if not columns:
            return None
        
        # Query all issues
        query = "SELECT * FROM issues"
        
        df = pd.read_sql(query, conn)
        print(f"SUCCESS: Retrieved {len(df)} issues from Index Align database")
        
        # Display sample data
        if len(df) > 0:
            print("\nSample issues data:")
            print(df.head().to_string())
        
        return df
        
    except Exception as e:
        print(f"ERROR: Failed to retrieve issues: {str(e)}")
        return None

def get_company_id_from_ticker(ref, ticker):
    """Get company_id from ticker using the /tickers mapping in Firebase"""
    try:
        ticker_ref = ref.child('tickers').child(ticker)
        company_id = ticker_ref.get()
        if company_id:
            return company_id
        else:
            return None
    except Exception as e:
        print(f"  ERROR: Failed to lookup company_id for ticker {ticker}: {str(e)}")
        return None

def transform_issues_data(df, ref):
    """Transform issues data for Firebase structure: /issues/[company_id]/[issue_name]/Against, Neutral, Pro"""
    if df is None or df.empty:
        return None
    
    print("\nTRANSFORMING ISSUES DATA FOR FIREBASE")
    print("=" * 50)
    
    try:
        # Make a copy to avoid SettingWithCopyWarning
        df_transformed = df.copy()
        
        # Identify required columns
        # Look for ticker column (could be ticker, TICKER, company_ticker, etc.)
        ticker_column = None
        for possible_ticker in ['ticker', 'TICKER', 'company_ticker', 'COMPANY_TICKER', 'symbol', 'SYMBOL']:
            if possible_ticker in df_transformed.columns:
                ticker_column = possible_ticker
                break
        
        if ticker_column is None:
            print("ERROR: No ticker column found in issues table")
            print(f"Available columns: {list(df_transformed.columns)}")
            return None
        
        # Look for issue name column
        issue_name_column = None
        for possible_name in ['issue_name', 'ISSUE_NAME', 'issue', 'ISSUE', 'name', 'NAME']:
            if possible_name in df_transformed.columns:
                issue_name_column = possible_name
                break
        
        if issue_name_column is None:
            print("ERROR: No issue name column found in issues table")
            print(f"Available columns: {list(df_transformed.columns)}")
            return None
        
        # Look for Against, Neutral, Pro columns (case insensitive)
        against_column = None
        neutral_column = None
        pro_column = None
        
        for col in df_transformed.columns:
            col_lower = str(col).lower()
            if col_lower in ['against', 'against_amount', 'against_value']:
                against_column = col
            elif col_lower in ['neutral', 'neutral_amount', 'neutral_value']:
                neutral_column = col
            elif col_lower in ['pro', 'pro_amount', 'pro_value', 'for', 'for_amount']:
                pro_column = col
        
        if not against_column or not neutral_column or not pro_column:
            print("ERROR: Missing required columns (Against, Neutral, Pro)")
            print(f"Available columns: {list(df_transformed.columns)}")
            return None
        
        print(f"Using columns:")
        print(f"  Ticker: {ticker_column}")
        print(f"  Issue Name: {issue_name_column}")
        print(f"  Against: {against_column}")
        print(f"  Neutral: {neutral_column}")
        print(f"  Pro: {pro_column}")
        
        # Convert numeric columns to float
        for col in [against_column, neutral_column, pro_column]:
            df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce').fillna(0.0).astype(float)
        
        # Replace NaN with None for JSON serialization
        df_transformed = df_transformed.where(pd.notnull(df_transformed), None)
        
        # Build nested structure: issues[company_id][issue_name] = {Against: float, Neutral: float, Pro: float}
        issues_dict = {}
        ticker_to_company_id = {}
        skipped_tickers = set()
        
        print("\nMapping tickers to company_ids...")
        for _, row in df_transformed.iterrows():
            ticker = str(row[ticker_column]).strip().upper()
            
            # Skip if ticker is missing or already failed
            if pd.isna(row[ticker_column]) or ticker == 'NAN' or ticker == '':
                continue
            
            if ticker in skipped_tickers:
                continue
            
            # Get company_id from Firebase ticker mapping
            if ticker not in ticker_to_company_id:
                company_id = get_company_id_from_ticker(ref, ticker)
                if company_id:
                    ticker_to_company_id[ticker] = str(company_id)
                else:
                    print(f"  WARNING: No company_id mapping found for ticker {ticker}, skipping...")
                    skipped_tickers.add(ticker)
                    continue
            
            company_id = ticker_to_company_id[ticker]
            issue_name = str(row[issue_name_column]).strip()
            
            # Skip if issue name is missing
            if pd.isna(row[issue_name_column]) or issue_name == '':
                continue
            
            # Initialize company_id if not exists
            if company_id not in issues_dict:
                issues_dict[company_id] = {}
            
            # Add issue data
            issues_dict[company_id][issue_name] = {
                'Against': float(row[against_column]) if pd.notna(row[against_column]) else 0.0,
                'Neutral': float(row[neutral_column]) if pd.notna(row[neutral_column]) else 0.0,
                'Pro': float(row[pro_column]) if pd.notna(row[pro_column]) else 0.0
            }
        
        print(f"\nSUCCESS: Transformed issues data")
        print(f"  Companies processed: {len(issues_dict)}")
        print(f"  Tickers skipped (no mapping): {len(skipped_tickers)}")
        
        # Validate exactly 8 issues per company
        companies_with_wrong_count = []
        for company_id, issues in issues_dict.items():
            if len(issues) != 8:
                companies_with_wrong_count.append((company_id, len(issues)))
        
        if companies_with_wrong_count:
            print(f"\nWARNING: {len(companies_with_wrong_count)} companies don't have exactly 8 issues:")
            for company_id, count in companies_with_wrong_count[:10]:  # Show first 10
                print(f"  Company {company_id}: {count} issues")
        else:
            print(f"  ✓ All companies have exactly 8 issues")
        
        # Show sample transformed data
        print("\nSample transformed data:")
        if issues_dict:
            sample_company_id = list(issues_dict.keys())[0]
            sample_issues = issues_dict[sample_company_id]
            sample_issue_name = list(sample_issues.keys())[0]
            print(f"  Company ID: {sample_company_id}")
            print(f"  Issue: {sample_issue_name}")
            print(json.dumps({sample_issue_name: sample_issues[sample_issue_name]}, indent=2))
        
        return issues_dict
        
    except Exception as e:
        print(f"ERROR: Data transformation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def upload_issues_to_firebase(ref, issues_dict, dry_run=False):
    """Upload issues to Firebase Realtime Database under /issues/[company_id]/[issue_name] path"""
    if dry_run:
        print("\nDRY RUN MODE - Testing issues upload")
        print("=" * 50)
        print(f"Would upload issues for {len(issues_dict)} companies")
        
        # Count total issues
        total_issues = sum(len(issues) for issues in issues_dict.values())
        print(f"Total issues: {total_issues}")
        
        print("\nSample company data:")
        for i, (company_id, issues) in enumerate(list(issues_dict.items())[:3]):
            print(f"\nCompany {i+1} (ID: {company_id}): {len(issues)} issues")
            for issue_name, values in list(issues.items())[:2]:
                print(f"  {issue_name}:")
                print(f"    Against: {values['Against']}")
                print(f"    Neutral: {values['Neutral']}")
                print(f"    Pro: {values['Pro']}")
        print("\nWould upload to Firebase path: /issues/[company_id]/[issue_name]")
        print("Note: This will overwrite all existing data for each company_id")
        return True
    else:
        print("\nUPLOADING ISSUES TO FIREBASE")
        print("=" * 50)
        
        try:
            # Upload to /issues path
            issues_ref = ref.child('issues')
            
            success_count = 0
            skipped_count = 0
            
            # Upload each company's issues (overwrites entire company object)
            for company_id, company_issues in issues_dict.items():
                try:
                    # Upload entire company object - this overwrites everything for that company_id
                    company_ref = issues_ref.child(str(company_id))
                    company_ref.set(company_issues)
                    
                    success_count += 1
                    issue_count = len(company_issues)
                    print(f"  ✓ Uploaded company {company_id}: {issue_count} issues")
                    
                except Exception as e:
                    print(f"  ✗ ERROR: Failed to upload company {company_id}: {str(e)}")
                    skipped_count += 1
                    continue
            
            print(f"\nSUCCESS: Uploaded issues for {success_count} companies")
            if skipped_count > 0:
                print(f"SKIPPED: {skipped_count} companies failed to upload")
            
            # Verify upload
            uploaded_companies = len(issues_ref.get() or {})
            print(f"VERIFIED: {uploaded_companies} companies now in Firebase /issues")
            
            return success_count > 0
            
        except Exception as e:
            print(f"ERROR: Failed to upload issues: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

def main():
    """Main function to run the Index Align to Firebase issues pipeline"""
    print("INDEX ALIGN TO FIREBASE ISSUES TRANSFER")
    print("=" * 60)
    
    tunnel = None
    conn = None
    
    try:
        # Step 1: Setup Firebase connection
        print("\nStep 1: Setting up Firebase connection...")
        firebase_ref = setup_firebase_realtime()
        if not firebase_ref:
            return False
        
        # Step 2: Connect to Index Align database
        print("\nStep 2: Connecting to Index Align database...")
        conn, tunnel = connect_to_index_align_db()
        if not conn or not tunnel:
            return False
        
        # Step 3: Get issues data
        print("\nStep 3: Retrieving issues from Index Align database...")
        df = get_issues_from_database(conn)
        if df is None or df.empty:
            print("ERROR: No issues retrieved from database")
            return False
        
        # Step 4: Transform data
        print("\nStep 4: Transforming issues data...")
        issues_dict = transform_issues_data(df, firebase_ref)
        if issues_dict is None:
            print("ERROR: Data transformation failed")
            return False
        
        # Step 5: Upload to Firebase
        print("\nStep 5: Uploading issues to Firebase...")
        
        # Ask user for confirmation
        try:
            user_input = input("\nDo you want to proceed with upload to Firebase? (y/n): ").lower().strip()
            if user_input == 'y':
                success = upload_issues_to_firebase(firebase_ref, issues_dict, dry_run=False)
            else:
                print("Running in dry run mode...")
                success = upload_issues_to_firebase(firebase_ref, issues_dict, dry_run=True)
        except EOFError:
            print("Running in dry run mode...")
            success = upload_issues_to_firebase(firebase_ref, issues_dict, dry_run=True)
        
        if success:
            print("\nSUCCESS: Index Align to Firebase issues pipeline completed successfully")
            return True
        else:
            print("\nERROR: Issues pipeline failed")
            return False
        
    except Exception as e:
        print(f"\nERROR: Pipeline failed with exception: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Step 6: Cleanup connections
        print("\nStep 6: Closing connections...")
        if conn:
            conn.close()
            print("SUCCESS: Database connection closed")
        if tunnel:
            tunnel.stop()
            print("SUCCESS: SSH tunnel closed")

if __name__ == "__main__":
    main()
