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

def transform_issues_data(df):
    """Transform issues data for Firebase"""
    if df is None or df.empty:
        return None
    
    print("\nTRANSFORMING ISSUES DATA FOR FIREBASE")
    print("=" * 50)
    
    try:
        # Make a copy to avoid SettingWithCopyWarning
        df_transformed = df.copy()
        
        # Convert datetime columns to ISO format strings
        datetime_columns = df_transformed.select_dtypes(include=['datetime64']).columns
        for col in datetime_columns:
            df_transformed[col] = df_transformed[col].apply(
                lambda x: x.isoformat() if pd.notna(x) else None
            )
        
        # Convert numeric columns - ensure they're JSON serializable
        numeric_columns = df_transformed.select_dtypes(include=['float', 'int']).columns
        for col in numeric_columns:
            df_transformed[col] = df_transformed[col].apply(
                lambda x: float(x) if pd.notna(x) else None
            )
        
        # Replace NaN with None for JSON serialization
        df_transformed = df_transformed.where(pd.notnull(df_transformed), None)
        
        # Convert to dictionary with id as key
        issues_dict = {}
        
        # First, identify the ID column name
        id_column_name = None
        for possible_id in ['id', 'issue_id', 'ID', 'ISSUE_ID']:
            if possible_id in df_transformed.columns:
                id_column_name = possible_id
                break
        
        if id_column_name is None:
            print("WARNING: No ID column found. Using row index as ID.")
        
        for _, row in df_transformed.iterrows():
            issue_id = None
            
            # Try to find id column (could be 'id', 'issue_id', etc.)
            for possible_id in ['id', 'issue_id', 'ID', 'ISSUE_ID']:
                if possible_id in row:
                    issue_id = row[possible_id]
                    break
            
            if issue_id is None:
                print(f"WARNING: No ID found for issue row, skipping...")
                continue
            
            # Create clean dictionary with all fields (excluding the ID column used as key)
            issue_dict = {}
            for key, value in row.items():
                # Exclude the ID column that's being used as the key to avoid duplication
                if key != id_column_name:
                    issue_dict[key] = value
            
            issues_dict[str(issue_id)] = issue_dict
        
        print(f"SUCCESS: Transformed {len(issues_dict)} issues")
        
        # Show sample transformed data
        print("\nSample transformed issue:")
        if issues_dict:
            sample_id = list(issues_dict.keys())[0]
            print(json.dumps(issues_dict[sample_id], indent=2, default=str))
        
        return issues_dict
        
    except Exception as e:
        print(f"ERROR: Data transformation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def upload_issues_to_firebase(ref, issues_dict, dry_run=False):
    """Upload issues to Firebase Realtime Database under /issues path"""
    if dry_run:
        print("\nDRY RUN MODE - Testing issues upload")
        print("=" * 50)
        print(f"Would upload {len(issues_dict)} issues")
        print("\nSample issues:")
        for i, (issue_id, issue_data) in enumerate(list(issues_dict.items())[:3]):
            print(f"\nIssue {i+1} (ID: {issue_id}):")
            for key, value in list(issue_data.items())[:5]:
                print(f"  {key}: {value}")
        print("\nWould upload to Firebase path: /issues")
        return True
    else:
        print("\nUPLOADING ISSUES TO FIREBASE")
        print("=" * 50)
        
        try:
            # Upload to /issues path
            issues_ref = ref.child('issues')
            
            # Upload all issues
            issues_ref.set(issues_dict)
            
            print(f"SUCCESS: Uploaded {len(issues_dict)} issues to Firebase /issues path")
            
            # Verify upload
            uploaded_count = len(issues_ref.get() or {})
            print(f"VERIFIED: {uploaded_count} issues now in Firebase")
            
            return True
            
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
        issues_dict = transform_issues_data(df)
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
