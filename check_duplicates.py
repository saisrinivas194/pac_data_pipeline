import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def connect_to_firebase():
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
        print("SUCCESS: Connected to Firebase")
        return db
        
    except Exception as e:
        print(f"ERROR: Could not connect to Firebase: {str(e)}")
        return None

def check_existing_data(db):
    """Check what data already exists in Firebase"""
    print("CHECKING EXISTING DATA IN FIREBASE")
    print("=" * 50)
    
    collections_to_check = ['pac_contributions', 'summary_analysis']
    existing_data = {}
    
    for collection_name in collections_to_check:
        try:
            collection_ref = db.collection(collection_name)
            docs = collection_ref.stream()
            
            doc_count = 0
            sample_records = []
            
            for doc in docs:
                doc_count += 1
                if doc_count <= 5:  # Get first 5 records as sample
                    doc_data = doc.to_dict()
                    sample_records.append({
                        'id': doc.id,
                        'data': doc_data
                    })
            
            existing_data[collection_name] = {
                'count': doc_count,
                'sample_records': sample_records
            }
            
            print(f"Collection '{collection_name}': {doc_count:,} records")
            
            if sample_records:
                print("Sample records:")
                for i, record in enumerate(sample_records, 1):
                    print(f"  {i}. ID: {record['id']}")
                    if 'ticker' in record['data']:
                        print(f"     Ticker: {record['data']['ticker']}")
                    if 'election_cycle' in record['data']:
                        print(f"     Election: {record['data']['election_cycle']}")
                    if 'receipt_amount' in record['data']:
                        print(f"     Amount: ${record['data']['receipt_amount']:,.0f}")
                    print()
            
        except Exception as e:
            print(f"ERROR reading collection '{collection_name}': {str(e)}")
            existing_data[collection_name] = {'count': 0, 'sample_records': []}
    
    return existing_data

def check_csv_files():
    """Check what's in our CSV files"""
    print("CHECKING CSV FILES")
    print("=" * 50)
    
    csv_files = {
        'company_pac_contributions.csv': ['TICKER', 'ELECTION_CYCLE', 'RECEIPT_AMOUNT'],
        'company_individual_vs_pac_contributions (3).csv': ['TICKER', 'ELECTION_CYCLE', 'RECEIPT_AMOUNT']
    }
    
    csv_data = {}
    
    for file_name, key_columns in csv_files.items():
        try:
            df = pd.read_csv(file_name)
            csv_data[file_name] = {
                'count': len(df),
                'columns': list(df.columns),
                'sample': df.head(3).to_dict('records')
            }
            
            print(f"File '{file_name}': {len(df):,} records")
            print(f"Columns: {list(df.columns)}")
            
            # Show totals by key columns
            if 'TICKER' in df.columns and 'RECEIPT_AMOUNT' in df.columns:
                totals = df.groupby('TICKER')['RECEIPT_AMOUNT'].sum().sort_values(ascending=False)
                print("Totals by company:")
                for ticker, total in totals.head(5).items():
                    print(f"  {ticker}: ${total:,.0f}")
            
            print()
            
        except Exception as e:
            print(f"ERROR reading '{file_name}': {str(e)}")
            csv_data[file_name] = {'count': 0, 'columns': [], 'sample': []}
    
    return csv_data

def find_potential_duplicates(csv_data, existing_data):
    """Find potential duplicates between CSV and Firebase"""
    print("CHECKING FOR POTENTIAL DUPLICATES")
    print("=" * 50)
    
    duplicates_found = []
    
    # Check pac_contributions
    if 'pac_contributions' in existing_data and 'company_pac_contributions.csv' in csv_data:
        csv_count = csv_data['company_pac_contributions.csv']['count']
        firebase_count = existing_data['pac_contributions']['count']
        
        if firebase_count > 0:
            print(f"PAC Contributions:")
            print(f"  CSV file: {csv_count:,} records")
            print(f"  Firebase: {firebase_count:,} records")
            
            if firebase_count >= csv_count:
                print("  WARNING: Firebase has same or more records than CSV")
                print("  Possible duplicates detected!")
                duplicates_found.append('pac_contributions')
            else:
                print("  INFO: Firebase has fewer records than CSV")
                print("  Safe to upload remaining records")
        else:
            print("PAC Contributions: No data in Firebase - safe to upload")
    
    # Check summary_analysis
    if 'summary_analysis' in existing_data and 'company_individual_vs_pac_contributions (3).csv' in csv_data:
        csv_count = csv_data['company_individual_vs_pac_contributions (3).csv']['count']
        firebase_count = existing_data['summary_analysis']['count']
        
        if firebase_count > 0:
            print()
            print(f"Summary Analysis:")
            print(f"  CSV file: {csv_count:,} records")
            print(f"  Firebase: {firebase_count:,} records")
            
            if firebase_count >= csv_count:
                print("  WARNING: Firebase has same or more records than CSV")
                print("  Possible duplicates detected!")
                duplicates_found.append('summary_analysis')
            else:
                print("  INFO: Firebase has fewer records than CSV")
                print("  Safe to upload remaining records")
        else:
            print("Summary Analysis: No data in Firebase - safe to upload")
    
    return duplicates_found

def show_duplicate_status(duplicates_found):
    """Show duplicate check results"""
    print("\nDUPLICATE CHECK RESULTS")
    print("=" * 50)
    
    if not duplicates_found:
        print("SUCCESS: No duplicates detected!")
        print("Firebase collections are empty or have fewer records than CSV files")
    else:
        print("WARNING: Potential duplicates found in these collections:")
        for collection in duplicates_found:
            print(f"  - {collection}")
        print("Firebase has same or more records compared to CSV files")

def main():
    """Main function to check for duplicates"""
    print("DUPLICATE CHECKER")
    print("=" * 50)
    print("Checking Firebase and CSV files for existing data...")
    print()
    
    # Check if .env exists
    if not os.path.exists('.env'):
        print("ERROR: .env file not found!")
        print("Run: cp ENVIRONMENT_TEMPLATE.txt .env")
        print("Then add your Firebase credentials to .env")
        return
    
    # Connect to Firebase
    db = connect_to_firebase()
    if not db:
        return
    
    print()
    
    # Check CSV files
    csv_data = check_csv_files()
    
    print()
    
    # Check Firebase data
    existing_data = check_existing_data(db)
    
    print()
    
    # Look for duplicates
    duplicates_found = find_potential_duplicates(csv_data, existing_data)
    
    # Show duplicate status
    show_duplicate_status(duplicates_found)
    
    print("\nCHECK COMPLETE")

if __name__ == "__main__":
    main()
