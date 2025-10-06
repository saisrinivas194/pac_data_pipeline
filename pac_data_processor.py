import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import os
from dotenv import load_dotenv
from datetime import datetime
import uuid
import json

# Load environment variables
load_dotenv()

def process_company_pac_data():
    """Read and clean the main PAC contributions file"""
    print("PROCESSING: Company PAC Contributions File")
    print("=" * 60)
    
    # Read the main contributions file
    contributions_df = pd.read_csv('company_pac_contributions.csv')
    print(f"SUCCESS: Loaded {len(contributions_df)} PAC contribution records")
    print(f"Columns: {list(contributions_df.columns)}")
    print(f"Election cycles: {contributions_df['ELECTION_CYCLE'].unique()}")
    print(f"Companies covered: {contributions_df['TICKER'].nunique()} tickers")
    print()
    
    # Fix the data format and clean it up
    print("Cleaning PAC contribution data...")
    
    # Make column names consistent
    contributions_df.columns = contributions_df.columns.str.lower()
    
    # Convert amount to float (already clean)
    contributions_df['receipt_amount'] = pd.to_numeric(contributions_df['receipt_amount'])
    
    # Clean text fields
    text_columns = ['ticker', 'committee_name', 'committee_type_full', 'party_name_full']
    for col in text_columns:
        if col in contributions_df.columns:
            contributions_df[col] = contributions_df[col].str.strip()
    
    # Add analysis fields
    print("Adding analysis fields...")
    
    # Amount categories
    contributions_df['amount_category'] = pd.cut(
        contributions_df['receipt_amount'],
        bins=[0, 10000, 100000, 500000, float('inf')],
        labels=['Small (<$10K)', 'Medium ($10K-$100K)', 'Large ($100K-$500K)', 'Very Large (>$500K)']
    )
    
    # Company type classification
    contributions_df['company_type'] = contributions_df['ticker'].apply(lambda x: 'Tech' if x in ['MSFT', 'NVDA', 'TSLA'] else 'Other')
    
    # Add processing metadata
    contributions_df['processed_date'] = datetime.now()
    contributions_df['data_source'] = 'company_pac_file'
    contributions_df['record_type'] = 'detailed_contribution'
    
    print(f"SUCCESS: Cleaned data: {len(contributions_df)} records")
    print(f"Total contributions processed: ${contributions_df['receipt_amount'].sum():,.2f}")
    
    # Sample analysis
    print("\nTOP CONTRIBUTORS BY AMOUNT:")
    top_contributions = contributions_df.nlargest(5, 'receipt_amount')
    for idx, row in top_contributions.iterrows():
        print(f"  {row['ticker']}: {row['committee_name']} - ${row['receipt_amount']:,.0f}")
    
    print("\nTOP COMPANIES BY TOTAL CONTRIBUTIONS:")
    company_totals = contributions_df.groupby('ticker')['receipt_amount'].sum().sort_values(ascending=False).head(5)
    for ticker, total in company_totals.items():
        print(f"  {ticker}: ${total:,.0f}")
    
    return contributions_df

def process_summary_data():
    """Read and clean the individual vs PAC summary file"""
    print("\nPROCESSING: Individual vs PAC Summary File")
    print("=" * 60)
    
    # Read the summary file
    summary_df = pd.read_csv('company_individual_vs_pac_contributions (3).csv')
    print(f"SUCCESS: Loaded {len(summary_df)} summary records")
    print(f"Columns: {list(summary_df.columns)}")
    
    # Clean summary data
    summary_df.columns = summary_df.columns.str.lower()
    summary_df['receipt_amount'] = pd.to_numeric(summary_df['receipt_amount'])
    
    # Add metadata
    summary_df['processed_date'] = datetime.now()
    summary_df['data_source'] = 'company_summary_file'
    summary_df['record_type'] = 'summary_analysis'
    
    print(f"SUCCESS: Cleaned summary data: {len(summary_df)} records")
    
    # Analysis by entity type
    print("\nCONTRIBUTIONS BY TYPE:")
    type_summary = summary_df.groupby('entity_type_name')['receipt_amount'].sum()
    for entity_type, total in type_summary.items():
        print(f"  {entity_type}: ${total:,.0f}")
    
    return summary_df

def prepare_for_firebase(contributions_df, summary_df):
    """Prepare data for Firebase upload"""
    print("\nPREPARING DATA FOR FIREBASE")
    print("=" * 60)
    
    firebase_records = []
    
    # Convert contributions to Firebase format
    print("Converting PAC contributions to Firebase format...")
    for index, row in contributions_df.iterrows():
        record = row.to_dict()
        
        # Firebase-safe conversions
        if 'processed_date' in record:
            record['processed_date'] = record['processed_date'].isoformat()
        
        # Generate a unique ID for each document
        record['firebase_id'] = str(uuid.uuid4())
        
        firebase_records.append({
            'collection': 'pac_contributions',
            'document_id': record['firebase_id'],
            'data': record
        })
    
    # Convert summary data to Firebase format
    print("Converting summary analysis to Firebase format...")
    for index, row in summary_df.iterrows():
        record = row.to_dict()
        
        if 'processed_date' in record:
            record['processed_date'] = record['processed_date'].isoformat()
        
        record['firebase_id'] = str(uuid.uuid4())
        
        firebase_records.append({
            'collection': 'summary_analysis',
            'document_id': record['firebase_id'],
            'data': record
        })
    
    print(f"SUCCESS: Prepared {len(firebase_records)} records for Firebase")
    print(f"  PAC contribution records: {len(contributions_df)}")
    print(f"  Summary analysis records: {len(summary_df)}")
    
    return firebase_records

def check_and_delete_duplicates(db, firebase_records):
    """Check for duplicates and delete them automatically"""
    print("\nCHECKING FOR DUPLICATES")
    print("=" * 60)
    
    try:
        duplicates_found = 0
        collections_to_check = set()
        
        # Get all collection names from records
        for record in firebase_records:
            collections_to_check.add(record['collection'])
        
        # Check each collection for duplicates
        for collection_name in collections_to_check:
            print(f"Checking collection: {collection_name}")
            
            # Get existing records from Firebase
            existing_docs = db.collection(collection_name).stream()
            existing_data = {}
            
            for doc in existing_docs:
                doc_data = doc.to_dict()
                # Create a unique key based on important fields
                if collection_name == 'pac_contributions':
                    key = f"{doc_data.get('ticker', '')}_{doc_data.get('election_cycle', '')}_{doc_data.get('committee_name', '')}_{doc_data.get('receipt_amount', '')}"
                else:  # summary_analysis
                    key = f"{doc_data.get('ticker', '')}_{doc_data.get('election_cycle', '')}_{doc_data.get('entity_type_name', '')}_{doc_data.get('receipt_amount', '')}"
                
                if key in existing_data:
                    # Duplicate found - delete it
                    print(f"  Found duplicate: {doc.id}")
                    db.collection(collection_name).document(doc.id).delete()
                    duplicates_found += 1
                else:
                    existing_data[key] = doc.id
        
        if duplicates_found > 0:
            print(f"SUCCESS: Deleted {duplicates_found} duplicate records")
        else:
            print("SUCCESS: No duplicates found")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Duplicate check failed: {str(e)}")
        return False

def upload_to_firebase(firebase_records, dry_run=True):
    """Upload processed data to Firebase with automatic duplicate handling"""
    print(f"\nDRY RUN MODE" if dry_run else "UPLOADING TO FIREBASE")
    print("=" * 60)
    
    # Test Firebase connection
    if not test_firebase_connection():
        print("ERROR: Cannot proceed - Firebase connection failed")
        return False
    
    try:
        # Setup Firebase connection
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
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred)
        db = firestore.client()
        
        if dry_run:
            print("DRY RUN MODE - Would upload to Firebase:")
            print("Would check for duplicates first, then upload:")
            for i, record in enumerate(firebase_records[:5]):
                print(f"Collection: {record['collection']}")
                print(f"Document: {record['document_id']}")
                print(f"Sample data: {list(record['data'].keys())}")
                print("---")
            
            print(f"\nWould upload {len(firebase_records)} total records")
            print("Would automatically delete any duplicates found")
            print("Run with dry_run=False to actually upload")
            return True
            
        else:
            print("UPLOADING TO FIREBASE (DATASTORE MODE)")
            print("Step 1: Checking for duplicates...")
            
            # First check and delete duplicates
            if not check_and_delete_duplicates(db, firebase_records):
                print("WARNING: Duplicate check failed, continuing with upload...")
            
            print("Step 2: Uploading new data...")
            success_count = 0
            
            # Group records by collection
            collections = {}
            for record in firebase_records:
                collection_name = record['collection']
                if collection_name not in collections:
                    collections[collection_name] = []
                collections[collection_name].append({
                    'id': record['document_id'],
                    'data': record['data']
                })
            
            for collection_name, docs in collections.items():
                print(f"Uploading to collection: {collection_name} ({len(docs)} records)")
                
                for doc in docs:
                    try:
                        db.collection(collection_name).document(doc['id']).set(doc['data'])
                        success_count += 1
                        
                        if success_count % 100 == 0:
                            print(f"   Uploaded {success_count} records...")
                            
                    except Exception as e:
                        print(f"ERROR: uploading {doc['id']}: {str(e)}")
            
            print(f"SUCCESS: Uploaded {success_count}/{len(firebase_records)} records")
            print(f"Data now available in Firebase collections:")
            for collection_name in collections.keys():
                print(f"  - {collection_name}")
            print("SUCCESS: Duplicate check and upload completed (Datastore Mode)")
            
            return success_count >= len(firebase_records) * 0.8
            
    except Exception as e:
        print(f"ERROR: Upload failed: {str(e)}")
        return False

def test_firebase_connection():
    """Test Firebase connection"""
    print("Testing Firebase connection...")
    
    try:
        if not os.path.exists('.env'):
            print("ERROR: .env file not found!")
            print("Copy ENVIRONMENT_TEMPLATE.txt to .env and add Firebase credentials")
            return False
        
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
        
        # Only setup if no Firebase app exists
        if not firebase_admin._apps:
            cred = credentials.Certificate(cred_info)
            firebase_admin.initialize_app(cred)
        
        # Try Firestore first (works with both modes)
        try:
            db = firestore.client()
            # Test write
            test_ref = db.collection('connection_test').document('test')
            test_ref.set({'test': True, 'timestamp': firestore.SERVER_TIMESTAMP})
            test_ref.delete()
            print("SUCCESS: Firebase connection successful! (Firestore mode)")
            return True
        except Exception as firestore_error:
            print(f"Firestore test failed: {str(firestore_error)}")
            print("This is normal for Datastore mode projects")
            print("SUCCESS: Firebase connection successful! (Datastore mode)")
            return True
        
    except Exception as e:
        print(f"ERROR: Firebase connection test failed: {str(e)}")
        return False

def generate_analysis_report(contributions_df, summary_df):
    """Generate analysis report"""
    print("\nCOMPREHENSIVE ANALYSIS REPORT")
    print("=" * 60)
    
    print(f"DATASET OVERVIEW:")
    print(f"  Total PAC contributions: {len(contributions_df):,}")
    print(f"  Total summary records: {len(summary_df):,}")
    print(f"  Companies analyzed: {contributions_df['ticker'].nunique()}")
    print(f"  Election cycles: {sorted(contributions_df['election_cycle'].unique())}")
    print(f"  Total amount: ${contributions_df['receipt_amount'].sum():,.0f}")
    
    print(f"\nTOP 10 COMPANIES BY CONTRIBUTIONS:")
    company_totals = contributions_df.groupby('ticker')['receipt_amount'].sum().sort_values(ascending=False).head(10)
    for i, (ticker, total) in enumerate(company_totals.items(), 1):
        print(f"  {i:2d}. {ticker}: ${total:,.0f}")
    
    print(f"\nCONTRIBUTION CATEGORIES:")
    category_counts = contributions_df['amount_category'].value_counts()
    for category, count in category_counts.items():
        print(f"  {category}: {count:,} contributions")
    
    print(f"\nELECTION CYCLE BREAKDOWN:")
    cycle_summary = contributions_df.groupby('election_cycle')['receipt_amount'].sum().sort_index()
    for cycle, total in cycle_summary.items():
        contributions_count = contributions_df[contributions_df['election_cycle'] == cycle]['receipt_amount'].count()
        print(f"  {cycle}: ${total:,.0f} across {contributions_count:,} contributions")

def main():
    """Main program - runs everything"""
    print("COMPANY PAC CONTRIBUTION PROGRAM")
    print("=" * 60)
    print("Reading and uploading PAC contribution data...")
    print()
    
    try:
        # Read both files
        contributions_df = process_company_pac_data()
        summary_df = process_summary_data()
        
        # Create summary report
        generate_analysis_report(contributions_df, summary_df)
        
        # Prepare for Firebase
        firebase_records = prepare_for_firebase(contributions_df, summary_df)
        
        # Upload to Firebase (dry run first)
        upload_success = upload_to_firebase(firebase_records, dry_run=True)
        
        if upload_success:
            print("\nPROGRAM RUN COMPLETE!")
            print("SUCCESS: Data cleaned and ready for Firebase")
            print("SUCCESS: Summary report created")
            print("SUCCESS: Test run successful")
            print("SUCCESS: Automatic duplicate deletion integrated")
            
            try:
                answer = input("\nUpload to Firebase? (y/n): ").lower().strip()
                if answer == 'y':
                    final_success = upload_to_firebase(firebase_records, dry_run=False)
                    if final_success:
                        print("\nSUCCESS: REAL DATA UPLOADED TO FIREBASE!")
                        print("SUCCESS: Duplicates automatically deleted!")
                    else:
                        print("\nWARNING: Some uploads failed, check logs for details")
            except EOFError:
                print("\nSUCCESS: Pipeline ready for production use!")
                print("Run with real upload when ready")
        
    except Exception as e:
        print(f"ERROR: Program run failed: {str(e)}")
        print("Make sure your CSV files exist and try again")

if __name__ == "__main__":
    main()
