import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_env_variables():
    """Test if all required environment variables are set"""
    print("TESTING ENVIRONMENT VARIABLES")
    print("=" * 40)
    
    required_vars = {
        'SNOWFLAKE_USER': 'Snowflake username',
        'SNOWFLAKE_PASSWORD': 'Snowflake password',
        'SNOWFLAKE_ACCOUNT': 'Snowflake account identifier',
        'SNOWFLAKE_WAREHOUSE': 'Snowflake warehouse name',
        'SNOWFLAKE_DATABASE': 'Snowflake database name',
        'SNOWFLAKE_SCHEMA': 'Snowflake schema name',
        'FIREBASE_PROJECT_ID': 'Firebase project ID',
        'FIREBASE_CLIENT_EMAIL': 'Firebase service account email',
        'FIREBASE_PRIVATE_KEY': 'Firebase private key'
    }
    
    missing_vars = []
    
    for var, description in required_vars.items():
        value = os.getenv(var)
        if not value:
            missing_vars.append((var, description))
            print(f"{var}: MISSING - {description}")
        elif value.startswith('your_') or value.startswith('YOUR_'):
            missing_vars.append((var, description))
            print(f"{var}: PLACEHOLDER VALUE - {description}")
        else:
            # Hide sensitive info but show it's set
            if 'password' in var.lower() or 'key' in var.lower():
                print(f"{var}: SET (hidden)")
            else:
                print(f"{var}: SET ({value})")
    
    if missing_vars:
        print(f"\nERROR: {len(missing_vars)} variables missing or contain placeholder text")
        print("Copy ENVIRONMENT_SNOWFLAKE_TEMPLATE.txt to .env and fill in real values")
        return False
    else:
        print("\nSUCCESS: All environment variables set correctly")
        return True

def test_snowflake_connection():
    """Test Snowflake connection"""
    print("\nTESTING SNOWFLAKE CONNECTION")
    print("=" * 40)
    
    try:
        import snowflake.connector
        
        # Try to connect
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            timeout=10  # 10 second timeout
        )
        
        # Test a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_TIMESTAMP() as current_date")
        result = cursor.fetchone()
        
        print("Snowflake connection successful")
        print(f"   Current time: {result[0]}")
        
        # Test warehouse is active
        cursor.execute("SELECT CURRENT_WAREHOUSE() as warehouse")
        warehouse = cursor.fetchone()
        print(f"   Active warehouse: {warehouse[0]}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except ImportError:
        print("Snowflake connector not installed")
        print("   Run: pip install snowflake-connector-python")
        return False
        
    except Exception as e:
        print(f"Snowflake connection failed: {str(e)}")
        print("   Check your Snowflake credentials and account details")
        return False

def test_firebase_connection():
    """Test Firebase connection"""
    print("\nTESTING FIREBASE CONNECTION")
    print("=" * 40)
    
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
        
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
        
        # Initialize Firebase
        cred = credentials.Certificate(cred_info)
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred)
        
        db = firestore.client()
        
        # Test write and read
        test_ref = db.collection('connection_test').document('test')
        test_ref.set({'test': True, 'timestamp': firestore.SERVER_TIMESTAMP})
        
        # Read it back
        doc = test_ref.get()
        if doc.exists:
            print("Firebase connection successful")
            print(f"   Project: {os.getenv('FIREBASE_PROJECT_ID')}")
            print("   Read/write test passed")
            
            # Clean up test document
            test_ref.delete()
            
            return True
        else:
            print("Firebase write/read test failed")
            return False
            
    except ImportError:
        print("Firebase admin SDK not installed")
        print("   Run: pip install firebase-admin")
        return False
        
    except Exception as e:
        print(f"Firebase connection failed: {str(e)}")
        print("   Check your Firebase credentials and project setup")
        return False

def test_sample_data():
    """Test data processing with sample data"""
    print("\nTESTING DATA PROCESSING")
    print("=" * 40)
    
    try:
        # Create sample DataFrame
        sample_data = {
            'TICKER': ['AAPL', 'MSFT', 'GOOGL'],
            'ELECTION_CYCLE': [2024, 2024, 2024],
            'COMMITTEE_NAME': ['Tech PAC', 'Microsoft PAC', 'Google PAC'],
            'RECEIPT_AMOUNT': [15000.50, 25000.75, 30000.25],
            'COMMITTEE_TYPE': ['Company', 'Company', 'Company'],
            'PARTY_NAME': ['Republican', 'Democrat', 'Republican']
        }
        
        df = pd.DataFrame(sample_data)
        print(f"Created sample data: {len(df)} records")
        print(f"   Columns: {list(df.columns)}")
        
        # Test data cleaning
        original_cols = list(df.columns)
        df.columns = df.columns.str.lower()
        
        # Test text cleaning
        df['ticker'] = df['ticker'].astype(str).str.strip()
        df['committee_name'] = df['committee_name'].astype(str).str.strip()
        
        # Test numeric conversion
        df['receipt_amount'] = pd.to_numeric(df['receipt_amount'])
        
        print("Data cleaning tests passed")
        print(f"   Money total: ${df['receipt_amount'].sum():.2f}")
        print(f"   Companies: {len(df['ticker'].unique())}")
        
        return True
        
    except Exception as e:
        print(f"Data processing test failed: {str(e)}")
        return False

def test_file_dependencies():
    """Test if all required files exist"""
    print("\nTESTING FILE DEPENDENCIES")
    print("=" * 40)
    
    required_files = [
        ('pac_snowflake_pipeline.py', 'Main PAC Snowflake program'),
        ('requirements_snowflake.txt', 'Dependencies file'),
        ('ENVIRONMENT_SNOWFLAKE_TEMPLATE.txt', 'Environment template'),
        ('.env', 'Environment file')
    ]
    
    missing_files = []
    
    for filename, description in required_files:
        if os.path.exists(filename):
            print(f"{filename}: EXISTS - {description}")
        else:
            missing_files.append(filename)
            print(f"{filename}: MISSING - {description}")
    
    if missing_files:
        print(f"\nWARNING: {len(missing_files)} files missing")
        if '.env' in missing_files:
            print("Run: cp ENVIRONMENT_SNOWFLAKE_TEMPLATE.txt .env")
        return False
    else:
        print("\nSUCCESS: All required files present")
        return True

def run_comprehensive_test():
    """Run all tests"""
    print("COMPREHENSIVE SNOWFLAKE PIPELINE TEST")
    print("=" * 50)
    print("Running all tests to verify setup...")
    print("")
    
    tests = [
        ("Environment Variables", test_env_variables),
        ("File Dependencies", test_file_dependencies),
        ("Snowflake Connection", test_snowflake_connection),
        ("Firebase Connection", test_firebase_connection),
        ("Data Processing", test_sample_data)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"{test_name} test crashed: {str(e)}")
            results.append((test_name, False))
    
    # Summary
    print("\nTEST SUMMARY")
    print("=" * 50)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "PASSED" if result else " FAILED"
        print(f"{test_name:20} : {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n ALL TESTS PASSED!")
        print("Your Snowflake to Firebase pipeline is ready to use!")
        print("\nNext steps:")
        print("1. Run: python pac_data_processor.py (for CSV data)")
        print("2. Run: python pac_snowflake_pipeline.py (for PAC Snowflake data)")
    else:
        print(f"\n  {total - passed} tests failed")
        print("Fix the failed tests before running the main programs")

def main():
    """Main test function"""
    import sys
    if len(sys.argv) > 1:
        test_type = sys.argv[1].lower()
        
        if test_type == 'env':
            test_env_variables()
        elif test_type == 'snowflake':
            test_snowflake_connection()
        elif test_type == 'firebase':
            test_firebase_connection()
        elif test_type == 'data':
            test_sample_data()
        elif test_type == 'files':
            test_file_dependencies()
        else:
            print("Usage: python test_snowflake_pipeline.py [test_type]")
            print("Test types: env, snowflake, firebase, data, files")
            
    else:
        run_comprehensive_test()

if __name__ == "__main__":
    main()
