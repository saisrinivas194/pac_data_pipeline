# How to Connect Index Align Issues to Firebase

Step-by-step guide to move issues from Index Align to Firebase.

## What you'll need

- Python 3.8 or newer
- Firebase Realtime Database enabled
- SSH access to the Index Align server
- Database password

## Step 1: Install packages

```bash
pip install -r requirements.txt
```

## Step 2: Setup .env file

```bash
cp ENVIRONMENT_TEMPLATE.txt .env
nano .env
```

Add these to your `.env` file:

```bash
# Firebase Configuration
FIREBASE_PROJECT_ID=goods-unite-us-4441a
FIREBASE_PRIVATE_KEY_ID=your_key_id
FIREBASE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\nyour_key_here\n-----END PRIVATE KEY-----\n"
FIREBASE_CLIENT_EMAIL=your_service_account@goods-unite-us.iam.gserviceaccount.com
FIREBASE_CLIENT_ID=your_client_id

# Index Align SSH Configuration
INDEX_ALIGN_SSH_HOST=indexalign
INDEX_ALIGN_SSH_USER=ubuntu
INDEX_ALIGN_SSH_PORT=22
INDEX_ALIGN_DB_HOST=127.0.0.1
INDEX_ALIGN_DB_PORT=3306
INDEX_ALIGN_DB_NAME=indexalign
INDEX_ALIGN_DB_USER=root
INDEX_ALIGN_DB_PASSWORD=your_database_password
```

Pick one - either use an SSH key or a password.

**SSH Key (easier):**
```bash
INDEX_ALIGN_SSH_KEY_PATH=/Users/yourname/.ssh/id_rsa
```

**Or use a password:**
```bash
INDEX_ALIGN_SSH_PASSWORD=your_password
```

## Step 3: Setup SSH key (recommended)

Makes it so you don't need to type a password every time:

```bash
ssh-keygen -t rsa -b 4096
ssh-copy-id ubuntu@indexalign
ssh ubuntu@indexalign  # test it
```

Then in `.env` add:
```bash
INDEX_ALIGN_SSH_KEY_PATH=/Users/yourusername/.ssh/id_rsa
```

## Step 4: Get Firebase keys

1. Go to [Firebase Console](https://console.firebase.google.com/)
2. Pick your project
3. Settings → Project Settings → Service accounts
4. Click "Generate new private key"
5. Copy the values to `.env`

### Enable Realtime Database

Firebase Console → Realtime Database → Create Database → pick a region

## Step 5: Test it works

```bash
# Test all connections at once
python test_index_align.py
```

Should see checkmarks for everything. If something fails:

```python
import pymysql
from sshtunnel import SSHTunnelForwarder

# Setup SSH tunnel
tunnel = SSHTunnelForwarder(
    ('indexalign', 22),
    ssh_username='ubuntu',
    ssh_pkey='/path/to/.ssh/id_rsa',
    remote_bind_address=('127.0.0.1', 3306),
    local_bind_address=('127.0.0.1', 0)
)

tunnel.start()
print(f"SSH tunnel established on local port: {tunnel.local_bind_port}")

# Connect to MySQL
conn = pymysql.connect(
    host='127.0.0.1',
    port=tunnel.local_bind_port,
    user='root',
    password='your_password',
    database='indexalign'
)

# Test query
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM issues")
count = cursor.fetchone()
print(f"Issues count: {count[0]}")

conn.close()
tunnel.stop()
```

## Step 6: Run the pipeline

Test it first:
```bash
python index_align_to_firebase.py
# Type 'n' for dry run
```

Then upload for real:
```bash
python index_align_to_firebase.py
# Type 'y' when ready
```

## Step 7: Check Firebase

Go to Firebase Console → Realtime Database → `/issues` to see your data

## Troubleshooting

**Can't connect via SSH?**
```bash
chmod 600 ~/.ssh/id_rsa
ssh ubuntu@indexalign  # test manually
```

**Database not working?**
```bash
ssh ubuntu@indexalign 'systemctl status mysql'
```

**Firebase errors?**
```bash
python test_firebase.py
```

**Missing packages?**
```bash
pip install --upgrade -r requirements.txt
```

## What happens

The script reads the `issues` table from Index Align and sends it to Firebase at `/issues`. It works with whatever columns you have.

## Security tips

- Don't commit your `.env` file
- Use SSH keys instead of passwords
- Maybe set up a read-only database user

## Logging

Save the output:
```bash
python index_align_to_firebase.py 2>&1 | tee run.log
```

