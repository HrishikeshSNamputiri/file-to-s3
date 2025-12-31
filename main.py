import os
import boto3
import hashlib
from dotenv import load_dotenv

# 1. Load Environment Variables
load_dotenv()

ENDPOINT = os.getenv("SUPABASE_S3_ENDPOINT")
REGION = os.getenv("SUPABASE_S3_REGION")
ACCESS_KEY = os.getenv("SUPABASE_S3_ACCESS_KEY")
SECRET_KEY = os.getenv("SUPABASE_S3_SECRET_KEY")
BUCKET_NAME = os.getenv("SUPABASE_S3_BUCKET")

# 2. Initialize S3 Client
s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=REGION
)

def calculate_md5(file_path):
    """Calculates the MD5 hash of a local file to compare with S3 ETag."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        # Read in chunks to handle larger files efficiently
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def get_remote_files(prefix):
    """
    Returns a dictionary of existing S3 files: { 'file_path': 'md5_hash' }
    """
    remote_files = {}
    paginator = s3.get_paginator('list_objects_v2')
    
    # We list everything in the bucket starting with the folder name
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                # S3 ETags are wrapped in quotes (e.g., '"abc123..."'), so we strip them
                etag = obj['ETag'].strip('"') 
                remote_files[key] = etag
    return remote_files

def get_local_files(directory):
    """
    Returns a dictionary of local files: { 'file_path': 'md5_hash' }
    """
    local_files = {}
    for root, _, files in os.walk(directory):
        for file in files:
            full_path = os.path.join(root, file)
            
            # Create a relative path (e.g., boarding_pass/amritapuri/doc.pdf)
            # This ensures the key matches the S3 structure
            rel_path = os.path.relpath(full_path, start=".")
            
            # Ensure forward slashes for S3 keys even if on Windows
            s3_key = rel_path.replace(os.sep, "/")
            
            # Calculate hash
            file_hash = calculate_md5(full_path)
            local_files[s3_key] = file_hash
            
    return local_files

def sync_folder(folder_name):
    print(f"\n--- Syncing Folder: {folder_name} ---")
    
    # Get state of both Local and Remote
    local_map = get_local_files(folder_name)
    remote_map = get_remote_files(folder_name)
    
    local_keys = set(local_map.keys())
    remote_keys = set(remote_map.keys())

# --- STEP 1: DELETE (In Remote but not in Local) ---
    to_delete = remote_keys - local_keys
    if to_delete:
        print(f"Deleting {len(to_delete)} orphaned files from bucket...")
        
        for key in to_delete:
            # We use delete_object (singular) instead of delete_objects (batch)
            try:
                s3.delete_object(Bucket=BUCKET_NAME, Key=key)
                print(f" 🗑️  Deleted: {key}")
            except Exception as e:
                print(f" ❌ Failed to delete {key}: {e}")
    else:
        print("Check: No files to delete.")

    # --- STEP 2: UPLOAD (New or Changed) ---
    to_upload = []
    
    for key in local_keys:
        # Case A: File is completely new
        if key not in remote_keys:
            print(f" 🆕 New file detected: {key}")
            to_upload.append(key)
        
        # Case B: File exists but content (hash) is different
        elif local_map[key] != remote_map[key]:
            print(f" 📝 Change detected in: {key}")
            to_upload.append(key)
        
        # Case C: Identical (Do nothing)
        # else: pass 

    if to_upload:
        print(f"Uploading {len(to_upload)} files...")
        for key in to_upload:
            # Convert S3 key back to local OS path
            local_path = key.replace("/", os.sep)
            s3.upload_file(local_path, BUCKET_NAME, key)
            print(f" ✅ Uploaded: {key}")
    else:
        print("Check: All files are up to date.")

if __name__ == "__main__":
    # The specific folders you want to sync
    target_folders = ["boarding_pass", "passport"]

    for folder in target_folders:
        if os.path.exists(folder):
            sync_folder(folder)
        else:
            print(f"Warning: Local folder '{folder}' does not exist.")
            
    print("\nSync completed successfully.")
