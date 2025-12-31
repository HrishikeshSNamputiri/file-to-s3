import os
import boto3
from dotenv import load_dotenv

# 1. Load environment variables
load_dotenv()

# 2. Configuration
ENDPOINT = os.getenv("SUPABASE_S3_ENDPOINT")
REGION = os.getenv("SUPABASE_S3_REGION")
ACCESS_KEY = os.getenv("SUPABASE_S3_ACCESS_KEY")
SECRET_KEY = os.getenv("SUPABASE_S3_SECRET_KEY")
BUCKET_NAME = os.getenv("SUPABASE_S3_BUCKET")

# 3. Initialize the S3 Client
s3_client = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=REGION
)

def upload_directory(directory_name):
    """
    Walks through a local directory and uploads files to Supabase S3
    maintaining the folder structure.
    """
    if not os.path.exists(directory_name):
        print(f"Warning: Directory '{directory_name}' not found. Skipping.")
        return

    print(f"--- Starting upload for: {directory_name} ---")
    
    # os.walk yields a 3-tuple (root, directories, files)
    for root, dirs, files in os.walk(directory_name):
        for file in files:
            # Construct the full local path
            local_path = os.path.join(root, file)
            
            # Construct the S3 Key (path inside the bucket).
            # We use the local_path but ensure forward slashes for S3 compatibility.
            # This preserves the structure: boarding_pass/amritapuri/file.pdf
            s3_key = local_path.replace(os.sep, "/")

            try:
                print(f"Uploading: {s3_key}...")
                s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
            except Exception as e:
                print(f"Failed to upload {local_path}: {e}")

# 4. Main Execution
if __name__ == "__main__":
    # List of specific folders you want to upload
    target_folders = ["boarding_pass", "passport"]

    for folder in target_folders:
        upload_directory(folder)

    print("\nAll uploads completed!")
