"""
upload_to_minio.py

Uploads generated raw_data files to MinIO (S3-compatible storage).
Run this after generate_sample_data.py to simulate files arriving
from an upstream system into object storage.

Usage:
    pip install boto3
    python3 scripts/upload_to_minio.py
    python3 scripts/upload_to_minio.py --folder orders
"""

import argparse
import os

import boto3
from botocore.client import Config

# ---------------------------------------------------------------------------
# Configuration — reads from environment variables
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "retail-data")

RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "raw_data")

SOURCE_FOLDERS = ["orders", "order_items", "customers", "products", "shipments", "payments"]


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def upload_folder(s3, folder: str) -> int:
    """Upload all files in a raw_data subfolder to MinIO."""
    folder_path = os.path.join(RAW_DATA_DIR, folder)
    if not os.path.exists(folder_path):
        print(f"[minio] Folder not found, skipping: {folder_path}")
        return 0

    uploaded = 0
    for file_name in sorted(os.listdir(folder_path)):
        if file_name.startswith("."):
            continue
        file_path = os.path.join(folder_path, file_name)
        s3_key = f"raw_data/{folder}/{file_name}"

        s3.upload_file(file_path, MINIO_BUCKET, s3_key)
        print(f"[minio] Uploaded: {s3_key}")
        uploaded += 1

    return uploaded


def main(folder: str = None):
    print("=" * 55)
    print("Retail Analytics — Upload Raw Data to MinIO")
    print("=" * 55)
    print(f"Endpoint : {MINIO_ENDPOINT}")
    print(f"Bucket   : {MINIO_BUCKET}")
    print()

    s3 = get_s3_client()

    # Ensure bucket exists
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=MINIO_BUCKET)
        print(f"[minio] Created bucket: {MINIO_BUCKET}")

    folders = [folder] if folder else SOURCE_FOLDERS
    total = 0
    for f in folders:
        count = upload_folder(s3, f)
        total += count

    print(f"\nDone. {total} file(s) uploaded to s3://{MINIO_BUCKET}/raw_data/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload raw data files to MinIO")
    parser.add_argument("--folder", type=str, default=None,
                        help="Upload only this folder (e.g. orders). Default: all folders.")
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()
    main(folder=args.folder)
