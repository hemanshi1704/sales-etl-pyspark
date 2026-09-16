"""
Upload/download helpers for pushing the processed_zone/ output to AWS S3.

This project's pipeline itself reads/writes to the local filesystem (raw_zone/,
processed_zone/) to keep it runnable without AWS credentials. This module is the
bridge to real S3 storage: point BUCKET_NAME at a real bucket, set AWS credentials
as environment variables (or an AWS CLI profile), and run this script to sync the
processed output up to S3 — or swap the Spark read/write paths in etl_pipeline.py
directly to "s3a://your-bucket/..." (see README "Running against real S3").
"""

import os
import sys

try:
    import boto3
except ImportError:
    boto3 = None

BUCKET_NAME = os.environ.get("SALES_ETL_BUCKET", "your-bucket-name-here")
LOCAL_PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "processed_zone")


def upload_processed_zone(bucket_name: str = BUCKET_NAME, prefix: str = "processed/"):
    """Recursively upload processed_zone/ to s3://bucket_name/prefix/"""
    if boto3 is None:
        raise RuntimeError("boto3 is not installed. Run: pip install boto3")

    s3 = boto3.client("s3")
    uploaded = 0

    for root, _, files in os.walk(LOCAL_PROCESSED_DIR):
        for filename in files:
            local_path = os.path.join(root, filename)
            rel_path = os.path.relpath(local_path, LOCAL_PROCESSED_DIR)
            s3_key = os.path.join(prefix, rel_path).replace(os.sep, "/")

            s3.upload_file(local_path, bucket_name, s3_key)
            uploaded += 1
            print(f"Uploaded s3://{bucket_name}/{s3_key}")

    print(f"\nDone. Uploaded {uploaded} files to s3://{bucket_name}/{prefix}")


if __name__ == "__main__":
    if BUCKET_NAME == "your-bucket-name-here":
        print(
            "Set SALES_ETL_BUCKET env var to your real bucket name first, e.g.\n"
            "  export SALES_ETL_BUCKET=my-sales-etl-bucket\n"
            "  python scripts/s3_utils.py"
        )
        sys.exit(1)

    upload_processed_zone()
