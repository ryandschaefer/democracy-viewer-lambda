import boto3
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv
import os
import shutil
import sys

FOLDER = sys.argv[1]
load_dotenv()

# Use TransferConfig to optimize the download
config = TransferConfig(
    multipart_threshold=1024 * 500,  # 100MB
    max_concurrency=10,
    multipart_chunksize=1024 * 500,  # 100MB
    use_threads=True
)
shutil.make_archive(FOLDER, "zip", FOLDER)

s3_client = boto3.client(
    "s3",
    aws_access_key_id = os.environ.get("S3_KEY"),
    aws_secret_access_key = os.environ.get("S3_SECRET"),
    region_name = os.environ.get("S3_REGION")
)
s3_client.upload_file(
    f"{ FOLDER }.zip",
    os.environ.get("S3_BUCKET"),
    f"lambda/{ FOLDER }.zip",
    Config = config
)