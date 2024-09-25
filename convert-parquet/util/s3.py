import boto3
from boto3.s3.transfer import TransferConfig
import datetime as dt
import humanize
import jwt
import os
import pandas as pd
import polars as pl
from time import time

BASE_PATH = "files/s3"

# Use TransferConfig to optimize the download
config = TransferConfig(
    multipart_threshold=1024 * 500,  # 100MB
    max_concurrency=10,
    multipart_chunksize=1024 * 500,  # 100MB
    use_threads=True
)

def get_creds() -> dict[str, str]:
    return {
        "region": os.environ.get("S3_REGION"),
        "bucket": os.environ.get("S3_BUCKET"),
        "key_": os.environ.get("S3_KEY"),
        "secret": os.environ.get("S3_SECRET")
    }

def upload(df: pl.DataFrame, folder: str, name: str) -> None:
    distributed = get_creds()
    
    # Convert file to parquet
    start_time = time()
    local_file = "{}/{}/{}.parquet".format(BASE_PATH, folder, name)
    df.write_parquet(local_file, use_pyarrow=True)
    print("Conversion time: {}".format(humanize.precisedelta(dt.timedelta(seconds = time() - start_time))))
    
    # Upload file to s3
    if "key_" in distributed.keys() and "secret" in distributed.keys():
        s3_client = boto3.client(
            "s3",
            aws_access_key_id = distributed["key_"],
            aws_secret_access_key = distributed["secret"],
            region_name = distributed["region"]
        )
    else:
        s3_client = boto3.client(
            "s3",
            region_name = distributed["region"]
        )
        
    path = "tables/{}_{}/{}.parquet".format(folder, name, name)
        
    start_time = time()
    s3_client.upload_file(
        local_file,
        distributed["bucket"],
        path,
        Config = config
    )
    print("Upload time: {}".format(humanize.precisedelta(dt.timedelta(seconds = time() - start_time))))
    
def download_data(file: str) -> str:
    distributed = get_creds()
    
    # Download file from s3
    if "key_" in distributed.keys() and "secret" in distributed.keys():
        s3_client = boto3.client(
            "s3",
            aws_access_key_id = distributed["key_"],
            aws_secret_access_key = distributed["secret"],
            region_name = distributed["region"]
        )
    else:
        s3_client = boto3.client(
            "s3",
            region_name = distributed["region"]
        )
        
    start_time = time()
    response = s3_client.get_object(
        Bucket = distributed["bucket"],
        Key = file
    )
    data = response["Body"].read()
    print("Download time: {}".format(humanize.precisedelta(dt.timedelta(seconds = time() - start_time))))
    
    return data
