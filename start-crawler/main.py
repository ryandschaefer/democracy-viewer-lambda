import boto3
import datetime as dt
from dotenv import load_dotenv
import humanize
import os
import time
import util.athena_queries as athena
import util.sql_queries as sql
from util.sql_connect import sql_connect
from util.email import send_email
load_dotenv()

engine, meta = sql_connect()
client = boto3.client(
    "glue"
)

def update_sql(table_name: str, file_type: str):
    if file_type == "datasets":
        sql.complete_upload(engine, table_name)
        template = "upload_complete"
        subject = "Upload Complete"
    elif file_type == "tokens":
        sql.complete_processing(engine, table_name, "tokens")
        template = "processing_complete"
        subject = "Processing Complete"
    else:
        print("Unrecognized file type:", file_type)
        return
    
    print(f"Updated SQL database for { file_type }_{ table_name }")
    
    print("Sending confirmation email...")
    metadata = sql.get_metadata(engine, meta, table_name)
    user = sql.get_user(engine, meta, metadata["email"])
    params = {
        "title": metadata["title"]
    }
    send_email(template, params, subject, user["email"], user)
    print("Email sent to", user["email"])
    
def wait_crawler(crawler_name: str, table_name: str, file_type: str):
    start_time = time.time()
    while True:
        crawler = client.get_crawler(
            Name = crawler_name
        )
        state = crawler["Crawler"]["State"]
        curr_time = humanize.precisedelta(dt.timedelta(seconds = time.time() - start_time))
        print(f"{ curr_time }: Crawler state: { state }")
        
        if state in ["READY", "STOPPING"]:
            print("Crawler run complete")
            update_sql(table_name, file_type)
            return
        else:
            time.sleep(5)

def lambda_handler(event, context):
    crawler_name = os.environ.get("CRAWLER_NAME")
    key = event["Records"][0]["s3"]["object"]["key"]
    table_name = key.split("/")[-1].replace(".parquet", "")
    if key.startswith("tables/datasets_"):
        file_type = "datasets"
    elif key.startswith("tables/tokens_"):
        file_type = "tokens"
    else:
        print("Unrecognized file type:", key)
        return
    
    start_time = time.time()
    while True:
        if athena.table_exists(table_name, file_type):
            print(f"{ file_type }_{ table_name } already uploaded")
            update_sql(table_name, file_type)
            return
        
        crawler = client.get_crawler(
            Name = crawler_name
        )
        
        state = crawler["Crawler"]["State"]
        curr_time = humanize.precisedelta(dt.timedelta(seconds = time.time() - start_time))
        print(f"{ curr_time }: Crawler state: { state }")
        
        if state == "READY":
            client.start_crawler(
                Name = crawler_name    
            )
            print("Crawler started")
            wait_crawler(crawler_name, table_name, file_type)
            return
        else:
            print("Trying again in 1 minute")
            time.sleep(60)
    