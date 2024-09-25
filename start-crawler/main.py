import boto3
from dotenv import load_dotenv
import json
import os
import time
import util.sql_queries as sql
from util.sql_connect import sql_connect
load_dotenv()

engine, meta = sql_connect()
client = boto3.client(
    "glue"    
)

def update_sql(table_name: str, file_type: str):
    if file_type == "dataset":
        sql.complete_upload(engine, table_name)
    elif file_type == "tokens":
        sql.complete_processing(engine, table_name, "tokens")
    else:
        print("Unrecognized file type:", key)
        return
    
def wait_crawler(crawler_name: str, table_name: str, file_type: str):
    while True:
        crawler = client.get_crawler(
            Name = crawler_name
        )
        state = crawler["Crawler"]["State"]
        print(f"Crawler state: { state }")
        
        if state == "READY":
            print("Crawler run complete")
            update_sql(table_name, file_type)
            return
        else:
            time.sleep(5)

def lambda_handler(event, context):
    attempts = 0
    crawler_name = os.environ.get("CRAWLER_NAME")
    key = event["Records"][0]["s3"]["object"]["key"]
    table_name = key.split("/")[-1].replace(".parquet", "")
    if key.startswith("tables/datasets_"):
        file_type = "dataset"
    elif key.startswith("tables/tokens_"):
        file_type = "tokens"
    else:
        print("Unrecognized file type:", key)
        return
    
    while attempts < 5:
        metadata = sql.get_metadata(engine, meta, table_name)
        if (file_type == "dataset" and metadata["uploaded"]) or (file_type == "tokens" and metadata["tokens_done"]):
            print(f"{ file_type } already uploaded")
            return
        
        crawler = client.get_crawler(
            Name = crawler_name
        )
        
        try:
            state = crawler["Crawler"]["State"]
            print(f"Crawler state: { state }")
            
            if state == "READY":
                response = client.start_crawler(
                    Name = crawler_name    
                )
                print(json.dumps(response, indent = 4))
                wait_crawler(crawler_name, table_name, file_type)
                return
            else:
                raise Exception()
        except:
            print("Trying again in 1 minute")
            time.sleep(60)
            attempts += 1
            
    print("Crawler failed after 5 attempts")