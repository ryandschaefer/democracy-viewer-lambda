import boto3
import datetime as dt
from dotenv import load_dotenv
import humanize
import os
import time
import util.athena_queries as athena
import util.sql_queries as sql
from util.sql_connect import sql_connect
load_dotenv()

engine, meta = sql_connect()
client = boto3.client(
    "glue"
)

def update_sql(table_name: str, file_type: str):
    if file_type == "datasets":
        sql.complete_upload(engine, table_name)
    elif file_type == "tokens":
        sql.complete_processing(engine, table_name, "tokens")
    else:
        print("Unrecognized file type:", file_type)
        return
    
    print("Updated SQL database")
    
def wait_crawler(crawler_name: str, table_name: str, file_type: str):
    start_time = time.time()
    while True:
        crawler = client.get_crawler(
            Name = crawler_name
        )
        state = crawler["Crawler"]["State"]
        curr_time = humanize.precisedelta(dt.timedelta(seconds = time.time() - start_time))
        print(f"{ curr_time }: Crawler state: { state }")
        
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
        file_type = "datasets"
    elif key.startswith("tables/tokens_"):
        file_type = "tokens"
    else:
        print("Unrecognized file type:", key)
        return
    
    start_time = time.time()
    while attempts < 5:
        if athena.table_exists(table_name, file_type):
            print(f"{ file_type } already uploaded")
            update_sql(table_name, file_type)
            return
        
        crawler = client.get_crawler(
            Name = crawler_name
        )
        
        try:
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
                raise Exception()
        except:
            print("Trying again in 1 minute")
            time.sleep(60)
            attempts += 1
            
    print("Crawler failed after 5 attempts")
    