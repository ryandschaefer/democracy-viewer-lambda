import datetime as dt
from dotenv import load_dotenv
import humanize
import os
import re
from time import time
import util.s3 as s3
import util.sql_queries as sql
from util.sql_connect import sql_connect
load_dotenv()

def lambda_handler(event, context):
    start_time = time()
    # Get file name
    key = event["Records"][0]["s3"]["object"]["key"]
    print(key)
    # Extract table name from file name
    table_name = key.split("/")[-1].replace(".csv", "")
    print(f"Table: { table_name }")
    # Extract batch from table name
    pattern = re.compile(r"([A-Za-z0-9]+(_[A-Za-z0-9]+)+)-[0-9]+")
    if pattern.match(table_name):
        batch_num = table_name.split("-")[-1]
        table_name = table_name.replace(f"-{ batch_num }", "")
        batch_num = int(batch_num)
    else:
        batch_num = None
    print(f"Batch: { 'N/A' if batch_num is None else batch_num }")
    
    # Set POLARS_TEMP_DIR to Lambda's /tmp directory
    os.environ["POLARS_TEMP_DIR"] = "/tmp"
    
    # Lazy load file
    df = s3.download(key)
    
    # Extract column names
    columns = df.collect_schema().names()
    # Remove columns with no name and rename record_id column
    for col in columns:
        if len(col.strip()) == 0:
            columns.remove(col)
        elif col == "record_id":
            columns.remove(col)
            columns.append("record_id_")
    # Sort columns for consistency
    columns.sort()
    print("Dataset columns:", columns)
    
    # Add columns to temp_cols
    engine, _ = sql_connect()
    sql.add_temp_cols(engine, table_name, columns)
    
    print("Total time: {}".format(humanize.precisedelta(dt.timedelta(seconds = time() - start_time))))
        