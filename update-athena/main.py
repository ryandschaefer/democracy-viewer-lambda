import boto3
import datetime as dt
from dotenv import load_dotenv
import humanize
import os
from time import time
import re
import util.s3 as s3
import util.sql_queries as sql
from util.sql_connect import sql_connect
from util.email import send_email
load_dotenv()

engine, meta = sql_connect()
glue = boto3.client("glue")

# Check if an athena table alreadys exists
def athena_table_exists(name: str):
    try:
        response = glue.get_table(
            DatabaseName=os.environ.get("ATHENA_DB"),
            Name=name
        )
        
        return True
    except glue.exceptions.EntityNotFoundException:
        return False

# Convert from pyarrow data types to athena data types
def arrow_to_glue_type(arrow_type: str) -> str:
    mapping = {
        "Utf8": "string",
        "Int64": "bigint",
        "Int32": "int",
        "UInt64": "bigint",
        "UInt32": "int",
        "Float64": "double",
        "Float32": "float",
        "Boolean": "boolean",
        "Datetime": "timestamp",
        "Date": "date"
    }
    return mapping.get(arrow_type, "string")

# Extract schema from parquet file
def infer_columns_from_parquet(name: str) -> list[dict[str, str]]:
    df = s3.download(name)
    schema = df.collect_schema()
    
    columns = []
    for col, dtype in schema.items():
        columns.append({
            "Name": col,
            "Type": arrow_to_glue_type(str(dtype))
        })
        
    return columns

# Create a new athena table with the correct schema
def create_athena_table(table_name: str, file_type: str, batch_num: int | None = None):
    start_time = time()
    
    # Load environment variables
    database = os.environ.get("ATHENA_DB")
    bucket = os.environ.get("S3_BUCKET")
    
    # Athena and S3 locations
    athena_table = f"{ file_type }_{ table_name }"
    s3_prefix = f"tables/{ athena_table }/"
    s3_location = f"s3://{ bucket }/{ s3_prefix }"
    print(f"Creating table { athena_table }...")
    
    # Get schema from file
    if batch_num is None:
        name = f"{ s3_prefix }{ table_name }.parquet"
    else:
        name = f"{ s3_prefix }{ table_name }-{ batch_num }.parquet"
    columns = infer_columns_from_parquet(name)
    print(columns)

    # Parquet file format
    input_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    serde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

    # Athena table config
    storage_descriptor = {
        "Columns": columns,
        "Location": s3_location,
        "InputFormat": input_format,
        "OutputFormat": output_format,
        "SerdeInfo": {
            "SerializationLibrary": serde,
            "Parameters": {"serialization.format": "1"}
        }
    }
    table_input = {
        "Name": athena_table,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet"},
        "StorageDescriptor": storage_descriptor,
        "PartitionKeys": []
    }
    
    # Create table
    glue.create_table(DatabaseName=database, TableInput=table_input)
    print(f"Created new Athena table: {athena_table} in { humanize.precisedelta(dt.timedelta(seconds = time() - start_time)) }")

def update_sql(table_name: str, file_type: str):
    # Update database and set email template based on which file this is
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
    
    # Send confirmation email to dataset owner
    print("Sending confirmation email...")
    metadata = sql.get_metadata(engine, meta, table_name)
    user = sql.get_user(engine, meta, metadata["email"])
    params = {
        "title": metadata["title"]
    }
    send_email(template, params, subject, user["email"], user)
    print("Email sent to", user["email"])

def lambda_handler(event, context):
    start_time = time()
    
    # Get table name from file path
    key = event["Records"][0]["s3"]["object"]["key"]
    print(key)
    table_name = key.split("/")[-1].replace(".parquet", "")
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
    # Determine if this is a raw dataset or a tokenized file
    if key.startswith("tables/datasets_"):
        file_type = "datasets"
    elif key.startswith("tables/tokens_"):
        file_type = "tokens"
    else:
        raise Exception("Unrecognized file type:", key)
    
    # Check if this table has already been created
    athena_table_name = f"{ file_type }_{ table_name }"
    if athena_table_exists(athena_table_name):
        print(f"{ athena_table_name } already uploaded")
    else:
        # If not, create the table
        create_athena_table(table_name, file_type, batch_num)
      
        # Update dataset metadata and send confirmation email  
        update_sql(table_name, file_type)
    
    print(f"Table created in { humanize.precisedelta(dt.timedelta(seconds = time() - start_time)) }")
    