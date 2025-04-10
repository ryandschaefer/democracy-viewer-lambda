import boto3
import datetime as dt
from dotenv import load_dotenv
import humanize
import os
from time import time
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import util.athena_queries as athena
import util.sql_queries as sql
from util.sql_connect import sql_connect
from util.email import send_email
load_dotenv()

engine, meta = sql_connect()
glue = boto3.client("glue")
s3 = boto3.client(
    "s3",
    region_name = os.environ.get("S3_REGION")
)
s3_fs = pafs.S3FileSystem(
    region=os.environ.get("S3_REGION")
)

# Convert from pyarrow data types to athena data types
def arrow_to_glue_type(arrow_type: str) -> str:
    mapping = {
        "string": "string",
        "int64": "bigint",
        "int32": "int",
        "uint64": "bigint",
        "uint32": "int",
        "double": "double",
        "float": "float",
        "boolean": "boolean",
        "timestamp[us]": "timestamp",
        "timestamp[ns]": "timestamp",
        "date32[day]": "date"
    }
    return mapping.get(arrow_type, "string")

# Extract schema from parquet file
def infer_columns_from_parquet(s3_uri: str) -> list[dict[str, str]]:
    path = s3_uri.replace("s3://", "")
    with s3_fs.open_input_file(path) as f:
        table = pq.read_table(f)
        schema = table.schema
        columns = []
        for field in schema:
            columns.append({
                "Name": field.name,
                "Type": arrow_to_glue_type(str(field.type))
            })
        return columns

# Create a new athena table with the correct schema
def create_athena_table(table_name: str, file_type: str):
    # Load environment variables
    database = os.environ.get("ATHENA_DB")
    bucket = os.environ.get("S3_BUCKET")
    
    # Athena and S3 locations
    athena_table = f"{ file_type }_{ table_name }"
    s3_prefix = f"tables/{ athena_table }/"
    s3_location = f"s3://{ bucket }/{ s3_prefix }"
    
    # Check for parquet file in s3 location
    s3_response = s3.list_objects_v2(Bucket = bucket, Prefix = s3_prefix)
    files = [ obj["Key"] for obj in s3_response.get("Contents", []) if obj["Key"].endswith(".parquet") ]
    if not files:
        raise Exception(f"No valid files found in directory '{ s3_prefix }'")
    
    # Get schema from file
    s3_uri = f"s3://{ bucket }/{ files[0] }"
    columns = infer_columns_from_parquet(s3_uri)

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
    print(f"Created new Athena table: {athena_table}")

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
    table_name = key.split("/")[-1].replace(".parquet", "")
    # Determine if this is a raw dataset or a tokenized file
    if key.startswith("tables/datasets_"):
        file_type = "datasets"
    elif key.startswith("tables/tokens_"):
        file_type = "tokens"
    else:
        raise Exception("Unrecognized file type:", key)
    
    # Check if this table has already been created
    if athena.table_exists(table_name, file_type):
        print(f"{ file_type }_{ table_name } already uploaded")
    else:
        # If not, create the table
        create_athena_table(table_name, file_type)
      
    # Update dataset metadata and send confirmation email  
    update_sql(table_name, file_type)
    
    print(f"Table created in { humanize.precisedelta(dt.timedelta(seconds = time() - start_time)) }")
    