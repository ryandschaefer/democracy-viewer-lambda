from dotenv import load_dotenv
import re
import util.s3 as s3
import util.sql_queries as sql
from util.sql_connect import sql_connect
load_dotenv()

def lambda_handler(event, context):
    # Get file name
    key = event["Records"][0]["s3"]["object"]["key"]
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
  
    # Check if metadata exists for this table  
    # engine, meta = sql_connect()
    # try:
    #     metadata = sql.get_metadata(engine, meta, table_name)
    # except Exception as err:
    #     if str(err) == "Query failed":
    #         metadata = None
    #     else:
    #         raise Exception(str(err))
    
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
    engine, meta = sql_connect()
    sql.add_temp_cols(engine, table_name, columns)
        
# lambda_handler({
#   "Records": [
#     {
#       "eventVersion": "2.0",
#       "eventSource": "aws:s3",
#       "awsRegion": "us-east-1",
#       "eventTime": "1970-01-01T00:00:00.000Z",
#       "eventName": "ObjectCreated:Put",
#       "userIdentity": {
#         "principalId": "EXAMPLE"
#       },
#       "requestParameters": {
#         "sourceIPAddress": "127.0.0.1"
#       },
#       "responseElements": {
#         "x-amz-request-id": "EXAMPLE123456789",
#         "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
#       },
#       "s3": {
#         "s3SchemaVersion": "1.0",
#         "configurationId": "testConfigRule",
#         "bucket": {
#           "name": "example-bucket",
#           "ownerIdentity": {
#             "principalId": "EXAMPLE"
#           },
#           "arn": "arn:aws:s3:::example-bucket"
#         },
#         "object": {
#           "key": "temp_uploads/rdschaefer_smu_edu_1744041121737.csv",
#           "size": 1024,
#           "eTag": "0123456789abcdef0123456789abcdef",
#           "sequencer": "0A1B2C3D4E5F678901"
#         }
#       }
#     }
#   ]
# }, None)