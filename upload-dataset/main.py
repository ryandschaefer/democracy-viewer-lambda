from dotenv import load_dotenv
import util.s3 as s3
import util.sql_queries as sql
from util.sql_connect import sql_connect
load_dotenv()

def lambda_handler(event, context):
    # Get file name
    key = event["Records"][0]["s3"]["object"]["key"]
    # Extract table name from file name
    table_name = key.split("/")[-1].replace(".csv", "")
  
    # Check if metadata exists for this table  
    engine, meta = sql_connect()
    try:
        metadata = sql.get_metadata(engine, meta, table_name)
    except Exception as err:
        if str(err) == "Query failed":
            metadata = None
        else:
            raise Exception(str(err))
    
    # Lazy load file
    df = s3.download(key)
    # If metadata, batch upload
    if metadata is not None:
        # Make sure columns match
        old_columns = sql.get_cols(engine, table_name).sort()
        new_columns = df.collect_schema().names().sort()
        
        print("Old columns:", old_columns)
        print("New columns:", new_columns)
        
        if str(old_columns) != str(new_columns):
            raise Exception("Old and new columns do not match")
        
        columns = new_columns
        batch_num = metadata["num_batches"] + 1
        
        sql.update_num_batches(engine, table_name, batch_num)
    else:
        # If no metadata, new dataset
        columns = df.collect_schema().names()
        columns.sort()
        
        sql.add_temp_cols(engine, table_name, columns)
        batch_num = None
        
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