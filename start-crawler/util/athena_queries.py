import boto3
import os
import time

athena_client = boto3.client(
    "athena",
    aws_access_key_id = os.environ.get("S3_KEY"),
    aws_secret_access_key = os.environ.get("S3_SECRET"),
    region_name = os.environ.get("S3_REGION")
)

def table_exists(table_name: str, file_type: str):
    # Query to check if the table exists
    query = f"SHOW TABLES IN { os.environ.get('ATHENA_DB') } LIKE '{ file_type }_{ table_name }'"
    
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': os.environ.get("ATHENA_DB")
        },
        ResultConfiguration = {
            "OutputLocation": os.environ.get("ATHENA_OUTPUT")
        }
    )
    
    query_execution_id = response['QueryExecutionId']
    
    # Wait for the query to complete
    while True:
        result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = result['QueryExecution']['Status']['State']
        if state == 'SUCCEEDED':
            break
        elif state in ['FAILED', 'CANCELLED']:
            raise Exception(f"Query failed or was cancelled: {state}")
        time.sleep(1)  # wait before checking the status again

    # Fetch the query results
    result = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    rows = result['ResultSet']['Rows']
    
    # If the table exists, there will be a row with the table name in the result
    if len(rows) > 1:
        return True
    else:
        return False