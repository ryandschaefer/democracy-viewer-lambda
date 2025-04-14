import boto3
from dotenv import load_dotenv
import os
import polars as pl
import re
import util.s3 as s3
import util.sql_queries as sql
from util.sql_connect import sql_connect
load_dotenv()

engine, meta = sql_connect()

# Check how many chunks the dataset should be broken into
def data_split_batches(data: pl.DataFrame, table_name: str):
    text_cols = sql.get_text_cols(engine, table_name)
    df = data.clone()
    
    # Compute the total length of the text columns
    df = df.with_columns(text_col_length_ = pl.lit(0))
    for col in text_cols:
        df = df.with_columns(text_col_length_ = pl.col("text_col_length_") + pl.col(col).str.len_chars())
    
    # Max text character length in a batch
    batch_size = 100000000 
    # Get cumulative sum of text column lengths
    df = df.with_columns(text_col_length_cum_ = pl.col("text_col_length_").cum_sum())
    # Loop until all batches have been found
    i = 1
    all_batches: list[pl.DataFrame] = []
    while True:
        # Extract next batch
        df_batch = df.filter(
            (pl.col("text_col_length_cum_") >= (i - 1) * batch_size) &
            (pl.col("text_col_length_cum_") < i * batch_size)
        )
        
        if len(df_batch) > 0:
            # Add to list if at least one record
            all_batches.append(df_batch.drop(["text_col_length_", "text_col_length_cum_"]))
        else:
            # Found all batches if no records
            
            # Return batches with the total text character length of this dataset
            total_length = df["text_col_length_"].sum()
            return all_batches, total_length
        
# Function to submit a batch job to process a dataset or batch
def submit_batch_job(table_name: str, batch_num: int | None, total_length: int):
    # Initialize the AWS Batch client
    batch_client = boto3.client('batch')
    
    # Choose whether to use a small or large processing environment
    if total_length < 10000000:
        batch_queue = os.getenv('BATCH_QUEUE')
        batch_def = os.getenv('BATCH_DEF')
        num_threads = 4
    else:
        batch_queue = os.getenv('BATCH_QUEUE_LARGE')
        batch_def = os.getenv('BATCH_DEF_LARGE')
        num_threads = 14
    
    # Setup input parameters
    if batch_num is None:
        name = table_name
        params = {
            "table_name": table_name,
            "num_threads": num_threads
        }
    else:
        name = f"table_name-{ batch_num }"
        params = {
            "table_name": table_name,
            "num_threads": num_threads,
            "batch_num": batch_num
        }

    # Submit the job
    response = batch_client.submit_job(
        jobName=name,
        jobQueue=batch_queue,
        jobDefinition=batch_def,
        parameters=params
    )
    
    print("Batch job submitted:")
    print(response)
        
def lambda_handler(event, context):
    # Get file name
    key: str = event["Records"][0]["s3"]["object"]["key"]
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
    
    # Load data
    df = s3.download(key).collect()
    
    # Rename any column called "record_id"
    if "record_id" in df.columns:
        df = df.rename({ "record_id": "record_id_" })
    # Drop columns with no name
    for col in df.columns:
        if len(col.strip()) == 0:
            df = df.drop(col)
    # Add record id column
    df = df.with_row_index("record_id")
    
    # Determine how many batches are in the file
    batches, total_length = data_split_batches(df, table_name)
    del df
    
    if len(batches) == 1:
        # Dataset is small enough for only 1 batch
        df = batches[0]
        
        if batch_num is None:
            # This is the whole dataset
            s3.upload(batches[0], "datasets", table_name)
        else:
            # This is adding to an existing dataset
            s3.upload(batches[0], "datasets", table_name, batch_num)
    else:
        # Dataset required multiple batches
    
        if batch_num is None:
            # This is the first set of batches for this dataset
            batch_num = 0
            
        for i, batch in enumerate(batches):
            s3.upload(batch, "datasets", table_name, batch_num + i)
            
    # Update the number of batches in sql
    if batch_num is not None:
        sql.update_num_batches(engine, table_name, batch_num + len(batches) - 1)
      
    # Submit processing job to batch      
    submit_batch_job(table_name, batch_num + 1, total_length)
