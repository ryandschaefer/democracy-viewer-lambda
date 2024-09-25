from time import time
init_time = time()
from dotenv import load_dotenv
load_dotenv()
import io
import polars as pl
from time import time
import util.s3 as s3
print("Import time: {} seconds".format(time() - init_time))

def lambda_handler(event, context):
    # Get table name and file extension from command line argument
    s3_key: str = event["Records"][0]["s3"]["object"]["key"]
    file = s3_key.split("/")[-1].split(".")
    ext = file[-1]
    table_name = ".".join(file[:-1])

    start_time = time()
    # Download raw upload data
    data = s3.download_data(s3_key)
    # Parse the file based on the specified file type
    if ext.lower() == 'csv':
        # Parse CSV file
        df = pl.read_csv(io.BytesIO(data))
    elif ext.lower() in ['xlsx', 'xls']:
        # Parse Excel file
        df = pl.read_excel(io.BytesIO(data), engine='openpyxl')
    else:
        raise ValueError("Unsupported file type. Use 'csv', 'xlsx', or 'xls'.")
    print("Download time: {} seconds".format(time() - start_time))
    df = df.with_row_index("record_id")
    start_time = time()
    # Upload file as parquet to s3
    s3.upload(df, "datasets", table_name)
    print("Upload time: {} seconds".format(time() - start_time))
    print("Total time: {} seconds".format(time() - init_time))