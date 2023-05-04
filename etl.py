import dask.dataframe as dd
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
import boto3
from datetime import datetime, timedelta
from dateutil.tz import tzutc
from typing import List

# Create an S3 client object
s3 = boto3.client('s3')

# Function to read log files from raw bucket, transform &  load into 'transformed' bucket
def transform_logs(raw_bucket: str, transformed_bucket: str, prefix: str):
    # Define the timestamp of the last log file that was read
    last_read_timestamp = datetime.now(tz=tzutc()) - timedelta(minutes=1)  # Read files modified in the last 1 hour
    # Get a list of all the log file keys in the S3 bucket that were modified after the last read timestamp
    paginator = s3.get_paginator('list_objects_v2')
    filtered_objs = [obj for page in paginator.paginate(Bucket=raw_bucket, Prefix=prefix) for obj in page["Contents"]\
                if obj['LastModified'] < last_read_timestamp]
    obj_keys_path = [f"s3://{raw_bucket}/{obj.get('Key')}" for obj in filtered_objs[1:]]
    # Read the new log files into a Dask dataframe
    if len(obj_keys_path) > 0:
        log_data = dd.read_json(obj_keys_path, orient='records', lines=False)
        # Write data into a different bucket in parquet format
        output_file = f"s3://{transformed_bucket}/{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
        log_data.to_json(output_file)
        print('transformed logs written to parquet file')

def load_to_snowflake(conn: snowflake.connector.connection.SnowflakeConnection, table_name: str, bucket_name: str,):
    last_read_timestamp = datetime.now(tz=tzutc()) - timedelta(minutes=1)  # Read files modified in the last 1 hour
    # Get a list of all the log file keys in the S3 bucket that were modified after the last read timestamp
    paginator = s3.get_paginator('list_objects_v2')
    filtered_objs = [obj for page in paginator.paginate(Bucket=bucket_name) for obj in page["Contents"]\
                if obj['LastModified'] < last_read_timestamp]
    obj_keys_path = [f"s3://{bucket_name}/{obj.get('Key')}" for obj in filtered_objs]
    # Read the new log files into a Dask dataframe
    if len(obj_keys_path) > 0:
        log_data = dd.read_parquet(obj_keys_path, orient='records', lines=False)
        # Write data to Snowflake
        print(f'attempting to load data to {table_name} table in Snowflake...')
        try:
            # the Snowflake connector write_pandas function is used dataframe efficiently into the Snowflake table.
            success, nchunks, nrows, _ = write_pandas(conn= conn, df= log_data.compute(), table_name= table_name.upper(), \
            compression= 'snappy', parallel= 20, chunk_size= 100000)
            print(f"data successfully loaded to '{table_name}' table in Snowflake")
        except ConnectionError: # Check for database connection error
            print("Unable to connect to database!")
        except Exception:
            print('Error writing data to database')
