from etl import transform_logs, load_to_snowflake
import snowflake.connector
from datetime import datetime
from dotenv import dotenv_values
dotenv_values()

raw_bucket = 'raw-application-logs'
prefix = '2022'
transformed_bucket = 'transformed-application-logs'

config = dict(dotenv_values('.env'))

# Get credentials from environment variable file
def get_database_conn():
    USER= config.get('USER')
    PASSWORD= config.get('PASSWORD')
    ACCOUNT= config.get('ACCOUNT')
    WAREHOUSE= config.get('WAREHOUSE')
    DATABASE= config.get('DATABASE')
    ROLE= config.get('ROLE')
    SCHEMA= config.get('SCHEMA')
    # Create Snowflake database connection object
    conn = snowflake.connector.connect(user= USER, password= PASSWORD, account= ACCOUNT, database= DATABASE, \
    schema= SCHEMA, role= ROLE)
    return conn


# transform_logs(raw_bucket, transformed_bucket, prefix)

load_to_snowflake()

