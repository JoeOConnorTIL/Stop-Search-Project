import pandas as pd
from pathlib import Path
import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# Load Excel file
excel_path = Path(__file__).parent.parent / "Data" / "LSOA2011_2021_Lookup.xlsx"

df = pd.read_excel(excel_path)

print(df.head())
print(f"Shape of aggregated DataFrame: {df.shape}")

# Load Snowflake credentials from .env
dotenv_path = Path(__file__).parent / ".env"  # .env is in Python folder
load_dotenv(dotenv_path)

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
TABLE_NAME = "LSOA_2011_2021_LOOKUP"

# Connect to Snowflake 
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)

#  Upload to Snowflake
success, nchunks, nrows, _ = write_pandas(
    conn,
    df,
    TABLE_NAME,
    auto_create_table=True  # automatically create table if it doesn't exist
)

print(f"Uploaded {nrows} rows to {TABLE_NAME} ({nchunks} chunk(s))")
