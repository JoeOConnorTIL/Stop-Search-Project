import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Load IMD 2019 data
file_path = Path(__file__).parent.parent / "Data" / "imd2019lsoa.csv.xlsx"
df = pd.read_excel(file_path)
df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
df["MEASUREMENT"] = df["MEASUREMENT"].str.strip()
print(f"Loaded {len(df):,} rows | Columns: {list(df.columns)}")

# Load Snowflake credentials
load_dotenv(Path(__file__).parent / ".env")

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)
cur = conn.cursor()

table_name = "IMD_2019_LSOA_STAGING"

# Build table definition dynamically
cols = []
for c in df.columns:
    t = "FLOAT" if df[c].dtype == "float64" else "NUMBER" if df[c].dtype == "int64" else "STRING"
    cols.append(f"{c} {t}")

create_sql = f"CREATE OR REPLACE TABLE {table_name} ({', '.join(cols)})"
cur.execute(create_sql)
print(f"Created or replaced table: {table_name}")

# Upload data
success, nchunks, nrows, _ = write_pandas(conn, df, table_name)
print(f"Uploaded {nrows:,} rows ({nchunks} chunk(s)) to {table_name}")

cur.close()
conn.close()
print("Upload complete.")
