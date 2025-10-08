import requests
import pandas as pd
import os
import time
import json
import logging
from shapely.geometry import Polygon, MultiPolygon, Point
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from pathlib import Path

# 1 Setting up a log to record which records have been fetched.

log_path = Path(__file__).parent / "fetch_LSOA.log"
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger().addHandler(console)

# 2 Defining API URL

url = (
    "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
    "Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/"
    "FeatureServer/0/query"
)

# 3 Get total record count - as limit for one call is 2000 so need to cycle through

params_count = {"where": "1=1", "returnCountOnly": "true", "f": "json"}
response = requests.get(url, params=params_count)
total_records = response.json().get("count", 0)
logging.info(f"Total LSOAs to fetch: {total_records}")


# 4 Pagination loop with retries for if time out is reached 

all_features = []
batch_size = 2000
offset = 0
max_retries = 3

while offset < total_records:
    params = {
        "where": "1=1",
        "outSR": 4326,
        "f": "json",
        "outFields": "*",
        "resultOffset": offset,
        "resultRecordCount": batch_size,
    }

    success = False
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=60)
            data = response.json()

            if "features" not in data:
                raise ValueError(f"No 'features' in response: {data.get('error', 'Unknown error')}")

            features_batch = data["features"]
            if not features_batch:
                raise ValueError("Empty feature batch returned")

            all_features.extend(features_batch)
            offset += len(features_batch)
            logging.info(f"Fetched {len(all_features)} / {total_records} features")
            success = True
            break  # exit retry loop on success

        except Exception as e:
            logging.warning(f"Attempt {attempt+1} failed at offset {offset}: {e}")
            time.sleep(5 * (attempt + 1))  # exponential backoff

    if not success:
        logging.error(f"Failed to fetch offset {offset} after {max_retries} retries — skipping batch.")
        offset += batch_size  # skip and move on

logging.info(f"Completed fetching. Total features retrieved: {len(all_features)}")

# 5 Build DataFrame

records = [f["attributes"] for f in all_features]
df = pd.DataFrame(records)
df = df.rename(columns=lambda x: x.upper().replace("__", "_").replace(" ", "_"))

# 6 Convert geometries to WKT(Well Known Text) for easier load to snowflake

geometry_wkt = []
for f in all_features:
    geom = f.get("geometry")
    wkt = None
    try:
        if geom:
            if "rings" in geom:
                rings = geom["rings"]
                if len(rings) == 1:
                    wkt = Polygon(rings[0]).wkt
                else:
                    wkt = MultiPolygon([Polygon(r) for r in rings]).wkt
            elif "x" in geom and "y" in geom:
                wkt = Point(geom["x"], geom["y"]).wkt
    except Exception as e:
        logging.warning(f"Geometry conversion failed: {e}")
    geometry_wkt.append(wkt)

df["GEOMETRY_WKT"] = geometry_wkt

logging.info(f"DataFrame created with {len(df)} rows and {len(df.columns)} columns.")
logging.info(f"Columns: {list(df.columns)}")

# 7 Load Snowflake credentials from .env file

dotenv_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path)

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
TABLE_NAME = os.getenv("TABLE_NAME_LSOA", "LSOA_STAGING")


# 8 Connecting to Snowflake

conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
)
cur = conn.cursor()


# 9 Create or replace table dynamically

column_defs = []
for col in df.columns:
    if col.upper() == "GEOMETRY_WKT":
        column_defs.append(f"{col} STRING")
    elif df[col].dtype == "float64":
        column_defs.append(f"{col} FLOAT")
    elif df[col].dtype == "int64":
        column_defs.append(f"{col} NUMBER")
    else:
        column_defs.append(f"{col} STRING")

create_sql = f"CREATE OR REPLACE TABLE {TABLE_NAME} ({', '.join(column_defs)});"
cur.execute(create_sql)
logging.info(f"Table {TABLE_NAME} created or replaced.")

# Upload data to Snowflake

success, nchunks, nrows, _ = write_pandas(conn, df, TABLE_NAME)
logging.info(f"Uploaded {nrows} rows to {TABLE_NAME} ({nchunks} chunk(s))")

cur.close()
conn.close()
logging.info("All done — LSOA geometries successfully loaded into Snowflake.")
