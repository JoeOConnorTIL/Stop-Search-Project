import os
import time
import json
import requests
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timedelta

# 1. Load environment variables (.env should be in the same folder)

dotenv_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path)

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
TABLE_NAME = os.getenv("TABLE_NAME", "STOP_SEARCH_STAGING")

FORCE = os.getenv("POLICE_FORCE", "metropolitan")  # e.g. 'metropolitan'
START_MONTH = os.getenv("START_MONTH", "2024-01")  # YYYY-MM format
MONTH_LOG_PATH = Path(__file__).parent / "api_month_log.csv"


# 2. Defining Functions

def get_month_list(start_month):
    start_date = datetime.strptime(start_month, "%Y-%m")
    current_date = datetime.today().replace(day=1)
    months = []
    while start_date < current_date:
        months.append(start_date.strftime("%Y-%m"))
        start_date += timedelta(days=32)
        start_date = start_date.replace(day=1)
    return months

def read_month_log():
    if MONTH_LOG_PATH.exists():
        return pd.read_csv(MONTH_LOG_PATH)
    return pd.DataFrame(columns=["month", "endpoint", "timestamp"])

def append_month_log(month, endpoint):
    log_df = read_month_log()
    new_row = pd.DataFrame({"month": [month], "endpoint": [endpoint], "timestamp": [datetime.now()]})
    log_df = pd.concat([log_df, new_row], ignore_index=True)
    log_df.to_csv(MONTH_LOG_PATH, index=False)

def throttle_requests():
    # Sleep to stay within the 15 requests/sec limit
    time.sleep(0.1)

# 3. Fetch Data and Insert to Snowflake

def fetch_stop_search_data(force, date):
    url = f"https://data.police.uk/api/stops-force?force={force}&date={date}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if not data:
            print(f"No stop & search data for {force} in {date}.")
            return None
        return pd.DataFrame(data)
    else:
        print(f"API call failed with status {response.status_code} for {date}.")
        return None

def upload_to_snowflake(df, conn, table_name):
    if df.empty:
        print("No data to upload.")
        return

    cursor = conn.cursor()
    try:
        # Convert any dicts/lists into JSON strings for compatibility
        for col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

        # Dynamically build the insert statement based on DataFrame columns
        cols = list(df.columns)
        placeholders = ", ".join(["%s"] * len(cols))
        columns = ", ".join(cols)

        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Convert NaN to None for Snowflake
        data_tuples = [tuple(None if pd.isna(x) else x for x in row) for row in df.itertuples(index=False)]

        cursor.executemany(sql, data_tuples)
        conn.commit()
        print(f"Inserted {len(df)} records into {table_name}.")
    except Exception as e:
        print(f"Error uploading to Snowflake: {e}")
    finally:
        cursor.close()


# 4. Main Logic

def main():
    months = get_month_list(START_MONTH)
    log_df = read_month_log()

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    for month in months:
        # Skip if already logged
        if ((log_df["month"] == month) & (log_df["endpoint"] == "stop_search")).any():
            print(f"Skipping {month} — already fetched.")
            continue

        print(f"Fetching stop & search data for {FORCE} in {month}...")
        df = fetch_stop_search_data(FORCE, month)
        throttle_requests()

        if df is not None and not df.empty:
            upload_to_snowflake(df, conn, TABLE_NAME)
            append_month_log(month, "stop_search")

    conn.close()
    print("Done.")

# 5. Run

if __name__ == "__main__":
    main()
