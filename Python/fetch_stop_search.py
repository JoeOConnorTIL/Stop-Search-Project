import os
import time
import json
import requests
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timedelta

# 1. Load environment variables
dotenv_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path)

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
TABLE_NAME = os.getenv("TABLE_NAME_SS", "STOP_SEARCH_STAGING")
START_MONTH = os.getenv("START_MONTH", "2024-01")
MONTH_LOG_PATH = Path(__file__).parent / "api_month_log.csv"

# 2. Helper Functions
def get_month_list(start_month):
    start_date = datetime.strptime(start_month, "%Y-%m")
    # Last full month
    current_date = (datetime.today().replace(day=1) - timedelta(days=1)).replace(day=1)
    months = []
    while start_date <= current_date:
        months.append(start_date.strftime("%Y-%m"))
        start_date += timedelta(days=32)
        start_date = start_date.replace(day=1)
    return months

def read_month_log():
    if MONTH_LOG_PATH.exists():
        return pd.read_csv(MONTH_LOG_PATH)
    return pd.DataFrame(columns=["month", "force", "endpoint", "timestamp"])

def append_month_log(month, force, endpoint):
    log_df = read_month_log()
    new_row = pd.DataFrame({
        "month": [month],
        "force": [force],
        "endpoint": [endpoint],
        "timestamp": [datetime.now()]
    })
    log_df = pd.concat([log_df, new_row], ignore_index=True)
    log_df.to_csv(MONTH_LOG_PATH, index=False)

def throttle_requests():
    time.sleep(0.1)  # Keep within API limits

def fetch_forces():
    url = "https://data.police.uk/api/forces"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return [f['id'] for f in response.json()]
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch forces list: {e}")
        return []

def fetch_stop_search_data(force, date, max_retries=5):
    url = f"https://data.police.uk/api/stops-force?force={force}&date={date}"
    attempt = 0
    while attempt < max_retries:
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            if not data:
                print(f"No stop & search data for {force} in {date}.")
                return None
            df = pd.DataFrame(data)
            df["force_name"] = force
            return df
        except requests.exceptions.RequestException as e:
            attempt += 1
            wait = 2 ** attempt
            print(f"Request failed (attempt {attempt}/{max_retries}): {e}. Retrying in {wait} seconds...")
            time.sleep(wait)
    print(f"Failed to fetch data for {force} in {date} after {max_retries} attempts. Skipping.")
    return None

def upload_to_snowflake(df, conn, table_name):
    if df.empty:
        print("No data to upload.")
        return False

    cursor = conn.cursor()
    try:
        # Convert dicts/lists to JSON strings
        for col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

        cols = list(df.columns)
        placeholders = ", ".join(["%s"] * len(cols))
        columns = ", ".join(cols)

        # Create table if not exists
        col_defs = ", ".join([f"{c} STRING" for c in cols])
        sql_create = f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs})"
        cursor.execute(sql_create)

        # Check for new columns and add dynamically
        cursor.execute(f"SHOW COLUMNS IN TABLE {table_name}")
        existing_cols = {row[2].lower() for row in cursor.fetchall()}  # column_name is index 2
        for c in cols:
            if c.lower() not in existing_cols:
                cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN "{c}" STRING')
                print(f"Added new column {c} to table {table_name}")

        # Insert data
        sql_insert = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
        data_tuples = [tuple(None if pd.isna(x) else x for x in row) for row in df.itertuples(index=False)]
        cursor.executemany(sql_insert, data_tuples)
        conn.commit()
        print(f"Inserted {len(df)} records into {table_name}.")
        return True
    except Exception as e:
        print(f"Error uploading to Snowflake: {e}")
        return False
    finally:
        cursor.close()

# 3. Main Logic
def main():
    months = get_month_list(START_MONTH)
    log_df = read_month_log()
    forces = fetch_forces()
    if not forces:
        print("No forces found. Exiting.")
        return

    print(f"Found {len(forces)} forces.")

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    total_forces = len(forces)
    total_months = len(months)

    # Collect summary info
    summary = []

    for i, force in enumerate(forces, start=1):
        for j, month in enumerate(months, start=1):
            if ((log_df["month"] == month) & (log_df["force"] == force) & (log_df["endpoint"] == "stop_search")).any():
                print(f"Skipping {force} {month} — already fetched.")
                continue

            print(f"Fetching stop & search data for {force} in {month}...")
            df = fetch_stop_search_data(force, month)
            throttle_requests()

            if df is not None and not df.empty:
                success = upload_to_snowflake(df, conn, TABLE_NAME)
                if success:
                    append_month_log(month, force, "stop_search")

                # Record summary
                summary.append({
                    "force": force,
                    "month": month,
                    "rows": len(df),
                    "columns": list(df.columns),
                    "null_percentage": df.isna().mean().mean() * 100
                })

            print(f"Progress: Force {i}/{total_forces}, Month {j}/{total_months} processed.\n")

    conn.close()
    print("Done.\n")

    # Print summary report
    if summary:
        summary_df = pd.DataFrame(summary)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 200)
        print("Summary Report (per force/month):")
        print(summary_df.sort_values(["force", "month"]).to_string(index=False))
    else:
        print("No data fetched, so no summary to show.")

if __name__ == "__main__":
    main()
