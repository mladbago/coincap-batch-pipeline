import os

import boto3
import requests
import pandas as pd
import pendulum
from datetime import datetime, timezone
from airflow.sdk import dag, task

API_KEY = os.getenv("COINCAP_API_KEY")
URL = "https://rest.coincap.io/v3/assets"
BASE_PATH = "/opt/airflow/data/raw"
S3_PREFIX = "raw"

def normalize_schema(raw_json: dict) -> pd.DataFrame:
    """Flattens nested JSON and enforces strict Parquet schema."""
    if not raw_json or 'data' not in raw_json:
        return pd.DataFrame()

    df = pd.DataFrame(raw_json['data'])

    if 'tokens' in df.columns:
        df['tokens'] = df['tokens'].apply(lambda x: str(x) if x is not None else None)

    df['ingested_at'] = datetime.now(timezone.utc)

    return df

def generate_s3_key(local_path: str, base_dir: str, s3_prefix: str) -> str:
    """Calculates the S3 destination path while preserving Hive partitions."""
    # Example: converts "/opt/airflow/data/raw/year=2026/..." to "market_data/year=2026/..."
    relative_path = os.path.relpath(local_path, base_dir)
    return os.path.join(s3_prefix, relative_path).replace("\\", "/")

def execute_s3_upload(local_path: str, bucket: str, s3_key: str) -> None:
    """Handles the boto3 client authentication and file upload."""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    s3_client.upload_file(local_path, bucket, s3_key)

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 2, 27, tz="UTC"),
    catchup=False,
    tags=["market_data", "coincap", "ELT"],
    default_args={"owner": "airflow", "retries": 1}
)
def market_data_pipeline():
    @task()
    def extract_data() -> dict:
        headers = {"Authorization": f"Bearer {API_KEY}"} if API_KEY else {}
        params = {'limit': 50}

        try:
            response = requests.get(URL, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"API Error: {e}")
            raise

    @task()
    def process_and_save_locally(raw_json: dict) -> str:
        df = normalize_schema(raw_json)

        if df.empty:
            print("No data to save, skipping.")
            return

        now = datetime.now(timezone.utc)
        partition = f"year={now.year}/month={now.strftime('%m')}/day={now.strftime('%d')}"

        full_path = os.path.join(BASE_PATH, partition)
        os.makedirs(full_path, exist_ok=True)

        timestamp_str = now.strftime('%H%M%S')
        local_file = os.path.join(full_path, f"assets_{timestamp_str}.parquet")

        df.to_parquet(local_file, index=False, engine='pyarrow', compression='snappy')
        print(f"✅ Data successfully saved to: {local_file}")

        return local_file

    @task()
    def upload_to_s3(local_file_path: str):
        if not local_file_path or not os.path.exists(local_file_path):
            print(f"File not found: {local_file_path}")
            return

        bucket_name = os.getenv("S3_BUCKET_NAME")
        if not bucket_name:
            raise ValueError("S3_BUCKET_NAME environment variable is not set.")

        s3_key = generate_s3_key(local_file_path, BASE_PATH, S3_PREFIX)

        print(f"Uploading to s3://{bucket_name}/{s3_key}...")
        execute_s3_upload(local_file_path, bucket_name, s3_key)

        os.remove(local_file_path)
        print("✅ Upload successful. Local file deleted.")

    raw_data = extract_data()
    saved_file_path = process_and_save_locally(raw_data)
    upload_to_s3(saved_file_path)

market_data_pipeline()