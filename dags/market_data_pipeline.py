import os
import requests
import pandas as pd
import pendulum
from datetime import datetime, timezone
from airflow.sdk import dag, task

API_KEY = os.getenv("COINCAP_API_KEY")
URL = "https://rest.coincap.io/v3/assets"
BASE_PATH = "/opt/airflow/data/raw"


def normalize_schema(raw_json: dict) -> pd.DataFrame:
    """Flattens nested JSON and enforces strict Parquet schema."""
    if not raw_json or 'data' not in raw_json:
        return pd.DataFrame()

    df = pd.DataFrame(raw_json['data'])

    if 'tokens' in df.columns:
        df['tokens'] = df['tokens'].apply(lambda x: str(x) if x is not None else None)

    df['ingested_at'] = datetime.now(timezone.utc)

    return df

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
    def process_and_load(raw_json: dict):
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

    raw_data = extract_data()
    process_and_load(raw_data)


market_data_pipeline()