# Cryptocurrency Market Data Pipeline

An automated, containerized Data Engineering pipeline that extracts real-time cryptocurrency market data from the CoinCap API and stages it in a local Data Lakehouse architecture.

## 🏗️ Architecture (Current State)
This project currently implements the **Bronze Layer** (Raw Ingestion) of a Medallion Architecture.

1. **Extract:** Apache Airflow hits the CoinCap REST API `v3/assets` endpoint daily.
2. **Normalize:** Nested JSON structures are flattened and schema-enforced.
3. **Load:** Data is partitioned by execution date (`year/month/day`) and saved as Snappy-compressed Parquet files.

## 🛠️ Tech Stack
* **Orchestration:** Apache Airflow 3.1.7 (TaskFlow API)
* **Processing:** Python, Pandas, PyArrow
* **Storage format:** Apache Parquet (Snappy compression)
* **Infrastructure:** Docker & Docker Compose (LocalExecutor)

## 🚀 How to Run Locally

### Prerequisites
* Docker and Docker Compose installed
* A free [CoinCap API Key](https://coincap.io/)