
# Banking Transaction Data Pipeline

**A Config-Driven Data Engineering Project**

An end-to-end banking transaction data engineering project demonstrating data ingestion, quality validation, incremental processing, workflow orchestration, and streaming.

## Architecture

![Banking Data Pipeline Architecture](docs/architecture.png)

The project separates data processing, orchestration, validation, and monitoring into modular components.

### Data Flow

Raw CSV → Bronze → Silver / Quarantine → Gold

- **Bronze:** Stores raw transactions with ingestion metadata.
- **Silver:** Contains validated and standardized transactions.
- **Quarantine:** Preserves rejected records and their rejection reasons.
- **Gold:** Provides aggregated business and data quality metrics.

## Technology Stack

Python · PySpark · Databricks · Delta Lake · Apache Airflow · Docker · YAML · pytest


## Data Quality & Validation

The pipeline implements configurable validation rules using YAML and reusable Python modules.

Validation checks include:
- Transaction timestamp format
- Required account fields
- Positive transaction amounts
- Allowed payment currencies
- Multiple rejection reasons per invalid record

Rejected transactions are preserved in a Quarantine output rather than silently discarded.

The Airflow workflow also performs row-count reconciliation and enforces a configurable rejection-rate threshold.

### Controlled Bad-Data Testing

A seven-record test batch was used to verify failure handling:

| Metric | Result |
|---|---:|
| Input records | 7 |
| Valid records | 2 |
| Rejected records | 5 |
| Rejection rate | 71.43% |

All five rejected records were routed to Quarantine with diagnostic reasons. The quality gate correctly failed because the rejection rate exceeded the configured 5% threshold.

## Incremental Processing & Idempotency

Incremental loading is implemented using Delta Lake MERGE and deterministic transaction identifiers.

The pipeline matches incoming transaction IDs against existing Silver records and inserts only previously unseen transactions.

In the duplicate-only test, reprocessing 1,000 existing transactions left the Silver table unchanged at 200,000 records.

## Structured Streaming

A file-based streaming ingestion prototype was implemented using Spark Structured Streaming.

The experiment demonstrated:
- Processing the first batch of 1,000 records
- Restarting the query without duplicate ingestion
- Processing a second batch of 1,000 records
- Reaching 2,000 total records in the Delta streaming table

Checkpointing tracks streaming progress across query restarts.

## Workflow Orchestration

Apache Airflow runs locally using Docker Compose.

The DAG includes:

1. `validate_input`
2. `bronze_ingestion`
3. `silver_validation`
4. `gold_layer_check`
5. `quality_check`

The local Airflow workflow validates CSV inputs, executes reusable Python validation logic, produces Quarantine CSV output, exchanges row-count metadata through XCom, and enforces data quality checks.

**Architecture boundary:** Databricks notebooks independently implement the Delta Lake Bronze, Silver, Quarantine, and Gold processing layers. The local Airflow DAG does not currently trigger Databricks jobs.

## Testing

Automated tests cover validation functions, invalid transaction scenarios, and Silver/Quarantine routing.

Latest local test result:

**29 passed**

Run the test suite:

```bash
pytest -v
```

## Engineering Design Decisions

- **Separation of concerns:** Reusable validation logic is separated from orchestration code.
- **Config-driven rules:** YAML defines currency lists, timestamp formats, and quality thresholds.
- **Traceability:** Rejection reasons, ingestion metadata, and pipeline run logs support investigation.
- **Idempotency:** Deterministic identifiers and Delta MERGE reduce duplicate insertion during retries.
- **Reliability:** Reconciliation checks and quality gates detect problems beyond execution failures.
- **Modularity:** Local Airflow orchestration and Databricks processing are implemented as distinct components.



## How to Run

### Prerequisites

- Python 3
- Docker Desktop and Docker Compose
- Apache Airflow (via Docker Compose)
- Databricks workspace with Delta Lake support

### 1. Set up the local environment

Clone the repository:

```bash
git clone <your-repository-url>
cd banking-data-platform
```

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

### 2. Prepare the dataset

Download the IBM synthetic AML transaction dataset.

Place a sample CSV at:

```text
data/raw/HI-Small_Trans_200k.csv
```

The development dataset contains 200,000 transaction records.

### 3. Configure validation rules

Edit:

```text
config/transactions.yaml
```

This file controls:

- Allowed currencies
- Timestamp format
- Maximum rejection rate

### 4. Run automated tests

```bash
python -m pytest -v
```

### 5. Start Apache Airflow

The local Airflow environment uses Docker Compose.

Initialize Airflow on first setup:

```bash
docker compose up airflow-init
```

Start the services:

```bash
docker compose up -d
```

Open:

http://localhost:8080

Trigger the DAG:

```text
banking_data_pipeline
```

Review task execution and data quality logs in the Airflow UI.

### 6. Run Databricks processing


The Databricks notebooks implement the Delta Lake
processing layers and two additional processing demonstrations.

**Core pipeline:**

1. `01_bronze_ingestion.py`
2. `02_silver_validation.py`
3. `03_gold_tables.py`

**Additional demonstrations:**

4. `04_incremental_processing.py` — Delta MERGE and idempotency
5. `05_streaming_ingestion.py` — File-based Structured Streaming and checkpointing

Note: The Databricks notebooks are executed separately
from the local Airflow DAG.


### 7. Review the outputs

Databricks Delta tables include:

- `bronze_transactions`
- `silver_transactions`
- `quarantine_transactions`
- `gold_account_daily`
- `gold_currency_flow_daily`
- `gold_data_quality_daily`
- `pipeline_run_log`
- `streaming_bronze_transactions`

The local Airflow workflow also produces:

```text
data/quarantine/quarantine_transactions.csv
```

## Limitations and Future Improvements

This is a portfolio prototype rather than a production banking system.

Current limitations include:

- Local Airflow does not directly trigger Databricks jobs.
- The local pipeline validates transactions but does not reproduce the full Databricks Silver and Gold transformations.
- File-based streaming is demonstrated without Kafka.
- Transaction identifiers are derived from transaction content rather than upstream-issued unique identifiers.

Potential future improvements include Azure storage integration, automated Databricks job orchestration, stronger schema enforcement, centralized monitoring, and production-grade deployment.
