import sys
import csv



from pathlib import Path
from datetime import datetime

INPUT_FILE = Path(
    "/opt/airflow/data/raw/HI-Small_Trans_200k.csv"
)

# 先告诉 Python 去哪里找 src
sys.path.append("/opt/airflow")

# 再 import src 里的代码
from src.validators import validate_transaction
from src.config_loader import load_config

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def validate_input():
    file_path = INPUT_FILE

    if not file_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {file_path}"
        )

    file_size = file_path.stat().st_size

    if file_size == 0:
        raise ValueError("Input file is empty")

    print(f"Input file found: {file_path}")
    print(f"File size: {file_size} bytes")


import csv
from pathlib import Path


def bronze_ingestion():
    file_path = INPUT_FILE

    row_count = 0

    with file_path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)

        header = next(reader)

        for _ in reader:
            row_count += 1

    print(f"Columns: {header}")
    print(f"Row count: {row_count}")

    if row_count == 0:
        raise ValueError("Bronze ingestion found no rows")

    return {
    "input_count": row_count
    }

def silver_validation():
    config = load_config(
        "/opt/airflow/config/transactions.yaml"
    )

    timestamp_format = config["validation"]["timestamp_format"]

    allowed_currencies = set(
        config["validation"]["allowed_currencies"]
    )

    print(
        f"Timestamp format from config: "
        f"{timestamp_format}"
    )

    print(
        f"Allowed currencies count: "
        f"{len(allowed_currencies)}"
    )

    input_file = INPUT_FILE

    quarantine_dir = Path(
        "/opt/airflow/data/quarantine"
    )

    quarantine_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    quarantine_file = (
        quarantine_dir
        / "quarantine_transactions.csv"
    )

    valid_count = 0
    rejected_count = 0

    quarantine_records = []

    with input_file.open(
        "r",
        encoding="utf-8"
    ) as f:

        reader = csv.reader(f)

        # 跳过 header
        next(reader)

        for row in reader:
            transaction = {
                "transaction_time": row[0],
                "from_account": row[2],
                "to_account": row[4],
                "amount_paid": float(row[7]),
                "payment_currency": row[8],
            }

            result = validate_transaction(
                transaction,
                allowed_currencies,
                timestamp_format
            )

            if result["is_valid"]:
                valid_count += 1

            else:
                rejected_count += 1

                quarantine_record = {
                    **transaction,
                    "rejection_reasons": "|".join(
                        result["rejection_reasons"]
                    ),
                }

                quarantine_records.append(
                    quarantine_record
                )

    quarantine_columns = [
        "transaction_time",
        "from_account",
        "to_account",
        "amount_paid",
        "payment_currency",
        "rejection_reasons",
    ]

    with quarantine_file.open(
        "w",
        encoding="utf-8",
        newline=""
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=quarantine_columns
        )

        writer.writeheader()

        writer.writerows(
            quarantine_records
        )

    print(f"Valid rows: {valid_count}")
    print(f"Rejected rows: {rejected_count}")

    print(
        f"Quarantine file written to: "
        f"{quarantine_file}"
    )

    if valid_count + rejected_count == 0:
        raise ValueError(
            "No transactions were validated"
        )

    return {
        "valid_count": valid_count,
        "rejected_count": rejected_count,
        "total_count": (
            valid_count + rejected_count
        ),
        "quarantine_file": str(
            quarantine_file
        ),
    }

def gold_layer_check():
    print(
        "Gold layer is implemented in Databricks notebooks "
        "and stored as Delta tables."
    )

    print(
        "Expected Gold tables: "
        "gold_currency_flow_daily, "
        "gold_account_daily, "
        "gold_data_quality_daily"
    )


def quality_check(ti):
    config = load_config(
        "/opt/airflow/config/transactions.yaml"
    )

    max_rejection_rate = config["quality"]["max_rejection_rate"]

    validation_result = ti.xcom_pull(
        task_ids="silver_validation"
    )

    bronze_result = ti.xcom_pull(
        task_ids="bronze_ingestion"
    )

    input_count = bronze_result["input_count"]

    valid_count = validation_result["valid_count"]
    rejected_count = validation_result["rejected_count"]
    total_count = validation_result["total_count"]

    print(f"Input rows: {input_count}")
    print(f"Total validated: {total_count}")
    print(f"Valid rows: {valid_count}")
    print(f"Rejected rows: {rejected_count}")

    if total_count != input_count:
        raise ValueError(
            f"Row count mismatch: input {input_count}, "
            f"validated {total_count}"
        )

    if valid_count + rejected_count != total_count:
        raise ValueError(
            "Reconciliation failed: "
            "valid + rejected != total"
        )

    rejection_rate = rejected_count / total_count

    print(f"Rejection rate: {rejection_rate:.2%}")
    print(
        f"Maximum allowed rejection rate: "
        f"{max_rejection_rate:.2%}"
    )

    if rejection_rate > max_rejection_rate:
        raise ValueError(
            f"Rejection rate too high: "
            f"{rejection_rate:.2%}"
        )

    print("Quality check passed")


with DAG(
    dag_id="banking_data_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["banking", "data-engineering"],
) as dag:

    task_validate_input = PythonOperator(
        task_id="validate_input",
        python_callable=validate_input,
    )

    task_bronze = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=bronze_ingestion,
    )

    task_silver = PythonOperator(
        task_id="silver_validation",
        python_callable=silver_validation,
    )

    task_gold = PythonOperator(
    task_id="gold_layer_check",
    python_callable=gold_layer_check,
    )

    task_quality = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
    )

    (
        task_validate_input
        >> task_bronze
        >> task_silver
        >> task_gold
        >> task_quality
    )