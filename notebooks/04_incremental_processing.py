# Databricks notebook source
df_silver = spark.table(
    "workspace.banking_project.silver_transactions"
)
df_new_batch = df_silver.limit(1000)
from pyspark.sql.functions import lit, current_timestamp

df_new_batch = (
    df_new_batch
    .withColumn("batch_id", lit("batch_002"))
    .withColumn("processed_time", current_timestamp())
)

# COMMAND ----------

from delta.tables import DeltaTable

silver_table = DeltaTable.forName(
    spark,
    "workspace.banking_project.silver_transactions"
)

(
    silver_table.alias("target")
    .merge(
        df_new_batch.alias("source"),
        "target.transaction_id = source.transaction_id"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

before_count = spark.table(
    "workspace.banking_project.silver_transactions"
).count()

print("Before:", before_count)

after_count = spark.table(
    "workspace.banking_project.silver_transactions"
).count()

print("After:", after_count)

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, sha2

df_10_new = (
    df_silver
    .limit(10)
    .withColumn(
        "from_account",
        concat(col("from_account"), lit("_NEW"))
    )
)

# COMMAND ----------

from pyspark.sql.functions import concat_ws, coalesce

df_10_new = df_10_new.withColumn(
    "transaction_id",
    sha2(
        concat_ws(
            "|",
            coalesce(col("transaction_time").cast("string"), lit("")),
            coalesce(col("from_bank").cast("string"), lit("")),
            coalesce(col("from_account"), lit("")),
            coalesce(col("to_bank").cast("string"), lit("")),
            coalesce(col("to_account"), lit("")),
            coalesce(col("amount_paid").cast("string"), lit("")),
            coalesce(col("payment_currency"), lit("")),
            coalesce(col("amount_received").cast("string"), lit("")),
            coalesce(col("receiving_currency"), lit("")),
            coalesce(col("payment_format"), lit(""))
        ),
        256
    )
)

# COMMAND ----------

df_mixed_batch = df_new_batch.unionByName(
    df_10_new,
    allowMissingColumns=True
)

print("Incoming:", df_mixed_batch.count())

# COMMAND ----------

before_count = spark.table(
    "workspace.banking_project.silver_transactions"
).count()

print("Before:", before_count)

# COMMAND ----------

from delta.tables import DeltaTable

silver_table = DeltaTable.forName(
    spark,
    "workspace.banking_project.silver_transactions"
)

(
    silver_table.alias("target")
    .merge(
        df_mixed_batch.alias("source"),
        "target.transaction_id = source.transaction_id"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

after_count = spark.table(
    "workspace.banking_project.silver_transactions"
).count()

print("Before:", before_count)
print("Incoming:", df_mixed_batch.count())
print("After:", after_count)
print("Actually inserted:", after_count - before_count)

# COMMAND ----------

from delta.tables import DeltaTable

silver_table = DeltaTable.forName(
    spark,
    "workspace.banking_project.silver_transactions"
)

(
    silver_table.alias("target")
    .merge(
        df_mixed_batch.alias("source"),
        "target.transaction_id = source.transaction_id"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

second_after_count = spark.table(
    "workspace.banking_project.silver_transactions"
).count()

print("After first run:", after_count)
print("After second run:", second_after_count)

assert after_count == second_after_count

# COMMAND ----------

from datetime import datetime

run_start_time = datetime.now()

incoming_count = df_mixed_batch.count()
before_count = before_count
after_count = after_count

# COMMAND ----------

inserted_count = after_count - before_count

run_status = "SUCCESS"
error_message = None
run_end_time = datetime.now()

# COMMAND ----------

run_id = run_start_time.strftime("run_%Y%m%d_%H%M%S")

# COMMAND ----------

from pyspark.sql.types import (
    StringType, TimestampType, LongType, StructType, StructField
)

run_log_data = [
    (
        run_id,
        "silver_incremental_load",
        "incremental",
        run_start_time,
        run_end_time,
        run_status,
        incoming_count,
        inserted_count,
        0,
        error_message
    )
]

run_log_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("pipeline_name", StringType(), True),
    StructField("pipeline_type", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("input_rows", LongType(), True),
    StructField("inserted_rows", LongType(), True),
    StructField("rejected_rows", LongType(), True),
    StructField("error_message", StringType(), True),
])

df_run_log = spark.createDataFrame(
    run_log_data,
    run_log_schema
)

display(df_run_log)

# COMMAND ----------

(
    df_run_log.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "workspace.banking_project.pipeline_run_log"
    )
)

spark.sql("""
SELECT *
FROM workspace.banking_project.pipeline_run_log
ORDER BY start_time DESC
""").display()

# COMMAND ----------

from datetime import datetime
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType
)
from delta.tables import DeltaTable


run_start_time = datetime.now()
run_id = run_start_time.strftime("run_%Y%m%d_%H%M%S")

incoming_count = df_mixed_batch.count()
inserted_count = 0
rejected_count = 0

run_status = "SUCCESS"
error_message = None

try:
    silver_table = DeltaTable.forName(
        spark,
        "workspace.banking_project.silver_transactions"
    )

    before_count = spark.table(
        "workspace.banking_project.silver_transactions"
    ).count()

    (
        silver_table.alias("target")
        .merge(
            df_mixed_batch.alias("source"),
            "target.transaction_id = source.transaction_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    after_count = spark.table(
        "workspace.banking_project.silver_transactions"
    ).count()

    inserted_count = after_count - before_count

except Exception as e:
    run_status = "FAILED"
    error_message = str(e)

# COMMAND ----------

run_end_time = datetime.now()

# COMMAND ----------

run_log_data = [
    (
        run_id,
        "silver_incremental_load",
        "incremental",
        run_start_time,
        run_end_time,
        run_status,
        incoming_count,
        inserted_count,
        rejected_count,
        error_message
    )
]

run_log_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("pipeline_type", StringType(), False),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), False),
    StructField("status", StringType(), False),
    StructField("input_rows", LongType(), False),
    StructField("inserted_rows", LongType(), False),
    StructField("rejected_rows", LongType(), False),
    StructField("error_message", StringType(), True),
])

# COMMAND ----------

df_run_log = spark.createDataFrame(
    run_log_data,
    schema=run_log_schema
)

(
    df_run_log.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "workspace.banking_project.pipeline_run_log"
    )
)

spark.sql("""
SELECT *
FROM workspace.banking_project.pipeline_run_log
ORDER BY start_time DESC
""").display()

# COMMAND ----------

from datetime import datetime

run_start_time = datetime.now()
run_id = run_start_time.strftime("run_%Y%m%d_%H%M%S")

incoming_count = df_mixed_batch.count()
inserted_count = 0
rejected_count = 0

run_status = "SUCCESS"
error_message = None

try:
    silver_table = DeltaTable.forName(
        spark,
        "workspace.banking_project.silver_transactions"
    )

    before_count = spark.table(
        "workspace.banking_project.silver_transactions"
    ).count()

    (
        silver_table.alias("target")
        .merge(
            df_mixed_batch.alias("source"),
            "target.transaction_id = source.fake_transaction_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    after_count = spark.table(
        "workspace.banking_project.silver_transactions"
    ).count()

    inserted_count = after_count - before_count

except Exception as e:
    run_status = "FAILED"
    error_message = str(e)

run_end_time = datetime.now()

# COMMAND ----------

print("Status:", run_status)
print("Inserted:", inserted_count)
print("Error:", error_message)

# COMMAND ----------

run_log_data = [
    (
        run_id,
        "silver_incremental_load",
        "incremental",
        run_start_time,
        run_end_time,
        run_status,
        incoming_count,
        inserted_count,
        rejected_count,
        error_message
    )
]

df_run_log = spark.createDataFrame(
    run_log_data,
    schema=run_log_schema
)

(
    df_run_log.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "workspace.banking_project.pipeline_run_log"
    )
)

spark.sql("""
SELECT
    run_id,
    pipeline_name,
    status,
    input_rows,
    inserted_rows,
    error_message,
    start_time,
    end_time
FROM workspace.banking_project.pipeline_run_log
ORDER BY start_time DESC
LIMIT 5
""").display()

# COMMAND ----------



# COMMAND ----------

