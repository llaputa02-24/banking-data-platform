# Databricks notebook source
streaming_input_path = (
    "/Volumes/workspace/banking_project/raw_data/streaming_input"
)

checkpoint_path = (
    "/Volumes/workspace/banking_project/raw_data/checkpoints/"
    "transactions_stream"
)

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    IntegerType,
)

stream_schema = StructType([
    StructField("Timestamp", StringType(), True),
    StructField("From Bank", LongType(), True),
    StructField("Account", StringType(), True),
    StructField("To Bank", LongType(), True),

    # 原始 CSV 两个 Account 同名。
    # Spark CSV 读取时我们后面仍按位置/标准化处理。
    StructField("Account.1", StringType(), True),

    StructField("Amount Received", DoubleType(), True),
    StructField("Receiving Currency", StringType(), True),
    StructField("Amount Paid", DoubleType(), True),
    StructField("Payment Currency", StringType(), True),
    StructField("Payment Format", StringType(), True),
    StructField("Is Laundering", IntegerType(), True),
])

# COMMAND ----------

df_source = spark.table(
    "workspace.banking_project.bronze_transactions"
)

print(df_source.count())

# COMMAND ----------

df_stream_demo = df_source.limit(3000)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

df_stream_demo = (
    df_stream_demo
    .withColumn("_stream_id", monotonically_increasing_id())
)

# COMMAND ----------

df_batch_1 = df_stream_demo.limit(1000)

# COMMAND ----------

df_batch_1 = df_batch_1.select(
    "transaction_time_raw",
    "from_bank",
    "from_account",
    "to_bank",
    "to_account",
    "amount_received",
    "receiving_currency",
    "amount_paid",
    "payment_currency",
    "payment_format",
    "is_laundering",
)

batch_1_path = (
    "/Volumes/workspace/banking_project/raw_data/"
    "streaming_input/batch_001"
)

(
    df_batch_1
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(batch_1_path)
)

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    IntegerType,
)

stream_schema = StructType([
    StructField("transaction_time_raw", StringType(), True),
    StructField("from_bank", LongType(), True),
    StructField("from_account", StringType(), True),
    StructField("to_bank", LongType(), True),
    StructField("to_account", StringType(), True),
    StructField("amount_received", DoubleType(), True),
    StructField("receiving_currency", StringType(), True),
    StructField("amount_paid", DoubleType(), True),
    StructField("payment_currency", StringType(), True),
    StructField("payment_format", StringType(), True),
    StructField("is_laundering", IntegerType(), True),
])

df_stream = (
    spark.readStream
    .schema(stream_schema)
    .option("header", True)
    .option("recursiveFileLookup", "true")
    .csv(streaming_input_path)
)

# COMMAND ----------

print(df_stream.isStreaming)

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    current_timestamp,
)

df_stream_transformed = (
    df_stream
    .withColumn(
        "transaction_time",
        to_timestamp(
            col("transaction_time_raw"),
            "yyyy/MM/dd HH:mm"
        )
    )
    .withColumn(
        "transaction_date",
        to_date(col("transaction_time"))
    )
    .withColumn(
        "stream_processed_time",
        current_timestamp()
    )
)

# COMMAND ----------

streaming_table_path = (
    "workspace.banking_project."
    "streaming_bronze_transactions"
)

query = (
    df_stream_transformed.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        checkpoint_path
    )
    .trigger(availableNow=True)
    .toTable(streaming_table_path)
)

# COMMAND ----------

query.awaitTermination()

# COMMAND ----------

df_streaming_result = spark.table(
    "workspace.banking_project.streaming_bronze_transactions"
)

print(df_streaming_result.count())

# COMMAND ----------

query = (
    df_stream_transformed.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        checkpoint_path
    )
    .trigger(availableNow=True)
    .toTable(
        "workspace.banking_project.streaming_bronze_transactions"
    )
)

query.awaitTermination()

print(
    spark.table(
        "workspace.banking_project.streaming_bronze_transactions"
    ).count()
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.orderBy("transaction_time_raw")

df_numbered = (
    df_source
    .withColumn(
        "_row_num",
        row_number().over(window_spec)
    )
)

df_batch_2 = (
    df_numbered
    .filter(
        (col("_row_num") > 1000)
        & (col("_row_num") <= 2000)
    )
    .drop("_row_num")
)

df_batch_2 = df_batch_2.select(
    "transaction_time_raw",
    "from_bank",
    "from_account",
    "to_bank",
    "to_account",
    "amount_received",
    "receiving_currency",
    "amount_paid",
    "payment_currency",
    "payment_format",
    "is_laundering",
)

batch_2_path = (
    "/Volumes/workspace/banking_project/raw_data/"
    "streaming_input/batch_002"
)

(
    df_batch_2
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(batch_2_path)
)

query = (
    df_stream_transformed.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        checkpoint_path
    )
    .trigger(availableNow=True)
    .toTable(
        "workspace.banking_project.streaming_bronze_transactions"
    )
)

query.awaitTermination()

print(
    spark.table(
        "workspace.banking_project.streaming_bronze_transactions"
    ).count()
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

