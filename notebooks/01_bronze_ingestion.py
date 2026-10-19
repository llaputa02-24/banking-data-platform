# Databricks notebook source
display(dbutils.fs.ls("/Volumes/workspace/banking_project/raw_data/"))

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    IntegerType
)

schema = StructType([
    StructField("Timestamp", StringType(), True),
    StructField("From Bank", LongType(), True),
    StructField("Account", StringType(), True),
    StructField("To Bank", LongType(), True),
    StructField("Account.1", StringType(), True),
    StructField("Amount Received", DoubleType(), True),
    StructField("Receiving Currency", StringType(), True),
    StructField("Amount Paid", DoubleType(), True),
    StructField("Payment Currency", StringType(), True),
    StructField("Payment Format", StringType(), True),
    StructField("Is Laundering", IntegerType(), True)
])

# COMMAND ----------

file_path = "/Volumes/workspace/banking_project/raw_data/HI-Small_Trans_200k.csv"

df_raw = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv(file_path)
)

# COMMAND ----------

display(df_raw.limit(10))

# COMMAND ----------

df_raw.printSchema()

# COMMAND ----------

df_raw.count()

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumnRenamed("Timestamp", "transaction_time_raw")
    .withColumnRenamed("From Bank", "from_bank")
    .withColumnRenamed("Account", "from_account")
    .withColumnRenamed("To Bank", "to_bank")
    .withColumnRenamed("Account.1", "to_account")
    .withColumnRenamed("Amount Received", "amount_received")
    .withColumnRenamed("Receiving Currency", "receiving_currency")
    .withColumnRenamed("Amount Paid", "amount_paid")
    .withColumnRenamed("Payment Currency", "payment_currency")
    .withColumnRenamed("Payment Format", "payment_format")
    .withColumnRenamed("Is Laundering", "is_laundering")
)

# COMMAND ----------

from pyspark.sql.functions import (
    current_timestamp,
    col,
    lit
)

df_bronze = (
    df_bronze
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("batch_id", lit("batch_20220901_dev"))
)

# COMMAND ----------

file_path = "/Volumes/workspace/banking_project/raw_data/HI-Small_Trans_200k.csv"

df_raw = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv(file_path)
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

df_bronze = (
    df_raw
    .withColumnRenamed("Timestamp", "transaction_time_raw")
    .withColumnRenamed("From Bank", "from_bank")
    .withColumnRenamed("Account", "from_account")
    .withColumnRenamed("To Bank", "to_bank")
    .withColumnRenamed("Account.1", "to_account")
    .withColumnRenamed("Amount Received", "amount_received")
    .withColumnRenamed("Receiving Currency", "receiving_currency")
    .withColumnRenamed("Amount Paid", "amount_paid")
    .withColumnRenamed("Payment Currency", "payment_currency")
    .withColumnRenamed("Payment Format", "payment_format")
    .withColumnRenamed("Is Laundering", "is_laundering")
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("batch_id", lit("batch_20220901_dev"))
)

# COMMAND ----------

df_bronze.select(
    "source_file",
    "ingestion_time",
    "batch_id"
).show(5, truncate=False)

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.banking_project.bronze_transactions")
)

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS row_count
FROM workspace.banking_project.bronze_transactions
""").display()

# COMMAND ----------

spark.sql("""
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT from_account) AS unique_from_accounts,
    COUNT(DISTINCT to_account) AS unique_to_accounts,
    SUM(is_laundering) AS laundering_transactions
FROM workspace.banking_project.bronze_transactions
""").display()