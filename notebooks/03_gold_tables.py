# Databricks notebook source
df_silver = spark.table(
    "workspace.banking_project.silver_transactions"
)

print(df_silver.count())
display(df_silver.limit(5))

# COMMAND ----------

from pyspark.sql.functions import (
    count,
    sum,
    avg
)

df_gold_currency = (
    df_silver
    .groupBy(
        "transaction_date",
        "payment_currency",
        "receiving_currency"
    )
    .agg(
        count("*").alias("transaction_count"),
        sum("amount_paid").alias("total_amount_paid"),
        sum("amount_received").alias("total_amount_received"),
        avg("amount_paid").alias("avg_amount_paid")
    )
)

# COMMAND ----------

display(
    df_gold_currency
    .orderBy(
        "transaction_date",
        "transaction_count",
        ascending=[True, False]
    )
)

# COMMAND ----------

gold_transaction_count = (
    df_gold_currency
    .agg(
        sum("transaction_count").alias("total")
    )
    .collect()[0]["total"]
)

silver_count = df_silver.count()

print("Silver:", silver_count)
print("Gold transaction total:", gold_transaction_count)

assert silver_count == gold_transaction_count

# COMMAND ----------

(
    df_gold_currency.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.banking_project.gold_currency_flow_daily"
    )
)

spark.sql("""
SELECT *
FROM workspace.banking_project.gold_currency_flow_daily
ORDER BY transaction_date, transaction_count DESC
LIMIT 20
""").display()

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg
)

df_outgoing = (
    df_silver
    .groupBy(
        "transaction_date",
        "from_account"
    )
    .agg(
        count("*").alias("outgoing_transaction_count"),
        sum("amount_paid").alias("total_outgoing_amount"),
        avg("amount_paid").alias("avg_outgoing_amount")
    )
    .withColumnRenamed(
        "from_account",
        "account_id"
    )
)

# COMMAND ----------

df_incoming = (
    df_silver
    .groupBy(
        "transaction_date",
        "to_account"
    )
    .agg(
        count("*").alias("incoming_transaction_count"),
        sum("amount_received").alias("total_incoming_amount"),
        avg("amount_received").alias("avg_incoming_amount")
    )
    .withColumnRenamed(
        "to_account",
        "account_id"
    )
)

# COMMAND ----------

df_gold_account = (
    df_outgoing
    .join(
        df_incoming,
        on=["transaction_date", "account_id"],
        how="full"
    )
)

# COMMAND ----------

df_gold_account = df_gold_account.fillna({
    "outgoing_transaction_count": 0,
    "incoming_transaction_count": 0,
    "total_outgoing_amount": 0.0,
    "total_incoming_amount": 0.0,
    "avg_outgoing_amount": 0.0,
    "avg_incoming_amount": 0.0,
})

# COMMAND ----------

display(
    df_gold_account
    .orderBy(
        "transaction_date",
        "total_outgoing_amount",
        ascending=[True, False]
    )
    .limit(20)
)

# COMMAND ----------

outgoing_total = (
    df_gold_account
    .agg(
        sum("outgoing_transaction_count").alias("total")
    )
    .collect()[0]["total"]
)

silver_count = df_silver.count()

print("Silver:", silver_count)
print("Outgoing transaction total:", outgoing_total)

assert silver_count == outgoing_total

# COMMAND ----------

incoming_total = (
    df_gold_account
    .agg(
        sum("incoming_transaction_count").alias("total")
    )
    .collect()[0]["total"]
)

print("Incoming transaction total:", incoming_total)

assert silver_count == incoming_total

# COMMAND ----------

(
    df_gold_account.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.banking_project.gold_account_daily"
    )
)

spark.sql("""
SELECT *
FROM workspace.banking_project.gold_account_daily
ORDER BY transaction_date, total_outgoing_amount DESC
LIMIT 20
""").display()

# COMMAND ----------

bronze_count = spark.table(
    "workspace.banking_project.bronze_transactions"
).count()

silver_count = spark.table(
    "workspace.banking_project.silver_transactions"
).count()

quarantine_count = spark.table(
    "workspace.banking_project.quarantine_transactions"
).count()

from pyspark.sql.functions import col

duplicate_count = (
    spark.table(
        "workspace.banking_project.silver_transactions"
    )
    .groupBy("transaction_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

# COMMAND ----------

rejection_rate = (
    quarantine_count / bronze_count
    if bronze_count > 0
    else 0
)

# COMMAND ----------

from datetime import datetime

quality_data = [
    (
        datetime.now().date(),
        bronze_count,
        silver_count,
        quarantine_count,
        duplicate_count,
        rejection_rate
    )
]

quality_columns = [
    "processing_date",
    "input_rows",
    "valid_rows",
    "rejected_rows",
    "duplicate_rows",
    "rejection_rate"
]

df_gold_quality = spark.createDataFrame(
    quality_data,
    quality_columns
)

# COMMAND ----------

display(df_gold_quality)

# COMMAND ----------

(
    df_gold_quality.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.banking_project.gold_data_quality_daily"
    )
)

spark.sql("""
SELECT *
FROM workspace.banking_project.gold_data_quality_daily
""").display()