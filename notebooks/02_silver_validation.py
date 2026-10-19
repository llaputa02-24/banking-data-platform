# Databricks notebook source
df_bronze = spark.table(
    "workspace.banking_project.bronze_transactions"
)

display(df_bronze.limit(5))

# COMMAND ----------

df_bronze.count()

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    concat_ws,
    sha2,
    coalesce,
    lit
)

df_with_id = df_bronze.withColumn(
    "transaction_id",
    sha2(
        concat_ws(
            "|",
            coalesce(col("transaction_time_raw"), lit("")),
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

df_with_id.select(
    "transaction_id",
    "transaction_time_raw",
    "from_account",
    "to_account",
    "amount_paid"
).show(5, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, to_date

df_parsed = (
    df_with_id
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
)

# COMMAND ----------

df_parsed.select(
    "transaction_time_raw",
    "transaction_time"
).show(10, truncate=False)

# COMMAND ----------

VALID_CURRENCIES = [
    "Australian Dollar",
    "Bitcoin",
    "Brazil Real",
    "Canadian Dollar",
    "Euro",
    "Mexican Peso",
    "Ruble",
    "Rupee",
    "Saudi Riyal",
    "Shekel",
    "Swiss Franc",
    "UK Pound",
    "US Dollar",
    "Yen",
    "Yuan",
]

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    array,
    array_compact,
    when,
    lit,
    trim,
    size
)

df_validated = df_parsed.withColumn(
    "rejection_reasons",
    array_compact(
        array(
            when(
                col("transaction_time").isNull(),
                lit("INVALID_TIMESTAMP")
            ),

            when(
                col("from_account").isNull() |
                (trim(col("from_account")) == ""),
                lit("NULL_FROM_ACCOUNT")
            ),

            when(
                col("to_account").isNull() |
                (trim(col("to_account")) == ""),
                lit("NULL_TO_ACCOUNT")
            ),

            when(
                col("amount_paid").isNull() |
                (col("amount_paid") <= 0),
                lit("INVALID_AMOUNT_PAID")
            ),

            when(
                col("amount_received").isNull() |
                (col("amount_received") <= 0),
                lit("INVALID_AMOUNT_RECEIVED")
            ),

            when(
                col("payment_currency").isNull() |
                (~col("payment_currency").isin(VALID_CURRENCIES)),
                lit("INVALID_PAYMENT_CURRENCY")
            ),

            when(
                col("receiving_currency").isNull() |
                (~col("receiving_currency").isin(VALID_CURRENCIES)),
                lit("INVALID_RECEIVING_CURRENCY")
            )
        )
    )
)

# COMMAND ----------

df_validated = df_validated.withColumn(
    "is_valid",
    size(col("rejection_reasons")) == 0
)

# COMMAND ----------

df_validated.groupBy("is_valid").count().show()

# COMMAND ----------

from pyspark.sql.functions import explode

(
    df_validated
    .filter(~col("is_valid"))
    .select(explode("rejection_reasons").alias("reason"))
    .groupBy("reason")
    .count()
    .orderBy(col("count").desc())
    .show(truncate=False)
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df_silver = (
    df_validated
    .filter(col("is_valid"))
    .withColumn("processed_time", current_timestamp())
    .drop(
        "transaction_time_raw",
        "rejection_reasons",
        "is_valid"
    )
)

# COMMAND ----------

df_quarantine = (
    df_validated
    .filter(~col("is_valid"))
    .withColumn("rejection_time", current_timestamp())
)

# COMMAND ----------

print("Bronze:", df_bronze.count())
print("Silver:", df_silver.count())
print("Quarantine:", df_quarantine.count())

# COMMAND ----------

bronze_count = df_bronze.count()
silver_count = df_silver.count()
quarantine_count = df_quarantine.count()

assert bronze_count == silver_count + quarantine_count

# COMMAND ----------

duplicate_count = (
    df_silver
    .groupBy("transaction_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Duplicate transaction IDs:", duplicate_count)

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.banking_project.silver_transactions")
)

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS row_count
FROM workspace.banking_project.silver_transactions
""").display()

# COMMAND ----------

(
    df_quarantine.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.banking_project.quarantine_transactions")
)

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS row_count
FROM workspace.banking_project.quarantine_transactions
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

assert bronze_count == silver_count + quarantine_count

print("Reconciliation passed")

# COMMAND ----------

