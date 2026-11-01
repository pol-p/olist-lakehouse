# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze layer: raw ingestion
# MAGIC
# MAGIC Loads each Olist CSV from the Unity Catalog volume into a Delta table.
# MAGIC
# MAGIC - All columns stay as strings. Types and cleaning belong to the silver layer.
# MAGIC - Each row records its source file and load time, for traceability.
# MAGIC - Tables are overwritten, so the notebook can be re-run safely.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "olist"
RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

# Bronze table name -> (source file, expected row count)
SOURCES = {
    "customers": ("olist_customers_dataset.csv", 99441),
    "geolocation": ("olist_geolocation_dataset.csv", 1000163),
    "order_items": ("olist_order_items_dataset.csv", 112650),
    "order_payments": ("olist_order_payments_dataset.csv", 103886),
    "order_reviews": ("olist_order_reviews_dataset.csv", 99224),
    "orders": ("olist_orders_dataset.csv", 99441),
    "products": ("olist_products_dataset.csv", 32951),
    "sellers": ("olist_sellers_dataset.csv", 3095),
    "category_translation": ("product_category_name_translation.csv", 71),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest
# MAGIC
# MAGIC `multiLine` and `escape` are required: review comments contain line breaks
# MAGIC and quotes inside quoted fields, which would otherwise split rows.

# COMMAND ----------


def ingest_csv(table, file_name):
    df = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(f"{RAW_PATH}/{file_name}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_{table}")
    )


for table, (file_name, _) in SOURCES.items():
    ingest_csv(table, file_name)
    print(f"Loaded bronze_{table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify row counts
# MAGIC
# MAGIC Fails the notebook if any table does not match its source file.

# COMMAND ----------

results = []
for table, (_, expected) in SOURCES.items():
    actual = spark.table(f"{CATALOG}.{SCHEMA}.bronze_{table}").count()
    results.append((f"bronze_{table}", expected, actual, expected == actual))

display(spark.createDataFrame(results, ["table", "expected", "actual", "matches"]))

mismatches = [row[0] for row in results if not row[3]]
if mismatches:
    raise ValueError(f"Row count mismatch in: {mismatches}")
