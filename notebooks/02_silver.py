# Databricks notebook source
# MAGIC %md
# MAGIC # Silver layer: cleaning and data quality
# MAGIC
# MAGIC Turns bronze tables into typed, trusted tables.
# MAGIC
# MAGIC - Only clearly invalid rows are removed: exact duplicates and impossible coordinates.
# MAGIC - Suspicious rows are kept and marked with `dq_*` columns, so each KPI can decide
# MAGIC   whether to exclude them.
# MAGIC - Every run appends its check results to `dq_results`. A failed `error` check stops
# MAGIC   the notebook; a failed `warning` check is only recorded.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "olist"

# Bounding box of Brazil
LAT_RANGE = (-34, 6)
LNG_RANGE = (-74, -34)


def bronze(table):
    return spark.table(f"{CATALOG}.{SCHEMA}.bronze_{table}")


def silver(table):
    return spark.table(f"{CATALOG}.{SCHEMA}.silver_{table}")


def write_silver(df, table):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_{table}")
    )
    print(f"Wrote silver_{table}")


def cast_columns(df, columns, data_type):
    return df.withColumns({c: F.col(c).cast(data_type) for c in columns})


def dq_flag(condition):
    # Comparing against a null gives null, which here means the check does not apply
    return F.coalesce(condition, F.lit(False))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders
# MAGIC
# MAGIC Null delivery dates are expected on undelivered orders, so they are not flagged alone.

# COMMAND ----------

orders = cast_columns(
    bronze("orders"),
    [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "timestamp",
)

delivered = F.col("order_status") == "delivered"

orders = orders.withColumns(
    {
        "dq_carrier_before_approval": dq_flag(
            F.col("order_delivered_carrier_date") < F.col("order_approved_at")
        ),
        "dq_delivered_before_carrier": dq_flag(
            F.col("order_delivered_customer_date")
            < F.col("order_delivered_carrier_date")
        ),
        "dq_delivered_without_date": delivered
        & F.col("order_delivered_customer_date").isNull(),
        "dq_date_without_delivered_status": ~delivered
        & F.col("order_delivered_customer_date").isNotNull(),
    }
)

write_silver(orders, "orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order items and payments

# COMMAND ----------

order_items = bronze("order_items").withColumns(
    {
        "order_item_id": F.col("order_item_id").cast("int"),
        "shipping_limit_date": F.col("shipping_limit_date").cast("timestamp"),
        "price": F.col("price").cast("decimal(10,2)"),
        "freight_value": F.col("freight_value").cast("decimal(10,2)"),
    }
)

write_silver(order_items, "order_items")

# COMMAND ----------

order_payments = bronze("order_payments").withColumns(
    {
        "payment_sequential": F.col("payment_sequential").cast("int"),
        "payment_installments": F.col("payment_installments").cast("int"),
        "payment_value": F.col("payment_value").cast("decimal(10,2)"),
    }
)

order_payments = order_payments.withColumns(
    {
        "dq_non_positive_value": F.col("payment_value") <= 0,
        "dq_undefined_payment_type": F.col("payment_type") == "not_defined",
    }
)

write_silver(order_payments, "order_payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reviews
# MAGIC
# MAGIC `review_id` alone repeats across orders, so the key is `review_id` + `order_id`.
# MAGIC Comments are optional, so nulls in them are valid.

# COMMAND ----------

order_reviews = bronze("order_reviews").withColumns(
    {
        "review_score": F.col("review_score").cast("int"),
        "review_creation_date": F.col("review_creation_date").cast("timestamp"),
        "review_answer_timestamp": F.col("review_answer_timestamp").cast("timestamp"),
    }
)

write_silver(order_reviews, "order_reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products and categories
# MAGIC
# MAGIC Fixes the misspelled source columns (`lenght`), adds the two categories missing from
# MAGIC the translation file, and treats a weight of 0 as unknown.

# COMMAND ----------

missing_translations = spark.createDataFrame(
    [
        ("pc_gamer", "pc_gamer"),
        (
            "portateis_cozinha_e_preparadores_de_alimentos",
            "portable_kitchen_and_food_preparers",
        ),
    ],
    ["product_category_name", "product_category_name_english"],
)

category_translation = (
    bronze("category_translation")
    .select("product_category_name", "product_category_name_english")
    .unionByName(missing_translations)
)

write_silver(category_translation, "category_translation")

# COMMAND ----------

products = (
    bronze("products")
    .withColumnsRenamed(
        {
            "product_name_lenght": "product_name_length",
            "product_description_lenght": "product_description_length",
        }
    )
    .transform(
        lambda df: cast_columns(
            df,
            [
                "product_name_length",
                "product_description_length",
                "product_photos_qty",
                "product_weight_g",
                "product_length_cm",
                "product_height_cm",
                "product_width_cm",
            ],
            "int",
        )
    )
    .withColumn(
        "product_weight_g",
        F.when(F.col("product_weight_g") > 0, F.col("product_weight_g")),
    )
    .join(category_translation, "product_category_name", "left")
    .withColumns(
        {
            "product_category_name": F.coalesce(
                F.col("product_category_name"), F.lit("unknown")
            ),
            "product_category_name_english": F.coalesce(
                F.col("product_category_name_english"), F.lit("unknown")
            ),
        }
    )
)

write_silver(products, "products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers and sellers
# MAGIC
# MAGIC Zip code prefixes stay as strings to keep leading zeros (`01037`).
# MAGIC `customer_id` is unique per order: use `customer_unique_id` to count real customers.

# COMMAND ----------

write_silver(bronze("customers"), "customers")
write_silver(bronze("sellers"), "sellers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geolocation
# MAGIC
# MAGIC Removes exact duplicates and coordinates outside Brazil. Duplicates are compared on the
# MAGIC raw text, before casting.

# COMMAND ----------

geolocation = (
    bronze("geolocation")
    .dropDuplicates(
        [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state",
        ]
    )
    .transform(
        lambda df: cast_columns(df, ["geolocation_lat", "geolocation_lng"], "double")
    )
    .filter(
        F.col("geolocation_lat").between(*LAT_RANGE)
        & F.col("geolocation_lng").between(*LNG_RANGE)
    )
)

write_silver(geolocation, "geolocation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality checks

# COMMAND ----------

PRIMARY_KEYS = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id", "order_id"],
    "products": ["product_id"],
    "customers": ["customer_id"],
    "sellers": ["seller_id"],
    "category_translation": ["product_category_name"],
}

# (child table, column, parent table)
FOREIGN_KEYS = [
    ("orders", "customer_id", "customers"),
    ("order_items", "order_id", "orders"),
    ("order_items", "product_id", "products"),
    ("order_items", "seller_id", "sellers"),
    ("order_payments", "order_id", "orders"),
    ("order_reviews", "order_id", "orders"),
]

# (table, check name, condition that identifies a failing row)
ERROR_RULES = [
    ("order_items", "price_is_positive", "price <= 0"),
    ("order_items", "freight_is_not_negative", "freight_value < 0"),
    ("order_reviews", "score_between_1_and_5", "review_score NOT BETWEEN 1 AND 5"),
    (
        "orders",
        "delivery_not_before_purchase",
        "order_delivered_customer_date < order_purchase_timestamp",
    ),
]

WARNING_FLAGS = {
    "orders": [
        "dq_carrier_before_approval",
        "dq_delivered_before_carrier",
        "dq_delivered_without_date",
        "dq_date_without_delivered_status",
    ],
    "order_payments": ["dq_non_positive_value", "dq_undefined_payment_type"],
}

# COMMAND ----------

results = []


def record(table, check, severity, failed_rows):
    results.append((table, check, severity, failed_rows, failed_rows == 0))


for table, keys in PRIMARY_KEYS.items():
    df = silver(table)
    record(
        table,
        "primary_key_is_unique",
        "error",
        df.count() - df.dropDuplicates(keys).count(),
    )
    record(
        table,
        "primary_key_is_not_null",
        "error",
        df.filter(" OR ".join(f"{k} IS NULL" for k in keys)).count(),
    )

for child, column, parent in FOREIGN_KEYS:
    orphans = silver(child).join(silver(parent).select(column), column, "left_anti")
    record(child, f"{column}_exists_in_{parent}", "error", orphans.count())

for table, check, condition in ERROR_RULES:
    record(table, check, "error", silver(table).filter(condition).count())

for table, flags in WARNING_FLAGS.items():
    for flag in flags:
        record(table, flag, "warning", silver(table).filter(F.col(flag)).count())

# COMMAND ----------

dq_results = spark.createDataFrame(
    results,
    "table_name STRING, check_name STRING, severity STRING, failed_rows LONG, passed BOOLEAN",
).withColumn("checked_at", F.current_timestamp())

dq_results.write.format("delta").mode("append").saveAsTable(
    f"{CATALOG}.{SCHEMA}.dq_results"
)

display(dq_results.orderBy("passed", "severity", "table_name", "check_name"))

# COMMAND ----------

failed = [f"{r[0]}.{r[1]}" for r in results if r[2] == "error" and not r[4]]
if failed:
    raise ValueError(f"Data quality errors: {failed}")
