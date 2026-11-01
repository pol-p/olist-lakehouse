-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Governance: table and column documentation
-- MAGIC
-- MAGIC Documents the tables in Unity Catalog so they can be found and understood from Catalog Explorer.
-- MAGIC
-- MAGIC Run this after the pipeline: rebuilding a table with `CREATE OR REPLACE` or `overwriteSchema` can remove its comments.

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

USE SCHEMA olist;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze tables

-- COMMAND ----------

COMMENT ON TABLE bronze_customers IS 'Raw customers from olist_customers_dataset.csv. All columns are strings.';
COMMENT ON TABLE bronze_geolocation IS 'Raw zip code coordinates from olist_geolocation_dataset.csv. All columns are strings.';
COMMENT ON TABLE bronze_order_items IS 'Raw order items from olist_order_items_dataset.csv. All columns are strings.';
COMMENT ON TABLE bronze_order_payments IS 'Raw payments from olist_order_payments_dataset.csv. All columns are strings.';
COMMENT ON TABLE bronze_order_reviews IS 'Raw reviews from olist_order_reviews_dataset.csv. All columns are strings.';
COMMENT ON TABLE bronze_orders IS 'Raw orders from olist_orders_dataset.csv. All columns are strings.';
COMMENT ON TABLE bronze_products IS 'Raw products from olist_products_dataset.csv. All columns are strings.';
COMMENT ON TABLE bronze_sellers IS 'Raw sellers from olist_sellers_dataset.csv. All columns are strings.';
COMMENT ON TABLE bronze_category_translation IS 'Raw category translations from product_category_name_translation.csv. All columns are strings.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver tables

-- COMMAND ----------

COMMENT ON TABLE silver_orders IS 'One row per order, typed. Suspicious rows are kept and marked in dq_ columns.';
ALTER TABLE silver_orders ALTER COLUMN order_id COMMENT 'Primary key.';
ALTER TABLE silver_orders ALTER COLUMN customer_id COMMENT 'Customer key for this order only. Use customer_unique_id in silver_customers to count real customers.';
ALTER TABLE silver_orders ALTER COLUMN order_estimated_delivery_date COMMENT 'Delivery date promised to the customer. Always at 00:00, so compare it as a date.';
ALTER TABLE silver_orders ALTER COLUMN dq_carrier_before_approval COMMENT 'True if the carrier received the order before its approval.';
ALTER TABLE silver_orders ALTER COLUMN dq_delivered_before_carrier COMMENT 'True if the customer delivery date is earlier than the carrier date.';
ALTER TABLE silver_orders ALTER COLUMN dq_delivered_without_date COMMENT 'True if the status is delivered but there is no delivery date.';
ALTER TABLE silver_orders ALTER COLUMN dq_date_without_delivered_status COMMENT 'True if there is a delivery date but the status is not delivered.';

-- COMMAND ----------

COMMENT ON TABLE silver_order_items IS 'One row per item in an order. Primary key: order_id + order_item_id.';
ALTER TABLE silver_order_items ALTER COLUMN price COMMENT 'Item price in Brazilian reais, excluding freight.';
ALTER TABLE silver_order_items ALTER COLUMN freight_value COMMENT 'Freight charged for this item, in Brazilian reais.';

-- COMMAND ----------

COMMENT ON TABLE silver_order_payments IS 'One row per payment of an order. Primary key: order_id + payment_sequential.';
ALTER TABLE silver_order_payments ALTER COLUMN payment_value COMMENT 'Amount paid in Brazilian reais.';
ALTER TABLE silver_order_payments ALTER COLUMN dq_non_positive_value COMMENT 'True if the payment value is zero or negative.';
ALTER TABLE silver_order_payments ALTER COLUMN dq_undefined_payment_type COMMENT 'True if the payment type is not_defined.';

-- COMMAND ----------

COMMENT ON TABLE silver_order_reviews IS 'One row per review and order. Primary key: review_id + order_id, because review_id alone repeats.';
ALTER TABLE silver_order_reviews ALTER COLUMN review_score COMMENT 'Customer score from 1 to 5.';
ALTER TABLE silver_order_reviews ALTER COLUMN review_comment_message COMMENT 'Free text in Portuguese. Optional, so nulls are valid.';

-- COMMAND ----------

COMMENT ON TABLE silver_products IS 'One row per product, with its category in English.';
ALTER TABLE silver_products ALTER COLUMN product_id COMMENT 'Primary key.';
ALTER TABLE silver_products ALTER COLUMN product_category_name_english COMMENT 'Category in English, or unknown if the source has none.';
ALTER TABLE silver_products ALTER COLUMN product_weight_g COMMENT 'Weight in grams. Null if unknown; source values of 0 are treated as unknown.';

-- COMMAND ----------

COMMENT ON TABLE silver_customers IS 'One row per customer_id, which means one row per order.';
ALTER TABLE silver_customers ALTER COLUMN customer_id COMMENT 'Primary key. Different on every order.';
ALTER TABLE silver_customers ALTER COLUMN customer_unique_id COMMENT 'Identifies the real customer across orders.';
ALTER TABLE silver_customers ALTER COLUMN customer_zip_code_prefix COMMENT 'String, to keep leading zeros.';

-- COMMAND ----------

COMMENT ON TABLE silver_sellers IS 'One row per seller.';
ALTER TABLE silver_sellers ALTER COLUMN seller_id COMMENT 'Primary key.';
ALTER TABLE silver_sellers ALTER COLUMN seller_zip_code_prefix COMMENT 'String, to keep leading zeros.';

-- COMMAND ----------

COMMENT ON TABLE silver_geolocation IS 'Coordinates by zip code prefix, without exact duplicates or points outside Brazil. A prefix can have many rows.';
COMMENT ON TABLE silver_category_translation IS 'Portuguese to English category names, including two categories missing from the source file.';
COMMENT ON TABLE dq_results IS 'History of data quality checks. Each silver run appends one row per check. Failed error checks stop the pipeline; failed warning checks are only recorded.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold tables

-- COMMAND ----------

COMMENT ON TABLE gold_monthly_sales IS 'Sales by purchase month, excluding canceled and unavailable orders.';
ALTER TABLE gold_monthly_sales ALTER COLUMN sales_month COMMENT 'First day of the purchase month.';
ALTER TABLE gold_monthly_sales ALTER COLUMN orders COMMENT 'Distinct orders with at least one item.';
ALTER TABLE gold_monthly_sales ALTER COLUMN customers COMMENT 'Distinct real customers in the month, by customer_unique_id. Do not add across months.';
ALTER TABLE gold_monthly_sales ALTER COLUMN items_sold COMMENT 'Number of items sold.';
ALTER TABLE gold_monthly_sales ALTER COLUMN revenue COMMENT 'Sum of item prices in Brazilian reais, excluding freight.';
ALTER TABLE gold_monthly_sales ALTER COLUMN freight COMMENT 'Sum of freight in Brazilian reais.';
ALTER TABLE gold_monthly_sales ALTER COLUMN avg_order_value COMMENT 'Revenue divided by orders.';

-- COMMAND ----------

COMMENT ON TABLE gold_category_sales IS 'Sales by product category, excluding canceled and unavailable orders.';
ALTER TABLE gold_category_sales ALTER COLUMN category COMMENT 'Category in English, or unknown.';
ALTER TABLE gold_category_sales ALTER COLUMN orders COMMENT 'Distinct orders with this category. An order can count in several categories, so do not add them.';
ALTER TABLE gold_category_sales ALTER COLUMN items_sold COMMENT 'Number of items sold.';
ALTER TABLE gold_category_sales ALTER COLUMN revenue COMMENT 'Sum of item prices in Brazilian reais, excluding freight.';
ALTER TABLE gold_category_sales ALTER COLUMN avg_item_price COMMENT 'Average item price in Brazilian reais.';

-- COMMAND ----------

COMMENT ON TABLE gold_delivery_performance IS 'Delivery times by purchase month and customer state. Delivered orders only, excluding rows flagged dq_delivered_before_carrier.';
ALTER TABLE gold_delivery_performance ALTER COLUMN purchase_month COMMENT 'First day of the purchase month.';
ALTER TABLE gold_delivery_performance ALTER COLUMN customer_state COMMENT 'Brazilian state of the customer.';
ALTER TABLE gold_delivery_performance ALTER COLUMN delivered_orders COMMENT 'Number of delivered orders.';
ALTER TABLE gold_delivery_performance ALTER COLUMN avg_delivery_days COMMENT 'Average calendar days from purchase to delivery. Weight by delivered_orders when combining rows.';
ALTER TABLE gold_delivery_performance ALTER COLUMN late_orders COMMENT 'Orders delivered on a later calendar day than estimated.';

-- COMMAND ----------

COMMENT ON TABLE gold_seller_performance IS 'Sales and review scores by seller, excluding canceled and unavailable orders.';
ALTER TABLE gold_seller_performance ALTER COLUMN seller_id COMMENT 'Seller key.';
ALTER TABLE gold_seller_performance ALTER COLUMN seller_state COMMENT 'Brazilian state of the seller.';
ALTER TABLE gold_seller_performance ALTER COLUMN orders COMMENT 'Distinct orders of this seller. An order with two sellers counts once for each.';
ALTER TABLE gold_seller_performance ALTER COLUMN items_sold COMMENT 'Number of items sold.';
ALTER TABLE gold_seller_performance ALTER COLUMN revenue COMMENT 'Sum of item prices in Brazilian reais, excluding freight.';
ALTER TABLE gold_seller_performance ALTER COLUMN reviewed_orders COMMENT 'Orders of this seller with at least one review.';
ALTER TABLE gold_seller_performance ALTER COLUMN avg_review_score COMMENT 'Average review score, with one score per order. Reviews rate the whole order, not only this seller.';

-- COMMAND ----------

COMMENT ON TABLE gold_review_sentiment IS 'Sentiment of a fixed sample of 1,000 review comments, classified with ai_analyze_sentiment. Not representative of all reviews.';
ALTER TABLE gold_review_sentiment ALTER COLUMN review_id COMMENT 'Review key, together with order_id.';
ALTER TABLE gold_review_sentiment ALTER COLUMN order_id COMMENT 'Order key, together with review_id.';
ALTER TABLE gold_review_sentiment ALTER COLUMN review_score COMMENT 'Customer score from 1 to 5.';
ALTER TABLE gold_review_sentiment ALTER COLUMN review_comment_message COMMENT 'Free text in Portuguese.';
ALTER TABLE gold_review_sentiment ALTER COLUMN sentiment COMMENT 'Sentiment returned by the model: positive, negative, neutral or mixed.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tags
-- MAGIC
-- MAGIC Tags mark the layer of each gold table and the column that identifies a person.

-- COMMAND ----------

ALTER TABLE gold_monthly_sales SET TAGS ('layer' = 'gold', 'domain' = 'sales');
ALTER TABLE gold_category_sales SET TAGS ('layer' = 'gold', 'domain' = 'sales');
ALTER TABLE gold_delivery_performance SET TAGS ('layer' = 'gold', 'domain' = 'logistics');
ALTER TABLE gold_seller_performance SET TAGS ('layer' = 'gold', 'domain' = 'sales');
ALTER TABLE silver_customers ALTER COLUMN customer_unique_id SET TAGS ('sensitivity' = 'pseudonymous_id');
