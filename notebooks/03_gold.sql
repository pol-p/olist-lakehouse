-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold layer: business KPIs
-- MAGIC
-- MAGIC Business-ready tables built from silver.
-- MAGIC
-- MAGIC Shared definitions:
-- MAGIC - **Revenue** is the sum of item prices, excluding freight.
-- MAGIC - **Sales** exclude `canceled` and `unavailable` orders. Orders without items have no revenue, so they do not appear.
-- MAGIC - **Customers** are counted with `customer_unique_id`, because `customer_id` changes on every order.
-- MAGIC - **Late** means delivered on a later calendar day than estimated. Estimated dates have no time of day (00:00), so comparing timestamps would mark same-day deliveries as late.

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

USE SCHEMA olist;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Monthly sales

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_monthly_sales AS
SELECT
  CAST(DATE_TRUNC('MONTH', o.order_purchase_timestamp) AS DATE) AS sales_month,
  COUNT(DISTINCT o.order_id) AS orders,
  COUNT(DISTINCT c.customer_unique_id) AS customers,
  COUNT(*) AS items_sold,
  SUM(i.price) AS revenue,
  SUM(i.freight_value) AS freight,
  ROUND(SUM(i.price) / COUNT(DISTINCT o.order_id), 2) AS avg_order_value
FROM silver_orders o
JOIN silver_order_items i ON o.order_id = i.order_id
JOIN silver_customers c ON o.customer_id = c.customer_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sales by category

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_category_sales AS
SELECT
  p.product_category_name_english AS category,
  COUNT(DISTINCT o.order_id) AS orders,
  COUNT(*) AS items_sold,
  SUM(i.price) AS revenue,
  ROUND(AVG(i.price), 2) AS avg_item_price
FROM silver_orders o
JOIN silver_order_items i ON o.order_id = i.order_id
JOIN silver_products p ON i.product_id = p.product_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Delivery performance
-- MAGIC
-- MAGIC Only delivered orders with a reliable date: rows flagged `dq_delivered_before_carrier` are excluded.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_delivery_performance AS
SELECT
  CAST(DATE_TRUNC('MONTH', o.order_purchase_timestamp) AS DATE) AS purchase_month,
  c.customer_state,
  COUNT(*) AS delivered_orders,
  ROUND(AVG(DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp)), 2) AS avg_delivery_days,
  SUM(CASE WHEN CAST(o.order_delivered_customer_date AS DATE) > CAST(o.order_estimated_delivery_date AS DATE) THEN 1 ELSE 0 END) AS late_orders
FROM silver_orders o
JOIN silver_customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
  AND o.order_delivered_customer_date IS NOT NULL
  AND NOT o.dq_delivered_before_carrier
GROUP BY 1, 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Seller performance
-- MAGIC
-- MAGIC An order can hold several items and several reviews. Items and reviews are reduced to one row per seller and order before joining, so neither inflates the other.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_seller_performance AS
WITH seller_orders AS (
  SELECT
    i.seller_id,
    i.order_id,
    COUNT(*) AS items_sold,
    SUM(i.price) AS revenue
  FROM silver_order_items i
  JOIN silver_orders o ON i.order_id = o.order_id
  WHERE o.order_status NOT IN ('canceled', 'unavailable')
  GROUP BY 1, 2
),
order_scores AS (
  SELECT order_id, AVG(review_score) AS review_score
  FROM silver_order_reviews
  GROUP BY 1
)
SELECT
  so.seller_id,
  s.seller_state,
  COUNT(*) AS orders,
  SUM(so.items_sold) AS items_sold,
  SUM(so.revenue) AS revenue,
  COUNT(os.review_score) AS reviewed_orders,
  ROUND(AVG(os.review_score), 2) AS avg_review_score
FROM seller_orders so
JOIN silver_sellers s ON so.seller_id = s.seller_id
LEFT JOIN order_scores os ON so.order_id = os.order_id
GROUP BY 1, 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Reconcile revenue
-- MAGIC
-- MAGIC All three sales tables must add up to the same revenue. A wrong join would break this, and the cell then fails.

-- COMMAND ----------

SELECT
  monthly,
  category,
  seller,
  ASSERT_TRUE(monthly = category AND monthly = seller) AS reconciled
FROM (
  SELECT
    (SELECT SUM(revenue) FROM gold_monthly_sales) AS monthly,
    (SELECT SUM(revenue) FROM gold_category_sales) AS category,
    (SELECT SUM(revenue) FROM gold_seller_performance) AS seller
);
