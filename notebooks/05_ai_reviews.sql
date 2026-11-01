-- Databricks notebook source
-- MAGIC %md
-- MAGIC # AI: review sentiment
-- MAGIC
-- MAGIC Classifies the sentiment of customer comments with the `ai_analyze_sentiment` function, then compares it with the score the customer gave.
-- MAGIC
-- MAGIC Uses a fixed sample of 1,000 comments, because AI functions call a model for every row and have usage limits.

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

USE SCHEMA olist;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_review_sentiment AS
WITH sampled AS (
  SELECT review_id, order_id, review_score, review_comment_message
  FROM silver_order_reviews
  WHERE LENGTH(TRIM(review_comment_message)) > 0
  ORDER BY review_id, order_id
  LIMIT 1000
)
SELECT
  *,
  ai_analyze_sentiment(review_comment_message) AS sentiment
FROM sampled;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Compare sentiment with review score
-- MAGIC
-- MAGIC The score is not a perfect label, but high scores should be mostly positive and low scores mostly negative.

-- COMMAND ----------

SELECT
  review_score,
  COUNT(*) AS comments,
  SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive,
  SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END) AS neutral,
  SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative,
  SUM(CASE WHEN sentiment = 'mixed' THEN 1 ELSE 0 END) AS mixed,
  SUM(CASE WHEN sentiment IS NULL THEN 1 ELSE 0 END) AS not_classified
FROM gold_review_sentiment
GROUP BY review_score
ORDER BY review_score;
