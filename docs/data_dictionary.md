# Data dictionary

All tables live in `workspace.olist`. Column-level descriptions are also available in Unity Catalog.

## Layers

| Layer | Tables | Purpose |
|---|---|---|
| Bronze | `bronze_*` | Source CSVs as strings, plus `_source_file` and `_ingested_at` |
| Silver | `silver_*` | Typed and cleaned data, with suspicious rows marked in `dq_*` columns |
| Gold | `gold_*` | Business KPIs |
| Quality | `dq_results` | History of data quality checks |

## Gold tables

### `gold_monthly_sales`
One row per purchase month.

| Column | Description |
|---|---|
| `sales_month` | First day of the purchase month |
| `orders` | Distinct orders with at least one item |
| `customers` | Distinct real customers in the month. Do not add across months |
| `items_sold` | Number of items sold |
| `revenue` | Sum of item prices in BRL, excluding freight |
| `freight` | Sum of freight in BRL |
| `avg_order_value` | Revenue divided by orders |

### `gold_category_sales`
One row per product category.

| Column | Description |
|---|---|
| `category` | Category in English, or `unknown` |
| `orders` | Distinct orders with this category. An order can count in several categories |
| `items_sold` | Number of items sold |
| `revenue` | Sum of item prices in BRL, excluding freight |
| `avg_item_price` | Average item price in BRL |

### `gold_delivery_performance`
One row per purchase month and customer state.

| Column | Description |
|---|---|
| `purchase_month` | First day of the purchase month |
| `customer_state` | Brazilian state of the customer |
| `delivered_orders` | Number of delivered orders |
| `avg_delivery_days` | Average calendar days from purchase to delivery. Weight by `delivered_orders` when combining rows |
| `late_orders` | Orders delivered on a later calendar day than estimated |

### `gold_seller_performance`
One row per seller.

| Column | Description |
|---|---|
| `seller_id` | Seller key |
| `seller_state` | Brazilian state of the seller |
| `orders` | Distinct orders of this seller. An order with two sellers counts once for each |
| `items_sold` | Number of items sold |
| `revenue` | Sum of item prices in BRL, excluding freight |
| `reviewed_orders` | Orders of this seller with at least one review |
| `avg_review_score` | Average review score, with one score per order. Reviews rate the whole order, not only this seller |

### `gold_review_sentiment`
One row per review and order, for a fixed sample of 1,000 comments. Not representative of all reviews.

| Column | Description |
|---|---|
| `review_id` | Review key, together with `order_id` |
| `order_id` | Order key, together with `review_id` |
| `review_score` | Customer score from 1 to 5 |
| `review_comment_message` | Free text in Portuguese |
| `sentiment` | Sentiment returned by the model: positive, negative, neutral or mixed |

## Silver tables

| Table | Grain | Primary key |
|---|---|---|
| `silver_orders` | Order | `order_id` |
| `silver_order_items` | Item in an order | `order_id`, `order_item_id` |
| `silver_order_payments` | Payment of an order | `order_id`, `payment_sequential` |
| `silver_order_reviews` | Review of an order | `review_id`, `order_id` |
| `silver_products` | Product | `product_id` |
| `silver_customers` | Customer per order | `customer_id` |
| `silver_sellers` | Seller | `seller_id` |
| `silver_category_translation` | Category | `product_category_name` |
| `silver_geolocation` | Coordinates of a zip code prefix | None: a prefix can have many rows |

## Data quality flags

Marked rows are kept, so each KPI can decide whether to exclude them.

| Table | Column | True when |
|---|---|---|
| `silver_orders` | `dq_carrier_before_approval` | The carrier received the order before its approval |
| `silver_orders` | `dq_delivered_before_carrier` | The customer delivery date is earlier than the carrier date |
| `silver_orders` | `dq_delivered_without_date` | The status is `delivered` but there is no delivery date |
| `silver_orders` | `dq_date_without_delivered_status` | There is a delivery date but the status is not `delivered` |
| `silver_order_payments` | `dq_non_positive_value` | The payment value is zero or negative |
| `silver_order_payments` | `dq_undefined_payment_type` | The payment type is `not_defined` |
