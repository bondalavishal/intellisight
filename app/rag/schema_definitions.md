# Olist E-Commerce Schema Definitions

## Database
Platform: Databricks Free Edition.
ROUTING RULE: Use VIEWS by default. Use RAW TABLES only when views cannot answer the question.
ONE source per query. NEVER join views. NEVER invent columns.

## WHEN TO USE VIEWS (default)
- Revenue, orders, delivery, seller rankings, product/category performance → use views
- Any question answerable from a single view → always prefer the view

## WHEN TO USE RAW TABLES
- Category-level cancellations → views have no category+status combination
- Freight cost as % of price per category → needs olist_order_items + olist_products
- Review scores per product or category from raw data → olist_order_reviews + olist_products
- Seller performance by category → olist_order_items + olist_products + olist_sellers
- Payment method analysis → olist_order_payments
- Questions explicitly needing joins across order + product + seller dimensions

---

## VIEWS (use by default)

## vw_orders_metrics
Use for: order revenue, delivery performance, order status, customer location.
NO category column. NO seller columns. NO year_month column.
```sql
CREATE VIEW vw_orders_metrics AS SELECT
    order_id       STRING,
    customer_id    STRING,
    order_status   STRING,   -- delivered/shipped/canceled/unavailable/invoiced/processing/created/approved
    order_date     DATE,
    order_year     INT,
    order_month    INT,      -- 1-12
    customer_city  STRING,
    customer_state STRING,   -- 2-letter Brazilian state code e.g. SP, RJ, MG
    order_revenue  DECIMAL,  -- item prices only, excludes freight
    order_freight  DECIMAL,  -- shipping cost
    order_total    DECIMAL,  -- order_revenue + order_freight
    item_count     INT,
    delivery_days  INT       -- NULL if not delivered
FROM ...;
```

## vw_seller_metrics
Use for: seller rankings, seller revenue, seller review scores.
NO time dimension. NO year. NO month. NO date.
```sql
CREATE VIEW vw_seller_metrics AS SELECT
    seller_id        STRING,
    seller_city      STRING,
    seller_state     STRING,
    total_orders     INT,
    total_revenue    DECIMAL,
    avg_order_value  DECIMAL,
    unique_products  INT,
    avg_review_score DECIMAL,
    total_reviews    INT
FROM ...;
```

## vw_product_metrics
Use for: product rankings, category analysis, pricing analysis.
NO time dimension. NO order_year. NO canceled_orders.
```sql
CREATE VIEW vw_product_metrics AS SELECT
    product_id       STRING,
    category         STRING,   -- English name e.g. health_beauty, computers
    product_weight_g INT,
    total_orders     INT,
    total_revenue    DECIMAL,
    avg_price        DECIMAL,
    avg_review_score DECIMAL
FROM ...;
```

## vw_monthly_revenue
Use for: revenue trends, month over month analysis, growth, anomaly detection.
NO order-level columns. NO delivery_days. NO customer columns.
```sql
CREATE VIEW vw_monthly_revenue AS SELECT
    year             INT,
    month            INT,
    year_month       STRING,  -- e.g. '2017-01'
    total_orders     INT,
    total_revenue    DECIMAL,
    avg_order_value  DECIMAL,
    unique_customers INT,
    canceled_orders  INT
FROM ...;
```

---

## RAW TABLES (use only when views cannot answer)

## olist_orders
Use for: order status + date analysis when joining to other raw tables.
```sql
SELECT
    order_id       STRING,
    customer_id    STRING,
    order_status   STRING,   -- delivered/shipped/canceled/unavailable/etc
    order_purchase_timestamp  TIMESTAMP,
    order_approved_at         TIMESTAMP,
    order_delivered_carrier_date   TIMESTAMP,
    order_delivered_customer_date  TIMESTAMP,
    order_estimated_delivery_date  TIMESTAMP
FROM olist_orders;
```

## olist_order_items
Use for: joining orders to products/sellers, freight vs price analysis per category.
Key join table — links order_id → product_id → seller_id.
```sql
SELECT
    order_id      STRING,
    order_item_id BIGINT,
    product_id    STRING,
    seller_id     STRING,
    price         DOUBLE,   -- item price
    freight_value DOUBLE    -- shipping cost for this item
FROM olist_order_items;
```

## olist_products
Use for: category-level analysis requiring joins. Category name is in Portuguese — always join to product_category_translation.
```sql
SELECT
    product_id               STRING,
    product_category_name    STRING,  -- Portuguese — join to translation table
    product_weight_g         BIGINT,
    product_length_cm        BIGINT,
    product_height_cm        BIGINT,
    product_width_cm         BIGINT
FROM olist_products;
```

## product_category_translation
Use for: translating Portuguese category names to English. Always join when using olist_products.
```sql
SELECT
    product_category_name          STRING,  -- Portuguese
    product_category_name_english  STRING   -- English
FROM product_category_translation;
```

## olist_order_reviews
Use for: review scores and comments at order level.
```sql
SELECT
    review_id              STRING,
    order_id               STRING,
    review_score           BIGINT,  -- 1-5
    review_comment_title   STRING,
    review_comment_message STRING,
    review_creation_date   TIMESTAMP
FROM olist_order_reviews;
```

## olist_sellers
Use for: seller location when joining raw tables.
```sql
SELECT
    seller_id             STRING,
    seller_zip_code_prefix BIGINT,
    seller_city           STRING,
    seller_state          STRING
FROM olist_sellers;
```

---

## Critical column rules
- vw_monthly_revenue revenue = total_revenue (NOT order_revenue)
- vw_product_metrics price = avg_price (NOT avg_order_price)
- vw_orders_metrics revenue = order_revenue (NOT total_revenue)
- delivery_days only in vw_orders_metrics
- category (English) only in vw_product_metrics
- For raw category names: join olist_products → product_category_translation on product_category_name
- seller_id in both vw_seller_metrics (aggregated) and olist_order_items (raw)

## Raw table join patterns

### Category cancellations:
```sql
SELECT t.product_category_name_english AS category,
    SUM(CASE WHEN o.order_status = 'canceled' THEN 1 ELSE 0 END) AS canceled,
    COUNT(*) AS total,
    ROUND(SUM(CASE WHEN o.order_status = 'canceled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cancel_pct
FROM olist_orders o
JOIN olist_order_items i ON o.order_id = i.order_id
JOIN olist_products p ON i.product_id = p.product_id
JOIN product_category_translation t ON p.product_category_name = t.product_category_name
GROUP BY t.product_category_name_english
ORDER BY cancel_pct DESC LIMIT 10;
```

### Freight as % of price by category:
```sql
SELECT t.product_category_name_english AS category,
    ROUND(AVG(i.freight_value / NULLIF(i.price, 0)) * 100, 2) AS freight_pct
FROM olist_order_items i
JOIN olist_products p ON i.product_id = p.product_id
JOIN product_category_translation t ON p.product_category_name = t.product_category_name
GROUP BY t.product_category_name_english
ORDER BY freight_pct DESC LIMIT 10;
```

### Seller performance by category:
```sql
SELECT t.product_category_name_english AS category,
    i.seller_id,
    COUNT(DISTINCT i.order_id) AS orders,
    SUM(i.price) AS revenue
FROM olist_order_items i
JOIN olist_products p ON i.product_id = p.product_id
JOIN product_category_translation t ON p.product_category_name = t.product_category_name
GROUP BY t.product_category_name_english, i.seller_id
ORDER BY revenue DESC LIMIT 20;
```
