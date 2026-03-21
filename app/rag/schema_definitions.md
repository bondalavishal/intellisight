# Olist E-Commerce Schema Definitions

## Database
Platform: Databricks Free Edition. Always query VIEWS not raw tables.
ONE view per query. NEVER join views. NEVER invent columns.

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
    order_revenue  DECIMAL,  -- item prices only, excludes freight. USE THIS for revenue questions.
    order_freight  DECIMAL,  -- shipping cost
    order_total    DECIMAL,  -- order_revenue + order_freight
    item_count     INT,
    delivery_days  INT       -- NULL if not delivered. Always filter IS NOT NULL for delivery analysis.
FROM ...;
```

## vw_seller_metrics
Use for: seller rankings, seller revenue, seller review scores.
NO time dimension. NO year. NO month. NO date. Cannot answer seller trend questions.
```sql
CREATE VIEW vw_seller_metrics AS SELECT
    seller_id        STRING,
    seller_city      STRING,
    seller_state     STRING,   -- 2-letter Brazilian state code
    total_orders     INT,      -- lifetime total, pre-aggregated
    total_revenue    DECIMAL,  -- lifetime revenue, pre-aggregated
    avg_order_value  DECIMAL,
    unique_products  INT,
    avg_review_score DECIMAL,  -- lifetime average 1.0-5.0
    total_reviews    INT
FROM ...;
```

## vw_product_metrics
Use for: product rankings, category analysis, pricing analysis.
NO time dimension. NO order_year. NO canceled_orders.
```sql
CREATE VIEW vw_product_metrics AS SELECT
    product_id       STRING,
    category         STRING,   -- English name e.g. health_beauty, computers, furniture
    product_weight_g INT,
    total_orders     INT,      -- pre-aggregated total order count
    total_revenue    DECIMAL,  -- pre-aggregated total revenue
    avg_price        DECIMAL,  -- USE THIS for price questions. NOT avg_order_price.
    avg_review_score DECIMAL   -- 1.0-5.0
FROM ...;
```

## vw_monthly_revenue
Use for: revenue trends, month over month analysis, growth analysis, anomaly detection.
NO order-level columns. NO delivery_days. NO customer columns.
```sql
CREATE VIEW vw_monthly_revenue AS SELECT
    year             INT,     -- e.g. 2017
    month            INT,     -- 1-12
    year_month       STRING,  -- e.g. '2017-01'
    total_orders     INT,
    total_revenue    DECIMAL, -- USE THIS for monthly revenue. NOT order_revenue.
    avg_order_value  DECIMAL,
    unique_customers INT,
    canceled_orders  INT
FROM ...;
```

## Critical column rules
- vw_monthly_revenue revenue column = total_revenue (NOT order_revenue)
- vw_product_metrics price column = avg_price (NOT avg_order_price, NOT price)
- vw_orders_metrics revenue column = order_revenue (NOT total_revenue)
- delivery_days only exists in vw_orders_metrics
- category only exists in vw_product_metrics
- seller_id only exists in vw_seller_metrics
