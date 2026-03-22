# Olist Business Logic

> IMPORTANT: This document contains business context and SQL patterns ONLY.
> "business_logic" is NOT a table or view in Databricks.
> NEVER use business_logic in any SQL query.
> Only use: vw_orders_metrics, vw_seller_metrics, vw_product_metrics, vw_monthly_revenue
> OR raw tables: olist_orders, olist_order_items, olist_products, product_category_translation, olist_order_reviews, olist_sellers

## Dataset Context
- Platform: Olist Brazilian e-commerce marketplace
- Date range: September 2016 to October 2018
- Currency: Brazilian Real (R$)
- Total delivered orders: ~96,478 | Canceled: 625 | Shipped: 1,107
- Avg order value: R$137.75 | Min: R$0.85 | Max: R$13,440
- Largest market: SP (41,746 orders), RJ (12,852), MG (11,635)
- Top categories: bed_bath_table, health_beauty, sports_leisure

## How the Marketplace Works
- Independent sellers list products on the Olist platform
- Customers place orders → seller ships directly to customer
- Olist handles payments and reviews

## Order Lifecycle
1. created → approved → invoiced → processing → shipped → delivered
2. canceled — can happen at any stage
3. unavailable — product became unavailable

## Revenue Recognition
- Revenue = sum of item prices (order_revenue) — excludes freight
- GMV = revenue + freight (order_total)
- "revenue" or "sales" → use order_revenue
- "total transaction value" → use order_total

## Geographic Context
- All locations are in Brazil. State codes are 2-letter Brazilian states.
- SP = São Paulo (largest), RJ = Rio de Janeiro, MG = Minas Gerais
- Remote states (AM, RR, AC, AP) → longer delivery times

## Delivery Performance
- Good: under 10 days | Average: ~12 days | Slow: over 20 days
- delivery_days is NULL for undelivered orders — always filter WHERE delivery_days IS NOT NULL

## Review Scores
- Scale 1–5. Platform average ~4.0
- Below 3.0 = seller quality issue | Below 3.5 = category quality concern

## Anomaly Thresholds
- Revenue drop > 10% MoM = anomaly
- Cancellation rate > 5% = anomaly
- Avg delivery days > 20 = delivery issue
- Seller review < 3.0 = quality issue
- Category review < 3.5 = quality concern

---

## SQL Pattern Library

### PATTERN 1 — Simple aggregation (use views)
Question: "What were total orders and revenue in 2018?"
```sql
SELECT COUNT(DISTINCT order_id) AS total_orders,
       ROUND(SUM(order_revenue), 2) AS total_revenue
FROM vw_orders_metrics
WHERE order_year = 2018
LIMIT 1
```

### PATTERN 2 — Ranking with HAVING filter
Question: "Which sellers have more than 100 orders but average review score below 3?"
```sql
SELECT seller_id, seller_state, total_orders, ROUND(avg_review_score, 2) AS avg_score
FROM vw_seller_metrics
WHERE total_orders > 100 AND avg_review_score < 3.0
ORDER BY avg_review_score ASC
LIMIT 20
```

### PATTERN 3 — Month over month with LAG window function
Question: "What is the month over month revenue change for 2018?"
```sql
WITH monthly AS (
    SELECT year, month, year_month, total_revenue,
           LAG(total_revenue, 1) OVER (ORDER BY year, month) AS prev_revenue
    FROM vw_monthly_revenue
    WHERE year = 2018
)
SELECT year_month,
       ROUND(total_revenue, 2) AS revenue,
       ROUND(prev_revenue, 2) AS prev_month_revenue,
       ROUND((total_revenue - prev_revenue) / NULLIF(prev_revenue, 0) * 100, 2) AS pct_change
FROM monthly
ORDER BY year, month
LIMIT 12
```

### PATTERN 4 — Year over year comparison
Question: "Compare total revenue between 2017 and 2018"
```sql
SELECT year,
       SUM(total_revenue) AS annual_revenue,
       SUM(total_orders) AS annual_orders
FROM vw_monthly_revenue
WHERE year IN (2017, 2018)
GROUP BY year
ORDER BY year
LIMIT 2
```

### PATTERN 5 — Running total / cumulative
Question: "Show me cumulative revenue by month for 2017"
```sql
SELECT year_month,
       ROUND(total_revenue, 2) AS monthly_revenue,
       ROUND(SUM(total_revenue) OVER (PARTITION BY year ORDER BY month), 2) AS cumulative_revenue
FROM vw_monthly_revenue
WHERE year = 2017
ORDER BY month
LIMIT 12
```

### PATTERN 6 — Percentile / top N percent
Question: "Who are the top 10% of sellers by revenue?"
```sql
WITH ranked AS (
    SELECT seller_id, seller_state, total_revenue,
           NTILE(10) OVER (ORDER BY total_revenue DESC) AS decile
    FROM vw_seller_metrics
)
SELECT seller_id, seller_state, ROUND(total_revenue, 2) AS revenue
FROM ranked
WHERE decile = 1
ORDER BY total_revenue DESC
LIMIT 50
```

### PATTERN 7 — Above/below average comparison
Question: "Which sellers perform above average in both revenue and review score?"
```sql
WITH averages AS (
    SELECT AVG(total_revenue) AS avg_rev,
           AVG(avg_review_score) AS avg_score
    FROM vw_seller_metrics
)
SELECT s.seller_id, s.seller_state,
       ROUND(s.total_revenue, 2) AS revenue,
       ROUND(s.avg_review_score, 2) AS review_score
FROM vw_seller_metrics s
CROSS JOIN averages a
WHERE s.total_revenue > a.avg_rev
  AND s.avg_review_score > a.avg_score
ORDER BY s.total_revenue DESC
LIMIT 20
```

### PATTERN 8 — Delivery time by state (geographic analysis)
Question: "Which states have the worst average delivery time?"
```sql
SELECT customer_state,
       ROUND(AVG(delivery_days), 1) AS avg_delivery_days,
       COUNT(*) AS total_delivered
FROM vw_orders_metrics
WHERE delivery_days IS NOT NULL
GROUP BY customer_state
ORDER BY avg_delivery_days DESC
LIMIT 10
```

### PATTERN 9 — Delivery performance year over year by state
Question: "How has delivery time changed year over year for SP?"
```sql
SELECT order_year,
       ROUND(AVG(delivery_days), 1) AS avg_delivery_days,
       COUNT(*) AS orders
FROM vw_orders_metrics
WHERE delivery_days IS NOT NULL
  AND customer_state = 'SP'
GROUP BY order_year
ORDER BY order_year
LIMIT 5
```

### PATTERN 10 — Cancellation rate overall
Question: "What percentage of orders were cancelled?"
```sql
SELECT ROUND(SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cancel_pct,
       SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END) AS canceled_orders,
       COUNT(*) AS total_orders
FROM vw_orders_metrics
LIMIT 1
```

### PATTERN 11 — Category cancellation rate (raw tables required)
Question: "Which product categories have the highest cancellation rates?"
```sql
SELECT t.product_category_name_english AS category,
       COUNT(*) AS total_orders,
       SUM(CASE WHEN o.order_status = 'canceled' THEN 1 ELSE 0 END) AS canceled,
       ROUND(SUM(CASE WHEN o.order_status = 'canceled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cancel_pct
FROM olist_orders o
JOIN olist_order_items i ON o.order_id = i.order_id
JOIN olist_products p ON i.product_id = p.product_id
JOIN product_category_translation t ON p.product_category_name = t.product_category_name
GROUP BY t.product_category_name_english
HAVING COUNT(*) > 100
ORDER BY cancel_pct DESC
LIMIT 10
```

### PATTERN 12 — Freight as % of price by category (raw tables required)
Question: "Which categories have the highest freight cost relative to price?"
```sql
SELECT t.product_category_name_english AS category,
       ROUND(AVG(i.freight_value / NULLIF(i.price, 0)) * 100, 2) AS freight_pct_of_price,
       ROUND(AVG(i.price), 2) AS avg_price,
       ROUND(AVG(i.freight_value), 2) AS avg_freight
FROM olist_order_items i
JOIN olist_products p ON i.product_id = p.product_id
JOIN product_category_translation t ON p.product_category_name = t.product_category_name
GROUP BY t.product_category_name_english
HAVING COUNT(*) > 200
ORDER BY freight_pct_of_price DESC
LIMIT 10
```

### PATTERN 13 — Revenue per unit sold by category
Question: "Which product categories have the highest revenue per unit sold?"
```sql
SELECT category,
       ROUND(total_revenue / NULLIF(total_orders, 0), 2) AS revenue_per_unit,
       total_orders,
       ROUND(total_revenue, 2) AS total_revenue
FROM vw_product_metrics
GROUP BY category, total_revenue, total_orders
ORDER BY revenue_per_unit DESC
LIMIT 10
```

### PATTERN 14 — Multi-condition filter with exclusion
Question: "Top 5 states by revenue excluding São Paulo"
```sql
SELECT customer_state,
       ROUND(SUM(order_revenue), 2) AS total_revenue,
       COUNT(DISTINCT order_id) AS total_orders
FROM vw_orders_metrics
WHERE customer_state != 'SP'
GROUP BY customer_state
ORDER BY total_revenue DESC
LIMIT 5
```

### PATTERN 15 — Average order value comparison across segments
Question: "What is the average order value for delivered vs cancelled orders?"
```sql
SELECT order_status,
       COUNT(*) AS order_count,
       ROUND(AVG(order_revenue), 2) AS avg_order_value,
       ROUND(SUM(order_revenue), 2) AS total_revenue
FROM vw_orders_metrics
WHERE order_status IN ('delivered', 'canceled')
GROUP BY order_status
LIMIT 2
```

### PATTERN 16 — Seller review score distribution
Question: "How many sellers fall into each review score bucket?"
```sql
SELECT CASE
           WHEN avg_review_score >= 4.5 THEN 'Excellent (4.5-5.0)'
           WHEN avg_review_score >= 4.0 THEN 'Good (4.0-4.5)'
           WHEN avg_review_score >= 3.0 THEN 'Average (3.0-4.0)'
           ELSE 'Poor (below 3.0)'
       END AS score_bucket,
       COUNT(*) AS seller_count
FROM vw_seller_metrics
GROUP BY score_bucket
ORDER BY MIN(avg_review_score) DESC
LIMIT 10
```

### PATTERN 17 — Month with highest/lowest metric
Question: "Which month had the highest revenue across all years?"
```sql
SELECT year_month, year, month,
       ROUND(total_revenue, 2) AS revenue
FROM vw_monthly_revenue
ORDER BY total_revenue DESC
LIMIT 1
```

### PATTERN 18 — Growth rate ranking
Question: "Which months had revenue growth above 20%?"
```sql
WITH growth AS (
    SELECT year_month, year, month, total_revenue,
           LAG(total_revenue, 1) OVER (ORDER BY year, month) AS prev_revenue
    FROM vw_monthly_revenue
)
SELECT year_month,
       ROUND(total_revenue, 2) AS revenue,
       ROUND((total_revenue - prev_revenue) / NULLIF(prev_revenue, 0) * 100, 2) AS growth_pct
FROM growth
WHERE prev_revenue IS NOT NULL
  AND (total_revenue - prev_revenue) / NULLIF(prev_revenue, 0) * 100 > 20
ORDER BY growth_pct DESC
LIMIT 12
```

### PATTERN 19 — Seller category performance (raw tables required)
Question: "Which sellers have the most orders in health_beauty?"
```sql
SELECT i.seller_id,
       COUNT(DISTINCT i.order_id) AS orders,
       ROUND(SUM(i.price), 2) AS revenue
FROM olist_order_items i
JOIN olist_products p ON i.product_id = p.product_id
JOIN product_category_translation t ON p.product_category_name = t.product_category_name
WHERE t.product_category_name_english = 'health_beauty'
GROUP BY i.seller_id
ORDER BY orders DESC
LIMIT 10
```

### PATTERN 20 — Review score trend over time (raw tables required)
Question: "How have average review scores changed over time?"
```sql
SELECT YEAR(r.review_creation_date) AS year,
       MONTH(r.review_creation_date) AS month,
       ROUND(AVG(r.review_score), 2) AS avg_score,
       COUNT(*) AS review_count
FROM olist_order_reviews r
GROUP BY YEAR(r.review_creation_date), MONTH(r.review_creation_date)
HAVING COUNT(*) > 100
ORDER BY year, month
LIMIT 30
```

### PATTERN 21 — States punching above their weight
Question: "Which states have high review scores but lower than average order volume?"
```sql
WITH state_stats AS (
    SELECT customer_state,
           COUNT(DISTINCT order_id) AS orders,
           ROUND(AVG(CASE WHEN delivery_days IS NOT NULL THEN delivery_days END), 1) AS avg_delivery
    FROM vw_orders_metrics
    GROUP BY customer_state
),
avg_orders AS (
    SELECT AVG(orders) AS avg_ord FROM state_stats
),
seller_scores AS (
    SELECT seller_state,
           ROUND(AVG(avg_review_score), 2) AS avg_score
    FROM vw_seller_metrics
    GROUP BY seller_state
)
SELECT s.customer_state,
       s.orders,
       sc.avg_score AS review_score,
       s.avg_delivery AS avg_delivery_days
FROM state_stats s
JOIN seller_scores sc ON s.customer_state = sc.seller_state
CROSS JOIN avg_orders a
WHERE s.orders < a.avg_ord
  AND sc.avg_score >= 4.0
ORDER BY sc.avg_score DESC
LIMIT 10
```

### PATTERN 22 — Seasonal analysis
Question: "Which months consistently have the highest order volume?"
```sql
SELECT month,
       ROUND(AVG(total_orders), 0) AS avg_orders,
       ROUND(AVG(total_revenue), 2) AS avg_revenue,
       COUNT(*) AS years_of_data
FROM vw_monthly_revenue
GROUP BY month
ORDER BY avg_orders DESC
LIMIT 12
```

### PATTERN 23 — Delivery vs estimated (raw tables required)
Question: "What percentage of orders were delivered late vs on time?"
```sql
SELECT
    SUM(CASE WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 1 ELSE 0 END) AS on_time,
    SUM(CASE WHEN order_delivered_customer_date > order_estimated_delivery_date THEN 1 ELSE 0 END) AS late,
    COUNT(*) AS total_delivered,
    ROUND(SUM(CASE WHEN order_delivered_customer_date > order_estimated_delivery_date THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS late_pct
FROM olist_orders
WHERE order_status = 'delivered'
  AND order_delivered_customer_date IS NOT NULL
  AND order_estimated_delivery_date IS NOT NULL
LIMIT 1
```

### PATTERN 24 — Price band analysis
Question: "How many orders fall into each price range?"
```sql
SELECT CASE
           WHEN order_revenue < 50 THEN 'Under R$50'
           WHEN order_revenue < 100 THEN 'R$50-100'
           WHEN order_revenue < 200 THEN 'R$100-200'
           WHEN order_revenue < 500 THEN 'R$200-500'
           ELSE 'Over R$500'
       END AS price_band,
       COUNT(*) AS order_count,
       ROUND(AVG(order_revenue), 2) AS avg_order_value
FROM vw_orders_metrics
GROUP BY price_band
ORDER BY MIN(order_revenue)
LIMIT 10
```

### PATTERN 25 — Seller concentration (market share)
Question: "What percentage of total revenue comes from the top 10 sellers?"
```sql
WITH total AS (
    SELECT SUM(total_revenue) AS grand_total FROM vw_seller_metrics
),
top10 AS (
    SELECT SUM(total_revenue) AS top10_revenue
    FROM (SELECT total_revenue FROM vw_seller_metrics ORDER BY total_revenue DESC LIMIT 10)
)
SELECT ROUND(top10.top10_revenue, 2) AS top10_revenue,
       ROUND(total.grand_total, 2) AS total_revenue,
       ROUND(top10.top10_revenue * 100.0 / total.grand_total, 2) AS market_share_pct
FROM top10
CROSS JOIN total
LIMIT 1
```

---

## Unanswerable Questions (return message, no SQL)
These cannot be answered from available data:
- Individual customer behaviour over time (no customer history table)
- Seller improvement trends over time (vw_seller_metrics has no time dimension)
- Real-time inventory or stock levels (static dataset)
- Profit margins (no cost data, only revenue)
- Marketing spend or ROI (no marketing data)
- Competitor analysis (single platform dataset)

Pattern for unanswerable:
```sql
SELECT 'This question cannot be answered from the available data.' AS message LIMIT 1
```
