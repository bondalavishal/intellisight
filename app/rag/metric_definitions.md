# Olist Metric Definitions

## Revenue Metrics

### Total Revenue
- Definition: Sum of all item prices excluding freight
- SQL: SUM(order_revenue) FROM vw_orders_metrics
- Use when: stakeholder asks about revenue, sales, earnings

### Total GMV (Gross Merchandise Value)
- Definition: Sum of all item prices including freight
- SQL: SUM(order_total) FROM vw_orders_metrics
- Use when: stakeholder asks about total transaction value

### Average Order Value (AOV)
- Definition: Average revenue per order
- SQL: AVG(order_revenue) FROM vw_orders_metrics
- Use when: stakeholder asks about average order size or ticket size

### Monthly Revenue Growth
- Definition: Percentage change in total_revenue vs previous month
- SQL: Use vw_monthly_revenue, compare current vs LAG(total_revenue,1) OVER (ORDER BY year, month)
- Use when: stakeholder asks about growth, trends, month over month

---

## Order Metrics

### Total Orders
- Definition: Count of distinct order_ids
- SQL: COUNT(DISTINCT order_id) FROM vw_orders_metrics
- Use when: stakeholder asks about order volume, number of orders

### Cancellation Rate
- Definition: Percentage of orders with status = canceled
- SQL: SUM(canceled_orders) * 100.0 / SUM(total_orders) FROM vw_monthly_revenue
- Use when: stakeholder asks about cancellations, failed orders

### Delivery Rate
- Definition: Percentage of orders successfully delivered
- SQL: COUNT(*) FILTER (WHERE order_status = 'delivered') * 100.0 / COUNT(*) FROM vw_orders_metrics
- Use when: stakeholder asks about fulfillment, delivery success

### Average Delivery Time
- Definition: Average number of days from order placement to delivery
- SQL: AVG(delivery_days) FROM vw_orders_metrics WHERE delivery_days IS NOT NULL
- Use when: stakeholder asks about delivery speed, shipping time

---

## Seller Metrics

### Top Sellers by Revenue
- Definition: Sellers ranked by total_revenue descending
- SQL: SELECT seller_id, seller_state, total_revenue FROM vw_seller_metrics ORDER BY total_revenue DESC
- Use when: stakeholder asks about best performing sellers

### Seller Review Score
- Definition: Average customer review score per seller (scale 1.0 to 5.0)
- SQL: AVG(avg_review_score) FROM vw_seller_metrics
- Use when: stakeholder asks about seller quality, customer satisfaction

---

## Product Metrics

### Top Categories by Revenue
- Definition: Product categories ranked by total_revenue
- SQL: SELECT category, SUM(total_revenue) FROM vw_product_metrics GROUP BY category ORDER BY SUM(total_revenue) DESC
- Use when: stakeholder asks about best selling categories

### Average Review Score by Category
- Definition: Average review score grouped by product category
- SQL: SELECT category, AVG(avg_review_score) FROM vw_product_metrics GROUP BY category
- Use when: stakeholder asks about product quality or customer satisfaction by category

---

## Anomaly Indicators

### Revenue Drop
- Definition: Month where total_revenue decreased more than 10% vs prior month
- Signal: LAG comparison in vw_monthly_revenue
- Possible causes: seasonal patterns, data gaps, external events

### Cancellation Spike
- Definition: Month where canceled_orders increased more than 20% vs prior month
- Signal: LAG comparison on canceled_orders in vw_monthly_revenue
- Possible causes: supply issues, seller problems, payment failures

### Delivery Time Spike
- Definition: State or month where avg delivery_days exceeded 20 days
- Signal: AVG(delivery_days) > 20 in vw_orders_metrics
- Possible causes: logistics issues, remote regions, high demand periods

---

## Important Notes
- Currency: All monetary values are in Brazilian Real (BRL)
- Nulls: delivery_days is NULL for undelivered orders — always filter WHERE delivery_days IS NOT NULL for delivery analysis
- Time period: September 2016 to October 2018
- Always prefer views over raw tables in SQL generation

## Category order count and price analysis
- total_orders in vw_product_metrics is per PRODUCT not per category
- To get category-level order counts: GROUP BY category and SUM(total_orders)
- To filter categories with more than N orders: HAVING SUM(total_orders) > N
- To filter by average price: HAVING AVG(avg_price) < N

Correct pattern:
SELECT category, SUM(total_orders) AS category_orders, ROUND(AVG(avg_price), 2) AS avg_price
FROM vw_product_metrics
GROUP BY category
HAVING SUM(total_orders) > 500 AND AVG(avg_price) < 50
ORDER BY category_orders DESC LIMIT 50
