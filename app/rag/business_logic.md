# Olist Business Logic

> IMPORTANT: This document contains business context ONLY.
> "business_logic" is NOT a table or view in Databricks.
> NEVER use business_logic in any SQL query.
> Only use: vw_orders_metrics, vw_seller_metrics, vw_product_metrics, vw_monthly_revenue

## How the Marketplace Works
- Olist is a Brazilian e-commerce marketplace
- Independent sellers list their products on the Olist platform
- Customers place orders through the platform
- Sellers ship directly to customers
- Olist handles payments and reviews

## Order Lifecycle
1. created    → order placed by customer
2. approved   → payment approved
3. invoiced   → invoice generated
4. processing → seller preparing the order
5. shipped    → order handed to logistics
6. delivered  → customer received the order
7. canceled   → order was canceled at any stage
8. unavailable → product became unavailable

## Revenue Recognition
- Revenue = sum of item prices (order_revenue) — does NOT include freight
- GMV = revenue + freight (order_total)
- When stakeholder says "revenue" or "sales" → use order_revenue
- When stakeholder says "total transaction value" → use order_total

## Geographic Context
- All locations are in Brazil
- State codes are Brazilian states
- SP (Sao Paulo) is the largest market by volume
- RJ (Rio de Janeiro) is second largest
- Remote states (AM, RR, AC) tend to have longer delivery times

## Delivery Performance
- Good delivery time = under 10 days
- Average delivery time = around 12 days
- Slow delivery = over 20 days
- delivery_days is NULL for orders not yet delivered
- Always filter WHERE delivery_days IS NOT NULL for delivery analysis

## Review Scores
- Scale: 1 to 5 stars
- 5 = excellent, 4 = good, 3 = average, below 3 = concerning
- Platform average is around 4.0

## Anomaly Thresholds
- Revenue drop > 10% month over month = anomaly worth flagging
- Cancellation rate > 5% in a month = anomaly worth flagging
- Avg delivery days > 20 = delivery performance issue
- Seller review score < 3.0 = seller quality issue
- Category review score < 3.5 = category quality issue
