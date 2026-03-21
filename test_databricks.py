from app.sql.connector import run_query

print("=== vw_monthly_revenue ===")
for row in run_query("SELECT * FROM vw_monthly_revenue LIMIT 3"):
    print(row)

print("\n=== vw_orders_metrics ===")
for row in run_query("SELECT * FROM vw_orders_metrics LIMIT 3"):
    print(row)

print("\n=== vw_product_metrics ===")
for row in run_query("SELECT * FROM vw_product_metrics LIMIT 3"):
    print(row)

print("\n=== vw_seller_metrics ===")
for row in run_query("SELECT * FROM vw_seller_metrics LIMIT 3"):
    print(row)
