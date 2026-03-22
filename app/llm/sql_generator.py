import os
import re
from groq import Groq

from app.rag.retriever import retrieve

_client = Groq(api_key=os.getenv("GROQ_API_KEY"))

SQL_PROMPT = """You are an expert Databricks SQL generator. Convert the question into a single SQL query.

STRICT RULES:
- Use views by default. Use raw tables (olist_orders, olist_order_items, olist_products, product_category_translation, olist_order_reviews, olist_sellers) ONLY when views cannot answer
- When using raw tables: always join olist_products to product_category_translation on product_category_name to get English category names
- NEVER join views to each other
- Never invent columns not listed in the DDL
- Never use SUM(*) — use COUNT(*) for row counts
- Never use aggregates in WHERE — use HAVING
- Always include LIMIT
- Never use spaces in aliases — underscores only
- For cancellations from vw_orders_metrics: SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END)
- For category-level cancellations: join olist_orders + olist_order_items + olist_products + product_category_translation
- For freight analysis by category: join olist_order_items + olist_products + product_category_translation
- For year comparisons: use vw_monthly_revenue GROUP BY year
- If the question cannot be answered from available data:
  SELECT 'This question cannot be answered from the available data.' AS message LIMIT 1

Question: {question}

DDL statements:
{context}

Reply with ONLY the SQL query inside a ```sql fence. No explanation, no commentary."""


def _extract_sql(raw: str) -> str:
    fence_match = re.search(r'```sql\s*(.*?)(?:```|$)', raw, re.DOTALL | re.IGNORECASE)
    if fence_match:
        sql = fence_match.group(1).strip()
    else:
        plain_match = re.search(r'```\s*(.*?)(?:```|$)', raw, re.DOTALL)
        if plain_match:
            sql = plain_match.group(1).strip()
        else:
            fallback = re.search(r'(?im)^(WITH|SELECT)\b', raw)
            sql = raw[fallback.start():].strip() if fallback else raw.strip()

    sql = sql.rstrip(";").strip()
    sql = re.sub(r'(?i)^(SELECT\s+)+', 'SELECT ', sql)
    return sql.strip()


def generate_sql(question: str) -> str:
    context = retrieve(question)
    prompt = SQL_PROMPT.format(question=question, context=context)

    response = _client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=512,
        timeout=45,
    )
    raw = response.choices[0].message.content.strip()
    return _extract_sql(raw)
