import os
import re
import httpx
from cerebras.cloud.sdk import Cerebras
from dotenv import load_dotenv

load_dotenv()

# ── Cerebras (primary) ────────────────────────────────────────────────────────
_cerebras = Cerebras(api_key=os.getenv("CEREBRAS_API_KEY"))
CEREBRAS_MODEL = "qwen-3-235b-a22b-instruct-2507"

# ── Ollama (fallback) ─────────────────────────────────────────────────────────
OLLAMA_URL  = "http://127.0.0.1:11434/api/generate"
OLLAMA_MODEL = "mannix/defog-llama3-sqlcoder-8b"

from app.rag.retriever import retrieve

SQL_PROMPT = """You are an expert Databricks SQL generator. Convert the question into a single SQL query.

STRICT RULES:
- Use views by default. Use raw tables (olist_orders, olist_order_items, olist_products, product_category_translation, olist_order_reviews, olist_sellers) ONLY when views cannot answer
- When using raw tables: ALWAYS join olist_products to product_category_translation on product_category_name to get English category names. English name column is t.product_category_name_english (on the translation table alias t) — NEVER p.product_category_name_english. olist_products has NO product_category_name_english column.
- NEVER join views to each other
- Never invent columns not listed in the DDL
- Never use SUM(*) — use COUNT(*) for row counts
- Never use aggregates in WHERE — use HAVING
- Always include LIMIT
- Never use spaces in aliases — underscores only
- For cancellations from vw_orders_metrics: SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END)
- For category-level cancellations: join olist_orders + olist_order_items + olist_products + product_category_translation
- For freight analysis by category: join olist_order_items + olist_products + product_category_translation
- For sellers across multiple categories: SELECT i.seller_id, COUNT(DISTINCT t.product_category_name_english) AS categories FROM olist_order_items i JOIN olist_products p ON i.product_id=p.product_id JOIN product_category_translation t ON p.product_category_name=t.product_category_name GROUP BY i.seller_id ORDER BY categories DESC LIMIT 10
- For top seller categories: WITH top AS (SELECT seller_id FROM vw_seller_metrics ORDER BY total_revenue DESC LIMIT 1) SELECT DISTINCT t.product_category_name_english FROM olist_order_items i JOIN top ON i.seller_id=top.seller_id JOIN olist_products p ON i.product_id=p.product_id JOIN product_category_translation t ON p.product_category_name=t.product_category_name LIMIT 20
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
    # Safety net: fix known alias mistake regardless of which model ran
    sql = sql.replace("p.product_category_name_english", "t.product_category_name_english")
    return sql.strip()


def _via_cerebras(prompt: str) -> str:
    response = _cerebras.chat.completions.create(
        model=CEREBRAS_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=512,
        timeout=45,
    )
    return response.choices[0].message.content.strip()


def _via_ollama(prompt: str) -> str:
    response = httpx.post(
        OLLAMA_URL,
        json={
            "model":   OLLAMA_MODEL,
            "prompt":  prompt,
            "stream":  False,
            "options": {"temperature": 0, "num_predict": 512, "stop": ["###", "\n\n\n"]}
        },
        timeout=120
    )
    return response.json()["response"].strip()


def generate_sql(question: str) -> str:
    context = retrieve(question)
    prompt  = SQL_PROMPT.format(question=question, context=context)

    # Primary: Cerebras
    try:
        raw = _via_cerebras(prompt)
        print("[LLM] SQL via Cerebras")
        return _extract_sql(raw)
    except Exception as e:
        print(f"[LLM] Cerebras failed ({e}) — falling back to Ollama")

    # Fallback: Ollama
    try:
        raw = _via_ollama(prompt)
        print("[LLM] SQL via Ollama (fallback)")
        return _extract_sql(raw)
    except Exception as e:
        print(f"[LLM] Ollama also failed ({e})")
        raise RuntimeError("Both Cerebras and Ollama unavailable for SQL generation.")
