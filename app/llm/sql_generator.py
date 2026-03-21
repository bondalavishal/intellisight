import re
import httpx

from app.rag.retriever import retrieve

OLLAMA_URL = "http://127.0.0.1:11434/api/generate"
MODEL = "mannix/defog-llama3-sqlcoder-8b"

SQL_PROMPT = """### Instructions:
Your task is to convert a question into a SQL query for Databricks SQL.
Use ONLY the views provided in the DDL below. ONE view per query. NEVER join views.
Never invent columns not listed. Never use SUM(*). Use COUNT(*) for row counts.
Never use aggregates in WHERE — use HAVING. Always include LIMIT.
Never use spaces in aliases — underscores only.
For cancellations from vw_orders_metrics: SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END)
For year comparisons: use vw_monthly_revenue GROUP BY year.
If unanswerable (seller time trends, category cancellations):
  SELECT 'This question cannot be answered from the available data.' AS message LIMIT 1

### Input:
Generate a SQL query that answers the question: `{question}`

### DDL statements:
{context}

### Response:
Based on the DDL statements, here is the SQL query that answers the question:
```sql"""


def _extract_sql(raw: str) -> str:
    # Primary: extract from ```sql fence
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
    # Phase 4: retrieve relevant context from ChromaDB instead of hardcoded DDL
    context = retrieve(question)

    prompt = SQL_PROMPT.format(question=question, context=context)
    response = httpx.post(
        OLLAMA_URL,
        json={
            "model": MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0,
                "num_predict": 512,
                "stop": ["```", "###", "\n\n\n"]
            }
        },
        timeout=120
    )
    raw = response.json()["response"].strip()
    return _extract_sql(raw)
