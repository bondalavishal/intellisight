import re
import time
import os
from groq import Groq

from app.llm.intent import classify_intent
from app.llm.sql_generator import generate_sql
from app.sql.guardrails import validate_sql, enforce_limit
from app.sql.connector import run_query
from app.eval.cache import get_cached, save_to_cache, cache_stats
from app.eval.logger import log, get_stats

_groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))

# ---------------------------------------------------------------------------
# Pre-flight: unanswerable questions
# ---------------------------------------------------------------------------
UNANSWERABLE_PATTERNS = [
    (
        r'seller.{0,40}(improv|trend|over time|month.by.month|month over month|histor)'
        r'|(improv|trend|over time).{0,40}seller'
        r'|seller.{0,30}review.{0,30}(over time|trend|month|improv)',
        "vw_seller_metrics has no time dimension — seller metrics are lifetime aggregates only."
    ),
    (
        r'categor.{0,30}cancel|cancel.{0,30}categor',
        "vw_orders_metrics has no category column — cancellation cannot be broken down by category."
    ),
]

# SQL overrides
DELIVERY_OVERRIDE_PATTERN = re.compile(
    r'(average|avg).{0,20}(delivery|shipping).{0,20}(time|days|duration).{0,20}(by state|per state|state)',
    re.IGNORECASE
)
CANCEL_MONTHS_OVERRIDE_PATTERN = re.compile(
    r'month.{0,30}cancel.{0,30}(rate|exceed|over|above|\d+%)',
    re.IGNORECASE
)
SELLER_REVIEW_OVERRIDE_PATTERN = re.compile(
    r'seller.{0,30}review.{0,30}(below|more than|point|1 point|1\.0)',
    re.IGNORECASE
)

# Stats command — user can ask InsightBot for its own performance metrics
STATS_PATTERN = re.compile(
    r'(insightbot|bot).{0,20}(stat|metric|performance|log|how many|pass rate)',
    re.IGNORECASE
)


def _check_unanswerable(question: str) -> str | None:
    q = question.lower()
    for pattern, reason in UNANSWERABLE_PATTERNS:
        if re.search(pattern, q):
            return reason
    return None


def _generate_sql_with_overrides(question: str) -> str:
    if DELIVERY_OVERRIDE_PATTERN.search(question):
        print("[InsightBot] Override: delivery by state")
        return """SELECT customer_state,
  ROUND(AVG(delivery_days), 1) AS avg_delivery_days
FROM vw_orders_metrics
WHERE delivery_days IS NOT NULL
GROUP BY customer_state
ORDER BY avg_delivery_days DESC
LIMIT 27"""

    if CANCEL_MONTHS_OVERRIDE_PATTERN.search(question):
        print("[InsightBot] Override: cancellation months")
        return """SELECT year_month,
  total_orders,
  canceled_orders,
  ROUND(canceled_orders * 100.0 / total_orders, 2) AS cancel_pct
FROM vw_monthly_revenue
WHERE total_orders > 0
  AND canceled_orders * 100.0 / total_orders > 5
ORDER BY cancel_pct DESC
LIMIT 20"""

    if SELLER_REVIEW_OVERRIDE_PATTERN.search(question):
        print("[InsightBot] Override: seller review below threshold")
        return """SELECT seller_id, seller_city, seller_state,
  avg_review_score,
  total_orders
FROM vw_seller_metrics
WHERE avg_review_score < (
  SELECT AVG(avg_review_score) FROM vw_seller_metrics
) - 1.0
ORDER BY avg_review_score ASC
LIMIT 50"""

    return generate_sql(question)


# ---------------------------------------------------------------------------
# Phase 5 — Anomaly Detection
# ---------------------------------------------------------------------------
DELIVERY_ANOMALY_DAYS    = 20.0
CANCELLATION_ANOMALY_PCT = 5.0
REVENUE_DROP_ANOMALY_PCT = 10.0
SELLER_REVIEW_ANOMALY    = 3.0


def detect_anomalies(question: str, results: list[dict]) -> list[str]:
    if not results:
        return []

    flags = []
    q = question.lower()
    keys = list(results[0].keys())

    delivery_cols = [k for k in keys if "delivery" in k.lower() and "day" in k.lower()]
    if delivery_cols:
        col = delivery_cols[0]
        for row in results:
            try:
                if float(row.get(col)) > DELIVERY_ANOMALY_DAYS:
                    state = row.get("customer_state", "Unknown")
                    flags.append(
                        f"⚠️ *Anomaly:* {state} avg delivery is {row.get(col)} days "
                        f"— exceeds the {int(DELIVERY_ANOMALY_DAYS)}-day threshold."
                    )
            except (TypeError, ValueError):
                pass

    cancel_cols = [k for k in keys if "cancel" in k.lower()
                   and any(w in k.lower() for w in ["pct", "rate", "percent"])]
    if cancel_cols:
        col = cancel_cols[0]
        for row in results:
            try:
                fval = float(row.get(col))
                if fval > CANCELLATION_ANOMALY_PCT:
                    period = row.get("year_month", row.get("month", ""))
                    label = f" in {period}" if period else ""
                    flags.append(
                        f"⚠️ *Anomaly:* Cancellation rate{label} is {round(fval, 1)}% "
                        f"— exceeds the {int(CANCELLATION_ANOMALY_PCT)}% threshold."
                    )
            except (TypeError, ValueError):
                pass

    growth_cols = [k for k in keys
                   if any(w in k.lower() for w in ["growth", "pct", "drop", "mom", "change"])]
    if growth_cols:
        col = growth_cols[0]
        for row in results:
            try:
                fval = float(row.get(col))
                if fval < -REVENUE_DROP_ANOMALY_PCT:
                    period = row.get("year_month", row.get("month", ""))
                    label = f" in {period}" if period else ""
                    flags.append(
                        f"⚠️ *Anomaly:* Revenue dropped {abs(round(fval, 1))}%{label} "
                        f"— exceeds the {int(REVENUE_DROP_ANOMALY_PCT)}% drop threshold."
                    )
            except (TypeError, ValueError):
                pass

    review_cols = [k for k in keys if "review" in k.lower() and "score" in k.lower()]
    if review_cols and ("seller" in q or "review" in q):
        col = review_cols[0]
        for row in results:
            try:
                fval = float(row.get(col))
                if fval < SELLER_REVIEW_ANOMALY:
                    seller = row.get("seller_id", "")
                    label = f" ({seller[:8]}...)" if seller else ""
                    flags.append(
                        f"⚠️ *Anomaly:* Seller{label} review score is {round(fval, 2)} "
                        f"— below the {SELLER_REVIEW_ANOMALY} quality threshold."
                    )
            except (TypeError, ValueError):
                pass

    seen = set()
    unique = []
    for f in flags:
        key = f[:60]
        if key not in seen:
            seen.add(key)
            unique.append(f)
        if len(unique) >= 3:
            break

    return unique


# ---------------------------------------------------------------------------
# Summary cleanup
# ---------------------------------------------------------------------------
_PADDING_PHRASES = [
    "There is no message", "No additional information",
    "Delivery durations are not", "Days of delivery are not",
    "The data does not contain", "No message to",
    "There are no messages", "No other information",
    "If you have any further", "Please note that", "Please feel free",
]


def _clean_summary(text: str) -> str:
    for phrase in _PADDING_PHRASES:
        idx = text.find(phrase)
        if idx != -1:
            text = text[:idx].strip()

    sentences = [s.strip() for s in re.split(r'(?<=[.!?])\s+', text) if s.strip()]
    text = " ".join(sentences[:3])
    if text and text[-1] not in ".!?":
        text += "."
    return text.strip()


# ---------------------------------------------------------------------------
# Summariser
# ---------------------------------------------------------------------------
SUMMARY_PROMPT = """You are a business analytics assistant.
A user asked: {question}

The data returned:
{results}

Write a clear 2-3 sentence answer in plain English.
Use actual numbers from the data.
Prefix monetary values with R$ (Brazilian Real).
Say 'days' for delivery durations.
Stop after 3 sentences. Do not add disclaimers or extra notes.

Answer:"""


def summarise_results(question: str, results: list[dict]) -> str:
    if not results:
        return "The query returned no results for that question."

    if len(results) == 1 and "message" in results[0]:
        return results[0]["message"]

    sample = results[:20]
    results_text = "\n".join(str(row) for row in sample)
    if len(results) > 20:
        results_text += f"\n... and {len(results) - 20} more rows."

    response = _groq_client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": SUMMARY_PROMPT.format(question=question, results=results_text)}],
        temperature=0,
        max_tokens=150,
    )
    raw = response.choices[0].message.content.strip()
    return _clean_summary(raw)


# ---------------------------------------------------------------------------
# Single question pipeline
# ---------------------------------------------------------------------------
def _answer_single(question: str, idx: int = None) -> tuple[str, str, str]:
    """
    Runs the full pipeline for one question.
    Returns (reply_text, sql, status) for logging.
    """
    prefix = f"*{idx}.* " if idx is not None else ""
    start  = time.time()

    # Step 0 — Semantic cache check
    cached = get_cached(question)
    if cached:
        latency = round(time.time() - start, 2)
        log(question=question, sql=cached["sql"], rows_returned=0,
            latency_sec=latency, cached=True, status="cache_hit")
        similarity = cached["similarity"]
        reply = (f"{prefix}{cached['answer']}\n"
                 f"_💾 Cached answer (similarity: {similarity})_")
        return reply, cached["sql"], "cache_hit"

    # Step 1 — Unanswerable check
    reason = _check_unanswerable(question)
    if reason:
        print(f"[InsightBot] Pre-flight blocked: {reason}")
        latency = round(time.time() - start, 2)
        log(question=question, latency_sec=latency, status="blocked", error=reason)
        return f"{prefix}Sorry, that can't be answered: {reason}", "", "blocked"

    # Step 2 — Generate SQL
    sql = _generate_sql_with_overrides(question)
    print(f"[InsightBot] Generated SQL: {sql}")

    # Step 3 — Guardrails
    is_valid, reason = validate_sql(sql)
    if not is_valid:
        print(f"[InsightBot] Blocked: {reason}")
        latency = round(time.time() - start, 2)
        log(question=question, sql=sql, latency_sec=latency, status="fail", error=reason)
        return f"{prefix}Couldn't generate a safe query — try rephrasing.", sql, "fail"

    sql = enforce_limit(sql)

    # Step 4 — Execute
    try:
        results = run_query(sql)
        print(f"[InsightBot] Rows returned: {len(results)}")
    except Exception as e:
        print(f"[InsightBot] Databricks error: {e}")
        latency = round(time.time() - start, 2)
        log(question=question, sql=sql, latency_sec=latency, status="fail", error=str(e))
        return f"{prefix}Query error — try rephrasing.", sql, "fail"

    # Step 5 — Anomaly detection
    anomaly_flags = detect_anomalies(question, results)
    if anomaly_flags:
        print(f"[InsightBot] Anomalies detected: {len(anomaly_flags)}")

    # Step 6 — Summarise
    summary = summarise_results(question, results)
    print(f"[InsightBot] Summary: {summary}")

    # Step 7 — Build reply
    reply_text = f"{prefix}{summary}"
    if anomaly_flags:
        reply_text += "\n" + "\n".join(anomaly_flags)

    # Step 8 — Save to cache + log
    latency = round(time.time() - start, 2)
    save_to_cache(question, summary, sql)
    log(question=question, sql=sql, rows_returned=len(results),
        latency_sec=latency, cached=False, status="pass",
        anomalies=len(anomaly_flags))

    return reply_text, sql, "pass"


# ---------------------------------------------------------------------------
# Multi-question parser
# ---------------------------------------------------------------------------
def _split_questions(text: str) -> list[str]:
    numbered = re.split(r'\n?\s*\d+[\.\)]\s+', text)
    numbered = [q.strip() for q in numbered if q.strip() and len(q.strip()) > 10]
    if len(numbered) > 1:
        return numbered

    bulleted = re.split(r'\n\s*[-•]\s+', text)
    bulleted = [q.strip() for q in bulleted if q.strip() and len(q.strip()) > 10]
    if len(bulleted) > 1:
        return bulleted

    lines = [l.strip() for l in text.split('\n') if l.strip() and len(l.strip()) > 10]
    if len(lines) > 1:
        return lines

    parts = re.split(r'\?\s+', text)
    parts = [p.strip() + "?" for p in parts if p.strip() and len(p.strip()) > 10]
    if len(parts) > 1:
        return parts

    return [text.strip()]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def handle_question(user_id: str, question: str) -> str:
    print(f"\n[InsightBot] User: {question}")

    # Stats command — "insightbot stats" or "bot performance"
    if STATS_PATTERN.search(question):
        stats = get_stats()
        cache = cache_stats()
        return (
            f"<@{user_id}> 📊 *InsightBot Performance*\n"
            f"• Total questions: {stats.get('total', 0)}\n"
            f"• Pass rate: {stats.get('pass_rate', 'N/A')}\n"
            f"• Cache hit rate: {stats.get('cache_hit_rate', 'N/A')}\n"
            f"• Avg latency: {stats.get('avg_latency_sec', 0)}s\n"
            f"• Avg cache latency: {stats.get('avg_cache_latency', 0)}s\n"
            f"• Total anomalies flagged: {stats.get('total_anomalies', 0)}\n"
            f"• Questions cached: {cache.get('total_cached', 0)}"
        )

    intent = classify_intent(question)
    print(f"[InsightBot] Intent: {intent}")

    if intent == "greeting":
        return (f"Hi <@{user_id}>! 👋 I'm InsightBot — ask me anything about "
                f"orders, revenue, sellers, products or delivery performance.\n\n"
                f"You can ask multiple questions at once — just number them or put each on a new line!")

    if intent == "out_of_scope":
        return (f"Sorry <@{user_id}>, I can only answer questions about "
                f"business data — orders, revenue, sellers, products, and delivery.")

    questions = _split_questions(question)

    if len(questions) == 1:
        reply, sql, status = _answer_single(questions[0])
        return f"<@{user_id}> {reply}"

    print(f"[InsightBot] Multi-question: {len(questions)} questions")
    parts = [f"<@{user_id}> Here are your {len(questions)} answers:\n"]
    for i, q in enumerate(questions, 1):
        print(f"\n[InsightBot] Question {i}/{len(questions)}: {q}")
        answer, _, _ = _answer_single(q, idx=i)
        parts.append(answer)

    return "\n\n".join(parts)
