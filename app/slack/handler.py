"""
handler.py — Phase 7 update
Changes from Phase 6:
  - handle_question() now returns (reply, results, csv_string) instead of just reply
  - Cache hit no longer shows similarity score to users
  - Download footer added to all data responses
  - Databricks interaction logging moved to main.py (needs Slack client for user info)
"""

import re
import time
import csv
import io
import httpx

from app.llm.intent import classify_intent
from app.llm.sql_generator import generate_sql
from app.sql.guardrails import validate_sql, enforce_limit
from app.sql.connector import run_query
from app.eval.cache import get_cached, save_to_cache, cache_stats, promote_suggestion
from app.eval.logger import log, get_stats
from app.utils.normaliser import normalise
from app.sql.error_classifier import classify, is_recoverable
from app.sql.recovery import attempt_recovery
from app.eval.feedback_engine import on_success, on_failure, on_cache_miss
from app.slack.suggestion_engine import find_alternatives, format_for_slack

import os
from cerebras.cloud.sdk import Cerebras as _Cerebras
_cerebras_client  = _Cerebras(api_key=os.getenv("CEREBRAS_API_KEY"))
CEREBRAS_MODEL    = "qwen-3-235b-a22b-instruct-2507"
OLLAMA_URL        = "http://127.0.0.1:11434/api/generate"
OLLAMA_MODEL      = "mannix/defog-llama3-sqlcoder-8b"

DOWNLOAD_FOOTER = "\n\n💾 *Want the full data?* Reply *download* to get a CSV."

# ── Pre-flight unanswerable patterns ─────────────────────────────────────────
UNANSWERABLE_PATTERNS = [
    (
        r'seller.{0,40}(improv|trend|over time|month.by.month|histor)'
        r'|(improv|trend|over time).{0,40}seller'
        r'|seller.{0,30}review.{0,30}(over time|trend|month|improv)',
        "vw_seller_metrics has no time dimension — seller metrics are lifetime aggregates only."
    ),
    (
        r'categor.{0,30}cancel|cancel.{0,30}categor',
        "vw_orders_metrics has no category column — cancellation cannot be broken down by category."
    ),
]

# ── SQL overrides ─────────────────────────────────────────────────────────────
DELIVERY_OVERRIDE = re.compile(
    r'(average|avg).{0,20}(delivery|shipping).{0,20}(time|days).{0,20}'
    r'(by state|per state|state)', re.I)
CANCEL_OVERRIDE = re.compile(
    r'month.{0,30}cancel.{0,30}(rate|exceed|over|above|\d+%)', re.I)
REVIEW_OVERRIDE = re.compile(
    r'seller.{0,30}review.{0,30}(below|more than|point|1 point)', re.I)
STATS_PATTERN = re.compile(
    r'(insightbot|bot).{0,20}(stat|metric|performance|pass rate)', re.I)

# ── Download trigger words ────────────────────────────────────────────────────
# "download" is shown to users in the footer
# others are god-mode silent triggers for dev/prod
DOWNLOAD_TRIGGERS = ["download", "csv", "export", "give me the data"]

# ── Anomaly thresholds ────────────────────────────────────────────────────────
DELIVERY_THRESHOLD     = 20.0
CANCEL_THRESHOLD       = 5.0
REVENUE_DROP_THRESHOLD = 10.0
REVIEW_THRESHOLD       = 3.0

_PADDING = [
    "There is no message", "No additional information",
    "Delivery durations are not", "The data does not contain",
    "No message to", "There are no messages",
    "If you have any further", "Please note that", "Please feel free"
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def is_download_request(text: str) -> bool:
    """Returns True if the message is a download/csv/export request."""
    t = text.lower().strip()
    return any(trigger in t for trigger in DOWNLOAD_TRIGGERS)


def results_to_csv_string(results: list[dict]) -> str:
    """Converts query results to a CSV string."""
    if not results:
        return ""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=results[0].keys())
    writer.writeheader()
    writer.writerows(results)
    return output.getvalue()


def _check_unanswerable(q: str):
    for pattern, reason in UNANSWERABLE_PATTERNS:
        if re.search(pattern, q.lower()):
            return reason
    return None


def _generate_sql_with_overrides(question: str) -> str:
    if DELIVERY_OVERRIDE.search(question):
        return (
            "SELECT customer_state, ROUND(AVG(delivery_days),1) AS avg_delivery_days"
            " FROM vw_orders_metrics WHERE delivery_days IS NOT NULL"
            " GROUP BY customer_state ORDER BY avg_delivery_days DESC LIMIT 27"
        )
    if CANCEL_OVERRIDE.search(question):
        return (
            "SELECT year_month, total_orders, canceled_orders,"
            " ROUND(canceled_orders*100.0/total_orders,2) AS cancel_pct"
            " FROM vw_monthly_revenue WHERE total_orders>0"
            " AND canceled_orders*100.0/total_orders>5"
            " ORDER BY cancel_pct DESC LIMIT 20"
        )
    if REVIEW_OVERRIDE.search(question):
        return (
            "SELECT seller_id, seller_city, seller_state, avg_review_score, total_orders"
            " FROM vw_seller_metrics"
            " WHERE avg_review_score <"
            " (SELECT AVG(avg_review_score) FROM vw_seller_metrics) - 1.0"
            " ORDER BY avg_review_score ASC LIMIT 50"
        )
    return generate_sql(question)


def detect_anomalies(question: str, results: list[dict]) -> list[str]:
    if not results:
        return []
    flags, q = [], question.lower()
    keys = list(results[0].keys())

    for col in [k for k in keys if "delivery" in k.lower() and "day" in k.lower()]:
        for row in results:
            try:
                if float(row.get(col)) > DELIVERY_THRESHOLD:
                    state = row.get("customer_state", "?")
                    flags.append(
                        f"⚠️ *Anomaly:* {state} avg delivery is {row.get(col)} days"
                        f" — exceeds {int(DELIVERY_THRESHOLD)}-day threshold.")
            except: pass

    for col in [k for k in keys if "cancel" in k.lower() and
                any(w in k.lower() for w in ["pct", "rate", "percent"])]:
        for row in results:
            try:
                fval = float(row.get(col))
                if fval > CANCEL_THRESHOLD:
                    p = row.get("year_month", row.get("month", ""))
                    flags.append(
                        f"⚠️ *Anomaly:* Cancellation rate{f' in {p}' if p else ''}"
                        f" is {round(fval,1)}% — exceeds {int(CANCEL_THRESHOLD)}%.")
            except: pass

    for col in [k for k in keys if any(w in k.lower()
                for w in ["growth", "pct", "drop", "mom", "change"])]:
        for row in results:
            try:
                fval = float(row.get(col))
                if fval < -REVENUE_DROP_THRESHOLD:
                    p = row.get("year_month", row.get("month", ""))
                    flags.append(
                        f"⚠️ *Anomaly:* Revenue dropped {abs(round(fval,1))}%"
                        f"{f' in {p}' if p else ''}"
                        f" — exceeds {int(REVENUE_DROP_THRESHOLD)}% threshold.")
            except: pass

    if (any(k for k in keys if "review" in k.lower() and "score" in k.lower())
            and ("seller" in q or "review" in q)):
        col = next(k for k in keys if "review" in k.lower() and "score" in k.lower())
        for row in results:
            try:
                fval = float(row.get(col))
                if fval < REVIEW_THRESHOLD:
                    s = row.get("seller_id", "")
                    flags.append(
                        f"⚠️ *Anomaly:* Seller{f' ({s[:8]}...)' if s else ''}"
                        f" review is {round(fval,2)} — below {REVIEW_THRESHOLD}.")
            except: pass

    seen, unique = set(), []
    for f in flags:
        if f[:60] not in seen:
            seen.add(f[:60]); unique.append(f)
        if len(unique) >= 3: break
    return unique


def _clean_summary(text: str) -> str:
    for phrase in _PADDING:
        idx = text.find(phrase)
        if idx != -1: text = text[:idx].strip()
    sentences = [s.strip() for s in re.split(r'(?<=[.!?])\s+', text) if s.strip()]
    text = " ".join(sentences[:3])
    if text and text[-1] not in ".!?": text += "."
    return text.strip()


SUMMARY_PROMPT = """You are a business analytics assistant.
A user asked: {question}
Data: {results}
Write a clear 2-3 sentence answer in plain English.
Use actual numbers. Prefix R$ for monetary values. Say 'days' for delivery.
Stop after 3 sentences. No disclaimers.
Answer:"""


def summarise_results(question: str, results: list[dict]) -> str:
    if not results:
        return "The query ran successfully but returned no data for those filters."
    if len(results) == 1 and "message" in results[0]:
        return results[0]["message"]
    sample       = results[:20]
    results_text = "\n".join(str(r) for r in sample)
    if len(results) > 20:
        results_text += f"\n... and {len(results)-20} more rows."
    prompt = SUMMARY_PROMPT.format(question=question, results=results_text)

    # Primary: Cerebras
    try:
        resp = _cerebras_client.chat.completions.create(
            model=CEREBRAS_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=100,
            timeout=30,
        )
        print("[LLM] Summary via Cerebras")
        return _clean_summary(resp.choices[0].message.content.strip())
    except Exception as e:
        print(f"[LLM] Cerebras summary failed ({e}) — falling back to Ollama")

    # Fallback: Ollama
    try:
        response = httpx.post(OLLAMA_URL, json={
            "model":   OLLAMA_MODEL,
            "prompt":  prompt,
            "stream":  False,
            "options": {"temperature": 0, "num_predict": 100}
        }, timeout=120)
        print("[LLM] Summary via Ollama (fallback)")
        return _clean_summary(response.json()["response"].strip())
    except Exception as e:
        print(f"[LLM] Ollama summary also failed ({e})")
        return "Summary unavailable — here is the raw data above."


def _split_questions(text: str) -> list[str]:
    for splitter in [r'\n?\s*\d+[\.)]\s+', r'\n\s*[-•]\s+']:
        parts = [p.strip() for p in re.split(splitter, text)
                 if p.strip() and len(p.strip()) > 10]
        if len(parts) > 1: return parts
    lines = [l.strip() for l in text.split('\n')
             if l.strip() and len(l.strip()) > 10]
    if len(lines) > 1: return lines
    parts = [p.strip()+"?" for p in re.split(r'\?\s+', text)
             if p.strip() and len(p.strip()) > 10]
    if len(parts) > 1: return parts
    return [text.strip()]


# ── Destructive SQL pattern (checked before normalisation) ────────────────────
_DESTRUCTIVE_INPUT_RE = re.compile(
    r'^\s*(DELETE|DROP|UPDATE|INSERT\s+INTO|ALTER|TRUNCATE|MERGE|GRANT|REVOKE)\b',
    re.IGNORECASE,
)

# ── Main handler ──────────────────────────────────────────────────────────────

def handle_question(user_id: str, question: str) -> tuple:
    """
    Returns: (reply, results, csv_string, pending_data)
      - reply        → text to post in Slack
      - results      → raw Databricks rows (empty list if not a data query)
      - csv_string   → CSV string of results (empty string if not a data query)
      - pending_data → dict with suggestion data if match_type=suggestion, else None
    """
    print(f"\n[InsightBot] User: {question}")
    start = time.time()

    # ── Pre-flight: block raw destructive SQL commands ────────────────────────
    if _DESTRUCTIVE_INPUT_RE.search(question):
        print(f"[Guardrails] Blocked destructive input: {question[:60]}")
        log(question=question, latency_sec=0, status="blocked",
            error="destructive_sql_attempt")
        return (
            f"<@{user_id}> 🚫 That looks like a destructive SQL command. "
            f"I only run *SELECT* queries — I can't modify or delete data.",
            [], "", None,
        )

    # ── Layer 1: Normalise input ──────────────────────────────────────────────
    # Spell correction + abbreviation expansion + punctuation normalisation
    # Runs before cache lookup so typos and abbreviations still hit the cache
    normalised = normalise(question)

    # ── Cache check ───────────────────────────────────────────────────────────
    cached = get_cached(normalised)
    if cached:
        match_type = cached.get("match_type", "direct_hit")

        if match_type == "direct_hit":
            log(question=normalised, sql=cached["sql"],
                latency_sec=round(time.time()-start, 2),
                cached=True, status="cache_hit",
                normalised_question=normalised)
            reply = f"<@{user_id}> {cached['answer']}{DOWNLOAD_FOOTER}"
            return reply, [], "", {
                "type":     "cache_hit_meta",
                "sql":      cached.get("sql", ""),
                "question": normalised,
            }

        if match_type == "suggestion":
            # If the question is unanswerable (e.g. asks for time-series data that
            # doesn't exist), don't confirm the cache suggestion as if it answers it.
            # Instead route to the unanswerable handler — but seed find_alternatives
            # with the cache match so we always have at least one ready-to-run option.
            unanswerable_reason = _check_unanswerable(normalised)
            if unanswerable_reason:
                log(question=normalised, latency_sec=round(time.time()-start, 2),
                    status="blocked", error=unanswerable_reason,
                    normalised_question=normalised)
                from app.slack.suggestion_engine import SuggestedAlternative, SuggestionResult, format_for_slack as _fmt
                # Seed the suggestion with the cache match we already have
                seed_alt = SuggestedAlternative(
                    question   = cached["matched_question"],
                    similarity = cached["similarity"],
                    source     = "cache",
                    answer     = cached["answer"],
                    sql        = cached["sql"],
                    can_run    = True,
                )
                suggestion = find_alternatives(
                    normalised, dead_end_type="unanswerable",
                    exclude_question=normalised,
                )
                # Prepend the cache match so it's always the first option
                all_alts = [seed_alt] + [
                    a for a in suggestion.alternatives
                    if a.question != seed_alt.question
                ]
                result = SuggestionResult(found=True, alternatives=all_alts[:3],
                                          dead_end_type="unanswerable")
                suggestion_text = _fmt(
                    result, user_id,
                    context=f"Sorry <@{user_id}>, I can't answer that directly: "
                            f"_{unanswerable_reason}_",
                )
                return suggestion_text, [], "", {"type": "alternatives", "alternatives": all_alts[:3]}

            # Normal suggestion-band path: question is answerable, cache is close
            log(question=normalised, sql=cached["sql"],
                latency_sec=round(time.time()-start, 2),
                cached=True, status="cache_suggestion",
                normalised_question=normalised)
            on_cache_miss(normalised)  # learn from the near-miss
            similarity_pct = round(cached["similarity"] * 100, 1)
            reply = (
                f"<@{user_id}> I found a similar question in my cache "
                f"({similarity_pct}% match):\n\n"
                f"> _{cached['matched_question']}_\n\n"
                f"*Answer:* {cached['answer']}\n\n"
                f"\ud83d\udca1 _If this answers your question, reply *yes* to confirm. "
                f"Otherwise rephrase and I'll run a fresh query._"
            )
            pending_data = {
                "question":   normalised,
                "answer":     cached["answer"],
                "sql":        cached["sql"],
                "results":    [],
                "csv_string": "",
            }
            return reply, [], "", pending_data

    # ── Intent ────────────────────────────────────────────────────────────────
    intent = classify_intent(normalised)
    if intent == "greeting":
        reply = (f"Hi <@{user_id}>! 👋 I'm InsightBot — ask me anything about "
                 f"orders, revenue, sellers, products or delivery performance.")
        return reply, [], "", None
    if intent == "out_of_scope":
        reply = (f"Sorry <@{user_id}>, I can only answer questions about "
                 f"business data — orders, revenue, sellers, products, delivery.")
        return reply, [], "", None

    # ── Pre-flight ────────────────────────────────────────────────────────────
    reason = _check_unanswerable(normalised)
    if reason:
        log(question=normalised, latency_sec=round(time.time()-start, 2),
            status="blocked", error=reason)
        # Suggestion engine — find closest answerable alternative
        suggestion = find_alternatives(normalised, dead_end_type="unanswerable",
                                       exclude_question=normalised)
        if suggestion.found:
            suggestion_text = format_for_slack(
                suggestion, user_id,
                context=f"Sorry <@{user_id}>, I can't answer that directly: _{reason}_"
            )
            return suggestion_text, [], "", {"type": "alternatives", "alternatives": suggestion.alternatives}
        reply = f"<@{user_id}> Sorry, that can't be answered: {reason}"
        return reply, [], "", None

    # ── SQL generation ────────────────────────────────────────────────────────
    sql = _generate_sql_with_overrides(normalised)
    print(f"[InsightBot] SQL: {sql[:80]}...")

    is_valid, reason = validate_sql(sql)
    if not is_valid:
        log(question=normalised, sql=sql,
            latency_sec=round(time.time()-start, 2),
            status="fail", error=reason)
        reply = f"Sorry <@{user_id}>, couldn't generate a safe query. Try rephrasing."
        return reply, [], "", None

    sql = enforce_limit(sql)

    # ── Databricks execution + Layer 4: Failure Classification + Recovery ───────
    MAX_RETRIES = 2
    attempt     = 0
    results     = None
    last_error  = None

    while attempt < MAX_RETRIES:
        try:
            results = run_query(sql)
            # Explicit no-rows sentinel — treated as a soft failure
            if not results:
                from app.sql.error_classifier import FailureType as _FT
                failure = _FT(
                    name="no_rows", user_message="Query returned no data.",
                    recovery_hint="broaden_query", raw_error="no_rows"
                )
                recovery = attempt_recovery(sql, normalised, failure)
                if recovery.success:
                    try:
                        results = run_query(recovery.sql)
                        sql = recovery.sql
                        print(f"[InsightBot] No-rows recovery succeeded")
                    except Exception:
                        results = []
                else:
                    results = []
            print(f"[InsightBot] Rows: {len(results)}")
            break  # success — exit retry loop

        except Exception as e:
            last_error = str(e)
            attempt   += 1
            print(f"[InsightBot] Databricks error (attempt {attempt}/{MAX_RETRIES}): {last_error[:120]}")

            # Classify the failure
            failure = classify(last_error)
            print(f"[InsightBot] Classified as: {failure.name} | hint: {failure.recovery_hint}")

            log(question=normalised, sql=sql,
                latency_sec=round(time.time()-start, 2),
                status=f"fail_attempt_{attempt}",
                error=f"{failure.name}: {last_error[:200]}")

            # Non-recoverable — break immediately, no point retrying
            if not is_recoverable(failure):
                reply = (
                    f"<@{user_id}> {failure.user_message}\n\n"
                    f"_This issue requires an admin to resolve — I can't fix it automatically._"
                )
                return reply, [], "", None

            # Attempt recovery
            if attempt < MAX_RETRIES:
                recovery = attempt_recovery(sql, normalised, failure)
                if recovery.success:
                    print(f"[InsightBot] Recovery produced rewritten SQL — retrying")
                    sql = recovery.sql  # retry with rewritten SQL
                else:
                    print(f"[InsightBot] Recovery produced no SQL — escalating")
                    break  # recovery failed, stop retrying

    # If we exhausted retries without results
    if results is None:
        failure = classify(last_error or "unknown error")
        log(question=normalised, sql=sql,
            latency_sec=round(time.time()-start, 2),
            status="fail", error=f"exhausted_retries: {failure.name}")
        # Self-learning: track failure pattern + learn abbreviations (background)
        on_failure(normalised, failure.name)

        log(question=normalised, sql=sql,
            latency_sec=round(time.time()-start, 2),
            status="exhausted_retries",
            failure_type=failure.name,
            recovery_attempted="yes",
            normalised_question=normalised)

        # Suggestion engine — find closest answerable alternative
        suggestion = find_alternatives(normalised, dead_end_type="failure",
                                       exclude_question=normalised)
        if suggestion.found:
            suggestion_text = format_for_slack(
                suggestion, user_id,
                context=(
                    f"<@{user_id}> I tried {MAX_RETRIES} times but couldn't run that query "
                    f"(_Reason: {failure.user_message}_)."
                )
            )
            return suggestion_text, [], "", {"type": "alternatives", "alternatives": suggestion.alternatives}

        reply = (
            f"<@{user_id}> I tried {MAX_RETRIES} times but couldn't run that query.\n\n"
            f"*Reason:* {failure.user_message}\n\n"
            f"💡 _Try rephrasing, or ask me something simpler about the same topic._"
        )
        return reply, [], "", None

    # ── Anomaly detection ─────────────────────────────────────────────────────
    flags  = detect_anomalies(normalised, results)
    summary = summarise_results(normalised, results)

    # ── Build reply ───────────────────────────────────────────────────────────
    reply = summary
    if flags:
        reply += "\n" + "\n".join(flags)
    reply += DOWNLOAD_FOOTER

    # ── Generate CSV string ───────────────────────────────────────────────────
    csv_string = results_to_csv_string(results)

    # ── Cache + eval log ──────────────────────────────────────────────────────
    latency = round(time.time()-start, 2)
    save_to_cache(normalised, summary, sql)
    log(question=normalised, sql=sql, rows_returned=len(results),
        latency_sec=latency, status="pass", anomalies=len(flags),
        normalised_question=normalised, recovery_attempted="no")

    # Self-learning: promote to RAG + tune thresholds (background thread)
    on_success(normalised, sql, summary)

    return f"<@{user_id}> {reply}", results, csv_string, None
