import os
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from app.llm.intent import classify_intent
from app.llm.sql_generator import generate_sql
from app.sql.guardrails import validate_sql, enforce_limit
from app.sql.connector import run_query
from app.slack.handler import (
    _check_unanswerable,
    _generate_sql_with_overrides,
    _split_questions,
    detect_anomalies,
    summarise_results,
    _clean_summary,
    STATS_PATTERN,
    get_stats,
    cache_stats,
    get_cached,
    save_to_cache,
    log,
)
import time
import re

load_dotenv()
app = App(token=os.getenv("SLACK_BOT_TOKEN"))


# ---------------------------------------------------------------------------
# Progress bar helper
# ---------------------------------------------------------------------------
def _progress_bar(pct: int, label: str) -> str:
    filled = int(pct / 10)
    empty  = 10 - filled
    bar    = "▓" * filled + "░" * empty
    return f"⏳ *InsightBot is thinking...*\n`{bar}` {pct}% — {label}"


# ---------------------------------------------------------------------------
# Single question pipeline with live progress updates
# ---------------------------------------------------------------------------
def _answer_with_progress(client, channel: str, ts: str, question: str, idx: int = None) -> tuple[str, str, str]:
    prefix = f"*{idx}.* " if idx is not None else ""
    start  = time.time()

    # Step 0 — Cache check (instant, no progress needed)
    cached = get_cached(question)
    if cached:
        latency = round(time.time() - start, 2)
        log(question=question, sql=cached["sql"], rows_returned=0,
            latency_sec=latency, cached=True, status="cache_hit")
        reply = (f"{prefix}{cached['answer']}\n"
                 f"_💾 Cached answer (similarity: {cached['similarity']})_")
        return reply, cached["sql"], "cache_hit"

    # Step 1 — Unanswerable check
    reason = _check_unanswerable(question)
    if reason:
        latency = round(time.time() - start, 2)
        log(question=question, latency_sec=latency, status="blocked", error=reason)
        return f"{prefix}Sorry, that can't be answered: {reason}", "", "blocked"

    # Step 2 — Generate SQL (20%)
    client.chat_update(channel=channel, ts=ts, text=_progress_bar(20, "Generating SQL"))
    sql = _generate_sql_with_overrides(question)
    print(f"[InsightBot] Generated SQL: {sql}")

    # Step 3 — Guardrails (40%)
    client.chat_update(channel=channel, ts=ts, text=_progress_bar(40, "Validating query"))
    is_valid, reason = validate_sql(sql)
    if not is_valid:
        print(f"[InsightBot] Blocked: {reason}")
        latency = round(time.time() - start, 2)
        log(question=question, sql=sql, latency_sec=latency, status="fail", error=reason)
        return f"{prefix}Couldn't generate a safe query — try rephrasing.", sql, "fail"

    sql = enforce_limit(sql)

    # Step 4 — Execute on Databricks (60%)
    client.chat_update(channel=channel, ts=ts, text=_progress_bar(60, "Querying Databricks"))
    try:
        results = run_query(sql)
        print(f"[InsightBot] Rows returned: {len(results)}")
    except Exception as e:
        print(f"[InsightBot] Databricks error: {e}")
        latency = round(time.time() - start, 2)
        log(question=question, sql=sql, latency_sec=latency, status="fail", error=str(e))
        return f"{prefix}Query error — try rephrasing.", sql, "fail"

    # Step 5 — Anomaly detection (80%)
    client.chat_update(channel=channel, ts=ts, text=_progress_bar(80, "Detecting anomalies"))
    anomaly_flags = detect_anomalies(question, results)
    if anomaly_flags:
        print(f"[InsightBot] Anomalies detected: {len(anomaly_flags)}")

    # Step 6 — Summarise (90%)
    client.chat_update(channel=channel, ts=ts, text=_progress_bar(90, "Summarising results"))
    summary = summarise_results(question, results)
    print(f"[InsightBot] Summary: {summary}")

    # Step 7 — Build final reply
    reply_text = f"{prefix}{summary}"
    if anomaly_flags:
        reply_text += "\n" + "\n".join(anomaly_flags)

    # Step 8 — Cache + log
    latency = round(time.time() - start, 2)
    save_to_cache(question, summary, sql)
    log(question=question, sql=sql, rows_returned=len(results),
        latency_sec=latency, cached=False, status="pass",
        anomalies=len(anomaly_flags))

    return reply_text, sql, "pass"


# ---------------------------------------------------------------------------
# Core handler — used by both DM and @mention
# ---------------------------------------------------------------------------
def process_message(client, user: str, text: str, channel: str):
    print(f"\n[InsightBot] User: {text}")

    # Stats command — no progress bar needed
    if STATS_PATTERN.search(text):
        stats = get_stats()
        cache = cache_stats()
        client.chat_postMessage(
            channel=channel,
            text=(
                f"<@{user}> 📊 *InsightBot Performance*\n"
                f"• Total questions: {stats.get('total', 0)}\n"
                f"• Pass rate: {stats.get('pass_rate', 'N/A')}\n"
                f"• Cache hit rate: {stats.get('cache_hit_rate', 'N/A')}\n"
                f"• Avg latency: {stats.get('avg_latency_sec', 0)}s\n"
                f"• Avg cache latency: {stats.get('avg_cache_latency', 0)}s\n"
                f"• Total anomalies flagged: {stats.get('total_anomalies', 0)}\n"
                f"• Questions cached: {cache.get('total_cached', 0)}"
            )
        )
        return

    # Intent check
    intent = classify_intent(text)
    print(f"[InsightBot] Intent: {intent}")

    if intent == "greeting":
        client.chat_postMessage(
            channel=channel,
            text=(f"Hi <@{user}>! 👋 I'm InsightBot — ask me anything about "
                  f"orders, revenue, sellers, products or delivery performance.\n\n"
                  f"You can ask multiple questions at once — just number them or put each on a new line!")
        )
        return

    if intent == "out_of_scope":
        client.chat_postMessage(
            channel=channel,
            text=(f"Sorry <@{user}>, I can only answer questions about "
                  f"business data — orders, revenue, sellers, products, and delivery.")
        )
        return

    # Split into individual questions
    questions = _split_questions(text)

    if len(questions) == 1:
        # Single question — post initial progress message then update it
        msg = client.chat_postMessage(
            channel=channel,
            text=_progress_bar(10, "Understanding your question")
        )
        ts = msg["ts"]

        reply, sql, status = _answer_with_progress(client, channel, ts, questions[0])

        # Final update — replace progress bar with real answer
        client.chat_update(
            channel=channel,
            ts=ts,
            text=f"<@{user}> {reply}"
        )

    else:
        # Multi-question — single progress bar for overall flow
        import concurrent.futures
        MAX_Q = 5
        questions = questions[:MAX_Q]
        print(f"[InsightBot] Multi-question: {len(questions)} questions")
        msg = client.chat_postMessage(
            channel=channel,
            text=_progress_bar(5, f"Processing {len(questions)} questions")
        )
        ts = msg["ts"]

        parts = [f"<@{user}> Here are your {len(questions)} answers:\n"]
        for i, q in enumerate(questions, 1):
            pct = int((i / len(questions)) * 90)
            client.chat_update(
                channel=channel,
                ts=ts,
                text=_progress_bar(pct, f"Question {i}/{len(questions)}: {q[:40]}...")
            )
            print(f"\n[InsightBot] Question {i}/{len(questions)}: {q}")
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                    future = ex.submit(_answer_with_progress, client, channel, ts, q, i)
                    answer, _, _ = future.result(timeout=90)
            except concurrent.futures.TimeoutError:
                answer = f"*{i}.* ⏱ Question timed out — try asking it separately."
            except Exception as e:
                answer = f"*{i}.* ❌ Error: {str(e)[:80]}"
            parts.append(answer)

        # Final update
        client.chat_update(
            channel=channel,
            ts=ts,
            text="\n\n".join(parts)
        )


# ---------------------------------------------------------------------------
# Slack event handlers
# ---------------------------------------------------------------------------
@app.message("")
def handle_message(message, say, client):
    user    = message.get("user", "unknown")
    text    = message.get("text", "").strip()
    channel = message.get("channel", "")
    if not text:
        return
    process_message(client, user, text, channel)


@app.event("app_mention")
def handle_mention(event, say, client):
    user    = event.get("user", "unknown")
    channel = event.get("channel", "")
    text    = event.get("text", "")
    # Strip the bot mention
    text = " ".join(w for w in text.split() if not w.startswith("<@")).strip()
    if not text:
        client.chat_postMessage(
            channel=channel,
            text=f"Hi <@{user}>! Ask me a question about the data."
        )
        return
    process_message(client, user, text, channel)


# ---------------------------------------------------------------------------
# Health check server (required for Render port detection)
# ---------------------------------------------------------------------------
import threading
from flask import Flask as _Flask
_health_app = _Flask(__name__)

@_health_app.route("/health")
def _health():
    return "ok", 200

def _run_health_server():
    port = int(os.getenv("FLASK_PORT", 3000))
    _health_app.run(host="0.0.0.0", port=port)


# ---------------------------------------------------------------------------
# Start
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("InsightBot starting...")
    t = threading.Thread(target=_run_health_server, daemon=True)
    t.start()
    print(f"Health check running on port {os.getenv('FLASK_PORT', 3000)}")
    import time as _time
    _time.sleep(3)  # Give Flask time to bind port before socket connection
    print("InsightBot connected to Slack.")
    handler = SocketModeHandler(app, os.getenv("SLACK_APP_TOKEN"))
    handler.start()

