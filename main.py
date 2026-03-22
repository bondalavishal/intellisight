import os
import re
import time
import threading
import concurrent.futures

from dotenv import load_dotenv
from flask import Flask as _Flask
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from app.llm.intent import classify_intent
from app.sql.guardrails import validate_sql, enforce_limit
from app.sql.connector import run_query
from app.slack.handler import (
    _check_unanswerable,
    _generate_sql_with_overrides,
    _split_questions,
    detect_anomalies,
    summarise_results,
    is_download_request,
    results_to_csv_string,
    STATS_PATTERN,
    DOWNLOAD_FOOTER,
    get_stats,
    cache_stats,
    get_cached,
    save_to_cache,
    log,
)
from app.eval.interaction_logger import (
    get_user_info,
    log_interaction,
    mark_csv_downloaded,
    csv_string_to_bytes,
)

load_dotenv()
app = App(token=os.getenv("SLACK_BOT_TOKEN"))

# ── In-memory store: last results per user ────────────────────────────────────
# { user_id: { "results": [...], "csv_string": "...", "index_id": 123 } }
_last_interaction: dict = {}


# ── Progress bar ──────────────────────────────────────────────────────────────
def _progress_bar(pct: int, label: str) -> str:
    filled = int(pct / 10)
    bar    = "▓" * filled + "░" * (10 - filled)
    return f"⏳ *InsightBot is thinking...*\n`{bar}` {pct}% — {label}"


# ── Single question pipeline ──────────────────────────────────────────────────
def _answer_with_progress(
    client, channel: str, ts: str, question: str, idx: int = None
) -> tuple[str, list, str, str]:
    """
    Returns: (reply, results, csv_string, status)
    """
    prefix = f"*{idx}.* " if idx is not None else ""
    start  = time.time()

    # Cache check — instant, no progress bar
    cached = get_cached(question)
    if cached:
        latency = round(time.time() - start, 2)
        log(question=question, sql=cached["sql"], rows_returned=0,
            latency_sec=latency, cached=True, status="cache_hit")
        # No similarity score shown to user
        reply = f"{prefix}{cached['answer']}{DOWNLOAD_FOOTER}"
        return reply, [], cached.get("csv_string", ""), "cache_hit"

    # Pre-flight unanswerable check
    reason = _check_unanswerable(question)
    if reason:
        latency = round(time.time() - start, 2)
        log(question=question, latency_sec=latency,
            status="blocked", error=reason)
        return f"{prefix}Sorry, that can't be answered: {reason}", [], "", "blocked"

    # Generate SQL (20%)
    client.chat_update(channel=channel, ts=ts,
                       text=_progress_bar(20, "Generating SQL"))
    sql = _generate_sql_with_overrides(question)
    print(f"[InsightBot] SQL: {sql}")

    # Guardrails (40%)
    client.chat_update(channel=channel, ts=ts,
                       text=_progress_bar(40, "Validating query"))
    is_valid, reason = validate_sql(sql)
    if not is_valid:
        latency = round(time.time() - start, 2)
        log(question=question, sql=sql,
            latency_sec=latency, status="fail", error=reason)
        return (f"{prefix}Couldn't generate a safe query — try rephrasing.",
                [], "", "fail")

    sql = enforce_limit(sql)

    # Databricks execution (60%)
    client.chat_update(channel=channel, ts=ts,
                       text=_progress_bar(60, "Querying Databricks"))
    try:
        results = run_query(sql)
        print(f"[InsightBot] Rows: {len(results)}")
    except Exception as e:
        latency = round(time.time() - start, 2)
        log(question=question, sql=sql,
            latency_sec=latency, status="fail", error=str(e))
        return f"{prefix}Query error — try rephrasing.", [], "", "fail"

    # Anomaly detection (80%)
    client.chat_update(channel=channel, ts=ts,
                       text=_progress_bar(80, "Detecting anomalies"))
    flags = detect_anomalies(question, results)

    # Summarise (90%)
    client.chat_update(channel=channel, ts=ts,
                       text=_progress_bar(90, "Summarising results"))
    summary = summarise_results(question, results)

    # Build reply
    reply = f"{prefix}{summary}"
    if flags:
        reply += "\n" + "\n".join(flags)
    reply += DOWNLOAD_FOOTER

    # Generate CSV string
    csv_string = results_to_csv_string(results)

    # Cache + eval log
    latency = round(time.time() - start, 2)
    save_to_cache(question, summary, sql)
    log(question=question, sql=sql, rows_returned=len(results),
        latency_sec=latency, cached=False, status="pass",
        anomalies=len(flags))

    return reply, results, csv_string, "pass"


# ── Core message handler ──────────────────────────────────────────────────────
def process_message(client, user: str, text: str, channel: str):
    print(f"\n[InsightBot] User={user} Text={text}")

    # ── Download request ──────────────────────────────────────────────────────
    if is_download_request(text):
        last = _last_interaction.get(user)
        if not last or not last.get("csv_string"):
            client.chat_postMessage(
                channel=channel,
                text=(f"<@{user}> No data to download yet — "
                      f"ask me a data question first, then reply *download*.")
            )
            return

        csv_bytes  = csv_string_to_bytes(last["csv_string"])
        filename   = "insightbot_data.csv"

        try:
            client.files_upload_v2(
                channel=channel,
                content=csv_bytes,
                filename=filename,
                title="InsightBot Data Export",
            )
            print(f"[InsightBot] CSV uploaded for user={user}")

            # Update Databricks row
            if last.get("index_id"):
                mark_csv_downloaded(last["index_id"])

        except Exception as e:
            print(f"[InsightBot] CSV upload failed: {e}")
            client.chat_postMessage(
                channel=channel,
                text=f"<@{user}> Sorry, couldn't upload the file. Try again."
            )
        return

    # ── Stats command ─────────────────────────────────────────────────────────
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

    # ── Intent check ──────────────────────────────────────────────────────────
    intent = classify_intent(text)
    print(f"[InsightBot] Intent: {intent}")

    if intent == "greeting":
        client.chat_postMessage(
            channel=channel,
            text=(f"Hi <@{user}>! 👋 I'm InsightBot — ask me anything about "
                  f"orders, revenue, sellers, products or delivery performance.\n\n"
                  f"You can ask multiple questions at once — "
                  f"just number them or put each on a new line!")
        )
        return

    if intent == "out_of_scope":
        client.chat_postMessage(
            channel=channel,
            text=(f"Sorry <@{user}>, I can only answer questions about "
                  f"business data — orders, revenue, sellers, products, delivery.")
        )
        return

    # ── Fetch user info for logging ───────────────────────────────────────────
    user_info = get_user_info(client, user)
    email     = user_info.get("email_id", "")
    full_name = user_info.get("full_name", "")

    # ── Split into individual questions ───────────────────────────────────────
    questions = _split_questions(text)
    MAX_Q     = 5
    questions = questions[:MAX_Q]

    if len(questions) == 1:
        # Single question
        msg = client.chat_postMessage(
            channel=channel,
            text=_progress_bar(10, "Understanding your question")
        )
        ts = msg["ts"]

        reply, results, csv_string, status = _answer_with_progress(
            client, channel, ts, questions[0]
        )

        # Log to Databricks
        index_id = log_interaction(
            user_id=user,
            email_id=email,
            full_name=full_name,
            question_asked=questions[0],
            question_answered=reply,
            generated_csv=csv_string if csv_string else None,
            csv_downloaded="no",
        )

        # Store last interaction in memory
        _last_interaction[user] = {
            "results":    results,
            "csv_string": csv_string,
            "index_id":   index_id,
        }

        client.chat_update(
            channel=channel,
            ts=ts,
            text=f"<@{user}> {reply}"
        )

    else:
        # Multi-question
        print(f"[InsightBot] Multi-question: {len(questions)} questions")
        msg = client.chat_postMessage(
            channel=channel,
            text=_progress_bar(5, f"Processing {len(questions)} questions")
        )
        ts    = msg["ts"]
        parts = [f"<@{user}> Here are your {len(questions)} answers:\n"]

        last_results    = []
        last_csv_string = ""
        last_index_id   = None

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
                    future = ex.submit(
                        _answer_with_progress, client, channel, ts, q, i
                    )
                    answer, results, csv_string, status = future.result(timeout=90)
            except concurrent.futures.TimeoutError:
                answer, results, csv_string, status = (
                    f"*{i}.* ⏱ Question timed out — try asking it separately.",
                    [], "", "fail"
                )
            except Exception as e:
                answer, results, csv_string, status = (
                    f"*{i}.* ❌ Error: {str(e)[:80]}",
                    [], "", "fail"
                )

            parts.append(answer)

            # Log each question to Databricks
            index_id = log_interaction(
                user_id=user,
                email_id=email,
                full_name=full_name,
                question_asked=q,
                question_answered=answer,
                generated_csv=csv_string if csv_string else None,
                csv_downloaded="no",
            )

            # Keep last successful result for download
            if results:
                last_results    = results
                last_csv_string = csv_string
                last_index_id   = index_id

        # Store last interaction in memory (last successful question)
        _last_interaction[user] = {
            "results":    last_results,
            "csv_string": last_csv_string,
            "index_id":   last_index_id,
        }

        client.chat_update(
            channel=channel,
            ts=ts,
            text="\n\n".join(parts)
        )


# ── Slack event handlers ──────────────────────────────────────────────────────
@app.message("")
def handle_message(message, client):
    user    = message.get("user", "unknown")
    text    = message.get("text", "").strip()
    channel = message.get("channel", "")
    if not text:
        return
    process_message(client, user, text, channel)


@app.event("app_mention")
def handle_mention(event, client):
    user    = event.get("user", "unknown")
    channel = event.get("channel", "")
    text    = " ".join(
        w for w in event.get("text", "").split()
        if not w.startswith("<@")
    ).strip()
    if not text:
        client.chat_postMessage(
            channel=channel,
            text=f"Hi <@{user}>! Ask me a question about the data."
        )
        return
    process_message(client, user, text, channel)


# ── Flask health check ────────────────────────────────────────────────────────
_health_app = _Flask(__name__)

@_health_app.route("/health")
def _health():
    return "ok", 200

def _run_health_server():
    port = int(os.getenv("FLASK_PORT", 3000))
    _health_app.run(host="0.0.0.0", port=port)


# ── Slack auto-reconnect ──────────────────────────────────────────────────────
def _run_slack():
    time.sleep(3)
    while True:
        try:
            print("InsightBot connecting to Slack...")
            handler = SocketModeHandler(app, os.getenv("SLACK_APP_TOKEN"))
            handler.start()
            print("Slack handler exited — reconnecting in 5s...")
        except Exception as e:
            print(f"Slack connection error: {e} — reconnecting in 5s...")
        time.sleep(5)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("InsightBot starting...")
    threading.Thread(target=_run_health_server, daemon=True).start()
    print(f"Health check running on port {os.getenv('FLASK_PORT', 3000)}")
    threading.Thread(target=_run_slack, daemon=True).start()
    while True:
        time.sleep(60)
