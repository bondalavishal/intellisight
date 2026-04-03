import os
import re
import json
import sys
import time
import threading
import concurrent.futures

# Force line-buffered stdout so logs from Slack Bolt's handler threads
# appear in the terminal immediately instead of being held in the buffer.
sys.stdout.reconfigure(line_buffering=True)

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
from app.eval.feedback_engine import on_suggestion_confirmed, get_learning_stats
from app.slack.suggestion_engine import find_alternatives, format_for_slack
from app.eval.interaction_logger import (
    get_user_info,
    log_interaction,
    mark_csv_downloaded,
    csv_string_to_bytes,
    fetch_latest_results_for_user,
    fetch_results_json_by_question,
    ensure_results_json_column,
)

load_dotenv()
app = App(token=os.getenv("SLACK_BOT_TOKEN"))
ensure_results_json_column()

# ── In-memory store: last results per user ────────────────────────────────────
# { user_id: { "results": [...], "csv_string": "...", "index_id": 123 } }
_last_interaction: dict = {}

# Stores pending cache suggestions per user for 'yes' confirmation flow
# { user_id: { "question": str, "answer": str, "sql": str } }
_pending_suggestion: dict = {}

# Stores pending suggestion alternatives per user for number reply (1/2/3) flow
# { user_id: [ SuggestedAlternative, ... ] }
_pending_alternatives: dict = {}


# ── Progress bar ──────────────────────────────────────────────────────────────
def _progress_bar(pct: int, label: str) -> str:
    filled = int(pct / 10)
    bar    = "▓" * filled + "░" * (10 - filled)
    return f"⏳ *InsightBot is thinking...*\n`{bar}` {pct}% — {label}"


# ── Single question pipeline ──────────────────────────────────────────────────
def _answer_with_progress(
    client, channel: str, ts: str, question: str, idx: int = None, user: str = "unknown"
) -> tuple[str, list, str, str]:
    """
    Routes through handle_question() which contains all Option B layers:
      - Layer 1: normalisation + spell correction
      - Layer 2: similarity band cache
      - Layer 4: failure classification + recovery
      - Layer 5: suggestion engine
      - Layer 3: feedback/self-learning

    Progress bar shown for non-cache queries.
    Returns: (reply, results, csv_string, status)
    """
    from app.slack.handler import handle_question

    prefix = f"*{idx}.* " if idx is not None else ""

    # Show progress bar only for fresh queries (cache hits are instant)
    client.chat_update(channel=channel, ts=ts,
                       text=_progress_bar(20, "Understanding your question"))

    result = handle_question(user, question)
    reply, results, csv_string = result[0], result[1], result[2]
    pending_data = result[3] if len(result) > 3 else None

    # Store pending data — route to the right dict based on type
    if pending_data:
        if isinstance(pending_data, dict) and pending_data.get("type") == "alternatives":
            _pending_alternatives[user] = pending_data["alternatives"]
            print(f"[InsightBot] Alternatives stored for user={user} ({len(pending_data['alternatives'])} options)")
        elif isinstance(pending_data, dict) and pending_data.get("type") == "cache_hit_meta":
            # Direct cache hit — store question + sql so download can find / re-run the data
            _last_interaction[user] = {
                "results":    [],
                "csv_string": "",
                "index_id":   None,
                "question":   pending_data.get("question", ""),
                "sql":        pending_data.get("sql", ""),
            }
            print(f"[InsightBot] Cache-hit meta stored for user={user} question={pending_data.get('question','')[:60]}")
        else:
            _pending_suggestion[user] = pending_data
            print(f"[InsightBot] Pending suggestion stored for user={user}")

    # Determine status from reply content for logging/routing
    if pending_data:
        status = "cache_suggestion"
    elif "similar question" in reply.lower():
        status = "cache_suggestion"
    elif "tried" in reply.lower() and "times" in reply.lower():
        status = "fail"
    elif "can't answer" in reply.lower() or "can not be answered" in reply.lower():
        status = "blocked"
    else:
        status = "pass"

    # Strip the @mention prefix handle_question adds (main.py adds its own)
    import re as _re
    reply = _re.sub(r'<@[A-Z0-9]+>\s*', '', reply).strip()

    # Add numbered prefix for multi-question
    if prefix:
        reply = f"{prefix}{reply}"

    return reply, results, csv_string, status


# ── Core message handler ──────────────────────────────────────────────────────
def process_message(client, user: str, text: str, channel: str):
    print(f"\n[InsightBot] User={user} Text={text}")

    # ── Download request ──────────────────────────────────────────────────────
    if is_download_request(text):
        last = _last_interaction.get(user)
        print(f"[InsightBot] Download request from user={user} | "
              f"last_interaction={'found' if last else 'MISSING'} | "
              f"csv_string={'yes' if last and last.get('csv_string') else 'empty'}")
        csv_string = last.get("csv_string") if last else ""

        # ── Tier 2: lookup by question in Databricks (survives restarts) ─────
        if not csv_string:
            question_key = (last or {}).get("question", "")
            if question_key:
                print(f"[InsightBot] Tier-2 fallback: lookup by question for user={user}")
                rows = fetch_results_json_by_question(question_key)
                if rows:
                    csv_string = results_to_csv_string(rows)
                    print(f"[InsightBot] Tier-2: found {len(rows)} rows by question")

        # ── Tier 3: latest row for this user in Databricks ────────────────────
        if not csv_string:
            print(f"[InsightBot] Tier-3 fallback: fetch latest for user={user}")
            rows = fetch_latest_results_for_user(user)
            if rows:
                csv_string = results_to_csv_string(rows)
                print(f"[InsightBot] Tier-3: found {len(rows)} rows from latest interaction")

        # ── Tier 4: re-run the cached SQL as absolute last resort ─────────────
        if not csv_string:
            sql_key = (last or {}).get("sql", "")
            if sql_key:
                print(f"[InsightBot] Tier-4 fallback: re-running SQL for user={user}")
                try:
                    rerun_results = run_query(sql_key)
                    if rerun_results:
                        csv_string = results_to_csv_string(rerun_results)
                        print(f"[InsightBot] Tier-4: SQL re-run returned {len(rerun_results)} rows")
                except Exception as _e:
                    print(f"[InsightBot] Tier-4 SQL re-run failed: {_e}")

        if not csv_string:
            client.chat_postMessage(
                channel=channel,
                text=(f"<@{user}> No data to download yet — "
                      f"ask me a data question first, then reply *download*.")
            )
            return

        csv_bytes  = csv_string_to_bytes(csv_string)
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
            if last and last.get("index_id"):
                mark_csv_downloaded(last["index_id"])

        except Exception as e:
            print(f"[InsightBot] CSV upload failed: {e}")
            client.chat_postMessage(
                channel=channel,
                text=f"<@{user}> Sorry, couldn't upload the file. Try again."
            )
        return

    # ── Stats command ─────────────────────────────────────────────────────────
    # Also catches plain "stats", "show stats", "bot stats" without the full pattern
    _STATS_LOOSE = re.compile(
        r'\b(insightbot|bot|show|my)?\s*(stat|stats|metric|metrics|performance|pass\s*rate|how\s*am\s*i\s*doing)\b',
        re.I,
    )
    if STATS_PATTERN.search(text) or _STATS_LOOSE.search(text):
        stats    = get_stats(days=7)   # last 7 days from eval_log
        all_time = get_stats()         # all time from eval_log
        learning = get_learning_stats()
        cache    = cache_stats()

        # ── Databricks fallback for persistent historical count ────────────────
        # eval_log is local and cleared on reset; Databricks survives restarts.
        db_total = 0
        try:
            db_rows = run_query(
                "SELECT COUNT(*) AS total FROM default.insightbot_interactions"
            )
            db_total = int(db_rows[0].get("total", 0)) if db_rows else 0
        except Exception:
            pass

        session_total  = stats.get("total", 0)
        # Databricks is the superset (includes all sessions, survives resets).
        # eval_log is a local subset — don't max() them, use Databricks directly.
        alltime_total  = db_total if db_total > 0 else all_time.get("total", 0)

        # ── No-data guard — show warm-up message instead of all-zeros ─────────
        if session_total == 0 and alltime_total == 0:
            client.chat_postMessage(
                channel=channel,
                text=(
                    f"<@{user}> 📊 *InsightBot Stats*\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"No questions logged yet in this session.\n"
                    f"Ask me a data question and come back — stats populate automatically! 🚀\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━"
                ),
            )
            return

        # ── Section 1: Core Performance ───────────────────────────────────────
        passed  = stats.get("passed", 0)
        failed  = stats.get("failed", 0)
        blocked = stats.get("blocked", 0)

        # If session log is empty but Databricks has history, surface that
        if session_total == 0:
            core = (
                f"*📊 InsightBot Stats*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"*🎯 Performance*\n"
                f"• Session log: fresh start  |  All time (Databricks): *{db_total}* questions\n"
                f"• Ask a few questions and check back for pass rates & latency 📈\n"
            )
        else:
            core = (
                f"*📊 InsightBot Stats — Last 7 Days*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"*🎯 Performance*\n"
                f"• Questions answered: *{session_total}*  "
                f"|  All time: *{alltime_total}*\n"
                f"• Pass rate: *{stats.get('pass_rate', 'N/A')}*  "
                f"|  All time: *{all_time.get('pass_rate', 'N/A')}*\n"
                f"• Failed: {failed}  |  Blocked: {blocked}  "
                f"|  Anomalies flagged: {stats.get('total_anomalies', 0)}\n"
            )

        # ── Section 2: Cache & Speed ──────────────────────────────────────────
        cache_section = (
            f"\n*⚡ Cache & Speed*\n"
            f"• Cache hit rate: *{stats.get('cache_hit_rate', 'N/A')}*  "
            f"({stats.get('cache_hits', 0)} direct hits)\n"
            f"• Suggestions shown: {stats.get('cache_suggestions', 0)}  "
            f"|  Confirmed: *{stats.get('suggestion_confirm_rate', 'N/A')}*\n"
            f"• Cached questions in store: *{cache.get('total_cached', 0)}*\n"
            f"• Avg query latency: *{stats.get('avg_latency_sec', 0)}s*  "
            f"|  p95: {stats.get('p95_latency_sec', 0)}s\n"
            f"• Avg cache latency: *{stats.get('avg_cache_latency', 0)}s*\n"
        )

        # ── Section 3: Recovery ───────────────────────────────────────────────
        recovery_section = (
            f"\n*🔄 Auto-Recovery*\n"
            f"• Retry attempts: {stats.get('retry_attempts', 0)}  "
            f"|  Success rate: *{stats.get('retry_success_rate', 'N/A')}*\n"
            f"• Exhausted retries (unresolved): {stats.get('exhausted_retries', 0)}\n"
        )

        # ── Section 4: Top Failure Patterns ──────────────────────────────────
        top_failures = stats.get("top_failures", [])
        if top_failures:
            failure_lines = "\n".join(
                f"  {i+1}. `{ftype}` — {count}x"
                for i, (ftype, count) in enumerate(top_failures)
            )
            failure_section = f"\n*⚠️ Top Failure Patterns (7d)*\n{failure_lines}\n"
        else:
            failure_section = "\n*⚠️ Top Failure Patterns* — None this week 🎉\n"

        # ── Section 5: Self-Learning ──────────────────────────────────────────
        last_tune = learning.get("last_threshold_tune", "never")
        if last_tune and last_tune != "never":
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(last_tune)
                last_tune = dt.strftime("%d %b %Y %H:%M")
            except Exception:
                pass

        learning_section = (
            f"\n*🧠 Self-Learning*\n"
            f"• RAG auto-promotions: *{learning.get('rag_auto_promotions', 0)}* examples learned\n"
            f"• Abbreviations learned: *{learning.get('learned_abbreviations', 0)}*  "
            f"|  Candidates tracking: {learning.get('candidate_abbreviations', 0)}\n"
            f"• Failure patterns auto-promoted: {learning.get('promoted_failure_patterns', 0)}\n"
            f"• Cache threshold tunes: {learning.get('threshold_tune_count', 0)}  "
            f"|  Last: {last_tune}\n"
        )

        full_reply = (
            f"<@{user}>\n"
            + core
            + cache_section
            + recovery_section
            + failure_section
            + learning_section
            + f"━━━━━━━━━━━━━━━━━━━━━━━━"
        )

        client.chat_postMessage(channel=channel, text=full_reply)
        return

    # ── Cache suggestion confirmation (BEFORE intent check) ─────────────────
    # Must run before classify_intent — "yes" would be classified as text_to_sql
    if text.lower().strip() in ("yes", "yes!", "yep", "yeah", "correct", "that's right", "confirmed"):
        print(f"[InsightBot] YES received. _pending_suggestion keys: {list(_pending_suggestion.keys())}, user={user}")
        pending = _pending_suggestion.get(user)
        if pending:
            # Run the SQL to get fresh results for download
            csv_string = ""
            results    = []
            sql        = pending.get("sql", "")
            if sql:
                try:
                    results    = run_query(sql)
                    csv_string = results_to_csv_string(results)
                except Exception as e:
                    print(f"[InsightBot] Could not re-run SQL for download: {e}")

            # Re-post the full answer with download option
            client.chat_postMessage(
                channel=channel,
                text=(
                    f"<@{user}> Great — here's that answer again:\n\n"
                    f"{pending['answer']}{DOWNLOAD_FOOTER}"
                )
            )
            # Store results so download works immediately
            _last_interaction[user] = {
                "results":    results,
                "csv_string": csv_string,
                "index_id":   None,
            }
            # Learn from confirmation in background
            on_suggestion_confirmed(
                question = pending["question"],
                answer   = pending["answer"],
                sql      = sql,
            )
            del _pending_suggestion[user]
            return

    # ── Intent check ──────────────────────────────────────────────────────────
    intent = classify_intent(text)
    print(f"[InsightBot] Intent: {intent}")

    # ── Number reply handler (1/2/3 to pick a suggestion) ───────────────────
    if re.match(r'^[123]$', text.strip()):
        pending_alts = _pending_alternatives.get(user)
        if pending_alts:
            idx = int(text.strip()) - 1
            if 0 <= idx < len(pending_alts):
                chosen = pending_alts[idx]
                del _pending_alternatives[user]

                if chosen.can_run and chosen.sql:
                    # We have the SQL — run it directly
                    client.chat_postMessage(
                        channel=channel,
                        text=f"<@{user}> Running: _{chosen.question}_..."
                    )
                    try:
                        results    = run_query(chosen.sql)
                        flags      = detect_anomalies(chosen.question, results)
                        summary    = summarise_results(chosen.question, results)
                        reply      = summary
                        if flags:
                            reply += "\n" + "\n".join(flags)
                        reply += DOWNLOAD_FOOTER
                        csv_string = results_to_csv_string(results)
                        client.chat_postMessage(channel=channel, text=f"<@{user}> {reply}")
                        # Log to Databricks with JSON results for persistent download
                        user_info = get_user_info(client, user)
                        index_id  = log_interaction(
                            user_id=user,
                            email_id=user_info.get("email_id", ""),
                            full_name=user_info.get("full_name", ""),
                            question_asked=chosen.question,
                            question_answered=reply,
                            generated_csv=csv_string if csv_string else None,
                            csv_downloaded="no",
                            query_results_json=json.dumps(results) if results else None,
                        )
                        _last_interaction[user] = {
                            "results":    results,
                            "csv_string": csv_string,
                            "index_id":   index_id,
                        }
                    except Exception as e:
                        client.chat_postMessage(
                            channel=channel,
                            text=f"<@{user}> Sorry, that query also failed: {str(e)[:100]}"
                        )
                else:
                    # No SQL — re-run as a fresh question
                    process_message(client, user, chosen.question, channel)
                return

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
            client, channel, ts, questions[0], user=user
        )

        # Log to Databricks — include query_results_json for Tier-2/3 download fallback
        index_id = log_interaction(
            user_id=user,
            email_id=email,
            full_name=full_name,
            question_asked=questions[0],
            question_answered=reply,
            generated_csv=csv_string if csv_string else None,
            csv_downloaded="no",
            query_results_json=json.dumps(results) if results else None,
        )

        # Preserve question + sql set by cache_hit_meta so Tier-4 re-run works
        _cached = _last_interaction.get(user, {})
        _last_interaction[user] = {
            "results":    results,
            "csv_string": csv_string,
            "index_id":   index_id,
            "question":   _cached.get("question", questions[0]),
            "sql":        _cached.get("sql", ""),
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
                        _answer_with_progress, client, channel, ts, q, i, user
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

        # Preserve question + sql from cache_hit_meta for multi-Q download
        _cached = _last_interaction.get(user, {})
        _last_interaction[user] = {
            "results":    last_results,
            "csv_string": last_csv_string,
            "index_id":   last_index_id,
            "question":   _cached.get("question", ""),
            "sql":        _cached.get("sql", ""),
        }

        client.chat_update(
            channel=channel,
            ts=ts,
            text="\n\n".join(parts)
        )


# ── Slack event handlers ──────────────────────────────────────────────────────
@app.message("")
def handle_message(message, client):
    # Only handle original user messages — skip edits, deletes, bot posts
    if message.get("subtype") in ("message_changed", "message_deleted", "bot_message"):
        return
    user    = message.get("user", "unknown")
    text    = message.get("text", "").strip()
    channel = message.get("channel", "")
    # Skip empty messages and bot messages
    if not text or message.get("bot_id"):
        return
    # Skip @mention messages — handled exclusively by handle_mention
    # Prevents double-processing and _last_interaction overwrite race conditions
    if re.search(r'<@[A-Z0-9]+>', text):
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
