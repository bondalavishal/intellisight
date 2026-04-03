"""
Interaction Logger — Phase 7 addition.
Logs every InsightBot interaction to default.insightbot_interactions in Databricks.
"""

import csv
import io
import json
from app.sql.connector import run_query


def ensure_results_json_column() -> None:
    """Add query_results_json column on startup; silent no-op if it already exists."""
    try:
        run_query(
            "ALTER TABLE default.insightbot_interactions "
            "ADD COLUMN query_results_json STRING"
        )
        print("[InteractionLogger] Added column query_results_json")
    except Exception as e:
        if "already exists" not in str(e).lower() and "duplicate" not in str(e).lower():
            print(f"[InteractionLogger] ensure_results_json_column: {e}")


def get_user_info(client, user_id: str) -> dict:
    try:
        response  = client.users_info(user=user_id)
        user      = response["user"]
        full_name = user.get("real_name") or user.get("name") or ""
        email_id  = user.get("profile", {}).get("email") or ""
        return {"full_name": full_name, "email_id": email_id}
    except Exception as e:
        print(f"[InteractionLogger] Could not fetch user info: {e}")
        return {"full_name": "", "email_id": ""}


def log_interaction(
    user_id:            str,
    email_id:           str,
    full_name:          str,
    question_asked:     str,
    question_answered:  str,
    generated_csv:      str = None,
    csv_downloaded:     str = "no",
    query_results_json: str = None,
) -> int | None:
    def esc(s):
        return (s or "").replace("'", "''")

    csv_val  = f"'{esc(generated_csv)}'"      if generated_csv      else "NULL"
    json_val = f"'{esc(query_results_json)}'" if query_results_json else "NULL"

    sql = f"""
    INSERT INTO default.insightbot_interactions
      (user_id, email_id, full_name, question_asked,
       question_answered, generated_csv, csv_downloaded, ts, query_results_json)
    VALUES (
      '{esc(user_id)}',
      '{esc(email_id)}',
      '{esc(full_name)}',
      '{esc(question_asked)}',
      '{esc(question_answered)}',
      {csv_val},
      '{esc(csv_downloaded)}',
      CURRENT_TIMESTAMP(),
      {json_val}
    )
    """
    try:
        run_query(sql)
        result = run_query(f"""
            SELECT MAX(index_id) AS last_id
            FROM default.insightbot_interactions
            WHERE user_id = '{esc(user_id)}'
        """)
        if result:
            return result[0].get("last_id")
    except Exception as e:
        print(f"[InteractionLogger] Failed to log interaction: {e}")
    return None


def mark_csv_downloaded(index_id: int) -> None:
    if not index_id:
        return
    try:
        run_query(f"""
            UPDATE default.insightbot_interactions
            SET csv_downloaded = 'yes'
            WHERE index_id = {index_id}
        """)
        print(f"[InteractionLogger] Marked index_id={index_id} as csv_downloaded=yes")
    except Exception as e:
        print(f"[InteractionLogger] Failed to update csv_downloaded: {e}")


def results_to_csv_string(results: list[dict]) -> str:
    if not results:
        return ""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=results[0].keys())
    writer.writeheader()
    writer.writerows(results)
    return output.getvalue()


def csv_string_to_bytes(csv_string: str) -> bytes:
    return csv_string.encode("utf-8")


def fetch_results_json_by_question(question: str) -> list[dict]:
    """
    Fetch the most recent query_results_json for a given question text.
    Used as a fallback when we know the question but not the user's latest row
    (e.g., after a direct cache hit that didn't re-run SQL).
    Returns a list of dicts, or [] if nothing found.
    """
    import json as _json
    def esc(s):
        return (s or "").replace("'", "''")
    try:
        rows = run_query(f"""
            SELECT query_results_json
            FROM default.insightbot_interactions
            WHERE question_asked = '{esc(question)}'
              AND query_results_json IS NOT NULL
            ORDER BY ts DESC
            LIMIT 1
        """)
        if rows and rows[0].get("query_results_json"):
            return _json.loads(rows[0]["query_results_json"])
    except Exception as e:
        print(f"[InteractionLogger] fetch_results_json_by_question failed: {e}")
    return []


def fetch_latest_results_for_user(user_id: str) -> list[dict]:
    """
    Fetch the most recent query_results_json for a user from Databricks.
    Used as a persistent fallback for the download command (survives bot restarts).
    Returns a list of dicts, or [] if nothing found.
    """
    def esc(s):
        return (s or "").replace("'", "''")
    try:
        rows = run_query(f"""
            SELECT query_results_json
            FROM default.insightbot_interactions
            WHERE user_id = '{esc(user_id)}'
              AND query_results_json IS NOT NULL
            ORDER BY ts DESC
            LIMIT 1
        """)
        if rows and rows[0].get("query_results_json"):
            return json.loads(rows[0]["query_results_json"])  # json imported at top
    except Exception as e:
        print(f"[InteractionLogger] fetch_latest_results_for_user failed: {e}")
    return []


def seed_cache_from_log() -> int:
    from app.eval.cache import save_to_cache

    print("Re-seeding ChromaDB cache from Databricks interaction log...")
    try:
        rows = run_query("""
            SELECT question_asked, question_answered
            FROM default.insightbot_interactions
            WHERE question_answered IS NOT NULL
              AND question_answered != ''
            ORDER BY ts ASC
        """)
        if not rows:
            print("  No rows found in log table. Cache not seeded.")
            return 0
        count = 0
        for row in rows:
            q = row.get("question_asked", "")
            a = row.get("question_answered", "")
            if q and a:
                save_to_cache(question=q, answer=a, sql="")
                count += 1
        print(f"  Seeded {count} Q&A pairs into ChromaDB cache.")
        return count
    except Exception as e:
        print(f"  Failed to seed cache: {e}")
        return 0


if __name__ == "__main__":
    seed_cache_from_log()
