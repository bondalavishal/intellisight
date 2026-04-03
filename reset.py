"""
reset.py — Full InsightBot state reset.

Clears:
  1. Databricks interaction log (TRUNCATE TABLE)
  2. ChromaDB insightbot_cache collection (keeps RAG intact)
  3. Local eval_log.csv (recreated with headers only)
  4. Local learned_state.json (reset to empty)

Run:
  source venv/bin/activate
  python reset.py

Then reload RAG if needed:
  python -m app.rag.loader

Then start the bot:
  python main.py
"""

import os
import json
import csv
from dotenv import load_dotenv

load_dotenv()

print("\n🔄  InsightBot full reset starting...\n")

# ── 1. Databricks ─────────────────────────────────────────────────────────────
print("1. Clearing Databricks interaction log...")
try:
    from app.sql.connector import run_query
    run_query("TRUNCATE TABLE default.insightbot_interactions")
    print("   ✅ default.insightbot_interactions truncated")
except Exception as e:
    print(f"   ⚠️  Could not truncate Databricks table: {e}")

# ── 2. ChromaDB cache (keep RAG) ──────────────────────────────────────────────
print("\n2. Clearing ChromaDB cache collection...")
try:
    import chromadb
    chroma_dir = os.path.join(os.path.dirname(__file__), "app", "rag", "chroma_db")
    client = chromadb.PersistentClient(path=chroma_dir)
    try:
        client.delete_collection("insightbot_cache")
        print("   ✅ insightbot_cache collection deleted")
    except Exception:
        print("   ℹ️  insightbot_cache was already empty / doesn't exist")

    # Confirm RAG is still intact
    try:
        rag = client.get_collection("insightbot_rag")
        print(f"   ✅ insightbot_rag intact ({rag.count()} chunks)")
    except Exception:
        print("   ⚠️  insightbot_rag not found — run: python -m app.rag.loader")
except Exception as e:
    print(f"   ⚠️  ChromaDB reset failed: {e}")

# ── 3. eval_log.csv ───────────────────────────────────────────────────────────
print("\n3. Resetting eval_log.csv...")
log_path = os.path.join(os.path.dirname(__file__), "app", "eval", "eval_log.csv")
FIELDNAMES = [
    "timestamp", "question", "sql", "rows_returned", "latency_sec",
    "cached", "status", "anomalies", "error", "failure_type",
    "recovery_attempted", "normalised_question",
]
try:
    with open(log_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
    print(f"   ✅ {log_path} reset (headers only)")
except Exception as e:
    print(f"   ⚠️  Could not reset eval_log.csv: {e}")

# ── 4. learned_state.json ─────────────────────────────────────────────────────
print("\n4. Resetting learned_state.json...")
state_path = os.path.join(os.path.dirname(__file__), "app", "eval", "learned_state.json")
try:
    with open(state_path, "w") as f:
        json.dump({}, f, indent=2)
    print(f"   ✅ {state_path} reset")
except Exception as e:
    print(f"   ⚠️  Could not reset learned_state.json: {e}")

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n✅  Reset complete.\n")
print("Next steps:")
print("  python main.py           ← start fresh")
print("  python -m app.rag.loader ← only if you also wiped chroma_db entirely\n")
