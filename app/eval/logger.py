"""
Phase 6 — Eval Logger
Logs every question to a local CSV for pass rate analysis.
Columns: timestamp, question, sql, rows_returned, latency_sec, cached, status, anomalies
"""

import os
import csv
import time
from datetime import datetime

LOG_DIR  = os.path.join(os.path.dirname(__file__))
LOG_FILE = os.path.join(LOG_DIR, "eval_log.csv")

FIELDNAMES = [
    "timestamp",
    "question",
    "sql",
    "rows_returned",
    "latency_sec",
    "cached",
    "status",        # pass | fail | blocked | cache_hit
    "anomalies",     # number of anomaly flags triggered
    "error",         # error message if status=fail
]


def _ensure_file():
    """Create CSV with headers if it doesn't exist."""
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()


def log(
    question:     str,
    sql:          str   = "",
    rows_returned: int  = 0,
    latency_sec:  float = 0.0,
    cached:       bool  = False,
    status:       str   = "pass",   # pass | fail | blocked | cache_hit
    anomalies:    int   = 0,
    error:        str   = "",
) -> None:
    """Append one row to the eval log CSV."""
    try:
        _ensure_file()
        with open(LOG_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writerow({
                "timestamp":     datetime.now().isoformat(),
                "question":      question,
                "sql":           sql.replace("\n", " "),
                "rows_returned": rows_returned,
                "latency_sec":   round(latency_sec, 2),
                "cached":        cached,
                "status":        status,
                "anomalies":     anomalies,
                "error":         error,
            })
    except Exception as e:
        print(f"[Logger] Failed to log: {e}")


def get_stats() -> dict:
    """
    Read the CSV and return summary stats.
    Useful for a /stats command or periodic reporting.
    """
    try:
        _ensure_file()
        rows = []
        with open(LOG_FILE, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            return {"total": 0}

        total      = len(rows)
        passed     = sum(1 for r in rows if r["status"] == "pass")
        failed     = sum(1 for r in rows if r["status"] == "fail")
        blocked    = sum(1 for r in rows if r["status"] == "blocked")
        cache_hits = sum(1 for r in rows if r["status"] == "cache_hit")
        anomalies  = sum(int(r.get("anomalies", 0)) for r in rows)

        latencies = [float(r["latency_sec"]) for r in rows
                     if r["latency_sec"] and r["status"] != "cache_hit"]
        avg_latency = round(sum(latencies) / len(latencies), 2) if latencies else 0

        cache_latencies = [float(r["latency_sec"]) for r in rows
                           if r["status"] == "cache_hit" and r["latency_sec"]]
        avg_cache_latency = round(sum(cache_latencies) / len(cache_latencies), 2) if cache_latencies else 0

        return {
            "total":              total,
            "pass_rate":          f"{round(passed/total*100, 1)}%",
            "passed":             passed,
            "failed":             failed,
            "blocked":            blocked,
            "cache_hits":         cache_hits,
            "cache_hit_rate":     f"{round(cache_hits/total*100, 1)}%",
            "total_anomalies":    anomalies,
            "avg_latency_sec":    avg_latency,
            "avg_cache_latency":  avg_cache_latency,
        }
    except Exception as e:
        print(f"[Logger] Failed to get stats: {e}")
        return {"total": 0, "error": str(e)}
