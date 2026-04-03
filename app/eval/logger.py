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
    "status",        # pass | fail | blocked | cache_hit | cache_suggestion
                     # cache_suggestion_confirmed | cache_miss | fail_attempt_N
                     # exhausted_retries
    "anomalies",     # number of anomaly flags triggered
    "error",         # error message if status=fail
    "failure_type",  # classified failure type (column_not_found, timeout, etc.)
    "recovery_attempted",  # yes | no
    "normalised_question", # what the normaliser produced (for abbreviation learning)
]


def _ensure_file():
    """Create CSV with headers if it doesn't exist."""
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()


def log(
    question:             str,
    sql:                  str   = "",
    rows_returned:        int   = 0,
    latency_sec:          float = 0.0,
    cached:               bool  = False,
    status:               str   = "pass",
    anomalies:            int   = 0,
    error:                str   = "",
    failure_type:         str   = "",
    recovery_attempted:   str   = "no",
    normalised_question:  str   = "",
) -> None:
    """Append one row to the eval log CSV."""
    try:
        _ensure_file()
        with open(LOG_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
            writer.writerow({
                "timestamp":            datetime.now().isoformat(),
                "question":             question,
                "sql":                  sql.replace("\n", " "),
                "rows_returned":        rows_returned,
                "latency_sec":          round(latency_sec, 2),
                "cached":               cached,
                "status":               status,
                "anomalies":            anomalies,
                "error":                error,
                "failure_type":         failure_type,
                "recovery_attempted":   recovery_attempted,
                "normalised_question":  normalised_question,
            })
    except Exception as e:
        print(f"[Logger] Failed to log: {e}")


def get_stats(days: int = None) -> dict:
    """
    Read the CSV and return comprehensive stats.
    If days is provided, only consider rows from the last N days.
    Returns all data needed for the /stats command.
    """
    from collections import Counter
    from datetime import datetime, timedelta

    try:
        _ensure_file()
        rows = []
        with open(LOG_FILE, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            return {"total": 0}

        # Apply time window if requested
        if days:
            cutoff = datetime.now() - timedelta(days=days)
            filtered = []
            for r in rows:
                try:
                    if datetime.fromisoformat(r.get("timestamp", "")) >= cutoff:
                        filtered.append(r)
                except Exception:
                    pass
            rows_window = filtered
        else:
            rows_window = rows

        if not rows_window:
            return {"total": 0, "window_days": days}

        total       = len(rows_window)
        passed      = sum(1 for r in rows_window if r.get("status") == "pass")
        failed      = sum(1 for r in rows_window if r.get("status") in ("fail", "exhausted_retries"))
        blocked     = sum(1 for r in rows_window if r.get("status") == "blocked")
        cache_hits  = sum(1 for r in rows_window if r.get("status") == "cache_hit")
        suggestions = sum(1 for r in rows_window if r.get("status") == "cache_suggestion")
        confirmed   = sum(1 for r in rows_window if r.get("status") == "cache_suggestion_confirmed")
        anomalies   = sum(int(r.get("anomalies", 0) or 0) for r in rows_window)
        recovered   = sum(1 for r in rows_window if r.get("recovery_attempted") == "yes"
                         and r.get("status") == "pass")
        exhausted   = sum(1 for r in rows_window if r.get("status") == "exhausted_retries")

        # Latency stats (exclude cache hits — they're instant)
        latencies = []
        for r in rows_window:
            try:
                if r.get("latency_sec") and r.get("status") not in ("cache_hit", "cache_suggestion"):
                    latencies.append(float(r["latency_sec"]))
            except Exception:
                pass
        avg_latency = round(sum(latencies) / len(latencies), 2) if latencies else 0
        p95_latency = round(sorted(latencies)[int(len(latencies) * 0.95)], 2) if len(latencies) >= 5 else 0

        cache_latencies = []
        for r in rows_window:
            try:
                if r.get("status") == "cache_hit" and r.get("latency_sec"):
                    cache_latencies.append(float(r["latency_sec"]))
            except Exception:
                pass
        avg_cache_latency = round(sum(cache_latencies) / len(cache_latencies), 2) if cache_latencies else 0

        # Top failure patterns (last 7 days window)
        failure_counts = Counter(
            r.get("failure_type", "unknown")
            for r in rows_window
            if r.get("failure_type") and r.get("status") in ("fail", "exhausted_retries")
        )
        top_failures = failure_counts.most_common(5)

        # Auto-retry success rate
        retry_attempts = sum(1 for r in rows_window if r.get("recovery_attempted") == "yes")
        retry_success_rate = (
            f"{round(recovered / retry_attempts * 100, 1)}%"
            if retry_attempts else "N/A"
        )

        # Cache suggestion confirmation rate
        suggestion_confirm_rate = (
            f"{round(confirmed / suggestions * 100, 1)}%"
            if suggestions else "N/A"
        )

        return {
            # Core
            "total":                    total,
            "pass_rate":                f"{round(passed/total*100, 1)}%",
            "passed":                   passed,
            "failed":                   failed,
            "blocked":                  blocked,
            "total_anomalies":          anomalies,
            "window_days":              days,

            # Cache
            "cache_hits":               cache_hits,
            "cache_hit_rate":           f"{round(cache_hits/total*100, 1)}%",
            "cache_suggestions":        suggestions,
            "cache_suggestion_rate":    f"{round(suggestions/total*100, 1)}%",
            "suggestion_confirm_rate":  suggestion_confirm_rate,
            "avg_latency_sec":          avg_latency,
            "p95_latency_sec":          p95_latency,
            "avg_cache_latency":        avg_cache_latency,

            # Recovery
            "retry_attempts":           retry_attempts,
            "retry_success_rate":       retry_success_rate,
            "exhausted_retries":        exhausted,

            # Failure patterns
            "top_failures":             top_failures,   # list of (type, count) tuples
        }
    except Exception as e:
        print(f"[Logger] Failed to get stats: {e}")
        return {"total": 0, "error": str(e)}
