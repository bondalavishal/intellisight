"""
feedback_engine.py — Phase 10 (Layer 6: Self-Evolution)

The self-learning brain of InsightBot. Runs automatically after every
successful interaction and on a periodic background schedule.

What it does without human intervention:
  1. Auto-promotes successful Q→SQL pairs into ChromaDB RAG store
  2. Detects new abbreviations from failed/missed queries and registers
     them into the normaliser's live abbreviation registry at runtime
  3. Accumulates failure patterns — when the same failure type hits a
     threshold, auto-promotes the pattern into the unanswerable list
  4. Self-tunes cache similarity thresholds based on hit/miss/suggestion
     confirmation rates from the eval log

Design principles:
  - Every public function is safe to call in a background thread
  - All mutations are logged so a human can audit what the system learned
  - Nothing is deleted — only additions and threshold adjustments
  - Humans can override anything by editing the config at the bottom
"""

import os
import re
import csv
import json
import time
import threading
from datetime import datetime, timedelta
from collections import Counter
from typing import Optional

from app.eval.logger import LOG_FILE, FIELDNAMES

# ── Config ────────────────────────────────────────────────────────────────────
# All tunable constants in one place. Change these to adjust learning behaviour.

FEEDBACK_CONFIG = {
    # Auto-promotion into RAG
    "rag_promotion_min_confidence": 0.0,   # promote all successful pairs for now
    "rag_promotion_source_label":   "auto_learned",

    # Failure pattern accumulation
    "failure_pattern_threshold":    5,     # N failures of same type → auto-promote
    "failure_window_days":          7,     # look back this many days for patterns

    # Cache threshold self-tuning
    "threshold_tune_min_samples":   20,    # min interactions before tuning
    "threshold_tune_interval_hrs":  24,    # how often to re-tune (hours)
    "threshold_direct_hit_floor":   0.88,  # never go below this
    "threshold_direct_hit_ceil":    0.97,  # never go above this
    "threshold_suggest_floor":      0.70,  # never go below this
    "threshold_suggest_ceil":       0.85,  # never go above this

    # Abbreviation learning
    "abbrev_min_occurrence":        3,     # seen N times before auto-registering
    "abbrev_max_length":            8,     # only learn short tokens as abbreviations
}

# Path for persisting what the engine has learned (audit trail)
_ENGINE_DIR  = os.path.join(os.path.dirname(__file__))
_LEARNED_FILE = os.path.join(_ENGINE_DIR, "learned_state.json")

# Lock for thread-safe writes
_lock = threading.Lock()


# ── Learned state persistence ─────────────────────────────────────────────────

def _load_state() -> dict:
    """Load persisted learned state from disk."""
    default = {
        "promoted_rag_ids":        [],    # cache IDs already promoted to RAG
        "failure_counts":          {},    # {failure_type: count}
        "promoted_patterns":       [],    # patterns already added to unanswerable
        "learned_abbreviations":   {},    # {token: expansion}
        "candidate_abbreviations": {},    # {token: count} — tracking towards threshold
        "last_threshold_tune":     None,
        "threshold_history":       [],    # audit trail of threshold changes
    }
    try:
        if os.path.exists(_LEARNED_FILE):
            with open(_LEARNED_FILE, "r") as f:
                loaded = json.load(f)
                default.update(loaded)
    except Exception as e:
        print(f"[FeedbackEngine] Could not load state ({e}) — starting fresh")
    return default


def _save_state(state: dict) -> None:
    """Persist learned state to disk."""
    try:
        with _lock:
            with open(_LEARNED_FILE, "w") as f:
                json.dump(state, f, indent=2, default=str)
    except Exception as e:
        print(f"[FeedbackEngine] Could not save state: {e}")


# ── 1. RAG Auto-Promotion ─────────────────────────────────────────────────────

def promote_to_rag(question: str, sql: str, answer: str) -> bool:
    """
    Add a successful Q→SQL pair to ChromaDB RAG store as a learned example.
    This improves future SQL generation for similar questions.

    Called automatically after every successful Databricks query.
    Returns True if promotion succeeded.
    """
    try:
        import chromadb
        from chromadb.utils import embedding_functions

        rag_dir    = os.path.join(os.path.dirname(__file__), '..', 'rag')
        chroma_dir = os.path.join(rag_dir, "chroma_db")

        ef     = embedding_functions.ONNXMiniLM_L6_V2()
        client = chromadb.PersistentClient(path=chroma_dir)

        collection = client.get_collection(
            name="insightbot_rag",
            embedding_function=ef,
        )

        # Use hash of question as stable ID — prevents duplicates
        doc_id = f"auto_{abs(hash(question.lower().strip()))}"

        # Check if already promoted
        state = _load_state()
        if doc_id in state["promoted_rag_ids"]:
            return False  # already in RAG, skip

        # Format as a few-shot example the LLM can learn from
        document = (
            f"LEARNED EXAMPLE\n"
            f"Question: {question}\n"
            f"SQL:\n{sql}\n"
            f"Answer summary: {answer[:200]}"
        )

        # Upsert — safe to call multiple times
        try:
            collection.delete(ids=[doc_id])
        except Exception:
            pass

        collection.add(
            ids       = [doc_id],
            documents = [document],
            metadatas = [{
                "source":    FEEDBACK_CONFIG["rag_promotion_source_label"],
                "heading":   "auto_learned_example",
                "question":  question[:200],
                "promoted_at": str(time.time()),
            }],
        )

        # Mark as promoted in state
        state["promoted_rag_ids"].append(doc_id)
        _save_state(state)

        print(f"[FeedbackEngine] Promoted to RAG: {question[:60]}...")
        return True

    except Exception as e:
        print(f"[FeedbackEngine] RAG promotion failed: {e}")
        return False


# ── 2. Abbreviation Learning ──────────────────────────────────────────────────

def learn_abbreviations_from_failures(question: str) -> None:
    """
    Analyse a question that resulted in a cache miss or failure.
    Look for short tokens (likely abbreviations) that don't appear in
    the normaliser's known map. Track them as candidates.
    Once a candidate token is seen N times, register it automatically.

    This runs on failed/missed questions — not on successes.
    """
    try:
        from app.utils.normaliser import ABBREVIATIONS, register_abbreviation

        # Tokenise
        tokens = re.findall(r'\b[a-z]{2,8}\b', question.lower())

        # Filter to likely abbreviations:
        # - Short (≤ max_length chars)
        # - Not already a known abbreviation key or value
        # - Not a common English stop word
        STOP_WORDS = {
            "the", "and", "for", "are", "was", "has", "had", "have",
            "with", "that", "this", "from", "what", "show", "tell",
            "give", "list", "find", "get", "how", "many", "much",
            "top", "all", "any", "per", "by", "in", "of", "to", "is",
            "me", "my", "our", "can", "did", "do", "does",
        }

        known_keys   = set(re.sub(r'\\b|\\b', '', k).strip() for k in ABBREVIATIONS.keys())
        known_values = set(v.lower() for v in ABBREVIATIONS.values())
        max_len      = FEEDBACK_CONFIG["abbrev_max_length"]
        threshold    = FEEDBACK_CONFIG["abbrev_min_occurrence"]

        state = _load_state()
        candidates = state.get("candidate_abbreviations", {})
        learned    = state.get("learned_abbreviations", {})

        changed = False
        for token in tokens:
            if (len(token) <= max_len
                    and token not in STOP_WORDS
                    and token not in known_keys
                    and token not in known_values
                    and token not in learned):

                candidates[token] = candidates.get(token, 0) + 1

                if candidates[token] >= threshold:
                    # Auto-register — use the token itself as a placeholder expansion
                    # A human can refine the expansion later via learned_state.json
                    # but the token will at least be protected from spell-correction
                    register_abbreviation(token, token)
                    learned[token] = token
                    print(f"[FeedbackEngine] Auto-registered abbreviation candidate: '{token}'")
                    changed = True

        if changed or candidates != state.get("candidate_abbreviations", {}):
            state["candidate_abbreviations"] = candidates
            state["learned_abbreviations"]   = learned
            _save_state(state)

    except Exception as e:
        print(f"[FeedbackEngine] Abbreviation learning failed: {e}")


# ── 3. Failure Pattern Accumulation ──────────────────────────────────────────

def accumulate_failure_pattern(failure_type: str, question: str) -> None:
    """
    Track repeated failures. When the same failure type reaches the
    threshold within the configured window, auto-promote it as a known
    unanswerable pattern in handler.py's UNANSWERABLE_PATTERNS.

    Called automatically on every Databricks failure after retries exhausted.
    """
    try:
        state = _load_state()
        counts = state.get("failure_counts", {})

        # Compound key: failure_type + day bucket (to enforce window)
        today  = datetime.now().strftime("%Y-%m-%d")
        key    = f"{failure_type}:{today}"
        counts[key] = counts.get(key, 0) + 1

        threshold = FEEDBACK_CONFIG["failure_pattern_threshold"]
        window    = FEEDBACK_CONFIG["failure_window_days"]

        # Count across the whole window
        window_start = datetime.now() - timedelta(days=window)
        window_total = sum(
            v for k, v in counts.items()
            if k.startswith(failure_type)
            and _parse_date_key(k) >= window_start
        )

        print(f"[FeedbackEngine] Failure '{failure_type}' count in window: {window_total}/{threshold}")

        already_promoted = state.get("promoted_patterns", [])

        if window_total >= threshold and failure_type not in already_promoted:
            _promote_failure_pattern(failure_type, question)
            already_promoted.append(failure_type)
            state["promoted_patterns"] = already_promoted
            print(f"[FeedbackEngine] Auto-promoted failure pattern: {failure_type}")

        state["failure_counts"] = counts
        _save_state(state)

    except Exception as e:
        print(f"[FeedbackEngine] Failure pattern accumulation error: {e}")


def _parse_date_key(key: str) -> datetime:
    """Parse date from failure count key like 'column_not_found:2026-03-31'."""
    try:
        date_str = key.split(":")[-1]
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return datetime.min


def _promote_failure_pattern(failure_type: str, example_question: str) -> None:
    """
    Dynamically add a failure type to handler.UNANSWERABLE_PATTERNS at runtime.
    This affects all future requests without restarting the bot.
    """
    try:
        from app.slack import handler as _handler

        # Build a broad pattern from the failure type name
        pattern_map = {
            "column_not_found": (
                r'column.{0,40}not.{0,10}found|unresolved.{0,20}column',
                f"Column referenced does not exist — auto-detected pattern from {failure_type}."
            ),
            "table_not_found": (
                r'table.{0,40}not.{0,10}found',
                f"Table or view referenced does not exist — auto-detected from {failure_type}."
            ),
        }

        if failure_type in pattern_map:
            pattern, reason = pattern_map[failure_type]
            entry = (pattern, reason)
            if entry not in _handler.UNANSWERABLE_PATTERNS:
                _handler.UNANSWERABLE_PATTERNS.append(entry)
                print(f"[FeedbackEngine] Added to UNANSWERABLE_PATTERNS: {failure_type}")

    except Exception as e:
        print(f"[FeedbackEngine] Pattern promotion failed: {e}")


# ── 4. Cache Threshold Self-Tuning ───────────────────────────────────────────

def tune_cache_thresholds() -> None:
    """
    Analyse recent eval log to determine if cache thresholds should be adjusted.

    Logic:
    - If suggestion confirmation rate is high → lower DIRECT_HIT_THRESHOLD
      (suggestions are reliable, promote them to direct hits)
    - If cache miss rate is high → lower SUGGEST_THRESHOLD
      (cast a wider net for suggestions)
    - If false positive rate is high → raise thresholds
      (be more conservative)

    All changes are bounded by floor/ceil in FEEDBACK_CONFIG.
    Runs at most once per threshold_tune_interval_hrs.
    """
    try:
        state = _load_state()
        last_tune = state.get("last_threshold_tune")

        if last_tune:
            hours_since = (datetime.now() - datetime.fromisoformat(last_tune)).total_seconds() / 3600
            if hours_since < FEEDBACK_CONFIG["threshold_tune_interval_hrs"]:
                return  # Too soon

        rows = _read_recent_log(days=7)
        if len(rows) < FEEDBACK_CONFIG["threshold_tune_min_samples"]:
            print(f"[FeedbackEngine] Not enough samples to tune thresholds ({len(rows)} rows)")
            return

        total      = len(rows)
        hits       = sum(1 for r in rows if r.get("status") == "cache_hit")
        misses     = sum(1 for r in rows if r.get("status") == "cache_miss")
        suggestions = sum(1 for r in rows if r.get("status") == "cache_suggestion")
        confirmed  = sum(1 for r in rows if r.get("status") == "cache_suggestion_confirmed")

        hit_rate         = hits / total if total else 0
        suggestion_rate  = suggestions / total if total else 0
        confirmation_rate = confirmed / suggestions if suggestions else 0

        from app.eval import cache as _cache

        new_direct = _cache.DIRECT_HIT_THRESHOLD
        new_suggest = _cache.SUGGEST_THRESHOLD

        # Tune direct hit threshold
        if confirmation_rate > 0.8 and suggestion_rate > 0.1:
            # Suggestions are highly reliable — lower direct hit threshold slightly
            new_direct = max(
                FEEDBACK_CONFIG["threshold_direct_hit_floor"],
                _cache.DIRECT_HIT_THRESHOLD - 0.01
            )
        elif hit_rate < 0.2 and total > 50:
            # Very low cache hit rate — lower threshold to cast wider net
            new_direct = max(
                FEEDBACK_CONFIG["threshold_direct_hit_floor"],
                _cache.DIRECT_HIT_THRESHOLD - 0.01
            )
        elif hit_rate > 0.7:
            # High hit rate — can afford to tighten threshold
            new_direct = min(
                FEEDBACK_CONFIG["threshold_direct_hit_ceil"],
                _cache.DIRECT_HIT_THRESHOLD + 0.01
            )

        # Tune suggestion threshold
        if suggestion_rate < 0.05 and misses / total > 0.5:
            # Too many misses, not enough suggestions — lower suggest threshold
            new_suggest = max(
                FEEDBACK_CONFIG["threshold_suggest_floor"],
                _cache.SUGGEST_THRESHOLD - 0.01
            )

        # Apply if changed
        changed = False
        if new_direct != _cache.DIRECT_HIT_THRESHOLD:
            old = _cache.DIRECT_HIT_THRESHOLD
            _cache.DIRECT_HIT_THRESHOLD = new_direct
            print(f"[FeedbackEngine] DIRECT_HIT_THRESHOLD: {old} → {new_direct}")
            changed = True

        if new_suggest != _cache.SUGGEST_THRESHOLD:
            old = _cache.SUGGEST_THRESHOLD
            _cache.SUGGEST_THRESHOLD = new_suggest
            print(f"[FeedbackEngine] SUGGEST_THRESHOLD: {old} → {new_suggest}")
            changed = True

        if changed:
            state["threshold_history"].append({
                "timestamp":    datetime.now().isoformat(),
                "direct_hit":   _cache.DIRECT_HIT_THRESHOLD,
                "suggest":      _cache.SUGGEST_THRESHOLD,
                "hit_rate":     round(hit_rate, 3),
                "confirm_rate": round(confirmation_rate, 3),
                "samples":      total,
            })

        state["last_threshold_tune"] = datetime.now().isoformat()
        _save_state(state)

    except Exception as e:
        print(f"[FeedbackEngine] Threshold tuning failed: {e}")


# ── Log reader helper ─────────────────────────────────────────────────────────

def _read_recent_log(days: int = 7) -> list[dict]:
    """Read eval_log.csv rows from the last N days."""
    rows = []
    cutoff = datetime.now() - timedelta(days=days)
    try:
        if not os.path.exists(LOG_FILE):
            return []
        with open(LOG_FILE, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    ts = datetime.fromisoformat(row.get("timestamp", ""))
                    if ts >= cutoff:
                        rows.append(row)
                except Exception:
                    continue
    except Exception as e:
        print(f"[FeedbackEngine] Log read failed: {e}")
    return rows


# ── Public entry points ───────────────────────────────────────────────────────

def on_success(question: str, sql: str, answer: str) -> None:
    """
    Call this after every successful Databricks query.
    Runs in a background thread — non-blocking.

    Triggers:
      - RAG auto-promotion
      - Cache threshold tuning check
    """
    def _run():
        promote_to_rag(question, sql, answer)
        tune_cache_thresholds()

    threading.Thread(target=_run, daemon=True).start()


def on_failure(question: str, failure_type: str) -> None:
    """
    Call this after every exhausted failure (retries exhausted, no recovery).
    Runs in a background thread — non-blocking.

    Triggers:
      - Abbreviation learning from the failed question
      - Failure pattern accumulation
    """
    def _run():
        learn_abbreviations_from_failures(question)
        accumulate_failure_pattern(failure_type, question)

    threading.Thread(target=_run, daemon=True).start()


def on_cache_miss(question: str) -> None:
    """
    Call this on every cache miss (question went to LLM generation).
    Runs in a background thread — non-blocking.

    Triggers:
      - Abbreviation learning (missed questions may contain unknown tokens)
    """
    def _run():
        learn_abbreviations_from_failures(question)

    threading.Thread(target=_run, daemon=True).start()


def on_suggestion_confirmed(question: str, answer: str, sql: str) -> None:
    """
    Call this when a user replies 'yes' confirming a cache suggestion was correct.
    Runs in a background thread — non-blocking.

    Triggers:
      - Promote the user's exact phrasing into the cache as a direct hit
      - Promote to RAG as a learned example
    """
    def _run():
        from app.eval.cache import promote_suggestion
        promote_suggestion(question, answer, sql)
        promote_to_rag(question, sql, answer)
        tune_cache_thresholds()

    threading.Thread(target=_run, daemon=True).start()


def get_learning_stats() -> dict:
    """Return a summary of what the system has learned. Used by /stats command."""
    state = _load_state()
    return {
        "rag_auto_promotions":       len(state.get("promoted_rag_ids", [])),
        "learned_abbreviations":     len(state.get("learned_abbreviations", {})),
        "candidate_abbreviations":   len(state.get("candidate_abbreviations", {})),
        "promoted_failure_patterns": len(state.get("promoted_patterns", [])),
        "threshold_tune_count":      len(state.get("threshold_history", [])),
        "last_threshold_tune":       state.get("last_threshold_tune", "never"),
    }
