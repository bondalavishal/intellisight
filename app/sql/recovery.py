"""
recovery.py — Phase 10 (Layer 4: Failure Recovery)

Attempts to fix or rewrite a failed SQL query based on the classified failure type.

Design principles:
  - Strategy registry: each recovery_hint maps to a handler function.
  - Adding a new strategy = add one function + one entry in STRATEGY_REGISTRY.
  - No if/elif chains. The dispatcher picks the right strategy from the registry.
  - Each strategy returns a RecoveryResult — either a rewritten SQL or a failure.
  - Max retries are enforced by the caller (handler.py), not here.

Adding a new recovery strategy:
    1. Write a function: def _your_strategy(sql, question, failure, schema_context) -> str | None
       Return the rewritten SQL string, or None if recovery is not possible.
    2. Add it to STRATEGY_REGISTRY with the matching recovery_hint key.
    That's it.
"""

import re
import os
import httpx
from dataclasses import dataclass
from typing import Optional, Callable

from cerebras.cloud.sdk import Cerebras as _Cerebras
from app.sql.error_classifier import FailureType
from app.sql.guardrails import ALLOWED_SOURCES

# ── LLM clients (reuse from sql_generator pattern) ───────────────────────────
_cerebras      = _Cerebras(api_key=os.getenv("CEREBRAS_API_KEY"))
CEREBRAS_MODEL = "qwen-3-235b-a22b-instruct-2507"
OLLAMA_URL     = "http://127.0.0.1:11434/api/generate"
OLLAMA_MODEL   = "mannix/defog-llama3-sqlcoder-8b"


# ── Result type ───────────────────────────────────────────────────────────────

@dataclass
class RecoveryResult:
    """
    Result of a recovery attempt.

    Fields:
      success         : True if a rewritten SQL was produced
      sql             : rewritten SQL (only valid if success=True)
      strategy_used   : which strategy ran
      failure_type    : original failure classification
      note            : optional explanation for logging
    """
    success:       bool
    sql:           str            = ""
    strategy_used: str            = ""
    failure_type:  Optional[FailureType] = None
    note:          str            = ""


# ── Shared LLM rewrite helper ─────────────────────────────────────────────────

REWRITE_PROMPT = """You are a Databricks SQL expert. A query failed with this error:

ERROR: {error}

ORIGINAL QUESTION: {question}

FAILED SQL:
{sql}

AVAILABLE TABLES/VIEWS:
{sources}

STRICT RULES:
- Fix ONLY what the error describes. Do not restructure unnecessarily.
- Never invent columns or tables not in AVAILABLE TABLES/VIEWS.
- Always include LIMIT.
- Use WITH clauses (CTEs) for complex joins.
- Reply with ONLY the fixed SQL inside a ```sql fence. No explanation.

FIXED SQL:"""


def _llm_rewrite(sql: str, question: str, failure: FailureType) -> Optional[str]:
    """
    Ask the LLM to rewrite a failed SQL query.
    Tries Cerebras first, falls back to Ollama.
    Returns cleaned SQL string or None if both fail.
    """
    sources = "\n".join(f"  - {s}" for s in ALLOWED_SOURCES)
    prompt  = REWRITE_PROMPT.format(
        error    = failure.raw_error[:500],
        question = question,
        sql      = sql,
        sources  = sources,
    )

    # Primary: Cerebras
    try:
        resp = _cerebras.chat.completions.create(
            model    = CEREBRAS_MODEL,
            messages = [{"role": "user", "content": prompt}],
            temperature = 0,
            max_tokens  = 512,
            timeout     = 45,
        )
        raw = resp.choices[0].message.content.strip()
        print("[Recovery] Rewrite via Cerebras")
        return _extract_sql(raw)
    except Exception as e:
        print(f"[Recovery] Cerebras rewrite failed ({e}) — trying Ollama")

    # Fallback: Ollama
    try:
        resp = httpx.post(OLLAMA_URL, json={
            "model":   OLLAMA_MODEL,
            "prompt":  prompt,
            "stream":  False,
            "options": {"temperature": 0, "num_predict": 512},
        }, timeout=120)
        raw = resp.json()["response"].strip()
        print("[Recovery] Rewrite via Ollama (fallback)")
        return _extract_sql(raw)
    except Exception as e:
        print(f"[Recovery] Ollama rewrite also failed ({e})")
        return None


def _extract_sql(raw: str) -> Optional[str]:
    """Extract SQL from LLM response — handles fenced and unfenced output."""
    fence = re.search(r'```sql\s*(.*?)(?:```|$)', raw, re.DOTALL | re.IGNORECASE)
    if fence:
        sql = fence.group(1).strip()
    else:
        plain = re.search(r'```\s*(.*?)(?:```|$)', raw, re.DOTALL)
        if plain:
            sql = plain.group(1).strip()
        else:
            fb = re.search(r'(?im)^(WITH|SELECT)\b', raw)
            sql = raw[fb.start():].strip() if fb else raw.strip()

    sql = sql.rstrip(";").strip()
    # Apply known alias safety fix
    sql = sql.replace("p.product_category_name_english", "t.product_category_name_english")
    return sql if sql else None


# ── Individual recovery strategies ───────────────────────────────────────────
# Each function signature: (sql, question, failure, context) -> str | None
# Return rewritten SQL string, or None if recovery not possible.

def _rewrite_sql_fix_column(
    sql: str, question: str, failure: FailureType, context: dict
) -> Optional[str]:
    """
    Recovery for column_not_found.
    Passes the column detail + full error to the LLM for a targeted rewrite.
    """
    print(f"[Recovery] Strategy: fix_column (detail={failure.matched_detail})")
    return _llm_rewrite(sql, question, failure)


def _rewrite_sql_fix_table(
    sql: str, question: str, failure: FailureType, context: dict
) -> Optional[str]:
    """
    Recovery for table_not_found.
    LLM rewrites using only allowed sources.
    """
    print(f"[Recovery] Strategy: fix_table (detail={failure.matched_detail})")
    return _llm_rewrite(sql, question, failure)


def _rewrite_sql_fix_syntax(
    sql: str, question: str, failure: FailureType, context: dict
) -> Optional[str]:
    """
    Recovery for syntax_error and unknown.
    LLM rewrites with emphasis on syntactic correctness.
    """
    print("[Recovery] Strategy: fix_syntax")
    return _llm_rewrite(sql, question, failure)


def _constrain_query(
    sql: str, question: str, failure: FailureType, context: dict
) -> Optional[str]:
    """
    Recovery for timeout.
    Attempts rule-based constraint first (add/tighten date filter, reduce LIMIT).
    Falls back to LLM rewrite if rule-based fails.
    """
    print("[Recovery] Strategy: constrain_query")

    constrained = sql

    # Rule 1: Reduce LIMIT aggressively
    limit_match = re.search(r'LIMIT\s+(\d+)', constrained, re.IGNORECASE)
    if limit_match:
        current_limit = int(limit_match.group(1))
        if current_limit > 10:
            constrained = re.sub(
                r'LIMIT\s+\d+', 'LIMIT 10', constrained, flags=re.IGNORECASE
            )
            print("[Recovery] Reduced LIMIT to 10")

    # Rule 2: Add date filter to order_purchase_timestamp if not present
    if ("order_purchase_timestamp" in constrained.lower()
            and "year" not in constrained.lower()
            and "where" not in constrained.lower()):
        # Inject a WHERE clause before GROUP BY / ORDER BY / LIMIT
        inject_point = re.search(
            r'\b(GROUP BY|ORDER BY|LIMIT)\b', constrained, re.IGNORECASE
        )
        if inject_point:
            pos = inject_point.start()
            constrained = (
                constrained[:pos]
                + "WHERE YEAR(order_purchase_timestamp) = 2018\n"
                + constrained[pos:]
            )
            print("[Recovery] Added year=2018 filter to reduce scan")

    # If we actually changed the SQL, return it
    if constrained != sql:
        return constrained

    # Otherwise fall back to LLM rewrite with timeout context
    return _llm_rewrite(constrained, question, failure)


def _broaden_query(
    sql: str, question: str, failure: FailureType, context: dict
) -> Optional[str]:
    """
    Recovery for no_rows.
    Attempts to remove restrictive filters to return something useful.
    Note: this is a suggestion, not a silent replacement — caller shows it to user.
    """
    print("[Recovery] Strategy: broaden_query")

    broadened = sql

    # Rule: remove HAVING clauses that may be too restrictive
    broadened = re.sub(
        r'\bHAVING\b.+?(?=\b(ORDER BY|LIMIT|$))', '',
        broadened, flags=re.IGNORECASE | re.DOTALL
    ).strip()

    # Rule: relax tight LIMIT
    broadened = re.sub(r'LIMIT\s+\d+', 'LIMIT 50', broadened, flags=re.IGNORECASE)

    if broadened != sql:
        return broadened

    return _llm_rewrite(sql, question, failure)


def _no_recovery(
    sql: str, question: str, failure: FailureType, context: dict
) -> Optional[str]:
    """
    For failure types where automated recovery is not possible
    (e.g. permission_denied, warehouse_unavailable).
    Always returns None — caller handles escalation.
    """
    print(f"[Recovery] Strategy: no_recovery (type={failure.name})")
    return None


# ── Strategy registry ─────────────────────────────────────────────────────────
# Maps recovery_hint (from error_classifier.py) to a handler function.
# To add a new strategy: write a function above and add one line here.

STRATEGY_REGISTRY: dict[str, Callable] = {
    "rewrite_sql_fix_column": _rewrite_sql_fix_column,
    "rewrite_sql_fix_table":  _rewrite_sql_fix_table,
    "rewrite_sql_fix_syntax": _rewrite_sql_fix_syntax,
    "constrain_query":        _constrain_query,
    "broaden_query":          _broaden_query,
    "no_recovery":            _no_recovery,
}


# ── Public dispatcher ─────────────────────────────────────────────────────────

def attempt_recovery(
    sql:      str,
    question: str,
    failure:  FailureType,
    context:  Optional[dict] = None,
) -> RecoveryResult:
    """
    Dispatch to the correct recovery strategy based on failure.recovery_hint.

    Args:
        sql      : the SQL that failed
        question : the original (normalised) user question
        failure  : FailureType from error_classifier.classify()
        context  : optional dict for passing extra state to strategies

    Returns:
        RecoveryResult with success flag and rewritten SQL if successful.
    """
    hint     = failure.recovery_hint
    strategy = STRATEGY_REGISTRY.get(hint, _no_recovery)
    ctx      = context or {}

    print(f"[Recovery] Dispatching: hint={hint}, failure={failure.name}")

    rewritten_sql = strategy(sql, question, failure, ctx)

    if rewritten_sql and rewritten_sql.strip():
        return RecoveryResult(
            success       = True,
            sql           = rewritten_sql,
            strategy_used = hint,
            failure_type  = failure,
            note          = f"Auto-recovered via {hint}",
        )

    return RecoveryResult(
        success       = False,
        sql           = "",
        strategy_used = hint,
        failure_type  = failure,
        note          = f"Recovery strategy {hint} produced no result",
    )
