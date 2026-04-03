"""
error_classifier.py — Phase 10 (Layer 4: Failure Classification)

Classifies a Databricks error string into a structured FailureType.

Design principles:
  - Registry-based: add a new failure type by adding one entry to FAILURE_REGISTRY.
    No if/elif chains anywhere.
  - Each entry defines: name, patterns to match, user-facing message, recovery hint.
  - Order in FAILURE_REGISTRY matters — first match wins.
  - All matching is case-insensitive against the full error string.

Adding a new failure type:
    1. Add an entry to FAILURE_REGISTRY below.
    2. Add a corresponding recovery strategy in recovery.py with the same name key.
    That's it. No other files need to change.
"""

import re
from dataclasses import dataclass, field
from typing import Optional


# ── Failure type definition ───────────────────────────────────────────────────

@dataclass
class FailureType:
    """
    Structured result of classifying a Databricks error.

    Fields:
      name            : machine-readable key (matches recovery strategy key)
      user_message    : plain English message shown to the Slack user
      recovery_hint   : internal hint passed to the recovery layer
      matched_detail  : extracted detail from the error (column name, table name, etc.)
      raw_error       : original error string for logging
    """
    name:           str
    user_message:   str
    recovery_hint:  str
    matched_detail: Optional[str] = None
    raw_error:      str           = ""


# ── Failure registry ──────────────────────────────────────────────────────────
# Each entry is a dict with:
#   name           : str   — unique key, must match a recovery strategy in recovery.py
#   patterns       : list  — regex patterns (any match → this type fires)
#   detail_pattern : str   — optional regex to extract a useful detail (e.g. column name)
#   user_message   : str   — shown to user in Slack (use {detail} placeholder if needed)
#   recovery_hint  : str   — passed to recovery.py to guide the rewrite strategy
#
# Order matters — first match wins. Put more specific patterns before general ones.

FAILURE_REGISTRY = [
    {
        "name": "column_not_found",
        "patterns": [
            r"column.{0,40}not found",
            r"unresolved column",
            r"cannot resolve.{0,40}column",
            r"no such column",
            r"ambiguous column",
            r"column.{0,40}does not exist",
        ],
        "detail_pattern": r"[`'\"]([a-zA-Z_][a-zA-Z0-9_.]*)[`'\"]",
        "user_message":   "I referenced a column that doesn't exist ({detail}). Trying to fix automatically...",
        "recovery_hint":  "rewrite_sql_fix_column",
    },
    {
        "name": "table_not_found",
        "patterns": [
            r"table.{0,40}not found",
            r"table or view not found",
            r"no such table",
            r"relation.{0,40}does not exist",
            r"object.{0,40}not found",
            r"database.{0,40}not found",
        ],
        "detail_pattern": r"[`'\"]([a-zA-Z_][a-zA-Z0-9_.]*)[`'\"]",
        "user_message":   "I referenced a table or view that doesn't exist ({detail}). Trying to fix automatically...",
        "recovery_hint":  "rewrite_sql_fix_table",
    },
    {
        "name": "syntax_error",
        "patterns": [
            r"syntax error",
            r"parse error",
            r"mismatched input",
            r"extraneous input",
            r"unexpected token",
            r"expecting.*got",
            r"invalid syntax",
        ],
        "detail_pattern": r"near ['\"](.{1,40})['\"]",
        "user_message":   "The generated SQL had a syntax error. Attempting to rewrite...",
        "recovery_hint":  "rewrite_sql_fix_syntax",
    },
    {
        "name": "timeout",
        "patterns": [
            r"query.*timed? ?out",
            r"timeout.*exceeded",
            r"operation.*timed? ?out",
            r"socket.*timeout",
            r"execution.*timeout",
            r"cancelled.*timeout",
        ],
        "detail_pattern": None,
        "user_message":   "The query timed out — it may be too broad. Trying a more constrained version...",
        "recovery_hint":  "constrain_query",
    },
    {
        "name": "permission_denied",
        "patterns": [
            r"permission denied",
            r"access denied",
            r"not authorized",
            r"insufficient privileges",
            r"user does not have",
        ],
        "detail_pattern": r"on ([a-zA-Z_][a-zA-Z0-9_.]*)",
        "user_message":   "I don't have permission to access that data ({detail}). This needs an admin to fix — I can't recover automatically.",
        "recovery_hint":  "no_recovery",
    },
    {
        "name": "no_rows",
        "patterns": [
            r"^no_rows$",   # sentinel raised explicitly by connector, not Databricks
        ],
        "detail_pattern": None,
        "user_message":   "The query ran successfully but returned no data.",
        "recovery_hint":  "broaden_query",
    },
    {
        "name": "warehouse_unavailable",
        "patterns": [
            r"warehouse.*stopped",
            r"warehouse.*not running",
            r"warehouse.*starting",
            r"could not connect",
            r"connection refused",
            r"failed to connect",
            r"warehouse.*unavailable",
        ],
        "detail_pattern": None,
        "user_message":   "The Databricks warehouse is currently unavailable. It may be starting up — please try again in 30 seconds.",
        "recovery_hint":  "no_recovery",
    },
    {
        "name": "unknown",
        "patterns": [
            r".*",   # catch-all — always last
        ],
        "detail_pattern": None,
        "user_message":   "An unexpected error occurred. Attempting to rewrite the query...",
        "recovery_hint":  "rewrite_sql_fix_syntax",
    },
]


# ── Classifier ────────────────────────────────────────────────────────────────

def classify(error: str) -> FailureType:
    """
    Classify a Databricks error string into a FailureType.

    Args:
        error: raw exception message from Databricks or connector

    Returns:
        FailureType with name, user_message, recovery_hint, matched_detail

    Usage:
        failure = classify(str(e))
        if failure.name == "column_not_found":
            ...
    """
    error_lower = error.lower().strip()

    for entry in FAILURE_REGISTRY:
        matched = False
        for pattern in entry["patterns"]:
            if re.search(pattern, error_lower, re.IGNORECASE | re.DOTALL):
                matched = True
                break

        if not matched:
            continue

        # Extract detail if pattern provided
        detail = None
        if entry.get("detail_pattern"):
            detail_match = re.search(
                entry["detail_pattern"], error, re.IGNORECASE
            )
            if detail_match:
                detail = detail_match.group(1)

        # Format user message with detail if present
        user_message = entry["user_message"]
        if "{detail}" in user_message:
            user_message = user_message.format(
                detail=detail if detail else "unknown"
            )

        return FailureType(
            name=entry["name"],
            user_message=user_message,
            recovery_hint=entry["recovery_hint"],
            matched_detail=detail,
            raw_error=error,
        )

    # Should never reach here because "unknown" is the catch-all
    return FailureType(
        name="unknown",
        user_message="An unexpected error occurred.",
        recovery_hint="rewrite_sql_fix_syntax",
        raw_error=error,
    )


def is_recoverable(failure: FailureType) -> bool:
    """Returns True if the recovery layer should attempt a fix."""
    return failure.recovery_hint != "no_recovery"
