"""
suggestion_engine.py — Phase 10 (Layer 5: Suggestion Engine)

When InsightBot hits a dead end (unanswerable, exhausted retries, no rows),
instead of returning a plain error it searches ChromaDB for the closest
question it *can* answer and offers that as an alternative.

Design principles:
  - Searches both insightbot_cache (past successful answers) and
    insightbot_rag (schema/business logic) for alternatives
  - Returns a ranked list of SuggestedAlternative objects
  - Caller decides how many to show and how to format them
  - Adding a new search source = one entry in SEARCH_SOURCES registry
  - All searches are similarity-based — no hardcoded Q&A pairs

Dead ends that trigger this engine (wired in handler.py):
  1. _check_unanswerable() fires
  2. Retries exhausted after Databricks failure
  3. Query returned no rows
"""

import os
import re
from dataclasses import dataclass, field
from typing import Optional

import chromadb
from chromadb.utils import embedding_functions

RAG_DIR    = os.path.join(os.path.dirname(__file__), '..', 'rag')
CHROMA_DIR = os.path.join(RAG_DIR, "chroma_db")

_ef = embedding_functions.ONNXMiniLM_L6_V2()

# ── Config ────────────────────────────────────────────────────────────────────

SUGGESTION_CONFIG = {
    "max_suggestions":        3,      # max alternatives to return to user
    "min_similarity":         0.45,   # minimum similarity to be worth suggesting
    "cache_search_top_k":     5,      # how many cache entries to pull per search
    "rag_search_top_k":       5,      # how many RAG chunks to pull per search
    "dedupe_threshold":       0.95,   # similarity above which two suggestions are dupes
}


# ── Result types ──────────────────────────────────────────────────────────────

@dataclass
class SuggestedAlternative:
    """
    A single suggested alternative question.

    Fields:
      question    : the suggested question to ask instead
      answer      : the cached answer (only available from cache source)
      sql         : the SQL that answers it (only available from cache source)
      similarity  : float 0-1, how close to the original question
      source      : "cache" | "rag" | "schema"
      can_run     : True if we have the SQL ready to execute immediately
    """
    question:   str
    similarity: float
    source:     str
    answer:     str  = ""
    sql:        str  = ""
    can_run:    bool = False


@dataclass
class SuggestionResult:
    """
    Full result from the suggestion engine.

    Fields:
      found        : True if at least one suggestion was found
      alternatives : ranked list of SuggestedAlternative (best first)
      dead_end_type: what triggered the suggestion (unanswerable|no_rows|failure)
    """
    found:         bool
    alternatives:  list = field(default_factory=list)
    dead_end_type: str  = ""


# ── ChromaDB collection helpers ───────────────────────────────────────────────

_client = None

def _get_client():
    global _client
    if _client is None:
        _client = chromadb.PersistentClient(path=CHROMA_DIR)
    return _client


def _get_cache_collection():
    return _get_client().get_collection(
        name="insightbot_cache",
        embedding_function=_ef,
    )


def _get_rag_collection():
    return _get_client().get_collection(
        name="insightbot_rag",
        embedding_function=_ef,
    )


# ── Search sources registry ───────────────────────────────────────────────────
# Each source is a function that takes (question, top_k) and returns
# a list of SuggestedAlternative objects.
#
# Adding a new source:
#   1. Write a function _search_your_source(question, top_k) -> list[SuggestedAlternative]
#   2. Add it to SEARCH_SOURCES below.
#   That's it.

def _search_cache(question: str, top_k: int) -> list[SuggestedAlternative]:
    """
    Search insightbot_cache for similar past successful questions.
    These are the best suggestions — we have the answer ready to go.
    """
    results = []
    try:
        collection = _get_cache_collection()
        if collection.count() == 0:
            return []

        res = collection.query(
            query_texts=[question],
            n_results=min(top_k, collection.count()),
            include=["documents", "metadatas", "distances"],
        )

        for doc, meta, dist in zip(
            res["documents"][0],
            res["metadatas"][0],
            res["distances"][0],
        ):
            similarity = round(1 - dist, 4)
            if similarity < SUGGESTION_CONFIG["min_similarity"]:
                continue

            results.append(SuggestedAlternative(
                question   = doc,
                similarity = similarity,
                source     = "cache",
                answer     = meta.get("answer", ""),
                sql        = meta.get("sql", ""),
                can_run    = True,
            ))

    except Exception as e:
        print(f"[SuggestionEngine] Cache search failed: {e}")

    return results


def _search_rag(question: str, top_k: int) -> list[SuggestedAlternative]:
    """
    Search insightbot_rag for relevant schema context.
    Extracts example questions embedded in the RAG docs.
    Used when cache is empty or sparse.
    """
    results = []
    try:
        collection = _get_rag_collection()
        if collection.count() == 0:
            return []

        res = collection.query(
            query_texts=[question],
            n_results=min(top_k, collection.count()),
            include=["documents", "metadatas", "distances"],
        )

        for doc, meta, dist in zip(
            res["documents"][0],
            res["metadatas"][0],
            res["distances"][0],
        ):
            similarity = round(1 - dist, 4)
            if similarity < SUGGESTION_CONFIG["min_similarity"]:
                continue

            # Try to extract an example question from the RAG chunk
            extracted = _extract_question_from_chunk(doc)
            if not extracted:
                continue

            results.append(SuggestedAlternative(
                question   = extracted,
                similarity = similarity,
                source     = "rag",
                answer     = "",
                sql        = "",
                can_run    = False,
            ))

    except Exception as e:
        print(f"[SuggestionEngine] RAG search failed: {e}")

    return results


def _extract_question_from_chunk(chunk: str) -> Optional[str]:
    """
    Extract a usable example question from a RAG chunk.
    Looks for lines starting with Q:, Question:, or ending with ?
    """
    # Pattern 1: explicit Q: prefix
    q_match = re.search(r'(?:^|\n)[Qq](?:uestion)?:\s*(.+)', chunk)
    if q_match:
        return q_match.group(1).strip()[:200]

    # Pattern 2: lines ending with ?
    lines = chunk.split('\n')
    for line in lines:
        line = line.strip()
        if line.endswith('?') and len(line) > 15 and len(line) < 200:
            return line

    # Pattern 3: LEARNED EXAMPLE format (from feedback_engine promotions)
    learned = re.search(r'Question:\s*(.+)', chunk)
    if learned:
        return learned.group(1).strip()[:200]

    return None


# Source registry — add new search sources here
SEARCH_SOURCES = [
    {
        "name":  "cache",
        "fn":    _search_cache,
        "top_k": SUGGESTION_CONFIG["cache_search_top_k"],
    },
    {
        "name":  "rag",
        "fn":    _search_rag,
        "top_k": SUGGESTION_CONFIG["rag_search_top_k"],
    },
]


# ── Deduplication ─────────────────────────────────────────────────────────────

def _dedupe(alternatives: list[SuggestedAlternative]) -> list[SuggestedAlternative]:
    """
    Remove near-duplicate suggestions.
    Two suggestions are dupes if their questions are very similar.
    Uses simple token overlap — no embedding needed.
    """
    seen    = []
    unique  = []
    thresh  = SUGGESTION_CONFIG["dedupe_threshold"]

    for alt in alternatives:
        q_tokens = set(alt.question.lower().split())
        is_dupe  = False

        for seen_tokens in seen:
            if not q_tokens or not seen_tokens:
                continue
            overlap = len(q_tokens & seen_tokens) / max(len(q_tokens), len(seen_tokens))
            if overlap >= thresh:
                is_dupe = True
                break

        if not is_dupe:
            unique.append(alt)
            seen.append(q_tokens)

    return unique


# ── Public API ────────────────────────────────────────────────────────────────

def find_alternatives(
    question:      str,
    dead_end_type: str = "unknown",
    exclude_question: Optional[str] = None,
) -> SuggestionResult:
    """
    Find the best alternative questions to suggest when a dead end is hit.

    Args:
        question         : the original (normalised) user question
        dead_end_type    : "unanswerable" | "no_rows" | "failure" | "unknown"
        exclude_question : skip suggestions identical to this (usually the original)

    Returns:
        SuggestionResult with ranked list of SuggestedAlternative objects.

    Usage:
        result = find_alternatives(normalised, dead_end_type="unanswerable")
        if result.found:
            # show result.alternatives[0] to user
    """
    all_alternatives = []

    # Gather from all registered sources
    for source in SEARCH_SOURCES:
        candidates = source["fn"](question, source["top_k"])
        all_alternatives.extend(candidates)

    # Filter out the original question itself
    if exclude_question:
        exclude_lower = exclude_question.lower().strip()
        all_alternatives = [
            a for a in all_alternatives
            if a.question.lower().strip() != exclude_lower
        ]

    # Sort by similarity descending
    all_alternatives.sort(key=lambda a: a.similarity, reverse=True)

    # Deduplicate
    all_alternatives = _dedupe(all_alternatives)

    # Trim to max
    max_n        = SUGGESTION_CONFIG["max_suggestions"]
    alternatives = all_alternatives[:max_n]

    return SuggestionResult(
        found         = len(alternatives) > 0,
        alternatives  = alternatives,
        dead_end_type = dead_end_type,
    )


def format_for_slack(
    result:   SuggestionResult,
    user_id:  str,
    context:  str = "",
) -> str:
    """
    Format a SuggestionResult as a Slack message string.

    Args:
        result   : SuggestionResult from find_alternatives()
        user_id  : Slack user ID for @mention
        context  : optional sentence explaining why we're suggesting
                   (e.g. "I couldn't answer that directly.")

    Returns:
        Formatted Slack message string ready to post.
    """
    if not result.found:
        return ""

    lines = []

    if context:
        lines.append(context)

    lines.append("💡 *Here's what I can answer instead:*\n")

    for i, alt in enumerate(result.alternatives, 1):
        source_tag = "✅ _ready to run_" if alt.can_run else "📋 _from schema_"
        lines.append(f"*{i}.* _{alt.question}_ {source_tag}")

        # If we have a cached answer, show a preview
        if alt.answer:
            preview = alt.answer[:120]
            if len(alt.answer) > 120:
                preview += "..."
            lines.append(f"   > {preview}")

    lines.append(
        f"\n_Reply with the number (*1*, *2*, or *3*) to run one of these, "
        f"or rephrase your question._"
    )

    return "\n".join(lines)
