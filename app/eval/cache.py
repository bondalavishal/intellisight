"""
Phase 6 — Semantic Cache (updated Phase 10: similarity band)
Uses ChromaDB (separate collection from RAG) + all-MiniLM-L6-v2.
Stores question+answer pairs.

Similarity band (replaces hard cutoff):
  >= DIRECT_HIT_THRESHOLD  -> direct cache hit, return answer immediately
  >= SUGGEST_THRESHOLD     -> suggest cached answer, ask user to confirm
  <  SUGGEST_THRESHOLD     -> cache miss, proceed to LLM generation

Questions are normalised before lookup (see app/utils/normaliser.py).
No expiry — dataset is static (Olist 2016-2018).
"""

import os
import time
import chromadb
from chromadb.utils import embedding_functions

RAG_DIR    = os.path.join(os.path.dirname(__file__), '..', 'rag')
CHROMA_DIR = os.path.join(RAG_DIR, "chroma_db")

# Similarity band thresholds
DIRECT_HIT_THRESHOLD = 0.92   # direct cache hit — return answer immediately
SUGGEST_THRESHOLD    = 0.75   # suggest cached answer — ask user to confirm

# Backwards compatibility alias
SIMILARITY_THRESHOLD = DIRECT_HIT_THRESHOLD

_ef = embedding_functions.ONNXMiniLM_L6_V2()

_client     = None
_collection = None


def _get_collection():
    global _client, _collection
    # Always create a fresh client — avoids stale UUID after collection deletion
    _client = chromadb.PersistentClient(path=CHROMA_DIR)
    try:
        _collection = _client.get_collection(
            name="insightbot_cache",
            embedding_function=_ef,
        )
    except Exception:
        _collection = _client.create_collection(
            name="insightbot_cache",
            embedding_function=_ef,
            metadata={"hnsw:space": "cosine"},
        )
    return _collection


def get_cached(question: str) -> dict | None:
    """
    Check if a semantically similar question exists in the cache.

    Returns dict with:
      - answer           : str
      - sql              : str
      - similarity       : float
      - match_type       : "direct_hit" | "suggestion"
      - matched_question : str  (the original cached question)

    Returns None on cache miss (similarity < SUGGEST_THRESHOLD).

    Caller checks match_type:
      "direct_hit"  -> return answer immediately, no confirmation needed
      "suggestion"  -> surface answer with confirmation prompt to user
    """
    try:
        collection = _get_collection()
        if collection.count() == 0:
            return None

        results = collection.query(
            query_texts=[question],
            n_results=1,
            include=["documents", "metadatas", "distances"],
        )

        if not results["documents"][0]:
            return None

        distance         = results["distances"][0][0]
        similarity       = round(1 - distance, 4)
        meta             = results["metadatas"][0][0]
        matched_question = results["documents"][0][0]

        if similarity >= DIRECT_HIT_THRESHOLD:
            print(f"[Cache] DIRECT HIT (similarity={similarity}) for: {question[:60]}...")
            return {
                "answer":           meta.get("answer", ""),
                "sql":              meta.get("sql", ""),
                "similarity":       similarity,
                "match_type":       "direct_hit",
                "matched_question": matched_question,
            }

        if similarity >= SUGGEST_THRESHOLD:
            print(f"[Cache] SUGGESTION (similarity={similarity}) for: {question[:60]}...")
            return {
                "answer":           meta.get("answer", ""),
                "sql":              meta.get("sql", ""),
                "similarity":       similarity,
                "match_type":       "suggestion",
                "matched_question": matched_question,
            }

        print(f"[Cache] MISS (similarity={similarity}) for: {question[:60]}...")
        return None

    except Exception as e:
        print(f"[Cache] Error during lookup: {e}")
        return None


def save_to_cache(question: str, answer: str, sql: str) -> None:
    """
    Save a question+answer pair to the cache.
    Uses hash of normalised question as ID — upsert behaviour.
    """
    try:
        collection = _get_collection()
        cache_id = f"cache_{abs(hash(question.lower().strip()))}"

        # Delete existing entry for this question if present (upsert behaviour)
        try:
            collection.delete(ids=[cache_id])
        except Exception:
            pass

        collection.add(
            ids       = [cache_id],
            documents = [question],
            metadatas = [{"answer": answer, "sql": sql, "cached_at": str(time.time())}],
        )
        print(f"[Cache] Saved: {question[:60]}...")

    except Exception as e:
        print(f"[Cache] Error during save: {e}")


def promote_suggestion(question: str, answer: str, sql: str) -> None:
    """
    Called when a user confirms a suggestion was correct.
    Re-saves with the user's exact question form so future direct hits
    fire more reliably.
    """
    save_to_cache(question, answer, sql)
    print(f"[Cache] Promoted suggestion to direct cache: {question[:60]}...")


def cache_stats() -> dict:
    """Return cache size and collection info."""
    try:
        collection = _get_collection()
        return {"total_cached": collection.count()}
    except Exception:
        return {"total_cached": 0}
