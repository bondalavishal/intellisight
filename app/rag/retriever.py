"""
Phase 4 — RAG retriever
Called by sql_generator.py on every question.
"""

import os
import chromadb
from chromadb.utils import embedding_functions

RAG_DIR    = os.path.dirname(__file__)
CHROMA_DIR = os.path.join(RAG_DIR, "chroma_db")

_ef = embedding_functions.ONNXMiniLM_L6_V2()

_client     = None
_collection = None


def _get_collection():
    global _client, _collection
    if _collection is None:
        _client = chromadb.PersistentClient(path=CHROMA_DIR)
        _collection = _client.get_collection(
            name="insightbot_rag",
            embedding_function=_ef,
        )
    return _collection


def retrieve(question: str, top_k: int = 8) -> str:
    """
    Returns the top_k most relevant RAG chunks as a formatted string
    ready to inject into the SQL prompt.
    Falls back to minimal hardcoded schema if ChromaDB is unavailable.
    """
    try:
        collection = _get_collection()
        results = collection.query(
            query_texts=[question],
            n_results=top_k,
            include=["documents", "metadatas", "distances"],
        )

        docs      = results["documents"][0]
        metadatas = results["metadatas"][0]
        distances = results["distances"][0]

        chunks = []
        for doc, meta, dist in zip(docs, metadatas, distances):
            relevance = round(1 - dist, 3)
            source    = meta.get("source", "unknown")
            chunks.append(f"[{source} | relevance: {relevance}]\n{doc}")

        context = "\n\n---\n\n".join(chunks)
        print(f"[RAG] {len(chunks)} chunks retrieved for: {question[:60]}...")
        return context

    except Exception as e:
        print(f"[RAG] ChromaDB unavailable ({e}), using fallback schema.")
        return _FALLBACK_SCHEMA


_FALLBACK_SCHEMA = """
CREATE VIEW vw_monthly_revenue AS SELECT
    year INT, month INT, year_month STRING,
    total_orders INT, total_revenue DECIMAL,
    avg_order_value DECIMAL, unique_customers INT, canceled_orders INT FROM ...;

CREATE VIEW vw_orders_metrics AS SELECT
    order_id STRING, customer_id STRING, order_status STRING,
    order_date DATE, order_year INT, order_month INT,
    customer_city STRING, customer_state STRING,
    order_revenue DECIMAL, order_freight DECIMAL, order_total DECIMAL,
    item_count INT, delivery_days INT FROM ...;

CREATE VIEW vw_product_metrics AS SELECT
    product_id STRING, category STRING, product_weight_g INT,
    total_orders INT, total_revenue DECIMAL,
    avg_price DECIMAL, avg_review_score DECIMAL FROM ...;

CREATE VIEW vw_seller_metrics AS SELECT
    seller_id STRING, seller_city STRING, seller_state STRING,
    total_orders INT, total_revenue DECIMAL, avg_order_value DECIMAL,
    unique_products INT, avg_review_score DECIMAL, total_reviews INT FROM ...;

RULES: Use views by default. Use raw tables only when views cannot answer. NEVER join views to each other. NEVER invent columns. Always LIMIT.
"""
