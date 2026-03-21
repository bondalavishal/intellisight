"""
Phase 4 — RAG loader
Run ONCE to embed the 3 RAG docs into ChromaDB.

Usage:
    cd /Users/vishalbondala/Applications/insightbot
    source venv/bin/activate
    python -m app.rag.loader
"""

import os
import chromadb
from chromadb.utils import embedding_functions

RAG_DIR   = os.path.dirname(__file__)
CHROMA_DIR = os.path.join(RAG_DIR, "chroma_db")

DOCS = {
    "schema":         os.path.join(RAG_DIR, "schema_definitions.md"),
    "metrics":        os.path.join(RAG_DIR, "metric_definitions.md"),
    "business_logic": os.path.join(RAG_DIR, "business_logic.md"),
}


def chunk_markdown(text: str, source: str) -> list[dict]:
    """Split on ## headings. Each heading + its content = one chunk."""
    chunks = []
    current_heading = "intro"
    current_lines = []

    for line in text.splitlines():
        if line.startswith("## "):
            if current_lines:
                content = "\n".join(current_lines).strip()
                if content:
                    chunks.append({
                        "heading": current_heading,
                        "content": content,
                        "source":  source,
                    })
            current_heading = line.lstrip("# ").strip()
            current_lines = []
        else:
            current_lines.append(line)

    if current_lines:
        content = "\n".join(current_lines).strip()
        if content:
            chunks.append({
                "heading": current_heading,
                "content": content,
                "source":  source,
            })

    return chunks


def load():
    print("Phase 4 RAG loader starting...")

    ef = embedding_functions.ONNXMiniLM_L6_V2()

    client = chromadb.PersistentClient(path=CHROMA_DIR)

    try:
        client.delete_collection("insightbot_rag")
        print("  Deleted existing collection.")
    except Exception:
        pass

    collection = client.create_collection(
        name="insightbot_rag",
        embedding_function=ef,
        metadata={"hnsw:space": "cosine"},
    )

    all_chunks = []
    for source, path in DOCS.items():
        with open(path, "r") as f:
            text = f.read()
        chunks = chunk_markdown(text, source)
        all_chunks.extend(chunks)
        print(f"  {source}: {len(chunks)} chunks")

    collection.add(
        ids       = [f"{c['source']}_{i}" for i, c in enumerate(all_chunks)],
        documents = [f"{c['heading']}\n\n{c['content']}" for c in all_chunks],
        metadatas = [{"source": c["source"], "heading": c["heading"]} for c in all_chunks],
    )

    print(f"\n✅ Loaded {len(all_chunks)} chunks into {CHROMA_DIR}")
    print("Run this once. Re-run only if RAG docs change.")


if __name__ == "__main__":
    load()
