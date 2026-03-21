import httpx

OLLAMA_URL = "http://127.0.0.1:11434/api/generate"
MODEL = "mannix/defog-llama3-sqlcoder-8b"


def classify_intent(question: str) -> str:
    prompt = (
        f"Classify this as text_to_sql, greeting, or out_of_scope: {question}\nLabel:"
    )
    response = httpx.post(
        OLLAMA_URL,
        json={
            "model": MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0, "num_predict": 10}
        },
        timeout=30
    )
    result = response.json()["response"].strip().lower()

    if "text_to_sql" in result:
        return "text_to_sql"
    elif "greeting" in result:
        return "greeting"
    else:
        return "out_of_scope"
