import os
from dotenv import load_dotenv

load_dotenv()

# Test 1 — Claude API
print("Testing Claude API...")
import anthropic
client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
response = client.messages.create(
    model="claude-sonnet-4-20250514",
    max_tokens=50,
    messages=[{"role": "user", "content": "Say hello in 5 words"}]
)
print(f"Claude: {response.content[0].text}")

# Test 2 — Databricks
print("\nTesting Databricks connection...")
from databricks import sql
conn = sql.connect(
    server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN")
)
cursor = conn.cursor()
cursor.execute("SELECT 1 AS test")
result = cursor.fetchone()
print(f"Databricks: Connected successfully — {result}")
cursor.close()
conn.close()

# Test 3 — ChromaDB
print("\nTesting ChromaDB...")
import chromadb
chroma_client = chromadb.Client()
collection = chroma_client.create_collection("test")
print("ChromaDB: Running successfully")

# Test 4 — Slack SDK
print("\nTesting Slack SDK...")
from slack_sdk import WebClient
slack_client = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))
auth = slack_client.auth_test()
print(f"Slack: Connected as bot ID {auth['bot_id']}")

print("\n✅ All connections verified. Phase 0 complete.")
