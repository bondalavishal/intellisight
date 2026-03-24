# InsightBot 🤖

A Slack-native analytics bot that lets your team ask plain English questions about business data — no SQL knowledge required. InsightBot translates natural language into SQL, runs it against Databricks, and returns a clear, human-readable summary right in Slack.

---

## Features

- **Natural language to SQL** — powered by SQLCoder 8B via Cerebras for fast inference
- **RAG-enhanced context** — ChromaDB vector store provides schema-aware SQL generation
- **Multi-question support** — ask up to 5 questions in one message, numbered or line-separated
- **Live progress updates** — Slack messages update in real time with a visual progress bar
- **Anomaly detection** — automatically flags unusual patterns in results
- **CSV export** — reply `download` after any query to get a `.csv` file of the results
- **Response caching** — repeated questions are answered instantly from cache
- **Guardrails** — SQL is validated and `LIMIT` is enforced before any query hits Databricks
- **Interaction logging** — every query, latency, and result is logged back to Databricks for eval
- **Stats command** — type `stats` to see pass rate, cache hit rate, avg latency, and more
- **Docker-ready** — includes `Dockerfile` and `docker-compose.yml` for easy deployment

---

## Architecture

```
Slack Message
     │
     ▼
Intent Classifier (LLM)
     │
     ├── greeting / out-of-scope → reply and exit
     │
     └── data query
           │
           ▼
     Question Splitter
           │
           ▼
     SQL Generator (SQLCoder 8B + RAG)
           │
           ▼
     SQL Guardrails (validate + enforce LIMIT)
           │
           ▼
     Databricks Execution
           │
           ▼
     Anomaly Detection
           │
           ▼
     LLM Summariser → Slack Reply
           │
           ▼
     Eval Logger (Databricks) + Cache
```

---

## Project Structure

```
insightbot/
├── app/
│   ├── llm/                  # Intent classification
│   ├── sql/                  # SQL generation, guardrails, Databricks connector
│   ├── slack/                # Handler utilities: summarise, anomaly detect, cache, stats
│   └── eval/                 # Interaction logger
├── main.py                   # Entry point — Slack bolt + Flask health server
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── start.sh
├── test_connections.py
├── test_databricks.py
├── test_guardrails.py
├── test_intent.py
├── test_pipeline.py
└── test_sql_generator.py
```

---

## Prerequisites

- Python 3.10+
- A Slack app with **Socket Mode** enabled and the following bot scopes:
  - `chat:write`, `app_mentions:read`, `im:history`, `files:write`
- A Databricks workspace with a running SQL warehouse
- A Cerebras Cloud API key (for SQLCoder 8B inference)
- An Anthropic API key (for summarisation and intent classification)

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/bondalavishal/insightbot.git
cd insightbot
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

```bash
cp .env.example .env
```

Fill in your `.env`:

```env
# Databricks
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_TOKEN=your-databricks-token

# Slack
SLACK_BOT_TOKEN=xoxb-your-bot-token
SLACK_APP_TOKEN=xapp-your-app-token
SLACK_SIGNING_SECRET=your-signing-secret

# Anthropic (for summarisation / intent)
ANTHROPIC_API_KEY=sk-ant-your-key

# App
FLASK_PORT=5000
```

### 4. Run locally

```bash
python main.py
```

Or use the start script:

```bash
bash start.sh
```

### 5. Run with Docker

```bash
docker-compose up --build
```

---

## Usage

Once InsightBot is in your Slack workspace, you can interact with it in any channel it's invited to, or by mentioning it directly.

**Single question:**
```
@InsightBot what were the top 5 sellers by revenue last month?
```

**Multiple questions at once:**
```
@InsightBot
1. How many orders were delivered late this week?
2. Which product category has the lowest return rate?
3. What is the average order value by region?
```

**Download results as CSV:**
```
download
```
(Reply with `download` after any data query to get a `.csv` file.)

**View bot performance stats:**
```
stats
```

---

## Running Tests

Individual test scripts are provided for each module:

```bash
python test_connections.py      # Databricks + Slack connectivity
python test_databricks.py       # Direct Databricks query test
python test_guardrails.py       # SQL validation rules
python test_intent.py           # Intent classifier
python test_sql_generator.py    # SQL generation from natural language
python test_pipeline.py         # Full end-to-end pipeline
```

---

## Health Check

A lightweight Flask server runs alongside the Slack bot and exposes a health endpoint:

```
GET /health  →  200 OK
```

The port defaults to `5000` and is configurable via `FLASK_PORT` in `.env`.

---

## Dependencies

| Package | Purpose |
|---|---|
| `slack-bolt` | Slack app framework |
| `anthropic` | LLM for intent classification and summarisation |
| `cerebras-cloud-sdk` | Fast inference for SQLCoder 8B |
| `databricks-sql-connector` | Query execution on Databricks |
| `chromadb` | Vector store for RAG-based SQL generation |
| `flask` | Health check HTTP server |
| `pandas` | Result processing and CSV generation |
| `python-dotenv` | Environment variable management |

---

## Contributing

Pull requests are welcome. For significant changes, please open an issue first to discuss what you'd like to change.

---

## License

MIT
