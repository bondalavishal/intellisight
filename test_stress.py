"""
InsightBot Stress Test — 34 questions across 8 groups.

Each group targets a specific layer:
  Group 1 — Spell correction + cache (Layer 1)
  Group 2 — Abbreviation expansion (Layer 1)
  Group 3 — Similarity band suggestions (Layer 2)
  Group 4 — Failure classification + recovery (Layer 4)
  Group 5 — Dead ends + suggestion engine (Layer 5)
  Group 6 — Multi-question (concurrent pipeline)
  Group 7 — Edge cases / guardrails
  Group 8 — Self-learning validation (Layer 3)

Run:
  cd /Users/vishal.bondala/insightbot
  source venv/bin/activate
  python test_stress.py
"""

import time
import textwrap
from dotenv import load_dotenv

load_dotenv()

from app.slack.handler import handle_question  # noqa: E402

# ── Colour helpers ─────────────────────────────────────────────────────────────
RESET  = "\033[0m"
BOLD   = "\033[1m"
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
GREY   = "\033[90m"
BLUE   = "\033[94m"
MAGENTA= "\033[95m"

WIDTH = 90

# ── Outcome detection ──────────────────────────────────────────────────────────
def detect_outcome(reply: str, results: list, pending_data) -> tuple[str, str]:
    """Return (label, colour) describing what the pipeline did."""
    r = reply.lower()

    if any(k in r for k in ["blocked", "not allowed", "cannot run", "dangerous"]):
        return "BLOCKED", RED
    if any(k in r for k in ["greeting", "hello", "hi there", "how can i help"]):
        return "GREETING", CYAN
    if any(k in r for k in ["out of scope", "weather", "not a data question",
                              "can't answer", "cannot answer that"]):
        return "OUT-OF-SCOPE", YELLOW
    if any(k in r for k in ["no rows", "no results", "couldn't find", "no data",
                              "0 rows", "returned no rows"]):
        return "NO-ROWS", YELLOW
    if any(k in r for k in ["similar question", "did you mean", "i found something close",
                              "here are some alternatives", "maybe try"]):
        return "SUGGESTION", MAGENTA
    if any(k in r for k in ["tried", "attempts", "could not generate",
                              "wasn't able", "unable to", "failed after"]):
        return "EXHAUSTED", RED
    if pending_data and isinstance(pending_data, dict):
        t = pending_data.get("type", "")
        if t == "direct_hit_meta":
            return "CACHE-HIT (direct ≥0.92)", GREEN
        if t == "cache_hit_meta":
            return "CACHE-HIT (suggestion 0.75–0.92)", GREEN
        if t == "alternatives":
            return "ALTERNATIVES", MAGENTA
    if results:
        return f"SUCCESS ({len(results)} rows)", GREEN
    # Reply exists but no structured pending — likely a direct cache hit returning text only
    if reply and not results:
        return "CACHE-HIT (text reply)", GREEN
    return "UNKNOWN", GREY


def _wrap(text: str, indent: int = 4) -> str:
    prefix = " " * indent
    return "\n".join(
        textwrap.fill(line, width=WIDTH - indent, initial_indent=prefix,
                      subsequent_indent=prefix)
        for line in text.splitlines()
    )


# ── Single-question runner ─────────────────────────────────────────────────────
RESULTS_STORE: list[dict] = []

def run_question(q: str, user_id: str = "STRESS_TEST", delay: float = 0.5) -> None:
    print(f"\n{GREY}  Q: {RESET}{BOLD}{q}{RESET}")
    t0 = time.time()
    try:
        reply, results, csv_string, pending_data = handle_question(user_id, q)
        elapsed = time.time() - t0
        label, colour = detect_outcome(reply, results, pending_data)
        print(f"  {colour}[{label}]{RESET}  {GREY}{elapsed:.1f}s{RESET}")
        # Print first 3 lines of reply, indented
        short_reply = "\n".join(reply.splitlines()[:5])
        print(_wrap(short_reply, indent=4))
        if results:
            print(f"    {GREY}→ {len(results)} row(s), first: {dict(list(results[0].items())[:3])}{RESET}")
        RESULTS_STORE.append({
            "question": q, "label": label, "elapsed": elapsed,
            "rows": len(results) if results else 0, "ok": colour in (GREEN, CYAN),
        })
    except Exception as exc:
        elapsed = time.time() - t0
        print(f"  {RED}[EXCEPTION]{RESET}  {GREY}{elapsed:.1f}s{RESET}")
        print(_wrap(str(exc), indent=4))
        RESULTS_STORE.append({
            "question": q, "label": "EXCEPTION", "elapsed": elapsed, "rows": 0, "ok": False,
        })
    if delay:
        time.sleep(delay)


# ── Group header printer ───────────────────────────────────────────────────────
def group_header(num: int, title: str, target_signal: str) -> None:
    bar = "─" * WIDTH
    print(f"\n{CYAN}{bar}{RESET}")
    print(f"{CYAN}{BOLD}  GROUP {num} — {title}{RESET}")
    print(f"  {GREY}Watch for: {target_signal}{RESET}")
    print(f"{CYAN}{bar}{RESET}")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{BOLD}{'═'*WIDTH}{RESET}")
print(f"{BOLD}  INSIGHTBOT STRESS TEST — 34 questions across 8 groups{RESET}")
print(f"{BOLD}{'═'*WIDTH}{RESET}")

# ── Group 1: Spell correction + cache ────────────────────────────────────────
group_header(1, "Spell correction + cache warm-up", "[Normaliser] firing, [Cache] DIRECT HIT")

# Run 1 clean to seed the cache
run_question("which sellers had revenue above the overall average seller revenue")
# Typo variants — should all hit cache after normalisation
run_question("which sellrs had revnue above the overal averge seller revnue")
run_question("which sellers had reveue above the overall avg seller revenue")
run_question("wich sellerss had revenue abve the overal average seller revnue")

# ── Group 2: Abbreviation expansion ──────────────────────────────────────────
group_header(2, "Abbreviation expansion", "[Normaliser] expanding abbrevs, [Cache] DIRECT HIT")

run_question("top 5 prod cats by avg freight cost per ord")
run_question("avg rev by stt for top sellrs")
run_question("monthly rev trend by yr")
run_question("avg ord val by cat for ords above r$500")

# ── Group 3: Similarity band suggestions (0.75–0.92) ─────────────────────────
group_header(3, "Similarity band suggestions", "[Cache] SUGGESTION + similarity % in reply")

run_question("which sellers earn the most money")
run_question("show me highest grossing sellers")
run_question("what product categories bring in the most money")
run_question("which categories sell the best overall")

# ── Group 4: Failure classification + recovery ───────────────────────────────
group_header(4, "Failure classification + recovery", "[Recovery] lines, retry attempt counts")

run_question("show me average delivery time by state")
run_question("which states have delivery time above 20 days")
run_question("sellers with review score below 3")
run_question("monthly cancellation rate where rate exceeds 5 percent")
run_question("top 5 product categories by freight cost")
run_question("sellers who sell across more than 3 categories")

# ── Group 5: Dead ends + suggestion engine ────────────────────────────────────
group_header(5, "Dead ends + suggestion engine", "[SuggestionEngine] firing, alternatives in reply")

run_question("show me revenue by seller over time broken down by month")
run_question("which sellers improved their review score in the last 3 months")
run_question("cancellation breakdown by product category for orders under R$50")
run_question("what is the net promoter score by region")

# ── Group 6: Multi-question (concurrent pipeline) ────────────────────────────
group_header(6, "Multi-question (5 concurrent)", "Progress bar cycling, all 5 completing")

# Send as a single batched string — same format main.py splits on newlines / numbered lists
multi_q = (
    "1. which product categories have the highest average freight cost per order\n"
    "2. average delivery days by state\n"
    "3. monthly revenue trend for 2018\n"
    "4. sellers with lowest average review score\n"
    "5. top 10 products by total orders placed"
)
print(f"\n{GREY}  MULTI-Q block sent as one message:{RESET}")
for line in multi_q.splitlines():
    print(f"    {line}")
run_question(multi_q, delay=1.0)

# ── Group 7: Edge cases / guardrails ─────────────────────────────────────────
group_header(7, "Edge cases / guardrails",
             "[Guardrails] blocked, intent=greeting, intent=out_of_scope")

run_question("DELETE FROM olist_orders")
run_question("UPDATE seller revenue to zero for all sellers")
run_question("what is the weather today")
run_question("hello")
run_question("show me revenue for products that dont exist in the database")
run_question("giv me averge delivry tym 4 evry stte")

# ── Group 8: Self-learning validation ────────────────────────────────────────
group_header(8, "Self-learning validation",
             "[FeedbackEngine] Promoted to RAG, /stats showing learning metrics")

run_question("insightbot stats")
# Re-run of Group 2 Q1 — should now be a direct cache hit
run_question("top 5 prod cats by avg freight cost per ord")

# ══════════════════════════════════════════════════════════════════════════════
#  SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{BOLD}{'═'*WIDTH}{RESET}")
print(f"{BOLD}  SUMMARY{RESET}")
print(f"{BOLD}{'═'*WIDTH}{RESET}\n")

total   = len(RESULTS_STORE)
ok      = sum(1 for r in RESULTS_STORE if r["ok"])
fail    = total - ok
avg_lat = sum(r["elapsed"] for r in RESULTS_STORE) / total if total else 0
p95_lat = sorted(r["elapsed"] for r in RESULTS_STORE)[int(total * 0.95)] if total > 1 else 0

print(f"  Questions run : {total}")
print(f"  {GREEN}Passed{RESET}        : {ok}")
print(f"  {RED}Failed{RESET}        : {fail}")
print(f"  Avg latency  : {avg_lat:.1f}s")
print(f"  P95 latency  : {p95_lat:.1f}s")

# Per-label breakdown
from collections import Counter
label_counts = Counter(r["label"] for r in RESULTS_STORE)
print(f"\n  Outcome breakdown:")
for label, count in label_counts.most_common():
    bar = "█" * count
    print(f"    {label:<35} {count:>3}  {GREY}{bar}{RESET}")

# Flag failures
failures = [r for r in RESULTS_STORE if not r["ok"]]
if failures:
    print(f"\n  {RED}Questions that did not pass:{RESET}")
    for r in failures:
        print(f"    [{r['label']}] {r['question'][:80]}")

print(f"\n{BOLD}{'═'*WIDTH}{RESET}\n")
