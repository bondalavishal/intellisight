from app.llm.intent import classify_intent

questions = [
    "What was total revenue last month?",
    "Which state has the most orders?",
    "Hey, what's up?",
    "Who are the top 5 sellers by revenue?",
    "What's the weather in Mumbai?",
    "Show me products with avg review score above 4",
    "Thanks!",
]

for q in questions:
    intent = classify_intent(q)
    print(f"[{intent}] {q}")
