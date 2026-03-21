from app.llm.sql_generator import generate_sql

questions = [
    "What was total revenue last month?",
    "Which state has the most orders?",
    "Who are the top 5 sellers by revenue?",
    "Show me products with avg review score above 4",
    "What is the average delivery days by state?",
]

for q in questions:
    print(f"\nQ: {q}")
    print(f"SQL: {generate_sql(q)}")
