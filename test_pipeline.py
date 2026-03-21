from app.slack.handler import handle_question

questions = [
    ("U001", "Hey, how are you?"),
    ("U001", "What's the weather?"),
    ("U001", "Who are the top 5 sellers by revenue?"),
    ("U001", "Which state has the most orders?"),
    ("U001", "What is the average delivery days by state?"),
]

for user_id, q in questions:
    print(f"\n{'='*50}")
    print(f"Q: {q}")
    print(f"A: {handle_question(user_id, q)}")
