import re

GREETING_PATTERNS = [
    r'\bhello\b', r'\bhi\b', r'\bhey\b', r'\bthanks\b', r'\bthank you\b',
    r'\bhelp\b', r'\bwho are you\b', r'\bwhat can you do\b'
]

SQL_PATTERNS = [
    r'\bhow many\b', r'\bshow me\b', r'\bwhat is\b', r'\bwhat are\b',
    r'\bwhich\b', r'\btop\b', r'\blist\b', r'\bcount\b', r'\btotal\b',
    r'\baverage\b', r'\bavg\b', r'\brevenue\b', r'\borders\b', r'\bsellers\b',
    r'\bproducts\b', r'\bcategories\b', r'\bstates\b', r'\bdelivery\b',
    r'\bmonth\b', r'\byear\b', r'\bcompare\b', r'\bhighest\b', r'\blowest\b',
    r'\bmost\b', r'\bleast\b', r'\btrend\b', r'\brate\b', r'\bscore\b',
    r'\bpercentage\b', r'\blatency\b', r'\branking\b', r'\bbest\b', r'\bworst\b'
]


def classify_intent(question: str) -> str:
    q = question.lower().strip()

    for pattern in GREETING_PATTERNS:
        if re.search(pattern, q):
            return "greeting"

    for pattern in SQL_PATTERNS:
        if re.search(pattern, q):
            return "text_to_sql"

    return "text_to_sql"  # default — attempt SQL for anything unclear
