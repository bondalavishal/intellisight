import re

GREETING_PATTERNS = [
    r'\bhello\b', r'\bhi\b', r'\bhey\b', r'\bthanks\b', r'\bthank you\b',
    r'\bhelp\b', r'\bwho are you\b', r'\bwhat can you do\b'
]

# Checked BEFORE SQL_PATTERNS — wins even when a SQL keyword also matches.
OUT_OF_SCOPE_PATTERNS = [
    r'\bweather\b', r'\bforecast\b', r'\bhoroscope\b',
    r'\brecipe\b', r'\bcooking\b',
    r'\bsports?\b', r'\bfootball\b', r'\bbasketball\b', r'\bsoccer\b', r'\bcricket\b',
    r'\bpolitics?\b', r'\bpolitician\b', r'\bpresident\b', r'\bprime minister\b',
    r'\bstock price\b', r'\bshare price\b', r'\bcryptocurren\b',
    r'\bnews\b', r'\bjoke\b', r'\btell me a\b',
    r'\btranslat\b', r'\bwhat time is it\b', r'\bwhat day is\b',
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

    for pattern in OUT_OF_SCOPE_PATTERNS:
        if re.search(pattern, q):
            return "out_of_scope"

    for pattern in SQL_PATTERNS:
        if re.search(pattern, q):
            return "text_to_sql"

    return "text_to_sql"  # default — attempt SQL for anything unclear
