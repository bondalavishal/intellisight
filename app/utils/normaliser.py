"""
normaliser.py -- Layer 1: Input Processing
Phase 10 addition -- Self-Learning Option B

Pipeline (runs before cache lookup and LLM call):
  1. Lowercase + strip
  2. Abbreviation expansion
  3. Punctuation normalisation
  4. Spell correction (symspellpy)
  5. Returns canonical clean question
"""

import re
import os

# -- Abbreviation map ---------------------------------------------------------
# Expand common business/analytics shorthand before embedding
ABBREVIATIONS = {
    r'\brev\b':       'revenue',
    r'\brevs\b':      'revenue',
    r'\bqty\b':       'quantity',
    r'\bqtys\b':      'quantities',
    r'\bord\b':       'order',
    r'\bords\b':      'orders',
    r'\bval\b':       'value',
    r'\bvals\b':      'values',
    r'\bcat\b':       'category',
    r'\bcats\b':      'categories',
    r'\bprod\b':      'product',
    r'\bprods\b':     'products',
    # NOTE: 'sell' removed — it is a real English verb, expanding it causes
    # "which categories sell the best" -> "which categories seller the best".
    # 'sells' kept as plural noun form is unambiguous in analytics context.
    r'\bsells\b':     'sellers',
    r'\bavg\b':       'average',
    r'\bpct\b':       'percent',
    r'\bperc\b':      'percent',
    r'\bno\.\b':      'number',
    r'\bvol\b':       'volume',
    r'\bmon\b':       'month',
    r'\bmos\b':       'months',
    r'\byr\b':        'year',
    r'\byrs\b':       'years',
    r'\bdeliv\b':     'delivery',
    r'\bcanc\b':      'cancellation',
    # NOTE: 'stat' and 'stats' removed — 'stats' means statistics in most contexts
    # (e.g. "insightbot stats" -> "insightbot states" was wrong).
    # 'stt' and 'stts' kept below as unambiguous state abbreviations.
    r'\btop5\b':      'top 5',
    r'\btop10\b':     'top 10',
    r'\bw/\b':        'with',
    r'\bw/o\b':       'without',
    r'\bstt\b':        'state',
    r'\bstts\b':       'states',
    r'\btym\b':        'time',
    r'\btyms\b':       'times',
    r'\bscr\b':        'score',
    r'\bscre\b':       'score',
    r'\bscrs\b':       'scores',
    r'\bfrght\b':      'freight',
    r'\bpmt\b':        'payment',
    r'\bpmts\b':       'payments',
    r'\binstl\b':      'installment',
    r'\binstls\b':     'installments',
    r'\bcust\b':       'customer',
    r'\bcusts\b':      'customers',
    r'\brgn\b':        'region',
    r'\brgns\b':       'regions',
    r'\brev\b':        'revenue',
    # Digit-as-word shortcuts (handle before symspell to avoid mis-correction)
    r'\b4\b':          'for',
    r'\b2\b':          'to',
    r'\bw8\b':         'wait',
    # Common contractions symspell gets wrong
    r'\bdont\b':       "don't",
    r'\bcant\b':       "can't",
    r'\bwont\b':       "won't",
    r'\bisnt\b':       "isn't",
    r'\barent\b':      "aren't",
    r'\bdoesnt\b':     "doesn't",
    # Common misspellings symspell gets wrong in analytics context
    r'\bstte\b':       'state',
    r'\bstts\b':       'states',
    r'\brvnue\b':      'revenue',
    r'\brevnue\b':     'revenue',
    r'\bdelivry\b':    'delivery',
    r'\bdeliv\b':      'delivery',
    r'\bavearge\b':    'average',
    r'\baverge\b':     'average',
    r'\bevry\b':       'every',
    r'\bgiv\b':        'give',
    r'\bwhr\b':        'where',
    r'\bwhre\b':       'where',
    # Common seller typos symspell mangles
    r'\bsellerrs\b':   'sellers',
    r'\bsellerss\b':   'sellers',
    r'\bsellres\b':    'sellers',
    r'\bsellrs\b':     'sellers',
    # Common double-letter / dropped-letter typos symspell handles,
    # but listed explicitly as safety net
    r'\btotl\b':       'total',
    r'\btotall\b':     'total',
    r'\btoal\b':       'total',
    r'\bprocuct\b':    'product',
    r'\bproduct\b':    'product',
    r'\bmonlthy\b':    'monthly',
    r'\bmonthy\b':     'monthly',
}

# -- Runtime abbreviation registration ----------------------------------------

def register_abbreviation(token: str, expansion: str) -> None:
    """
    Register a new abbreviation at runtime.
    Safe to call from background threads.
    Skips if already registered.
    """
    pattern = r'\b' + re.escape(token) + r'\b'
    if pattern not in ABBREVIATIONS:
        ABBREVIATIONS[pattern] = expansion
        print(f"[Normaliser] Registered new abbreviation: '{token}' -> '{expansion}'")


def get_abbreviation_count() -> int:
    """Return number of registered abbreviations (base + learned)."""
    return len(ABBREVIATIONS)


# -- Spell correction setup --------------------------------------------------
# Initialise eagerly at import time -- avoids multiple module instance issues

def _build_spell():
    """Build and return a ready SymSpell instance, or None if unavailable."""
    try:
        from symspellpy import SymSpell
        import symspellpy as _sp_module
        sym       = SymSpell(max_dictionary_edit_distance=2, prefix_length=7)
        pkg_dir   = os.path.dirname(_sp_module.__file__)
        dict_path = os.path.join(pkg_dir, "frequency_dictionary_en_82_765.txt")
        if not os.path.exists(dict_path):
            print(f"[Normaliser] Dict not found at {dict_path} -- disabled")
            return None
        loaded = sym.load_dictionary(dict_path, term_index=0, count_index=1)
        if loaded:
            print("[Normaliser] Spell corrector ready (symspellpy)")
            return sym
        print("[Normaliser] Dict load failed -- spell correction disabled")
        return None
    except Exception as e:
        print(f"[Normaliser] Spell corrector init failed: {type(e).__name__}: {e}")
        return None

_sym_spell   = _build_spell()
_spell_ready = _sym_spell is not None

def _init_spell():
    """No-op -- kept for compatibility. Spell corrector loads at import time."""
    pass

PROTECTED_TERMS = {
    'olist', 'databricks', 'insightbot', 'vw', 'cte',
    'sql', 'csv', 'sku', 'sao', 'paulo',
    # Abbreviations handled explicitly above — protect from symspell mangling
    'dont', 'cant', 'wont', 'isnt', 'arent', 'doesnt',
    'stte', 'stts', 'rvnue', 'revnue', 'delivry', 'averge', 'avearge',
    'evry', 'giv', 'whr', 'whre',
    'sellrs', 'sellres', 'sellerrs', 'sellerss',
}


def _correct_spelling(text: str) -> str:
    """Correct spelling word by word. Skips numbers, protected terms, short words."""
    if not _spell_ready or _sym_spell is None:
        return text

    try:
        from symspellpy import Verbosity
        words = text.split()
        corrected = []

        for word in words:
            if (word.isdigit()
                    or len(word) <= 2
                    or word.lower() in PROTECTED_TERMS
                    or not word.isalpha()):
                corrected.append(word)
                continue

            suggestions = _sym_spell.lookup(
                word,
                Verbosity.CLOSEST,
                max_edit_distance=2,
                include_unknown=True,
            )
            if suggestions:
                corrected.append(suggestions[0].term)
            else:
                corrected.append(word)

        return ' '.join(corrected)

    except Exception as e:
        print(f"[Normaliser] Spell correction error ({e}) -- returning original")
        return text


def _expand_abbreviations(text: str) -> str:
    """Replace known abbreviations with full words."""
    for pattern, replacement in ABBREVIATIONS.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    return text


def _normalise_punctuation(text: str) -> str:
    """Normalise punctuation and whitespace."""
    text = re.sub(r'\s+', ' ', text)
    # Strip Slack code-block/inline-code backticks (single or triple)
    text = re.sub(r'`+', '', text)
    text = re.sub(r'^[^\w]+', '', text)
    # Remove curly/smart quotes using unicode escapes (ASCII-safe)
    text = text.replace('\u201c', '').replace('\u201d', '').replace('\u2018', '').replace('\u2019', "'")
    return text.strip()


# -- Public API ---------------------------------------------------------------

def normalise(question: str) -> str:
    """
    Full normalisation pipeline. Returns clean canonical question.

    Steps:
      1. Lowercase + strip
      2. Punctuation normalise
      3. Abbreviation expand
      4. Spell correct
      5. Final strip
    """
    _init_spell()

    if not question or not question.strip():
        return question

    text = question.strip()
    text = text.lower()
    text = _normalise_punctuation(text)
    text = _expand_abbreviations(text)
    text = _correct_spelling(text)
    text = text.strip()

    if text != question.lower().strip():
        print(f"[Normaliser] '{question[:60]}' -> '{text[:60]}'")

    return text


def normalise_for_display(question: str) -> str:
    """Same as normalise() but re-capitalises first letter for display."""
    cleaned = normalise(question)
    return cleaned[0].upper() + cleaned[1:] if cleaned else cleaned
