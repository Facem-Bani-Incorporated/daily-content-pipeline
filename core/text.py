"""Cleanup applied to prose after the model has written it.

Prompt rules alone do not hold. A model told not to use a dash will keep most of them
out and then put three in the last paragraph, and the reader sees the three. So the
rules stay in the prompts and this runs over the result.
"""

import re

# A dash standing in for a comma, a colon or a full stop. This is the punctuation mark
# that makes generated prose read as generated: it lets a sentence bolt on an aside
# without committing to a clause, and models reach for it constantly.
#
# Three shapes are replaced and two are deliberately left alone. Ranges (1914-1918,
# 1914–1918) and compounds (record-breaking, well-documented) are correct English and
# survive untouched; only a dash acting as sentence punctuation is rewritten.
# A spaced dash counts as punctuation when at least ONE side is not a digit, which
# leaves "12 - 15 percent" and "1914 - 1918" alone while still catching "in 1914 — the
# war began". Numeric ranges are the only spaced dashes worth keeping.
_PROSE_DASH = re.compile(
    r"(?:^|(?<=\D))\s+[—–]\s+"             # spaced em/en dash, non-digit on the left
    r"|\s+[—–]\s+(?=\D)"                   # ... or on the right
    r"|(?:^|(?<=\D))\s+--?\s+"             # a hyphen or double hyphen doing the same job
    r"|\s+--?\s+(?=\D)"
    r"|(?<=[^\W\d_])--(?=[^\W\d_])"        # word--word. A SINGLE hyphen here is a
                                            # compound (record-breaking) and must live.
    r"|(?<=[^\W\d_])[—–](?=[^\W\d_])"
)

# The replacement is a comma rather than a full stop because a comma is right in the
# common case (an appositive or a trailing qualifier) and never leaves a fragment
# behind. A full stop would be better where the dash joined two independent clauses,
# but telling those apart reliably is not worth the sentences it would break.
_TIDY = [
    (re.compile(r",\s*,+"), ","),          # collapse the doubling this can produce
    (re.compile(r"\s+,"), ","),            # no space before a comma
    (re.compile(r",(?=[^\s\d])"), ", "),   # always a space after, except in numbers
    (re.compile(r"[ \t]{2,}"), " "),
]


def strip_prose_dashes(text: str) -> str:
    """Rewrite dashes used as punctuation into commas. Leaves ranges and compounds."""
    if not text or not isinstance(text, str):
        return text
    out = _PROSE_DASH.sub(", ", text)
    for pattern, repl in _TIDY:
        out = pattern.sub(repl, out)
    return out.strip()


def strip_dashes_in(value):
    """`strip_prose_dashes` over a string, a list of strings, or a dict's string values.

    Structural fields are NOT passed through this: a timeline reads "14:32 — the first
    signal reaches Lisbon" and an aftermath entry "By 1961 — ...", where the dash is a
    separator the app renders around, not punctuation the model chose.
    """
    if isinstance(value, str):
        return strip_prose_dashes(value)
    if isinstance(value, list):
        return [strip_dashes_in(v) for v in value]
    if isinstance(value, dict):
        return {k: strip_dashes_in(v) for k, v in value.items()}
    return value
