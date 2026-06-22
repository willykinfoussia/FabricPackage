"""Shared French month tokens and accent stripping for transform parsers."""

from __future__ import annotations

import re
import unicodedata

# Full French month names (index 1 = janvier), aligned with column-label parsers.
FR_MONTH_NAMES: tuple[str, ...] = (
    "janvier",
    "février",
    "mars",
    "avril",
    "mai",
    "juin",
    "juillet",
    "août",
    "septembre",
    "octobre",
    "novembre",
    "décembre",
)

# Extra abbreviations per month number (1–12); full names come from FR_MONTH_NAMES.
_FR_MONTH_ABBREVS: tuple[tuple[str, ...], ...] = (
    ("jan", "janv"),
    ("fev", "fevr"),
    ("mar", "mars"),
    ("avr", "avril"),
    ("mai", "mai"),
    ("jun", "juin", "jui"),
    ("juil", "juil", "jul"),
    ("aou", "août", "aug"),
    ("sep", "sept"),
    ("oct", "oct"),
    ("nov", "nov"),
    ("dec", "dec"),
)


def strip_accents(s: str) -> str:
    """Remove combining accents (NFKD), e.g. ``février`` -> ``fevrier``."""
    nk = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nk if not unicodedata.combining(c))


def _register_token(store: dict[str, int], token: str, month_num: int) -> None:
    key = token.strip().lower()
    if not key:
        return
    store[key] = month_num
    ascii_key = strip_accents(key).lower()
    if ascii_key:
        store[ascii_key] = month_num


def _build_fr_month_token_map() -> dict[str, int]:
    out: dict[str, int] = {}
    for month_num, full_name in enumerate(FR_MONTH_NAMES, start=1):
        _register_token(out, full_name, month_num)
        if month_num <= len(_FR_MONTH_ABBREVS):
            for abbrev in _FR_MONTH_ABBREVS[month_num - 1]:
                _register_token(out, abbrev, month_num)
    return out


FR_MONTH_TOKEN_TO_NUM: dict[str, int] = _build_fr_month_token_map()

MONTH_TOKENS_BY_LENGTH: list[tuple[str, int]] = sorted(
    FR_MONTH_TOKEN_TO_NUM.items(),
    key=lambda item: (-len(item[0]), item[0]),
)


def month_num_from_fr_text_label(text: str) -> int | None:
    """Parse month number (1–12) from a free-text label (Python, for tests and parity)."""
    norm = strip_accents(text.strip().lower())
    if not norm:
        return None
    for token, month_num in MONTH_TOKENS_BY_LENGTH:
        pattern = rf"(^|[^a-z]){re.escape(token)}([^a-z]|$)"
        if re.search(pattern, norm):
            return month_num
    return None


def year_from_fr_text_label(text: str) -> int | None:
    """Parse first calendar year (19xx|20xx) from a free-text label (Python)."""
    norm = strip_accents(text.strip().lower())
    m = re.search(r"(?:19|20)\d{2}", norm)
    if not m:
        return None
    return int(m.group(0))
