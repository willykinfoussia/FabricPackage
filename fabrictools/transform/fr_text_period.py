"""Extract year and month integers from free-form French text labels (Spark Column expressions)."""

from __future__ import annotations

import re
from typing import Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from fabrictools.transform._fr_month_tokens import MONTH_TOKENS_BY_LENGTH
from fabrictools.transform.columns import resolve_dataframe_column

# Spark-side accent folding (input is already lowercased).
_ACCENT_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    ("é", "e"),
    ("è", "e"),
    ("ê", "e"),
    ("ë", "e"),
    ("à", "a"),
    ("â", "a"),
    ("ä", "a"),
    ("ù", "u"),
    ("û", "u"),
    ("ü", "u"),
    ("î", "i"),
    ("ï", "i"),
    ("ô", "o"),
    ("ö", "o"),
    ("ç", "c"),
    ("ÿ", "y"),
)


def _as_column(expr: Union[Column, str]) -> Column:
    return F.lit(expr) if isinstance(expr, str) else expr


def _normalize_fr_text_column(expr: Union[Column, str]) -> Column:
    """Lowercase string with accents removed; spaces preserved."""
    c = _as_column(expr)
    as_str = F.lower(F.coalesce(c.cast("string"), F.lit("")))
    out = as_str
    for accented, plain in _ACCENT_REPLACEMENTS:
        out = F.regexp_replace(out, accented, plain)
    return out


def _month_token_pattern(token: str) -> str:
    escaped = re.escape(token)
    return rf"(^|[^a-z]){escaped}([^a-z]|$)"


def year_from_fr_text(expr: Union[Column, str]) -> Column:
    """Extract the first calendar year (1900–2099) from a French text label.

    If ``expr`` is a ``str``, it is wrapped with ``F.lit``.

    :param expr: Spark column or string literal (e.g. ``"OIT fev 2026"``).
    :type expr: ~pyspark.sql.Column | str

    :returns: Integer year column, or null when no year is found.
    :rtype: ~pyspark.sql.Column

    .. rubric:: Example

    >>> df.withColumn("annee", year_from_fr_text("periode_label"))  # doctest: +SKIP
    """
    norm = _normalize_fr_text_column(expr)
    extracted = F.regexp_extract(norm, r"((?:19|20)\d{2})", 1)
    return F.when(extracted != F.lit(""), extracted.cast("int")).otherwise(F.lit(None))


def month_from_fr_text(expr: Union[Column, str]) -> Column:
    """Extract month number (1–12) from a French text label (full or abbreviated month).

    Tokens are matched as whole words on accent-stripped lowercase text
    (longest token first, e.g. ``fevrier`` before ``fev``).

    :param expr: Spark column or string literal (e.g. ``"OIT fev 2026"``, ``"févr"``).
    :type expr: ~pyspark.sql.Column | str

    :returns: Integer month column (1–12), or null when no month token matches.
    :rtype: ~pyspark.sql.Column

    .. rubric:: Example

    >>> df.withColumn("mois", month_from_fr_text("periode_label"))  # doctest: +SKIP
    """
    norm = _normalize_fr_text_column(expr)
    out: Column = F.lit(None)
    for token, month_num in MONTH_TOKENS_BY_LENGTH:
        pattern = _month_token_pattern(token)
        out = F.when(norm.rlike(pattern), F.lit(month_num)).otherwise(out)
    return out.cast("int")


def with_year_month_from_fr_text(
    df: DataFrame,
    source_col: str,
    *,
    year_col: str = "annee",
    month_col: str = "mois",
) -> DataFrame:
    """Add year and month columns parsed from ``source_col`` (resolved like other transform helpers).

    :param df: Input dataframe.
    :param source_col: Source text column (physical, normalized, or snake_case label).
    :param year_col: Output year column name.
    :param month_col: Output month column name.
    :type df: ~pyspark.sql.DataFrame
    :type source_col: str
    :type year_col: str
    :type month_col: str

    :returns: Dataframe with two added columns.
    :rtype: ~pyspark.sql.DataFrame

    :raises ValueError: If ``source_col`` does not resolve on ``df``.

    .. rubric:: Example

    >>> out = with_year_month_from_fr_text(df, "libelle_periode")  # doctest: +SKIP
    """
    physical = resolve_dataframe_column(df, source_col)
    if physical is None:
        raise ValueError(
            f"with_year_month_from_fr_text: source column {source_col!r} does not resolve on the dataframe"
        )
    src = F.col(physical)
    return (
        df.withColumn(year_col, year_from_fr_text(src))
        .withColumn(month_col, month_from_fr_text(src))
    )
