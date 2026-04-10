"""Column expressions for normalized text and dimension defaults."""

from __future__ import annotations

from typing import Union

from pyspark.sql import Column
from pyspark.sql import functions as F


def norm_text(expr: Union[Column, str]) -> Column:
    """Équivalent M : Text.Lower(Text.Replace(Text.Clean(x), " ", "")). str = paramètre pivot (F.lit)."""
    c = F.lit(expr) if isinstance(expr, str) else expr
    as_str = F.coalesce(c.cast("string"), F.lit(""))
    cleaned = F.regexp_replace(as_str, r"[\x00-\x1f]", "")
    trimmed = F.trim(cleaned)
    no_spaces = F.regexp_replace(trimmed, " ", "")
    return F.lower(no_spaces)


def empty_or_null(c: Column) -> Column:
    s = F.coalesce(c.cast("string"), F.lit(""))
    return c.isNull() | (F.trim(s) == F.lit(""))


def coalesce_dim(src: Column) -> Column:
    return F.when(
        src.isNull() | (F.trim(F.coalesce(src.cast("string"), F.lit(""))) == F.lit("")),
        F.lit("0"),
    ).otherwise(src.cast("string"))
