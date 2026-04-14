"""Column expressions for normalized text and dimension defaults."""

from __future__ import annotations

from typing import Union

from pyspark.sql import Column
from pyspark.sql import functions as F


def norm_text(expr: Union[Column, str]) -> Column:
    """Lowercase string with control chars stripped and spaces removed (Power Query ``Text.Clean`` style).

    If ``expr`` is a ``str``, it is wrapped with ``F.lit``.

    :param expr: Spark column or string literal.
    :type expr: ~pyspark.sql.Column | str

    :returns: Transformed column expression.
    :rtype: ~pyspark.sql.Column
    """
    c = F.lit(expr) if isinstance(expr, str) else expr
    as_str = F.coalesce(c.cast("string"), F.lit(""))
    cleaned = F.regexp_replace(as_str, r"[\x00-\x1f]", "")
    trimmed = F.trim(cleaned)
    no_spaces = F.regexp_replace(trimmed, " ", "")
    return F.lower(no_spaces)


def empty_or_null(c: Column) -> Column:
    """Boolean column: true if ``c`` is null or blank after string cast and trim.

    :param c: Input column expression.
    :type c: ~pyspark.sql.Column

    :returns: Boolean ``Column``.
    :rtype: ~pyspark.sql.Column
    """
    s = F.coalesce(c.cast("string"), F.lit(""))
    return c.isNull() | (F.trim(s) == F.lit(""))


def coalesce_dim(src: Column) -> Column:
    """String cast of ``src``; null or blank becomes the literal ``0`` as string (dimension-friendly).

    :param src: Source column.
    :type src: ~pyspark.sql.Column

    :returns: String ``Column``.
    :rtype: ~pyspark.sql.Column
    """
    return F.when(
        src.isNull() | (F.trim(F.coalesce(src.cast("string"), F.lit(""))) == F.lit("")),
        F.lit("0"),
    ).otherwise(src.cast("string"))
