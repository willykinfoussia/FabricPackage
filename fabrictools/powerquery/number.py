"""Power Query ``Number.*`` column expressions."""

from __future__ import annotations

from typing import Union

from pyspark.sql import Column
from pyspark.sql import functions as F

from fabrictools.powerquery.text import Text

__all__ = ["Number"]


def _from_text_expr(expr: Union[Column, str]) -> Column:
    """Internal: parse text to nullable double (``fxToNumber`` rules)."""
    t0 = Text.From(expr)
    t1 = Text.Select(t0, list("0123456789,.-"))

    empty = (t1 == F.lit("")) | (t1 == F.lit("-"))
    has_comma = t1.contains(",")
    has_dot = t1.contains(".")
    comma_count = F.length(t1) - F.length(F.regexp_replace(t1, ",", ""))

    us_thousands = (has_comma & has_dot) | (comma_count > F.lit(1))
    us_stripped = F.regexp_replace(t1, ",", "")
    fr_normalized = F.regexp_replace(t1, ",", ".")

    parsed = (
        F.when(empty, F.lit(None).cast("double"))
        .when(us_thousands, us_stripped.cast("double"))
        .when(has_comma, fr_normalized.cast("double"))
        .when(has_dot, t1.cast("double"))
        .otherwise(t1.cast("double"))
    )
    return parsed


class Number:
    """Namespace for Power Query ``Number.*`` functions."""

    @staticmethod
    def FromText(expr: Union[Column, str]) -> Column:
        """Convert text to a nullable number with US/FR decimal rules (Power Query ``Number.FromText``).

        Implements the invoicing script ``fxToNumber`` logic: strips non-numeric characters,
        detects thousand separators vs decimal comma/point, and parses accordingly.

        :param expr: Spark column or string literal.
        :type expr: ~pyspark.sql.Column | str

        :returns: Nullable ``double`` column expression.
        :rtype: ~pyspark.sql.Column

        .. rubric:: Example

        >>> from fabrictools import Table, Number  # doctest: +SKIP
        >>> df = Table.TransformColumns(df, [  # doctest: +SKIP
        ...     ("Total Invoice amount without VAT", Number.FromText),
        ... ])
        """
        return _from_text_expr(expr)
