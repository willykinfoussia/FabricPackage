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

    @staticmethod
    def ToText(expr: Union[Column, str, int, float], format: str | None = None) -> Column:
        """Convert a number to text (Power Query ``Number.ToText``).

        When ``format`` is a zero-padded mask such as ``"0000"`` or ``"00"``,
        the value is left-padded with zeros to that width (e.g. month ``1`` →
        ``"01"``). Null input stays null.

        :param expr: Numeric column, string column, or numeric literal.
        :param format: Optional custom format (``"0"`` digits = pad width).
        :type expr: ~pyspark.sql.Column | str | int | float
        :type format: str | None

        :returns: String column expression.
        :rtype: ~pyspark.sql.Column

        .. rubric:: Example

        >>> from fabrictools import Number, Date  # doctest: +SKIP
        >>> from pyspark.sql import functions as F  # doctest: +SKIP
        >>> Number.ToText(Date.Month(F.col("Date contractuelle")), "00")  # doctest: +SKIP
        """
        if isinstance(expr, (int, float)):
            col = F.lit(expr)
        elif isinstance(expr, str):
            col = F.lit(expr)
        else:
            col = expr
        if format is None:
            return col.cast("string")
        mask = str(format)
        if mask and set(mask) <= {"0"}:
            return F.lpad(col.cast("long").cast("string"), len(mask), "0")
        return col.cast("string")
