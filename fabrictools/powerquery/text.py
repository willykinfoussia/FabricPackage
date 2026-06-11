"""Power Query ``Text.*`` column expressions."""

from __future__ import annotations

from typing import Sequence, Union

from pyspark.sql import Column
from pyspark.sql import functions as F

from fabrictools.transform.text import norm_text

__all__ = ["Text"]


def _as_column(expr: Union[Column, str]) -> Column:
    return F.lit(expr) if isinstance(expr, str) else expr


class Text:
    """Namespace for Power Query ``Text.*`` functions.

    All methods return Spark ``Column`` expressions for use in
    :py:meth:`fabrictools.powerquery.table.Table.AddColumn` or
    :py:meth:`fabrictools.powerquery.table.Table.TransformColumns`.
    """

    @staticmethod
    def Clean(expr: Union[Column, str]) -> Column:
        """Lowercase string with control chars stripped and spaces removed (Power Query ``Text.Clean``).

        Delegates to :py:func:`fabrictools.norm_text`.

        :param expr: Spark column or string literal.
        :type expr: ~pyspark.sql.Column | str

        :returns: Transformed column expression.
        :rtype: ~pyspark.sql.Column

        .. rubric:: Example

        >>> from fabrictools import Table, Text  # doctest: +SKIP
        >>> from pyspark.sql import functions as F  # doctest: +SKIP
        >>> df = Table.AddColumn(df, "Project No. 2", Text.Clean(F.col("Project No.")))  # doctest: +SKIP
        """
        return norm_text(expr)

    @staticmethod
    def Select(expr: Union[Column, str], allowed: Sequence[str]) -> Column:
        """Keep only characters present in ``allowed`` (Power Query ``Text.Select``).

        :param expr: Spark column or string literal.
        :param allowed: Characters to keep (e.g. digits and punctuation for amounts).
        :type expr: ~pyspark.sql.Column | str
        :type allowed: collections.abc.Sequence[str]

        :returns: Filtered string column expression.
        :rtype: ~pyspark.sql.Column

        .. rubric:: Example

        >>> from fabrictools import Text  # doctest: +SKIP
        >>> from pyspark.sql import functions as F  # doctest: +SKIP
        >>> digits = Text.Select(F.col("amount_raw"), list("0123456789,.-"))  # doctest: +SKIP
        """
        import re

        c = _as_column(expr)
        as_str = F.coalesce(c.cast("string"), F.lit(""))
        if not allowed:
            return F.lit("")
        escaped = "".join(re.escape(ch) for ch in allowed)
        return F.regexp_replace(as_str, f"[^{escaped}]", "")

    @staticmethod
    def Trim(expr: Union[Column, str]) -> Column:
        """Remove leading and trailing whitespace (Power Query ``Text.Trim``).

        :param expr: Spark column or string literal.
        :type expr: ~pyspark.sql.Column | str

        :returns: Trimmed string column expression.
        :rtype: ~pyspark.sql.Column

        .. rubric:: Example

        >>> from fabrictools import Text  # doctest: +SKIP
        >>> from pyspark.sql import functions as F  # doctest: +SKIP
        >>> df.withColumn("name", Text.Trim(F.col("name")))  # doctest: +SKIP
        """
        return F.trim(_as_column(expr).cast("string"))

    @staticmethod
    def Lower(expr: Union[Column, str]) -> Column:
        """Convert to lowercase (Power Query ``Text.Lower``).

        :param expr: Spark column or string literal.
        :type expr: ~pyspark.sql.Column | str

        :returns: Lowercase column expression.
        :rtype: ~pyspark.sql.Column

        .. rubric:: Example

        >>> from fabrictools import Text  # doctest: +SKIP
        >>> from pyspark.sql import functions as F  # doctest: +SKIP
        >>> df.withColumn("code", Text.Lower(F.col("code")))  # doctest: +SKIP
        """
        return F.lower(_as_column(expr).cast("string"))

    @staticmethod
    def Upper(expr: Union[Column, str]) -> Column:
        """Convert to uppercase (Power Query ``Text.Upper``).

        :param expr: Spark column or string literal.
        :type expr: ~pyspark.sql.Column | str

        :returns: Uppercase column expression.
        :rtype: ~pyspark.sql.Column

        .. rubric:: Example

        >>> from fabrictools import Text  # doctest: +SKIP
        >>> from pyspark.sql import functions as F  # doctest: +SKIP
        >>> df.withColumn("code", Text.Upper(F.col("code")))  # doctest: +SKIP
        """
        return F.upper(_as_column(expr).cast("string"))

    @staticmethod
    def Proper(expr: Union[Column, str]) -> Column:
        """Title-case each word (Power Query ``Text.Proper``).

        :param expr: Spark column or string literal.
        :type expr: ~pyspark.sql.Column | str

        :returns: Title-cased column expression.
        :rtype: ~pyspark.sql.Column

        .. rubric:: Example

        >>> from fabrictools import Text  # doctest: +SKIP
        >>> from pyspark.sql import functions as F  # doctest: +SKIP
        >>> df.withColumn("client", Text.Proper(F.col("client")))  # doctest: +SKIP
        """
        return F.initcap(_as_column(expr).cast("string"))

    @staticmethod
    def Combine(texts: Sequence[Union[Column, str]], separator: str = "") -> Column:
        """Concatenate text values with a separator (Power Query ``Text.Combine``).

        :param texts: Column expressions or string literals to join.
        :param separator: Delimiter between parts (default empty string).
        :type texts: collections.abc.Sequence[~pyspark.sql.Column | str]
        :type separator: str

        :returns: Concatenated string column expression.
        :rtype: ~pyspark.sql.Column

        .. rubric:: Example

        >>> from fabrictools import Text  # doctest: +SKIP
        >>> from pyspark.sql import functions as F  # doctest: +SKIP
        >>> full = Text.Combine([F.col("first"), F.col("last")], " ")  # doctest: +SKIP
        """
        cols = [_as_column(t).cast("string") for t in texts]
        return F.concat_ws(separator, *cols)

    @staticmethod
    def From(expr: Union[Column, str]) -> Column:
        """Cast a value to text (Power Query ``Text.From``).

        :param expr: Spark column or string literal.
        :type expr: ~pyspark.sql.Column | str

        :returns: Non-null string column expression (null becomes ``""``).
        :rtype: ~pyspark.sql.Column

        .. rubric:: Example

        >>> from fabrictools import Text  # doctest: +SKIP
        >>> from pyspark.sql import functions as F  # doctest: +SKIP
        >>> df.withColumn("id_text", Text.From(F.col("id")))  # doctest: +SKIP
        """
        return F.coalesce(_as_column(expr).cast("string"), F.lit(""))
