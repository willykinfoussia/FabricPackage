"""Power Query ``Date.*`` column expressions."""

from __future__ import annotations

from typing import Union

from pyspark.sql import Column
from pyspark.sql import functions as F

__all__ = ["Date", "DateTime"]


def _as_column(expr: Union[Column, str]) -> Column:
    return F.lit(expr) if isinstance(expr, str) else expr


class Date:
    """Namespace for Power Query ``Date.*`` functions.

    All methods return Spark ``Column`` expressions for use in
    :py:meth:`fabrictools.powerquery.table.Table.AddColumn`.
    """

    @staticmethod
    def Year(expr: Union[Column, str]) -> Column:
        """Extract the year from a date or timestamp (Power Query ``Date.Year``).

        :param expr: Date or timestamp column, or string literal.
        :type expr: ~pyspark.sql.Column | str

        :returns: Integer year column expression.
        :rtype: ~pyspark.sql.Column

        .. rubric:: Example

        >>> from fabrictools import Table, Date  # doctest: +SKIP
        >>> from pyspark.sql import functions as F  # doctest: +SKIP
        >>> df = Table.AddColumn(df, "Year", Date.Year(F.col("Date of Revenue recognition")))  # doctest: +SKIP
        """
        return F.year(_as_column(expr))

    @staticmethod
    def Month(expr: Union[Column, str]) -> Column:
        """Extract the month (1–12) from a date or timestamp (Power Query ``Date.Month``).

        :param expr: Date or timestamp column, or string literal.
        :type expr: ~pyspark.sql.Column | str

        :returns: Integer month column expression.
        :rtype: ~pyspark.sql.Column

        .. rubric:: Example

        >>> from fabrictools import Date  # doctest: +SKIP
        >>> from pyspark.sql import functions as F  # doctest: +SKIP
        >>> df.withColumn("month", Date.Month(F.col("order_date")))  # doctest: +SKIP
        """
        return F.month(_as_column(expr))

    @staticmethod
    def Day(expr: Union[Column, str]) -> Column:
        """Extract the day of month from a date or timestamp (Power Query ``Date.Day``).

        :param expr: Date or timestamp column, or string literal.
        :type expr: ~pyspark.sql.Column | str

        :returns: Integer day column expression.
        :rtype: ~pyspark.sql.Column

        .. rubric:: Example

        >>> from fabrictools import Date  # doctest: +SKIP
        >>> from pyspark.sql import functions as F  # doctest: +SKIP
        >>> df.withColumn("day", Date.Day(F.col("order_date")))  # doctest: +SKIP
        """
        return F.dayofmonth(_as_column(expr))

    @staticmethod
    def From(expr: Union[Column, str]) -> Column:
        """Parse or cast a value to date (Power Query ``Date.From``).

        :param expr: Column or string literal parseable as a date.
        :type expr: ~pyspark.sql.Column | str

        :returns: Date column expression.
        :rtype: ~pyspark.sql.Column

        .. rubric:: Example

        >>> from fabrictools import Date  # doctest: +SKIP
        >>> from pyspark.sql import functions as F  # doctest: +SKIP
        >>> df.withColumn("order_date", Date.From(F.col("order_date_str")))  # doctest: +SKIP
        """
        return F.to_date(_as_column(expr))

    @staticmethod
    def AddDays(expr: Union[Column, str], days: int) -> Column:
        """Add calendar days to a date (Power Query ``Date.AddDays``).

        :param expr: Date column or string literal.
        :param days: Number of days to add (may be negative).
        :type expr: ~pyspark.sql.Column | str
        :type days: int

        :returns: Shifted date column expression.
        :rtype: ~pyspark.sql.Column

        .. rubric:: Example

        >>> from fabrictools import Date  # doctest: +SKIP
        >>> from pyspark.sql import functions as F  # doctest: +SKIP
        >>> df.withColumn("due_date", Date.AddDays(F.col("order_date"), 30))  # doctest: +SKIP
        """
        return F.date_add(_as_column(expr), days)


class DateTime:
    """Namespace for Power Query ``DateTime.*`` functions."""

    @staticmethod
    def LocalNow() -> Column:
        """Current local timestamp (Power Query ``DateTime.LocalNow``).

        :returns: Timestamp column expression (``current_timestamp``).
        :rtype: ~pyspark.sql.Column

        .. rubric:: Example

        >>> from fabrictools import DateTime  # doctest: +SKIP
        >>> DateTime.Date(DateTime.LocalNow())  # doctest: +SKIP
        """
        return F.current_timestamp()

    @staticmethod
    def Date(expr: Union[Column, str]) -> Column:
        """Extract the date part of a datetime (Power Query ``DateTime.Date``).

        :param expr: Timestamp column or string literal.
        :type expr: ~pyspark.sql.Column | str

        :returns: Date column expression.
        :rtype: ~pyspark.sql.Column

        .. rubric:: Example

        >>> from fabrictools import DateTime  # doctest: +SKIP
        >>> today = DateTime.Date(DateTime.LocalNow())  # doctest: +SKIP
        """
        return F.to_date(_as_column(expr))
