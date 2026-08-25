"""Power Query-style API for Spark DataFrames (``Table.*``, ``Text.*``, ``Date.*``, ``Number.*``).

Use with :func:`fabrictools.read_lakehouse` instead of Excel load / ``Table.PromoteHeaders`` /
initial ``Table.TransformColumnTypes``.

Namespaces exported:

* :class:`~fabrictools.powerquery.table.Table` — ``Table.Group``, ``Table.SelectRows``, …
* :class:`~fabrictools.powerquery.text.Text` — ``Text.Clean``, ``Text.Select``, …
* :class:`~fabrictools.powerquery.date.Date` — ``Date.Year``, ``Date.Month``, …
* :class:`~fabrictools.powerquery.date.DateTime` — ``DateTime.LocalNow``, ``DateTime.Date``
* :class:`~fabrictools.powerquery.number.Number` — ``Number.FromText``, ``Number.ToText``
* :class:`~fabrictools.powerquery.list.List` — ``List.Sum``, ``List.Max``, … (for ``Table.Group``)
* :class:`~fabrictools.powerquery._common.Order` — ``Order.Ascending``, ``Order.Descending``
* :class:`~fabrictools.powerquery._common.type` — ``type.text``, ``type.date``, …
* :class:`~fabrictools.powerquery._common.Percentage` — ``Percentage.Type``
* :class:`~fabrictools.powerquery._common.Int64` — ``Int64.Type``
* :class:`~fabrictools.powerquery._common.Currency` — ``Currency.Type``

.. rubric:: Example

>>> from fabrictools import read_lakehouse, Table, List  # doctest: +SKIP
>>> df = read_lakehouse("Lakehouse", "dbo/my_table")  # doctest: +SKIP
>>> df = Table.Group(df, ["id"], [("total", "amount", List.Sum)])  # doctest: +SKIP
"""

from fabrictools.powerquery._common import Currency, Int64, Order, Percentage, type
from fabrictools.powerquery.date import Date, DateTime
from fabrictools.powerquery.list import List
from fabrictools.powerquery.number import Number
from fabrictools.powerquery.table import Table
from fabrictools.powerquery.text import Text

__all__ = [
    "Currency",
    "Date",
    "DateTime",
    "Int64",
    "List",
    "Number",
    "Order",
    "Percentage",
    "Table",
    "Text",
    "type",
]
