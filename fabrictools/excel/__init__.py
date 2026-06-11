"""Excel formula-style API for Spark DataFrames (``Excel.XLookup``, ``Excel.SumIf``, …).

Use with :func:`fabrictools.read_lakehouse` and prepared tables from Power Query scripts
instead of Excel structured tables and worksheet formulas.

Namespaces exported:

* :class:`~fabrictools.excel.Excel` — ``Excel.XLookup``, ``Excel.SumIf``, ``Excel.SumIfs``,
  ``Excel.If``, ``Excel.Round``, ``Excel.TextJoin``

.. rubric:: Example

>>> from fabrictools import read_lakehouse, Excel  # doctest: +SKIP
>>> projects = read_lakehouse("Lakehouse", "dbo/backlog_projects")  # doctest: +SKIP
>>> df = Excel.XLookup(projects, "RAO CODE", customer_projects, "RAO CODE", "Date")  # doctest: +SKIP
"""

from fabrictools.excel.aggregate import SumIf, SumIfs
from fabrictools.excel.logic import If, Round
from fabrictools.excel.lookup import XLookup
from fabrictools.excel.text import TextJoin

__all__ = ["Excel"]


class Excel:
    """Namespace for Excel worksheet functions on Spark DataFrames.

    Names match Excel EN function names (``XLookup`` = ``XLOOKUP`` / ``RECHERCHEX``).
    """

    XLookup = staticmethod(XLookup)
    SumIf = staticmethod(SumIf)
    SumIfs = staticmethod(SumIfs)
    If = staticmethod(If)
    Round = staticmethod(Round)
    TextJoin = staticmethod(TextJoin)
