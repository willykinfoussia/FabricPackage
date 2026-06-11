"""Power Query ``List.*`` aggregation strategy tokens for ``Table.Group``."""


class List:
    """Aggregation strategy constants mirroring Power Query ``List.Sum``, ``List.Max``, etc.

    Pass these tokens as the third element of each tuple in
    :py:meth:`fabrictools.powerquery.table.Table.Group` aggregations.

    .. rubric:: Example

    >>> from fabrictools import Table, List  # doctest: +SKIP
    >>> df = Table.Group(df, ["RAO CODE"], [  # doctest: +SKIP
    ...     ("Amount", "AMOUNT CNY", List.Sum),
    ...     ("Client", "END USER", List.Max),
    ... ])
    """

    Sum = "sum"
    """Sum numeric values (Power Query ``List.Sum``)."""

    Max = "max"
    """Maximum value (Power Query ``List.Max``)."""

    Min = "min"
    """Minimum value (Power Query ``List.Min``)."""

    Average = "avg"
    """Average value (Power Query ``List.Average``)."""

    Count = "count"
    """Count non-null values (Power Query ``List.Count``)."""

    First = "first"
    """First value in the group (Power Query ``List.First``)."""

    Last = "last"
    """Last value in the group (Power Query ``List.Last``)."""
