"""Row-level filters based on how many columns are empty (see ``empty_or_null``)."""

from __future__ import annotations

from functools import reduce
from operator import add
from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fabrictools.transform.columns import _resolve_column_name
from fabrictools.transform.text import empty_or_null


def drop_rows_over_empty_percent(
    df: DataFrame,
    max_empty_percent: float,
    *,
    columns: Sequence[str] | None = None,
) -> DataFrame:
    """Drop rows where the fraction of empty cells (see :py:func:`fabrictools.empty_or_null`) exceeds ``max_empty_percent``.

    :param df: Input dataframe.
    :param max_empty_percent: Upper bound in ``[0, 1]``; rows with empty ratio **strictly greater** than this are removed.
    :param columns: Columns to score; ``None`` means all columns. Names resolved like :py:func:`fabrictools.resolve_dataframe_column`.
    :type df: ~pyspark.sql.DataFrame
    :type max_empty_percent: float
    :type columns: collections.abc.Sequence[str] | None

    :returns: Filtered dataframe.
    :rtype: ~pyspark.sql.DataFrame

    :raises ValueError: If ``max_empty_percent`` is outside ``[0, 1]``, if ``columns`` is an empty sequence, or if no columns remain to score.

    .. rubric:: Example

    >>> pruned = drop_rows_over_empty_percent(  # doctest: +SKIP
    ...     df, 0.5, columns=["col_a", "col_b", "col_c"]
    ... )
    """
    if not 0 <= max_empty_percent <= 1:
        raise ValueError(
            f"max_empty_percent must be in [0, 1], got {max_empty_percent!r}"
        )

    if columns is not None and len(columns) == 0:
        raise ValueError("columns must be None or a non-empty sequence")

    if columns is None:
        resolved = [f.name for f in df.schema.fields]
    else:
        resolved = []
        seen: set[str] = set()
        for name in columns:
            actual = _resolve_column_name(df, name, side="DataFrame")
            if actual not in seen:
                seen.add(actual)
                resolved.append(actual)

    n = len(resolved)
    if n == 0:
        raise ValueError("No columns to evaluate empty ratio")

    empty_count = reduce(
        add,
        (
            F.when(empty_or_null(F.col(c)), F.lit(1)).otherwise(F.lit(0))
            for c in resolved
        ),
    )
    empty_ratio = empty_count / F.lit(float(n))
    return df.filter(empty_ratio <= F.lit(float(max_empty_percent)))
