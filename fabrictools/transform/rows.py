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
    """
    Drop rows where strictly more than ``max_empty_percent`` of the considered
    cells are empty.

    A cell is empty if ``empty_or_null`` is true for that column (null or blank
    string after string cast and trim).

    Parameters
    ----------
    df
        Input DataFrame.
    max_empty_percent
        Threshold in ``[0, 100]``. Rows with empty ratio **>** ``max_empty_percent / 100``
        are removed (e.g. ``50`` keeps rows with at most 50% empty cells).
    columns
        Physical or clean_data-style column names to include in the ratio.
        If ``None``, all columns of ``df`` are used.

    Raises
    ------
    ValueError
        If ``max_empty_percent`` is outside ``[0, 100]``, if ``columns`` is an
        empty sequence, or if there are no columns to evaluate.
    """
    if not 0 <= max_empty_percent <= 100:
        raise ValueError(
            f"max_empty_percent must be in [0, 100], got {max_empty_percent!r}"
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
    threshold = max_empty_percent / 100.0
    return df.filter(empty_ratio <= F.lit(threshold))
