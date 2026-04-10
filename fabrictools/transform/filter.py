"""Filter DataFrames by a list of values on one column (type-aware, no casts)."""

from __future__ import annotations

from typing import Any, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

try:
    from pyspark.sql.types import CharType, VarcharType
except ImportError:  # pragma: no cover
    CharType = VarcharType = None  # type: ignore[misc, assignment]

_STRING_TYPES: tuple[type, ...] = (StringType,)
if CharType is not None and VarcharType is not None:
    _STRING_TYPES = (StringType, CharType, VarcharType)


def _column_dtype(df: DataFrame, column: str):
    for field in df.schema.fields:
        if field.name == column:
            return field.dataType
    raise ValueError(f"Column not found: {column!r}")


def _is_string_like(data_type) -> bool:
    return isinstance(data_type, _STRING_TYPES)


def _prepare_values(values: Sequence[Any]) -> list[Any]:
    out: list[Any] = []
    for v in values:
        if isinstance(v, str):
            out.append(v.strip())
        else:
            out.append(v)
    return out


def filter_by_value_list(
    df: DataFrame,
    column: str,
    values: Sequence[Any],
    *,
    exclude: bool = True,
) -> DataFrame:
    """
    Keep or drop rows where ``column`` is in ``values``.

    Never casts the column. For string-like columns, compares ``trim(column)``
    to the given values. Python ``str`` entries in ``values`` are stripped.

    Parameters
    ----------
    df
        Input DataFrame.
    column
        Column name to test.
    values
        Values for membership; types should match the column semantics.
    exclude
        If True (default), drop rows whose value is in ``values``.
        If False, keep only rows whose value is in ``values``.
    """
    dtype = _column_dtype(df, column)
    expr = F.trim(F.col(column)) if _is_string_like(dtype) else F.col(column)
    literals = _prepare_values(values)
    in_list = expr.isin(literals)
    pred = ~in_list if exclude else in_list
    return df.filter(pred)
