"""Filter DataFrames by a list of values on one column (type-aware, no casts)."""

from __future__ import annotations

from typing import Any, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from fabrictools.transform.columns import _resolve_column_name

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
    """Keep or drop rows where ``column`` is in ``values`` (no column cast).

    For string-like dtypes, compares ``trim(column)`` to ``values``. ``str`` entries
    in ``values`` are stripped.

    :param df: Input dataframe.
    :param column: Logical or physical column name (resolved like :py:func:`fabrictools.resolve_dataframe_column`). If it does not resolve, ``df`` is returned unchanged.
    :param values: Membership list; non-strings kept as-is.
    :param exclude: If ``True`` (default), drop rows in ``values``; if ``False``, keep only those rows.
    :type df: ~pyspark.sql.DataFrame
    :type column: str
    :type values: collections.abc.Sequence
    :type exclude: bool

    :returns: Filtered dataframe.
    :rtype: ~pyspark.sql.DataFrame

    .. rubric:: Example

    >>> filtered = filter_by_value_list(  # doctest: +SKIP
    ...     df, "status", ["VOID", "CANCELLED"], exclude=True
    ... )
    """
    resolved = _resolve_column_name(df, column, side="DataFrame")
    if resolved is None:
        return df
    dtype = _column_dtype(df, resolved)
    expr = F.trim(F.col(resolved)) if _is_string_like(dtype) else F.col(resolved)
    literals = _prepare_values(values)
    in_list = expr.isin(literals)
    pred = ~in_list if exclude else in_list
    return df.filter(pred)
