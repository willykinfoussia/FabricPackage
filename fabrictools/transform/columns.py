"""Column name resolution (physical vs clean_data-style normalized) and helpers."""

from __future__ import annotations

from pyspark.sql import DataFrame

from fabrictools.quality.clean import _build_unique_column_names, _to_snake_case


def _resolve_column_name(df: DataFrame, name: str, *, side: str = "DataFrame") -> str:
    cols = [f.name for f in df.schema.fields]
    if name in cols:
        return name
    norm_list = _build_unique_column_names(cols)
    if name in norm_list:
        return cols[norm_list.index(name)]
    candidate = _to_snake_case(name)
    if candidate in norm_list:
        return cols[norm_list.index(candidate)]
    raise ValueError(
        f"{side} DataFrame has no column {name!r} "
        f"(not a physical name nor a name normalized like clean_data)"
    )


def remove_columns(df: DataFrame, *columns: str) -> DataFrame:
    """
    Drop columns by physical name or by the same resolution rules as ``merge_dataframes`` /
    ``clean_data`` (snake_case + unique suffixes).

    Parameters
    ----------
    df
        Input DataFrame.
    *columns
        One or more column labels to remove. Duplicate requests that resolve to the same
        physical column are dropped once.

    Raises
    ------
    ValueError
        If no column names are passed, or if a name cannot be resolved.
    """
    if not columns:
        raise ValueError("remove_columns requires at least one column name")
    resolved: list[str] = []
    seen: set[str] = set()
    for name in columns:
        actual = _resolve_column_name(df, name, side="DataFrame")
        if actual not in seen:
            seen.add(actual)
            resolved.append(actual)
    return df.drop(*resolved)
