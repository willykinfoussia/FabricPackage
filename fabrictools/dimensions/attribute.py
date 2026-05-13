"""Build a single-attribute dimension from one or many PySpark sources."""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from fabrictools.core import log


def _session_key(df: DataFrame) -> int:
    return id(df.sparkSession)


def _natural_column_expr(
    src_col: str,
    *,
    cast_to_string: bool,
    normalize_strings: bool,
) -> Column:
    col = F.col(src_col)
    if cast_to_string:
        col = col.cast("string")
    if normalize_strings:
        col = F.trim(col)
    return col


def build_dimension_from_columns(
    sources: Sequence[tuple[DataFrame, str]],
    dimension_column: str,
    *,
    surrogate_key_column: str | None = None,
    include_surrogate_key: bool = True,
    exclude_nulls: bool = True,
    normalize_strings: bool = True,
    cast_to_string: bool = True,
    log_distinct_count: bool = False,
) -> DataFrame:
    """Union distinct attribute values from many dataframes into one dimension table.

    Each source contributes rows from one column renamed to ``dimension_column``.
    Duplicate values are collapsed once after union (single shuffle for dedupe).
    Optionally adds a deterministic surrogate integer key ordered by ``dimension_column``.

    :param sources: Non-empty sequence of ``(dataframe, source_column_name)`` pairs.
    :param dimension_column: Output natural key column name (e.g. ``"compagnie"``).
    :param surrogate_key_column: Name of the surrogate key column when
        ``include_surrogate_key`` is ``True``. When ``None``, uses
        ``f"{dimension_column}_key"``.
    :param include_surrogate_key: When ``False``, return only ``dimension_column`` (deduped,
        optionally ordered).
    :param exclude_nulls: When ``True``, drop rows where the natural key is null after cast/trim.
    :param normalize_strings: When ``True``, apply :py:func:`~pyspark.sql.functions.trim`
        after any string cast (with ``cast_to_string=True``, values are trimmed as strings).
    :param cast_to_string: When ``True``, cast every source column to ``string`` so values
        from heterogeneous types merge safely (recommended for label dimensions).
    :param log_distinct_count: When ``True``, run an action and log the resulting row count.

    :returns: Deduped dimension with optional surrogate key first, then natural column.

    :raises ValueError: If ``sources`` is empty, a source column is missing, Spark sessions
        differ, or surrogate and natural column names collide.
    """
    ordered = list(sources)
    if not ordered:
        raise ValueError("sources must contain at least one (DataFrame, str) pair")
    base_key = _session_key(ordered[0][0])
    for df, src_col in ordered:
        if src_col not in df.columns:
            raise ValueError(
                f"column {src_col!r} not present in dataframe columns "
                f"{sorted(df.columns)!r}"
            )
        if _session_key(df) != base_key:
            raise ValueError(
                "all source dataframes must use the same SparkSession as the first source"
            )

    if include_surrogate_key:
        effective_key = surrogate_key_column or f"{dimension_column}_key"
        if effective_key == dimension_column:
            raise ValueError(
                "surrogate_key_column must differ from dimension_column "
                f"(both were {dimension_column!r})"
            )
    else:
        effective_key = ""

    projected: list[DataFrame] = []
    for df, src_col in ordered:
        nat = _natural_column_expr(
            src_col,
            cast_to_string=cast_to_string,
            normalize_strings=normalize_strings,
        ).alias(dimension_column)
        projected.append(df.select(nat))

    stacked = projected[0]
    for part in projected[1:]:
        stacked = stacked.unionByName(part, allowMissingColumns=False)

    if exclude_nulls:
        stacked = stacked.where(F.col(dimension_column).isNotNull())

    deduped = stacked.dropDuplicates([dimension_column])

    if include_surrogate_key:
        w = Window.orderBy(F.col(dimension_column).asc_nulls_last())
        out = (
            deduped.withColumn(effective_key, F.dense_rank().over(w).cast("int"))
            .select(effective_key, dimension_column)
            .orderBy(effective_key)
        )
    else:
        out = deduped.select(dimension_column).orderBy(
            F.col(dimension_column).asc_nulls_last()
        )

    if log_distinct_count:
        n = out.count()
        log(f"Built dimension {dimension_column!r} ({n:,} distinct rows)")

    return out
