"""Build one dimension from one or many PySpark sources."""

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


def _normalize_sources(
    sources: Sequence[tuple[DataFrame, ...]],
    dimension_columns: list[str],
) -> list[tuple[DataFrame, list[str]]]:
    """Normalize accepted source syntaxes to canonical ``(df, [source_cols...])`` blocks."""
    normalized: list[tuple[DataFrame, list[str]]] = []
    n_dim = len(dimension_columns)
    ordered = list(sources)
    if not ordered:
        raise ValueError("sources must contain at least one source definition")

    grouped_entries: list[tuple[DataFrame, list[str]]] = []
    detailed_entries: list[tuple[DataFrame, str]] = []
    for entry in ordered:
        if len(entry) < 2:
            raise ValueError(
                "each source entry must contain a dataframe and at least one source column"
            )
        df = entry[0]
        source_cols = [str(col) for col in entry[1:]]
        if len(source_cols) == 1:
            detailed_entries.append((df, source_cols[0]))
        elif len(source_cols) == n_dim:
            grouped_entries.append((df, source_cols))
        else:
            raise ValueError(
                "invalid source entry width: each entry must define either exactly one "
                f"column or exactly {n_dim} columns to match dimension_columns"
            )

    if grouped_entries and detailed_entries:
        raise ValueError(
            "ambiguous sources format: use only grouped entries like "
            "[(df, 'c1', 'c2')] or only detailed entries like "
            "[(df, 'c1'), (df, 'c2')]"
        )

    if grouped_entries:
        normalized.extend(grouped_entries)
        return normalized

    if len(detailed_entries) % n_dim != 0:
        raise ValueError(
            "detailed sources count must be a multiple of dimension_columns length "
            f"({n_dim})"
        )

    for i in range(0, len(detailed_entries), n_dim):
        chunk = detailed_entries[i : i + n_dim]
        chunk_dfs = [part[0] for part in chunk]
        first_df = chunk_dfs[0]
        if any(df is not first_df for df in chunk_dfs[1:]):
            raise ValueError(
                "each detailed source block must refer to the same dataframe in order, "
                "e.g. [(df, 'col1'), (df, 'col2')]"
            )
        chunk_cols = [part[1] for part in chunk]
        normalized.append((first_df, chunk_cols))

    return normalized


def build_dimension_from_columns(
    sources: Sequence[tuple[DataFrame, ...]],
    dimension_columns: Sequence[str],
    *,
    surrogate_key_column: str | None = None,
    include_surrogate_key: bool = True,
    exclude_nulls: bool = True,
    normalize_strings: bool = True,
    cast_to_string: bool = True,
    log_distinct_count: bool = False,
) -> DataFrame:
    """Union distinct dimension rows from many dataframes into one dimension table.

    ``sources`` accepts two equivalent syntaxes:
    - grouped: ``[(df, "source_col_1", "source_col_2")]``
    - detailed: ``[(df, "source_col_1"), (df, "source_col_2")]``
    Both map positionally to ``dimension_columns``.
    Duplicate rows are collapsed once after union (single shuffle for dedupe).
    Optionally adds a deterministic surrogate integer key ordered by natural columns.

    :param sources: Non-empty sequence of source definitions. Each definition must provide
        either one source column (detailed syntax) or exactly ``len(dimension_columns)``
        source columns (grouped syntax).
    :param dimension_columns: Output natural key column names (e.g.
        ``["ID", "Compagnie"]``).
    :param surrogate_key_column: Name of the surrogate key column when
        ``include_surrogate_key`` is ``True``. When ``None``, uses ``"dimension_key"``.
    :param include_surrogate_key: When ``False``, return only natural columns (deduped,
        ordered by natural columns).
    :param exclude_nulls: When ``True``, drop rows where at least one natural key column
        is null after cast/trim.
    :param normalize_strings: When ``True``, apply :py:func:`~pyspark.sql.functions.trim`
        after any string cast (with ``cast_to_string=True``, values are trimmed as strings).
    :param cast_to_string: When ``True``, cast every source column to ``string`` so values
        from heterogeneous types merge safely (recommended for label dimensions).
    :param log_distinct_count: When ``True``, run an action and log the resulting row count.

    :returns: Deduped dimension with optional surrogate key first, then natural columns.

    :raises ValueError: If ``dimension_columns`` is empty, sources are malformed, source
        columns are missing, Spark sessions differ, or surrogate and natural names collide.
    """
    dims = [str(col) for col in dimension_columns]
    if not dims:
        raise ValueError("dimension_columns must contain at least one column name")

    normalized_sources = _normalize_sources(sources=sources, dimension_columns=dims)
    base_key = _session_key(normalized_sources[0][0])
    for df, src_cols in normalized_sources:
        if len(src_cols) != len(dims):
            raise ValueError(
                "each normalized source block must match dimension_columns length "
                f"({len(dims)})"
            )
        missing = [src_col for src_col in src_cols if src_col not in df.columns]
        if missing:
            raise ValueError(
                f"columns {missing!r} not present in dataframe columns "
                f"{sorted(df.columns)!r}"
            )
        if _session_key(df) != base_key:
            raise ValueError(
                "all source dataframes must use the same SparkSession as the first source"
            )

    if include_surrogate_key:
        effective_key = surrogate_key_column or "dimension_key"
        if effective_key in dims:
            raise ValueError(
                "surrogate_key_column must differ from natural dimension columns "
                f"(collision with {effective_key!r})"
            )
    else:
        effective_key = ""

    projected: list[DataFrame] = []
    for df, src_cols in normalized_sources:
        selected = []
        for src_col, dim_col in zip(src_cols, dims):
            selected.append(
                _natural_column_expr(
                    src_col,
                    cast_to_string=cast_to_string,
                    normalize_strings=normalize_strings,
                ).alias(dim_col)
            )
        projected.append(df.select(*selected))

    stacked = projected[0]
    for part in projected[1:]:
        stacked = stacked.unionByName(part, allowMissingColumns=False)

    if exclude_nulls:
        not_null_condition = F.col(dims[0]).isNotNull()
        for dim_col in dims[1:]:
            not_null_condition = not_null_condition & F.col(dim_col).isNotNull()
        stacked = stacked.where(not_null_condition)

    deduped = stacked.dropDuplicates(dims)

    if include_surrogate_key:
        order_cols = [F.col(dim_col).asc_nulls_last() for dim_col in dims]
        w = Window.orderBy(*order_cols)
        out = (
            deduped.withColumn(effective_key, F.dense_rank().over(w).cast("int"))
            .select(effective_key, *dims)
            .orderBy(effective_key)
        )
    else:
        out = deduped.select(*dims).orderBy(
            *[F.col(dim_col).asc_nulls_last() for dim_col in dims]
        )

    if log_distinct_count:
        n = out.count()
        log(f"Built dimension columns {dims!r} ({n:,} distinct rows)")

    return out
