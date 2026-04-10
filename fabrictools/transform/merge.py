"""Join a main DataFrame to a right side with prefixed column names from the right alias."""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _join_df_prefix(join_df: DataFrame) -> str:
    analyzed = join_df._jdf.queryExecution().analyzed()

    def walk(node) -> str | None:
        try:
            if node.getClass().getSimpleName() == "SubqueryAlias":
                ident = node.alias()
                text = ident.toString() if hasattr(ident, "toString") else str(ident)
                return text.strip("`\"")
        except Exception:
            pass
        try:
            it = node.children().iterator()
            while it.hasNext():
                found = walk(it.next())
                if found is not None:
                    return found
        except Exception:
            pass
        return None

    prefix = walk(analyzed)
    if not prefix:
        raise ValueError(
            "join_df must use .alias('<prefix>') so prefixed column names are defined, "
            "e.g. projets.alias('projets')."
        )
    return prefix


def _require_columns(df: DataFrame, names: Sequence[str], side: str) -> None:
    cols = {f.name for f in df.schema.fields}
    for n in names:
        if n not in cols:
            raise ValueError(f"{side} DataFrame has no column {n!r}")


def merge_dataframes(
    main: DataFrame,
    join_df: DataFrame,
    join_columns: Sequence[str],
    keys: Sequence[tuple[str, str]],
    how: str = "left",
) -> DataFrame:
    """
    Join ``main`` to ``join_df`` using ``keys`` and add right attributes as ``{prefix}_{col}``.

    ``join_df`` must be the result of ``right.alias('<prefix>')``. The prefix is read from
    Spark's logical plan (``SubqueryAlias``), not passed as a separate argument.

    Parameters
    ----------
    main
        Left DataFrame.
    join_df
        Right DataFrame, already aliased (e.g. ``projets.alias('projets')``).
    join_columns
        Column names on the right to include in the result (each renamed to ``prefix_name``).
    keys
        Pairs ``(main_column, join_column)`` combined with AND.
    how
        Spark join type, e.g. ``left``, ``inner``.
    """
    if not keys:
        raise ValueError("keys must contain at least one (main_key, join_key) pair")

    prefix = _join_df_prefix(join_df)
    join_keys_rhs = [jk for _, jk in keys]
    _require_columns(main, [mk for mk, _ in keys], "main")
    _require_columns(join_df, join_keys_rhs, "join_df")
    _require_columns(join_df, join_columns, "join_df")

    temp_names = [f"_ft_join_k{i}" for i in range(len(keys))]
    exprs = []
    for i, (_mk, jk) in enumerate(keys):
        exprs.append(F.col(jk).alias(temp_names[i]))
    for col in join_columns:
        exprs.append(F.col(col).alias(f"{prefix}_{col}"))

    right_proj = join_df.select(*exprs)

    cond = None
    for i, (mk, _jk) in enumerate(keys):
        part = F.col(mk) == F.col(temp_names[i])
        cond = part if cond is None else (cond & part)

    out = main.join(right_proj, cond, how)
    for tn in temp_names:
        out = out.drop(tn)
    return out
