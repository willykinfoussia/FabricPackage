"""Spark DataFrame helpers powered by LLM inference."""

from __future__ import annotations

import re
from typing import Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from fabrictools.ai.inference import ai_response
from fabrictools.core.spark import get_spark
from fabrictools.transform.columns import resolve_dataframe_column

_TEMPLATE_PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


def _resolve_input_column_map(
    df: DataFrame,
    prompt_template: str,
    input_cols: Optional[Sequence[str]],
) -> dict[str, str]:
    """Map template placeholder names to physical column names on ``df``."""
    if input_cols is not None:
        placeholder_names = list(dict.fromkeys(input_cols))
    else:
        placeholder_names = list(dict.fromkeys(_TEMPLATE_PLACEHOLDER_RE.findall(prompt_template)))
        if not placeholder_names:
            raise ValueError(
                "prompt_template has no {placeholders} and input_cols was not provided."
            )

    mapping: dict[str, str] = {}
    for name in placeholder_names:
        physical = resolve_dataframe_column(df, name)
        if physical is None:
            raise ValueError(
                f"input column {name!r} does not resolve on the dataframe"
            )
        mapping[name] = physical
    return mapping


def _collect_ai_results(
    df: DataFrame,
    *,
    prompt_template: str,
    column_map: dict[str, str],
    result_col: str,
    system_prompt: str | None,
    limit: int | None,
) -> DataFrame:
    """Return a two-column dataframe ``(_ai_row_id, result_col)`` with LLM outputs."""
    df_with_id = df.withColumn("_ai_row_id", F.monotonically_increasing_id())
    to_process = df_with_id.limit(limit) if limit is not None else df_with_id
    physical_cols = list(dict.fromkeys(column_map.values()))
    rows = to_process.select("_ai_row_id", *physical_cols).collect()

    results: list[tuple[int, str | None]] = []
    for row in rows:
        values = {
            placeholder: "" if row[physical] is None else str(row[physical])
            for placeholder, physical in column_map.items()
        }
        prompt = prompt_template.format(**values)
        text = ai_response(prompt, system_prompt=system_prompt)
        results.append((row["_ai_row_id"], text))

    spark = get_spark()
    if not results:
        schema = StructType(
            [
                StructField("_ai_row_id", LongType(), False),
                StructField(result_col, StringType(), True),
            ]
        )
        return spark.createDataFrame([], schema)

    result_df = spark.createDataFrame(results, ["_ai_row_id", result_col])
    return result_df


def with_ai_column(
    df: DataFrame,
    output_col: str,
    prompt_template: str,
    *,
    input_cols: Sequence[str] | None = None,
    system_prompt: str | None = None,
    limit: int | None = None,
) -> DataFrame:
    """Add ``output_col`` filled with an LLM response for each row.

    The prompt is built from ``prompt_template`` using ``{column_name}`` placeholders
    resolved on ``df`` (physical, normalized, or snake_case labels).

    Processing runs row-by-row on the Spark driver (one HTTP call per row). Use
    ``limit`` to cap processed rows; unprocessed rows receive null in ``output_col``.

    :param df: Input dataframe.
    :param output_col: Name of the column to add.
    :param prompt_template: Prompt with ``{placeholder}`` tokens for input columns.
    :param input_cols: Explicit placeholder/column names; auto-detected from the template when omitted.
    :param system_prompt: Optional system message for the model.
    :param limit: Maximum number of rows to send to the model.
    :returns: Dataframe with ``output_col`` added.
    :rtype: ~pyspark.sql.DataFrame

    :raises ValueError: When placeholders or columns cannot be resolved.
    :raises AIError: When OpenRouter returns an error.

    .. rubric:: Example

    >>> out = with_ai_column(  # doctest: +SKIP
    ...     df,
    ...     "categorie_ia",
    ...     "Classe ce ticket : {sujet} — {description}",
    ...     input_cols=["sujet", "description"],
    ...     limit=100,
    ... )
    """
    if output_col in df.columns:
        raise ValueError(
            f"with_ai_column: output column {output_col!r} already exists on the dataframe"
        )

    column_map = _resolve_input_column_map(df, prompt_template, input_cols)
    result_df = _collect_ai_results(
        df,
        prompt_template=prompt_template,
        column_map=column_map,
        result_col=output_col,
        system_prompt=system_prompt,
        limit=limit,
    )

    df_with_id = df.withColumn("_ai_row_id", F.monotonically_increasing_id())
    out = df_with_id.join(result_df, on="_ai_row_id", how="left").drop("_ai_row_id")
    return out


def transform_ai_column(
    df: DataFrame,
    column: str,
    prompt_template: str,
    *,
    input_cols: Sequence[str] | None = None,
    system_prompt: str | None = None,
    limit: int | None = None,
) -> DataFrame:
    """Replace ``column`` with an LLM response for each processed row.

    Rows outside ``limit`` (when set) keep their original ``column`` value.

    :param df: Input dataframe.
    :param column: Target column to overwrite (resolved like other transform helpers).
    :param prompt_template: Prompt with ``{placeholder}`` tokens for input columns.
    :param input_cols: Explicit placeholder/column names; auto-detected from the template when omitted.
    :param system_prompt: Optional system message for the model.
    :param limit: Maximum number of rows to send to the model.
    :returns: Dataframe with ``column`` updated.
    :rtype: ~pyspark.sql.DataFrame

    :raises ValueError: When ``column`` or placeholders cannot be resolved.
    :raises AIError: When OpenRouter returns an error.

    .. rubric:: Example

    >>> out = transform_ai_column(  # doctest: +SKIP
    ...     df,
    ...     "resume",
    ...     "Résume en une phrase : {texte_long}",
    ...     system_prompt="Réponds uniquement avec le résumé.",
    ... )
    """
    physical_target = resolve_dataframe_column(df, column)
    if physical_target is None:
        raise ValueError(
            f"transform_ai_column: column {column!r} does not resolve on the dataframe"
        )

    column_map = _resolve_input_column_map(df, prompt_template, input_cols)
    temp_col = "_ai_transform_result"
    result_df = _collect_ai_results(
        df,
        prompt_template=prompt_template,
        column_map=column_map,
        result_col=temp_col,
        system_prompt=system_prompt,
        limit=limit,
    )

    df_with_id = df.withColumn("_ai_row_id", F.monotonically_increasing_id())
    joined = df_with_id.join(result_df, on="_ai_row_id", how="left")
    out = joined.withColumn(
        physical_target,
        F.coalesce(F.col(temp_col), F.col(physical_target)),
    ).drop("_ai_row_id", temp_col)
    return out
