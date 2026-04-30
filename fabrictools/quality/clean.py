"""Pure DataFrame cleaning helpers."""

from __future__ import annotations

import re
import unicodedata
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
)

from fabrictools.core import log
from fabrictools.io import resolve_lakehouse_read_candidate


def _to_snake_case(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", name.strip())
    cleaned = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_").lower()
    if not cleaned:
        return "col"
    if cleaned[0].isdigit():
        return f"col_{cleaned}"
    return cleaned


def _build_unique_column_names(columns: List[str]) -> List[str]:
    seen: dict[str, int] = {}
    result: List[str] = []
    for col_name in columns:
        base = _to_snake_case(col_name)
        count = seen.get(base, 0) + 1
        seen[base] = count
        if count == 1:
            result.append(base)
        else:
            result.append(f"{base}_{count}")
    return result


def _normalized_name_collisions(columns: List[str]) -> dict[str, List[str]]:
    grouped: dict[str, List[str]] = {}
    for col_name in columns:
        normalized = _to_snake_case(col_name)
        grouped.setdefault(normalized, []).append(col_name)
    return {
        normalized: originals
        for normalized, originals in grouped.items()
        if len(originals) > 1
    }


def _replace_empty_strings_with_nulls(df: DataFrame) -> DataFrame:
    string_columns = {
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    }
    if not string_columns:
        return df

    select_exprs = []
    for col_name in df.columns:
        if col_name in string_columns:
            select_exprs.append(
                F.when(F.trim(F.col(col_name)) == "", F.lit(None))
                .otherwise(F.trim(F.col(col_name)))
                .alias(col_name)
            )
        else:
            select_exprs.append(F.col(col_name))
    return df.select(*select_exprs)


# Date-only shape (no time suffix). Used for diagnostics in mismatch logs, not for casting rules.
# Allows 1–2 digit month/day; ISO yyyy-first dash, European dd-MM-yyyy / US MM-dd-yyyy hyphen, slash, dot.
_DATE_ONLY_PATTERN = (
    r"^("
    r"\d{4}-\d{1,2}-\d{1,2}|"
    r"\d{1,2}-\d{1,2}-\d{4}|"
    r"\d{4}/\d{1,2}/\d{1,2}|"
    r"\d{1,2}/\d{1,2}/\d{4}|"
    r"\d{1,2}\.\d{1,2}\.\d{4}|"
    r"\d{4}\.\d{1,2}\.\d{1,2}"
    r")$"
)
_DATE_CANDIDATE_PATTERN = r"^\d{1,4}[-/\.]\d{1,2}[-/\.]\d{1,4}"
_INT_TEXT_PATTERN = r"^[+-]?\d+$"
_FLOAT_TEXT_PATTERN = r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$"
_PARSED_DATE_SAMPLE_LIMIT = 5
_TIME_PARSER_POLICY_KEY = "spark.sql.legacy.timeParserPolicy"
_DATE_FORMATS: tuple[tuple[str, str], ...] = (
    ("date", "yyyy-MM-dd"),
    ("date", "yyyy/M/d"),
    ("date", "dd-MM-yyyy"),
    ("date", "d-M-yyyy"),
    ("date", "MM-dd-yyyy"),
    ("date", "M-d-yyyy"),
    ("date", "dd/MM/yyyy"),
    ("date", "d/M/yyyy"),
    ("date", "dd.MM.yyyy"),
    ("date", "d.M.yyyy"),
    ("date", "MM/dd/yyyy"),
    ("date", "M/d/yyyy"),
    ("date", "MM.dd.yyyy"),
    ("date", "M.d.yyyy"),
    ("timestamp", "M/d/yyyy h:mm:ss a"),
    ("timestamp", "MM/dd/yyyy h:mm:ss a"),
    ("timestamp", "M/d/yyyy h:mm a"),
    ("timestamp", "MM/dd/yyyy h:mm a"),
)
_TIMESTAMP_FORMATS: tuple[str, ...] = (
    "yyyy-MM-dd HH:mm:ss",
    "dd-MM-yyyy HH:mm:ss",
    "d-M-yyyy HH:mm:ss",
    "MM-dd-yyyy HH:mm:ss",
    "M-d-yyyy HH:mm:ss",
    "dd/MM/yyyy HH:mm:ss",
    "d/M/yyyy HH:mm:ss",
    "MM/dd/yyyy HH:mm:ss",
    "M/d/yyyy HH:mm:ss",
    "M/d/yyyy h:mm:ss a",
    "MM/dd/yyyy h:mm:ss a",
    "M/d/yyyy h:mm a",
    "MM/dd/yyyy h:mm a",
    "yyyy-MM-dd'T'HH:mm:ss",
)


def detect_and_cast_columns(df: DataFrame, verbose: bool = False) -> DataFrame:
    """Infer primitive types from string columns and cast when the column is uniform.

    Order of detection (first match wins): **date** (the first ``to_date`` /
    ``to_timestamp`` format that succeeds for every non-null value—European forms
    before US for ambiguous day/month; strings with a trailing time-of-day may still
    yield a calendar day and are cast to ``date``, dropping the time part; US slash
    dates with 12-hour clock and AM/PM suffix are handled via ``h:mm[:ss] a``
    patterns), **timestamp** (the first ``to_timestamp`` format that succeeds for
    every non-null value, including US 12h + AM/PM, 24h, plus ISO ``T``),
    **integer** (full string matches
    ``^[+-]?\\d+$``), **double** (decimal/scientific), else the column remains
    ``string``. Columns that are all-null are skipped; null cells are kept through
    casts.

    Sets ``spark.sql.legacy.timeParserPolicy`` to ``CORRECTED`` so Spark can
    evaluate the returned lazy DataFrame with the same parser policy.

    :param df: Input dataframe.
    :type df: ~pyspark.sql.DataFrame

    :returns: Dataframe with qualifying string columns cast.
    :rtype: ~pyspark.sql.DataFrame
    """
    spark = df.sparkSession
    # Use CORRECTED to let Spark handle ancient dates (before 1582/1900)
    # without throwing and to fix LEGACY failures like 4/14/2026.
    # Note: We do not restore the policy because the returned DataFrame is lazy
    # and requires CORRECTED policy during evaluation (e.g. write/count).
    spark.conf.set(_TIME_PARSER_POLICY_KEY, "CORRECTED")

    string_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]

    if not string_columns:
        return df

    def _get_parsed_date_expr(safe_trimmed, parser: tuple[str, str]):
        parser_kind, parser_format = parser
        if parser_kind == "timestamp":
            return F.to_timestamp(safe_trimmed, parser_format).cast(DateType())
        return F.to_date(safe_trimmed, parser_format)

    def _get_parsed_ts_expr(safe_trimmed, parser_format: str):
        return F.to_timestamp(safe_trimmed, parser_format)

    def _date_fail_key(col_name: str, idx: int) -> str:
        return f"{col_name}__date_fail_{idx}"

    def _ts_fail_key(col_name: str, idx: int) -> str:
        return f"{col_name}__ts_fail_{idx}"

    agg_exprs = []
    for col_name in string_columns:
        col_expr = F.col(col_name)
        trimmed = F.trim(col_expr)

        agg_exprs.append(
            F.sum(F.when(col_expr.isNotNull(), 1).otherwise(0)).alias(
                f"{col_name}__nn"
            )
        )
        agg_exprs.append(
            F.sum(
                F.when(
                    col_expr.isNotNull() & ~trimmed.rlike(_INT_TEXT_PATTERN),
                    1,
                ).otherwise(0)
            ).alias(f"{col_name}__int_fail")
        )
        agg_exprs.append(
            F.sum(
                F.when(
                    col_expr.isNotNull() & ~trimmed.rlike(_FLOAT_TEXT_PATTERN),
                    1,
                ).otherwise(0)
            ).alias(f"{col_name}__float_fail")
        )

        safe_trimmed = F.when(
            trimmed.rlike(_DATE_CANDIDATE_PATTERN), trimmed
        ).otherwise(F.lit(None))

        for idx, date_parser in enumerate(_DATE_FORMATS):
            parsed_date = _get_parsed_date_expr(safe_trimmed, date_parser)
            agg_exprs.append(
                F.sum(
                    F.when(
                        col_expr.isNotNull() & parsed_date.isNull(), 1
                    ).otherwise(0)
                ).alias(_date_fail_key(col_name, idx))
            )
        for idx, ts_parser in enumerate(_TIMESTAMP_FORMATS):
            parsed_ts = _get_parsed_ts_expr(safe_trimmed, ts_parser)
            agg_exprs.append(
                F.sum(
                    F.when(
                        col_expr.isNotNull() & parsed_ts.isNull(), 1
                    ).otherwise(0)
                ).alias(_ts_fail_key(col_name, idx))
            )

    stats = df.agg(*agg_exprs).collect()[0].asDict() if agg_exprs else {}

    select_exprs = []
    for col_name in df.columns:
        if col_name not in string_columns:
            select_exprs.append(F.col(col_name))
            continue

        nn = stats.get(f"{col_name}__nn", 0)
        if nn == 0:
            select_exprs.append(F.col(col_name))
            continue

        int_fail = stats.get(f"{col_name}__int_fail", 0)
        float_fail = stats.get(f"{col_name}__float_fail", 0)

        col_expr = F.col(col_name)
        trimmed = F.trim(col_expr)
        safe_trimmed = F.when(
            trimmed.rlike(_DATE_CANDIDATE_PATTERN), trimmed
        ).otherwise(F.lit(None))

        date_parser = next(
            (
                parser
                for idx, parser in enumerate(_DATE_FORMATS)
                if stats.get(_date_fail_key(col_name, idx), nn) == 0
            ),
            None,
        )
        ts_parser = next(
            (
                parser
                for idx, parser in enumerate(_TIMESTAMP_FORMATS)
                if stats.get(_ts_fail_key(col_name, idx), nn) == 0
            ),
            None,
        )

        if date_parser is not None:
            parsed_date = _get_parsed_date_expr(safe_trimmed, date_parser)
            select_exprs.append(
                F.when(col_expr.isNull(), None).otherwise(parsed_date).alias(col_name)
            )
        elif ts_parser is not None:
            parsed_ts = _get_parsed_ts_expr(safe_trimmed, ts_parser)
            select_exprs.append(
                F.when(col_expr.isNull(), None).otherwise(parsed_ts).alias(col_name)
            )
        elif int_fail == 0:
            select_exprs.append(
                F.when(col_expr.isNull(), None)
                .otherwise(col_expr.cast(IntegerType()))
                .alias(col_name)
            )
        elif float_fail == 0:
            select_exprs.append(
                F.when(col_expr.isNull(), None)
                .otherwise(col_expr.cast(DoubleType()))
                .alias(col_name)
            )
        else:
            select_exprs.append(col_expr)

    return df.select(*select_exprs)


def add_silver_metadata(
    df: DataFrame,
    source_lakehouse_name: str,
    source_relative_path: str,
    source_layer: str = "bronze",
    ingestion_timestamp_col: str = "ingestion_timestamp",
    source_layer_col: str = "ingestion_source_layer",
    source_path_col: str = "ingestion_source_path",
    year_col: str = "ingestion_year",
    month_col: str = "ingestion_month",
    day_col: str = "ingestion_day",
    spark: Optional[SparkSession] = None,
    verbose: bool = False,
) -> DataFrame:
    """Add Silver-layer metadata columns (ingestion time, source path, date parts).

    Resolves ``source_relative_path`` with
    :py:func:`fabrictools.io.lakehouse.resolve_lakehouse_read_candidate`. Date
    partition columns (``year_col`` / ``month_col`` / ``day_col``) are derived from
    the current ingestion date.

    :param df: Bronze or intermediate dataframe.
    :param source_lakehouse_name: Source Lakehouse display name.
    :param source_relative_path: Source path passed to path resolution.
    :param source_layer: Literal stored in ``source_layer_col`` (default ``bronze``).
    :param ingestion_timestamp_col: Column name for ``current_timestamp()``.
    :param source_layer_col: Column name for the layer literal.
    :param source_path_col: Column name for the resolved relative path string.
    :param year_col: Partition year column name.
    :param month_col: Partition month column name.
    :param day_col: Partition day-of-month column name.
    :param spark: Optional ``SparkSession`` for path resolution.
    :type df: ~pyspark.sql.DataFrame
    :type source_lakehouse_name: str
    :type source_relative_path: str
    :type source_layer: str
    :type ingestion_timestamp_col: str
    :type source_layer_col: str
    :type source_path_col: str
    :type year_col: str
    :type month_col: str
    :type day_col: str
    :type spark: ~pyspark.sql.SparkSession | None

    :returns: ``df`` with metadata and partition columns appended/overwritten.
    :rtype: ~pyspark.sql.DataFrame

    .. rubric:: Example

    >>> silver_df = add_silver_metadata(  # doctest: +SKIP
    ...     bronze_df,
    ...     source_lakehouse_name="BronzeLakehouse",
    ...     source_relative_path="dbo.RawOrders",
    ... )
    """
    resolved_source_path = resolve_lakehouse_read_candidate(
        lakehouse_name=source_lakehouse_name,
        relative_path=source_relative_path,
        spark=spark,
    )
    current_date_expr = F.current_date()

    metadata_df = (
        df.withColumn(ingestion_timestamp_col, F.current_timestamp())
        .withColumn(source_layer_col, F.lit(source_layer))
        .withColumn(source_path_col, F.lit(resolved_source_path))
        .withColumn(year_col, F.year(current_date_expr))
        .withColumn(month_col, F.month(current_date_expr))
        .withColumn(day_col, F.dayofmonth(current_date_expr))
    )
    if verbose:
        log(
            "Silver metadata added: "
            f"{ingestion_timestamp_col}, {source_layer_col}, {source_path_col}, "
            f"{year_col}, {month_col}, {day_col} "
            "(partition source: current_date())"
        )
    return metadata_df


def clean_data(
    df: DataFrame,
    drop_duplicates: bool = True,
    drop_all_null_rows: bool = True,
    verbose: bool = False,
) -> DataFrame:
    """Normalize names, trim empty strings to null, infer types, optionally dedupe.

    Renames columns to unique snake_case (via internal helpers), replaces blank
    strings with null on string columns, runs :py:func:`detect_and_cast_columns`,
    then optionally drops duplicate rows and rows that are all-null.

    :param df: Input dataframe.
    :param drop_duplicates: If ``True``, call ``dropDuplicates()`` after cleaning.
    :param drop_all_null_rows: If ``True``, call ``dropna(how="all")``.
    :type df: ~pyspark.sql.DataFrame
    :type drop_duplicates: bool
    :type drop_all_null_rows: bool

    :returns: Cleaned dataframe.
    :rtype: ~pyspark.sql.DataFrame

    .. rubric:: Example

    >>> cleaned = clean_data(raw_df, drop_duplicates=True, drop_all_null_rows=True)  # doctest: +SKIP
    """
    before_cols = len(df.columns)

    normalized_columns = _build_unique_column_names(df.columns)
    cleaned_df = df.toDF(*normalized_columns)
    cleaned_df = _replace_empty_strings_with_nulls(cleaned_df)
    cleaned_df = detect_and_cast_columns(cleaned_df, verbose=verbose)

    if drop_duplicates:
        cleaned_df = cleaned_df.dropDuplicates()
    if drop_all_null_rows:
        cleaned_df = cleaned_df.dropna(how="all")

    after_cols = len(cleaned_df.columns)
    if verbose:
        log(
            f"Data cleaned: columns {before_cols} -> {after_cols}"
        )
    return cleaned_df


__all__ = [
    "clean_data",
    "add_silver_metadata",
    "detect_and_cast_columns",
    "_to_snake_case",
    "_build_unique_column_names",
    "_normalized_name_collisions",
    "_replace_empty_strings_with_nulls",
]

if __name__ == "__main__":
    print("Test")
