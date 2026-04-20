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
    string_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]
    transformed_df = df
    for col_name in string_columns:
        transformed_df = transformed_df.withColumn(
            col_name,
            F.when(F.trim(F.col(col_name)) == "", F.lit(None)).otherwise(
                F.trim(F.col(col_name))
            ),
        )
    return transformed_df


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
_INT_TEXT_PATTERN = r"^[+-]?\d+$"
_FLOAT_TEXT_PATTERN = r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$"
_PARSED_DATE_SAMPLE_LIMIT = 5
_TIME_PARSER_POLICY_KEY = "spark.sql.legacy.timeParserPolicy"


def detect_and_cast_columns(df: DataFrame) -> DataFrame:
    """Infer primitive types from string columns and cast when the column is uniform.

    Order of detection (first match wins): **date** (uniform non-null success of a
    ``to_date`` / ``to_timestamp`` chain over several patterns—European forms before
    US for ambiguous day/month; strings with a trailing time-of-day may still yield a
    calendar day and are cast to ``date``, dropping the time part; US slash dates with
    12-hour clock and AM/PM suffix are handled via ``h:mm[:ss] a`` patterns), **timestamp**
    (``to_timestamp`` with several patterns including US 12h + AM/PM, 24h, plus ISO ``T``),
    **integer** (full string matches
    ``^[+-]?\\d+$``), **double** (decimal/scientific), else the column remains
    ``string``. Columns that are all-null are skipped; null cells are kept through
    casts.

    Sets ``spark.sql.legacy.timeParserPolicy`` to ``LEGACY`` for the duration of the
    call and restores the previous session value afterward.

    :param df: Input dataframe.
    :type df: ~pyspark.sql.DataFrame

    :returns: Dataframe with qualifying string columns cast.
    :rtype: ~pyspark.sql.DataFrame
    """
    spark = df.sparkSession
    previous_time_parser_policy = spark.conf.get(_TIME_PARSER_POLICY_KEY, None)
    spark.conf.set(_TIME_PARSER_POLICY_KEY, "LEGACY")
    try:
        transformed_df = df
        string_columns = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StringType)
        ]
        for col_name in string_columns:
            if df.filter(F.col(col_name).isNotNull()).limit(1).count() == 0:
                continue
            trimmed = F.trim(F.col(col_name))
            parsed_date = F.coalesce(
                F.to_date(trimmed, "yyyy-MM-dd"),
                F.to_date(trimmed, "yyyy/M/d"),
                F.to_date(trimmed, "dd-MM-yyyy"),
                F.to_date(trimmed, "d-M-yyyy"),
                F.to_date(trimmed, "MM-dd-yyyy"),
                F.to_date(trimmed, "M-d-yyyy"),
                F.to_date(trimmed, "dd/MM/yyyy"),
                F.to_date(trimmed, "d/M/yyyy"),
                F.to_date(trimmed, "dd.MM.yyyy"),
                F.to_date(trimmed, "d.M.yyyy"),
                F.to_date(trimmed, "MM/dd/yyyy"),
                F.to_date(trimmed, "M/d/yyyy"),
                F.to_date(trimmed, "MM.dd.yyyy"),
                F.to_date(trimmed, "M.d.yyyy"),
                F.to_timestamp(trimmed, "M/d/yyyy h:mm:ss a").cast(DateType()),
                F.to_timestamp(trimmed, "MM/dd/yyyy h:mm:ss a").cast(DateType()),
                F.to_timestamp(trimmed, "M/d/yyyy h:mm a").cast(DateType()),
                F.to_timestamp(trimmed, "MM/dd/yyyy h:mm a").cast(DateType()),
            )
            date_mismatch = df.filter(
                F.col(col_name).isNotNull() & parsed_date.isNull()
            ).limit(1)
            date_mismatch_count = date_mismatch.count()
            if date_mismatch_count == 0:
                transformed_df = transformed_df.withColumn(
                    col_name,
                    F.when(F.col(col_name).isNull(), None).otherwise(parsed_date),
                )
                continue

            parsed_ts = F.coalesce(
                F.to_timestamp(trimmed, "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(trimmed, "dd-MM-yyyy HH:mm:ss"),
                F.to_timestamp(trimmed, "d-M-yyyy HH:mm:ss"),
                F.to_timestamp(trimmed, "MM-dd-yyyy HH:mm:ss"),
                F.to_timestamp(trimmed, "M-d-yyyy HH:mm:ss"),
                F.to_timestamp(trimmed, "dd/MM/yyyy HH:mm:ss"),
                F.to_timestamp(trimmed, "d/M/yyyy HH:mm:ss"),
                F.to_timestamp(trimmed, "MM/dd/yyyy HH:mm:ss"),
                F.to_timestamp(trimmed, "M/d/yyyy HH:mm:ss"),
                F.to_timestamp(trimmed, "M/d/yyyy h:mm:ss a"),
                F.to_timestamp(trimmed, "MM/dd/yyyy h:mm:ss a"),
                F.to_timestamp(trimmed, "M/d/yyyy h:mm a"),
                F.to_timestamp(trimmed, "MM/dd/yyyy h:mm a"),
                F.to_timestamp(trimmed, "yyyy-MM-dd'T'HH:mm:ss"),
            )
            ts_mismatch = df.filter(
                F.col(col_name).isNotNull() & parsed_ts.isNull()
            ).limit(1)
            ts_mismatch_count = ts_mismatch.count()
            if ts_mismatch_count == 0:
                transformed_df = transformed_df.withColumn(
                    col_name,
                    F.when(F.col(col_name).isNull(), None).otherwise(parsed_ts),
                )
                continue

            int_mismatch = df.filter(
                F.col(col_name).isNotNull() & ~trimmed.rlike(_INT_TEXT_PATTERN)
            ).limit(1)
            int_mismatch_count = int_mismatch.count()
            if int_mismatch_count == 0:
                transformed_df = transformed_df.withColumn(
                    col_name,
                    F.when(F.col(col_name).isNull(), None).otherwise(
                        F.col(col_name).cast(IntegerType())
                    ),
                )
                continue

            float_mismatch = df.filter(
                F.col(col_name).isNotNull() & ~trimmed.rlike(_FLOAT_TEXT_PATTERN)
            ).limit(1)
            float_mismatch_count = float_mismatch.count()
            if float_mismatch_count == 0:
                transformed_df = transformed_df.withColumn(
                    col_name,
                    F.when(F.col(col_name).isNull(), None).otherwise(
                        F.col(col_name).cast(DoubleType())
                    ),
                )
            else:
                mismatch_row = float_mismatch.collect()
                if mismatch_row:
                    bad_value = mismatch_row[0][col_name]
                    log(
                        f"Column '{col_name}' could not be cast to a number. Mismatch value: {bad_value!r}",
                        level="warning",
                    )
                continue
        return transformed_df
    finally:
        if previous_time_parser_policy is None:
            spark.conf.unset(_TIME_PARSER_POLICY_KEY)
        else:
            spark.conf.set(_TIME_PARSER_POLICY_KEY, previous_time_parser_policy)


def add_silver_metadata(
    df: DataFrame,
    source_lakehouse_name: str,
    source_relative_path: str,
    source_layer: str = "bronze",
    ingestion_timestamp_col: str = "_ingestion_timestamp",
    source_layer_col: str = "_source_layer",
    source_path_col: str = "_source_path",
    year_col: str = "_year",
    month_col: str = "_month",
    day_col: str = "_day",
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """Add Silver-layer metadata columns (ingestion time, source path, date parts).

    Resolves ``source_relative_path`` with
    :py:func:`fabrictools.io.lakehouse.resolve_lakehouse_read_candidate`. Date
    partition columns (``year_col`` / ``month_col`` / ``day_col``) are derived from
    the first date/timestamp column on ``df`` (excluding ``ingestion_timestamp_col``),
    or from ``ingestion_timestamp_col`` if none.

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
    partition_source_col = next(
        (
            field.name
            for field in df.schema.fields
            if field.dataType.typeName()
            in {"date", "timestamp", "timestamp_ntz", "timestamp_ltz"}
            and field.name != ingestion_timestamp_col
        ),
        None,
    )

    resolved_source_path = resolve_lakehouse_read_candidate(
        lakehouse_name=source_lakehouse_name,
        relative_path=source_relative_path,
        spark=spark,
    )

    partition_expression = F.col(partition_source_col or ingestion_timestamp_col)

    metadata_df = (
        df.withColumn(ingestion_timestamp_col, F.current_timestamp())
        .withColumn(source_layer_col, F.lit(source_layer))
        .withColumn(source_path_col, F.lit(resolved_source_path))
        .withColumn(year_col, F.year(partition_expression))
        .withColumn(month_col, F.month(partition_expression))
        .withColumn(day_col, F.dayofmonth(partition_expression))
    )
    partition_source_label = partition_source_col or ingestion_timestamp_col
    log(
        "Silver metadata added: "
        f"{ingestion_timestamp_col}, {source_layer_col}, {source_path_col}, "
        f"{year_col}, {month_col}, {day_col} "
        f"(partition source: {partition_source_label})"
    )
    return metadata_df


def clean_data(
    df: DataFrame,
    drop_duplicates: bool = True,
    drop_all_null_rows: bool = True,
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
    before_rows = df.count()
    before_cols = len(df.columns)

    normalized_columns = _build_unique_column_names(df.columns)
    cleaned_df = df.toDF(*normalized_columns)
    cleaned_df = _replace_empty_strings_with_nulls(cleaned_df)
    cleaned_df = detect_and_cast_columns(cleaned_df)

    if drop_duplicates:
        cleaned_df = cleaned_df.dropDuplicates()
    if drop_all_null_rows:
        cleaned_df = cleaned_df.dropna(how="all")

    after_rows = cleaned_df.count()
    after_cols = len(cleaned_df.columns)
    log(
        f"Data cleaned: rows {before_rows:,} -> {after_rows:,} | "
        f"columns {before_cols} -> {after_cols}"
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
