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
    TimestampType,
)

from fabrictools.core import log
from fabrictools.io import resolve_lakehouse_read_candidate


def _to_snake_case(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", name.strip())
    cleaned = "".join(
        ch for ch in normalized if not unicodedata.combining(ch)
    )
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
        field.name for field in df.schema.fields if isinstance(field.dataType, StringType)
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


# Date-only strings (avoids classifying datetimes as date and dropping the time part).
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


def _log_parsed_date_sample(
    df: DataFrame,
    col_name: str,
    trimmed,
    parsed_date,
) -> None:
    """Materialize a few rows so the coalesce(to_date...) result is visible in logs."""
    rows = (
        df.filter(F.col(col_name).isNotNull())
        .select(
            F.col(col_name).alias("raw"),
            trimmed.alias("trimmed"),
            parsed_date.alias("parsed_date"),
        )
        .limit(_PARSED_DATE_SAMPLE_LIMIT)
        .collect()
    )
    preview = [tuple(row) for row in rows]
    log(
        f"detect_and_cast_columns: column {col_name!r} parsed_date sample "
        f"(raw, trimmed, parsed_date), up to {_PARSED_DATE_SAMPLE_LIMIT} rows: {preview!r}"
    )


def detect_and_cast_columns(df: DataFrame) -> DataFrame:
    """Infer primitive types from string columns and cast when the whole column is consistent.

    Order of detection (first match wins):

    1. **Date** — Non-null values match a date-only shape and parse with
       ``to_date`` using, in order: ISO ``yyyy-MM-dd`` / ``yyyy/M/d``, then
       European hyphen ``dd-MM-yyyy`` / ``d-M-yyyy``, then US hyphen
       ``MM-dd-yyyy`` / ``M-d-yyyy``, then ``dd/MM/yyyy`` and ``d/M/yyyy``, then
       dotted ``dd.MM.yyyy`` / ``d.M.yyyy``, then US ``MM/dd/yyyy`` /
       ``M/d/yyyy`` and ``MM.dd.yyyy`` / ``M.d.yyyy``. European forms are tried
       before US so ambiguous values like ``01-02-2024`` or ``01/02/2024``
       prefer day/month/year.
    2. **Timestamp** — Every non-null value parses with ``to_timestamp`` using,
       in order: ``yyyy-MM-dd HH:mm:ss``, ``dd-MM-yyyy HH:mm:ss`` /
       ``d-M-yyyy HH:mm:ss``, ``MM-dd-yyyy HH:mm:ss`` / ``M-d-yyyy HH:mm:ss``,
       European ``dd/MM/yyyy HH:mm:ss`` / ``d/M/yyyy HH:mm:ss``, US
       ``MM/dd/yyyy HH:mm:ss`` / ``M/d/yyyy HH:mm:ss``,
       then ``yyyy-MM-dd'T'HH:mm:ss``.
    3. **Integer** — All non-null values match ``^[+-]?\\d+$``.
    4. **Double** — All non-null values match a decimal/scientific pattern
       (including ``e`` / ``E``).
    5. **Text** — Otherwise the column stays ``string``.

    Columns that contain only nulls are left unchanged (no inferred type).
    Null cells are preserved for any cast branch.
    """
    transformed_df = df
    string_columns = [
        field.name for field in df.schema.fields if isinstance(field.dataType, StringType)
    ]
    for col_name in string_columns:
        if df.filter(F.col(col_name).isNotNull()).limit(1).count() == 0:
            log(
                f"detect_and_cast_columns: skip column {col_name!r} "
                "(all values null; no cast)"
            )
            continue
        log(
            f"detect_and_cast_columns: evaluating column {col_name!r} "
            f"(string column, non-null sample exists)"
        )
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
        )
        _log_parsed_date_sample(df, col_name, trimmed, parsed_date)
        date_mismatch = df.filter(
            F.col(col_name).isNotNull()
            & ~(trimmed.rlike(_DATE_ONLY_PATTERN) & parsed_date.isNotNull())
        ).limit(1)
        date_mismatch_count = date_mismatch.count()
        log(
            f"detect_and_cast_columns: column {col_name!r} date probe "
            f"date_mismatch_rows={date_mismatch_count} "
            "(0 => treat whole column as date)"
        )
        if date_mismatch_count == 0:
            transformed_df = transformed_df.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(), None).otherwise(parsed_date),
            )
            log(f"Column converted to {DateType().simpleString()}: {col_name}")
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
            F.to_timestamp(trimmed, "yyyy-MM-dd'T'HH:mm:ss"),
        )
        ts_mismatch = df.filter(
            F.col(col_name).isNotNull() & parsed_ts.isNull()
        ).limit(1)
        ts_mismatch_count = ts_mismatch.count()
        log(
            f"detect_and_cast_columns: column {col_name!r} timestamp probe "
            f"ts_mismatch_rows={ts_mismatch_count} "
            "(0 => treat whole column as timestamp)"
        )
        if ts_mismatch_count == 0:
            transformed_df = transformed_df.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(), None).otherwise(parsed_ts),
            )
            log(f"Column converted to {TimestampType().simpleString()}: {col_name}")
            continue

        int_mismatch = df.filter(
            F.col(col_name).isNotNull() & ~trimmed.rlike(_INT_TEXT_PATTERN)
        ).limit(1)
        int_mismatch_count = int_mismatch.count()
        log(
            f"detect_and_cast_columns: column {col_name!r} integer probe "
            f"int_mismatch_rows={int_mismatch_count}"
        )
        if int_mismatch_count == 0:
            transformed_df = transformed_df.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(), None).otherwise(
                    F.col(col_name).cast(IntegerType())
                ),
            )
            log(f"Column converted to {IntegerType().simpleString()}: {col_name}")
            continue

        float_mismatch = df.filter(
            F.col(col_name).isNotNull() & ~trimmed.rlike(_FLOAT_TEXT_PATTERN)
        ).limit(1)
        float_mismatch_count = float_mismatch.count()
        log(
            f"detect_and_cast_columns: column {col_name!r} float probe "
            f"float_mismatch_rows={float_mismatch_count}"
        )
        if float_mismatch_count == 0:
            transformed_df = transformed_df.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(), None).otherwise(
                    F.col(col_name).cast(DoubleType())
                ),
            )
            log(f"Column converted to {DoubleType().simpleString()}: {col_name}")
        else:
            log(
                f"detect_and_cast_columns: column {col_name!r} left as string "
                "(no single-type rule matched)"
            )

    return transformed_df

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