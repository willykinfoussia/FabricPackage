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


def to_snake_case(name: str) -> str:
    """Normalize a label to snake_case (same rules as :py:func:`fabrictools.clean_data`).

    Strips accents, replaces non-alphanumeric runs with ``_``, collapses repeated
    underscores, lowercases, and prefixes with ``col_`` when the result starts with a digit.
    Empty input yields ``"col"``.

    :param name: Source label (e.g. column name, file name, join prefix).
    :type name: str

    :returns: Snake-case identifier.
    :rtype: str

    .. rubric:: Example

    >>> to_snake_case("OIT avril 2026.xlsx")  # doctest: +SKIP
    'oit_avril_2026_xlsx'
    >>> to_snake_case("n_commande_OIT avril 2026")  # doctest: +SKIP
    'n_commande_oit_avril_2026'
    """
    normalized = unicodedata.normalize("NFKD", name.strip())
    cleaned = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_").lower()
    if not cleaned:
        return "col"
    if cleaned[0].isdigit():
        return f"col_{cleaned}"
    return cleaned


_to_snake_case = to_snake_case


def _build_unique_column_names(columns: List[str]) -> List[str]:
    normalized = [_to_snake_case(col_name) for col_name in columns]
    is_odata = "odata_context" in normalized

    seen: dict[str, int] = {}
    result: List[str] = []
    for base in normalized:
        if is_odata and base != "odata_context" and base.startswith("value_"):
            base = base[len("value_") :]
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


def _format_separator(format_str: str) -> str | None:
    if "'T'" in format_str:
        return "T"
    for sep in ("-", "/", "."):
        if sep in format_str:
            return sep
    return None


def _sample_date_part(sample: str) -> str:
    return re.split(r"[Tt]", sample.strip())[0].split()[0]


def _sample_looks_date_like(sample: str | None) -> bool:
    if not sample or not str(sample).strip():
        return False
    s = str(sample).strip()
    if re.match(_DATE_CANDIDATE_PATTERN, s):
        return True
    return bool(re.match(r"^\d{4}-\d{1,2}-\d{1,2}[Tt]", s))


def _candidate_date_format_indices(sample: str | None) -> list[int]:
    if not _sample_looks_date_like(sample):
        return []

    assert sample is not None
    s = sample.strip()
    date_part = _sample_date_part(s)
    sample_has_ampm = bool(re.search(r"\b[AP]M\b", s, re.I))
    yyyy_first = bool(re.match(r"^\d{4}", date_part))

    sample_sep: str | None = None
    if "T" in s.upper() and re.match(r"^\d{4}-\d{1,2}-\d{1,2}[Tt]", s):
        sample_sep = "T"
    else:
        for sep in ("-", "/", "."):
            if sep in date_part:
                sample_sep = sep
                break

    indices: list[int] = []
    for idx, (kind, fmt) in enumerate(_DATE_FORMATS):
        if sample_has_ampm and kind == "date" and " a" not in fmt:
            continue
        if not sample_has_ampm and kind == "timestamp" and " a" in fmt:
            continue

        fmt_sep = _format_separator(fmt)
        if sample_sep == "T":
            if fmt_sep != "T" and not (fmt_sep == "-" and fmt.startswith("yyyy")):
                if fmt_sep != "-":
                    continue
        elif sample_sep and fmt_sep and fmt_sep != sample_sep:
            continue

        fmt_yyyy_first = fmt.startswith("yyyy")
        if yyyy_first and not fmt_yyyy_first:
            continue
        if not yyyy_first and fmt_yyyy_first:
            continue

        indices.append(idx)

    if indices:
        return indices
    return list(range(len(_DATE_FORMATS)))


def _candidate_timestamp_format_indices(sample: str | None) -> list[int]:
    if not _sample_looks_date_like(sample):
        return []

    assert sample is not None
    s = sample.strip()
    sample_has_ampm = bool(re.search(r"\b[AP]M\b", s, re.I))
    sample_has_time = bool(re.search(r"[Tt]\d|\s+\d{1,2}:\d", s))
    if not sample_has_time and not sample_has_ampm:
        return []

    date_part = _sample_date_part(s)
    yyyy_first = bool(re.match(r"^\d{4}", date_part))

    sample_sep: str | None = None
    if "T" in s.upper() and re.match(r"^\d{4}-\d{1,2}-\d{1,2}[Tt]", s):
        sample_sep = "T"
    else:
        for sep in ("-", "/", "."):
            if sep in date_part:
                sample_sep = sep
                break

    indices: list[int] = []
    for idx, fmt in enumerate(_TIMESTAMP_FORMATS):
        fmt_has_ampm = " a" in fmt
        if sample_has_ampm and not fmt_has_ampm:
            continue
        if not sample_has_ampm and fmt_has_ampm:
            continue

        fmt_sep = _format_separator(fmt)
        if sample_sep == "T":
            if "'T'" not in fmt and fmt_sep != "-":
                continue
        elif sample_sep and fmt_sep and fmt_sep != sample_sep:
            continue

        fmt_yyyy_first = fmt.startswith("yyyy")
        if yyyy_first and not fmt_yyyy_first:
            continue
        if not yyyy_first and fmt_yyyy_first:
            continue

        indices.append(idx)

    if indices:
        return indices
    return list(range(len(_TIMESTAMP_FORMATS)))


def _normalized_string_expr(col_expr, *, normalize_strings: bool):
    trimmed = F.trim(col_expr)
    if normalize_strings:
        return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)
    return trimmed


def detect_and_cast_columns(
    df: DataFrame,
    verbose: bool = False,
    *,
    normalize_strings: bool = False,
) -> DataFrame:
    """Infer primitive types from string columns and cast when the column is uniform.

    Uses a two-pass strategy: a lightweight first aggregation collects per-column
    integer/float failure counts plus one representative non-empty sample value;
    candidate date/timestamp formats are derived from that sample on the driver,
    then a second aggregation validates only those formats across all rows.

    Order of detection (first match wins): **date**, **timestamp**, **integer**,
    **double**, else **string**. Columns that are all-null are skipped.

    When ``normalize_strings`` is ``True``, string columns are trimmed and blank
    strings are converted to null in the final projection (same behavior as
    :py:func:`_replace_empty_strings_with_nulls`).

    Sets ``spark.sql.legacy.timeParserPolicy`` to ``CORRECTED`` so Spark can
    evaluate the returned lazy DataFrame with the same parser policy.

    :param df: Input dataframe.
    :param verbose: Reserved for future logging; currently unused.
    :param normalize_strings: If ``True``, trim strings and map ``""`` to null.
    :type df: ~pyspark.sql.DataFrame
    :type verbose: bool
    :type normalize_strings: bool

    :returns: Dataframe with qualifying string columns cast.
    :rtype: ~pyspark.sql.DataFrame
    """
    _ = verbose
    spark = df.sparkSession
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

    phase1_exprs = []
    for col_name in string_columns:
        col_expr = F.col(col_name)
        trimmed = F.trim(col_expr)
        value_expr = (
            F.when(trimmed == "", F.lit(None)).otherwise(trimmed)
            if normalize_strings
            else trimmed
        )

        phase1_exprs.append(
            F.sum(F.when(value_expr.isNotNull(), 1).otherwise(0)).alias(f"{col_name}__nn")
        )
        phase1_exprs.append(
            F.sum(
                F.when(
                    value_expr.isNotNull() & ~value_expr.rlike(_INT_TEXT_PATTERN),
                    1,
                ).otherwise(0)
            ).alias(f"{col_name}__int_fail")
        )
        phase1_exprs.append(
            F.sum(
                F.when(
                    value_expr.isNotNull() & ~value_expr.rlike(_FLOAT_TEXT_PATTERN),
                    1,
                ).otherwise(0)
            ).alias(f"{col_name}__float_fail")
        )
        phase1_exprs.append(
            F.first(value_expr, ignorenulls=True).alias(f"{col_name}__sample")
        )

    phase1_stats = df.agg(*phase1_exprs).collect()[0].asDict()

    date_candidates: dict[str, list[int]] = {}
    ts_candidates: dict[str, list[int]] = {}
    for col_name in string_columns:
        sample = phase1_stats.get(f"{col_name}__sample")
        if sample is not None and not isinstance(sample, str):
            sample = str(sample)
        date_candidates[col_name] = _candidate_date_format_indices(sample)
        ts_candidates[col_name] = _candidate_timestamp_format_indices(sample)

    phase2_exprs = []
    for col_name in string_columns:
        if not date_candidates[col_name] and not ts_candidates[col_name]:
            continue

        col_expr = F.col(col_name)
        trimmed = F.trim(col_expr)
        value_expr = (
            F.when(trimmed == "", F.lit(None)).otherwise(trimmed)
            if normalize_strings
            else trimmed
        )
        safe_trimmed = F.when(
            value_expr.rlike(_DATE_CANDIDATE_PATTERN), value_expr
        ).otherwise(F.lit(None))

        for idx in date_candidates[col_name]:
            parsed_date = _get_parsed_date_expr(safe_trimmed, _DATE_FORMATS[idx])
            phase2_exprs.append(
                F.sum(
                    F.when(value_expr.isNotNull() & parsed_date.isNull(), 1).otherwise(0)
                ).alias(_date_fail_key(col_name, idx))
            )
        for idx in ts_candidates[col_name]:
            parsed_ts = _get_parsed_ts_expr(safe_trimmed, _TIMESTAMP_FORMATS[idx])
            phase2_exprs.append(
                F.sum(
                    F.when(value_expr.isNotNull() & parsed_ts.isNull(), 1).otherwise(0)
                ).alias(_ts_fail_key(col_name, idx))
            )

    phase2_stats = (
        df.agg(*phase2_exprs).collect()[0].asDict() if phase2_exprs else {}
    )

    select_exprs = []
    for col_name in df.columns:
        if col_name not in string_columns:
            select_exprs.append(F.col(col_name))
            continue

        nn = phase1_stats.get(f"{col_name}__nn", 0)
        if nn == 0:
            if normalize_strings:
                select_exprs.append(
                    _normalized_string_expr(F.col(col_name), normalize_strings=True).alias(
                        col_name
                    )
                )
            else:
                select_exprs.append(F.col(col_name))
            continue

        int_fail = phase1_stats.get(f"{col_name}__int_fail", 0)
        float_fail = phase1_stats.get(f"{col_name}__float_fail", 0)

        col_expr = F.col(col_name)
        value_expr = _normalized_string_expr(col_expr, normalize_strings=normalize_strings)
        safe_trimmed = F.when(
            value_expr.rlike(_DATE_CANDIDATE_PATTERN), value_expr
        ).otherwise(F.lit(None))

        date_parser = next(
            (
                _DATE_FORMATS[idx]
                for idx in date_candidates[col_name]
                if phase2_stats.get(_date_fail_key(col_name, idx), nn) == 0
            ),
            None,
        )
        ts_parser = next(
            (
                _TIMESTAMP_FORMATS[idx]
                for idx in ts_candidates[col_name]
                if phase2_stats.get(_ts_fail_key(col_name, idx), nn) == 0
            ),
            None,
        )

        if date_parser is not None:
            parsed_date = _get_parsed_date_expr(safe_trimmed, date_parser)
            select_exprs.append(
                F.when(value_expr.isNull(), None).otherwise(parsed_date).alias(col_name)
            )
        elif ts_parser is not None:
            parsed_ts = _get_parsed_ts_expr(safe_trimmed, ts_parser)
            select_exprs.append(
                F.when(value_expr.isNull(), None).otherwise(parsed_ts).alias(col_name)
            )
        elif int_fail == 0:
            select_exprs.append(
                F.when(value_expr.isNull(), None)
                .otherwise(value_expr.cast(IntegerType()))
                .alias(col_name)
            )
        elif float_fail == 0:
            select_exprs.append(
                F.when(value_expr.isNull(), None)
                .otherwise(value_expr.cast(DoubleType()))
                .alias(col_name)
            )
        elif normalize_strings:
            select_exprs.append(value_expr.alias(col_name))
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
    *,
    resolved_source_relative_path: Optional[str] = None,
) -> DataFrame:
    """Add Silver-layer metadata columns (ingestion time, source path, date parts).

    Resolves ``source_relative_path`` with
    :py:func:`fabrictools.io.lakehouse.resolve_lakehouse_read_candidate` unless a
    resolved path is provided. Date partition columns (``year_col`` /
    ``month_col`` / ``day_col``) are derived from the current ingestion date.

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
    :param resolved_source_relative_path: Optional already-resolved source path. When
        provided, path resolution is skipped.
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
    :type resolved_source_relative_path: str | None

    :returns: ``df`` with metadata and partition columns appended/overwritten.
    :rtype: ~pyspark.sql.DataFrame

    .. rubric:: Example

    >>> silver_df = add_silver_metadata(  # doctest: +SKIP
    ...     bronze_df,
    ...     source_lakehouse_name="BronzeLakehouse",
    ...     source_relative_path="dbo.RawOrders",
    ... )
    """
    resolved_source_path = resolved_source_relative_path
    if resolved_source_path is None:
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
    return metadata_df


def clean_data(
    df: DataFrame,
    drop_all_null_rows: bool = True,
    verbose: bool = False,
) -> DataFrame:
    """Normalize names, trim empty strings to null, and infer column types.

    Renames columns to unique snake_case (via internal helpers), then runs
    :py:func:`detect_and_cast_columns` with string normalization enabled, and
    optionally drops rows that are all-null.

    :param df: Input dataframe.
    :param drop_all_null_rows: If ``True``, call ``dropna(how="all")``.
    :type df: ~pyspark.sql.DataFrame
    :type drop_all_null_rows: bool

    :returns: Cleaned dataframe.
    :rtype: ~pyspark.sql.DataFrame

    .. rubric:: Example

    >>> cleaned = clean_data(raw_df, drop_all_null_rows=True)  # doctest: +SKIP
    """
    normalized_columns = _build_unique_column_names(df.columns)
    cleaned_df = df.toDF(*normalized_columns)
    cleaned_df = detect_and_cast_columns(
        cleaned_df, verbose=verbose, normalize_strings=True
    )

    if drop_all_null_rows:
        cleaned_df = cleaned_df.dropna(how="all")

    return cleaned_df


__all__ = [
    "clean_data",
    "add_silver_metadata",
    "detect_and_cast_columns",
    "to_snake_case",
    "_to_snake_case",
    "_build_unique_column_names",
    "_normalized_name_collisions",
    "_replace_empty_strings_with_nulls",
]

if __name__ == "__main__":
    print("Test")
