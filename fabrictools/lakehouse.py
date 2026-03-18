"""
Lakehouse read / write / merge helpers for Microsoft Fabric.

All functions resolve the ABFS base path automatically from the Lakehouse
display name using ``notebookutils``, and obtain the SparkSession via
``SparkSession.builder.getOrCreate()``.
"""

from __future__ import annotations

import re
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession  # type: ignore[reportMissingImports]
from pyspark.sql.types import IntegralType  # type: ignore[reportMissingImports]

from fabrictools._logger import log
from fabrictools._paths import (
    build_lakehouse_read_path_candidates,
    build_lakehouse_write_path,
    get_lakehouse_abfs_path,
)
from fabrictools._spark import get_spark

# ── Read ─────────────────────────────────────────────────────────────────────


def read_lakehouse(
    lakehouse_name: str,
    relative_path: str,
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """
    Read a dataset from a Fabric Lakehouse.

    The function tries formats in order: **Delta → Parquet → CSV**.  The first
    one that succeeds is used, and the detected format is logged together with
    the resulting shape.

    Parameters
    ----------
    lakehouse_name:
        Display name of the Lakehouse (e.g. ``"BronzeLakehouse"``).
    relative_path:
        Path inside the Lakehouse root, relative to the ABFS base path
        (e.g. ``"sales/2024"`` or ``"Tables/customers"``).
    spark:
        Optional SparkSession.  When omitted the active session is used.

    Returns
    -------
    DataFrame

    Raises
    ------
    RuntimeError
        When none of the supported formats can be read from the given path.

    Examples
    --------
    >>> df = read_lakehouse("BronzeLakehouse", "sales/2024")
    """
    _spark = spark or get_spark()
    base = get_lakehouse_abfs_path(lakehouse_name)
    candidate_relative_paths = build_lakehouse_read_path_candidates(relative_path)

    failures: list[str] = []
    for candidate_relative_path in candidate_relative_paths:
        full_path = f"{base}/{candidate_relative_path}"
        log(f"Reading Lakehouse '{lakehouse_name}' → {full_path}")
        try:
            df = _try_read_formats(_spark, full_path)
            if candidate_relative_path != relative_path:
                log(f"  Resolved relative_path '{relative_path}' -> '{candidate_relative_path}'")
            log(f"  {df.count():,} rows · {len(df.columns)} columns")
            return df
        except RuntimeError as exc:
            failures.append(f"{full_path} ({exc})")

    attempted_paths = ", ".join(f"'{base}/{candidate}'" for candidate in candidate_relative_paths)
    raise RuntimeError(
        f"Could not read from any candidate path for relative_path='{relative_path}'. "
        f"Tried: {attempted_paths}. Details: {' | '.join(failures)}"
    )


def resolve_lakehouse_read_candidate(
    lakehouse_name: str,
    relative_path: str,
    spark: Optional[SparkSession] = None,
) -> str:
    """
    Resolve the best candidate relative path for a Lakehouse read.

    Rules:
    - If candidate generation yields a single path, return it directly.
    - If multiple candidates exist, try each path and return the first readable one.
    """
    _spark = spark or get_spark()
    base = get_lakehouse_abfs_path(lakehouse_name)
    candidate_relative_paths = build_lakehouse_read_path_candidates(relative_path)

    if len(candidate_relative_paths) == 1:
        return candidate_relative_paths[0]

    failures: list[str] = []
    for candidate_relative_path in candidate_relative_paths:
        full_path = f"{base}/{candidate_relative_path}"
        try:
            _try_read_formats(_spark, full_path)
            if candidate_relative_path != relative_path:
                log(
                    f"  Resolved relative_path '{relative_path}' -> "
                    f"'{candidate_relative_path}'"
                )
            return candidate_relative_path
        except RuntimeError as exc:
            failures.append(f"{full_path} ({exc})")

    attempted_paths = ", ".join(f"'{base}/{candidate}'" for candidate in candidate_relative_paths)
    raise RuntimeError(
        f"Could not resolve a readable candidate for relative_path='{relative_path}'. "
        f"Tried: {attempted_paths}. Details: {' | '.join(failures)}"
    )


def _try_read_formats(spark: SparkSession, full_path: str) -> DataFrame:
    """Attempt Delta → Parquet → CSV, return the first successful DataFrame."""
    # Delta (preferred in Fabric)
    try:
        df = spark.read.format("delta").load(full_path)
        log("  Format detected: Delta")
        return df
    except Exception:
        pass

    # Parquet
    try:
        df = spark.read.format("parquet").load(full_path)
        log("  Format detected: Parquet")
        return df
    except Exception:
        pass

    # CSV — last resort
    try:
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .option("escape", '"')
            .csv(full_path)
        )
        log("  Format detected: CSV")
        return df
    except Exception as exc:
        raise RuntimeError(
            f"Could not read '{full_path}' as Delta, Parquet, or CSV: {exc}"
        ) from exc


# ── Write ────────────────────────────────────────────────────────────────────


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    """Return a list without duplicates while preserving insertion order."""
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            deduped.append(value)
    return deduped


def _detect_partition_columns(df: DataFrame) -> list[str]:
    """
    Auto-detect year/month/day partition columns.

    Detection order:
    1) standard names with underscore: _year/_month/_day
    2) standard names without underscore: year/month/day
    3) fallback on integer-like columns whose names match
       year|annee, month|mois, day|jour
    """
    standard_with_underscore = ["_year", "_month", "_day"]
    standard_without_underscore = ["year", "month", "day"]

    if all(col_name in df.columns for col_name in standard_with_underscore):
        return standard_with_underscore
    if all(col_name in df.columns for col_name in standard_without_underscore):
        return standard_without_underscore

    schema_fields = getattr(getattr(df, "schema", None), "fields", [])
    year_regex = re.compile(r"(?:^|_)(year|annee)(?:$|_)")
    month_regex = re.compile(r"(?:^|_)(month|mois)(?:$|_)")
    day_regex = re.compile(r"(?:^|_)(day|jour)(?:$|_)")

    detected: dict[str, str] = {}
    for field in schema_fields:
        if not isinstance(field.dataType, IntegralType):
            continue

        normalized_name = field.name.lower()
        if "year" not in detected and year_regex.search(normalized_name):
            detected["year"] = field.name
            continue
        if "month" not in detected and month_regex.search(normalized_name):
            detected["month"] = field.name
            continue
        if "day" not in detected and day_regex.search(normalized_name):
            detected["day"] = field.name

    ordered_detected = [detected[key] for key in ["year", "month", "day"] if key in detected]
    if len(ordered_detected) == 3:
        return ordered_detected
    return []


def write_lakehouse(
    df: DataFrame,
    lakehouse_name: str,
    relative_path: str,
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None,
    format: str = "delta",
    spark: Optional[SparkSession] = None,
) -> None:
    """
    Write a DataFrame to a Fabric Lakehouse as a Delta table (default).

    Parameters
    ----------
    df:
        DataFrame to persist.
    lakehouse_name:
        Display name of the target Lakehouse.
    relative_path:
        Destination path inside the Lakehouse
        (e.g. ``"sales_clean"`` or ``"Tables/sales_clean"``).
    mode:
        Spark write mode — ``"overwrite"`` (default), ``"append"``,
        ``"ignore"``, or ``"error"``.
    partition_by:
        Optional list of column names to partition the output by.
        Auto-detected date partitions are appended when found in the DataFrame.
    format:
        Output format — ``"delta"`` (default), ``"parquet"``, or ``"csv"``.
    spark:
        Optional SparkSession.  When omitted the active session is used.

    Examples
    --------
    >>> write_lakehouse(df, "SilverLakehouse", "sales_clean",
    ...                 mode="overwrite", partition_by=["year"])
    """
    _ = spark or get_spark()  # validates spark availability early
    base = get_lakehouse_abfs_path(lakehouse_name)
    resolved_relative_path = build_lakehouse_write_path(relative_path)
    full_path = f"{base}/{resolved_relative_path}"
    if resolved_relative_path != relative_path:
        log(
            f"Auto-corrected write relative_path '{relative_path}' "
            f"-> '{resolved_relative_path}'"
        )
    log(
        f"Writing to Lakehouse '{lakehouse_name}' → {full_path} "
        f"[format={format}, mode={mode}]"
    )

    user_partitions = list(partition_by or [])
    auto_detected_partitions = _detect_partition_columns(df)
    effective_partition_by = _dedupe_preserve_order(
        user_partitions + auto_detected_partitions
    )

    writer = df.write.format(format).option("overwriteSchema", "true").mode(mode)
    if effective_partition_by:
        writer = writer.partitionBy(*effective_partition_by)
        if auto_detected_partitions:
            log("  Auto-detected partitions: " + ", ".join(auto_detected_partitions))
        log("  Partition columns: " + ", ".join(effective_partition_by))
    writer.save(full_path)
    log(f"  Write complete → {full_path}")


# ── Merge (upsert) ────────────────────────────────────────────────────────────


def merge_lakehouse(
    source_df: DataFrame,
    lakehouse_name: str,
    relative_path: str,
    merge_condition: str,
    update_set: Optional[dict] = None,
    insert_set: Optional[dict] = None,
    spark: Optional[SparkSession] = None,
) -> None:
    """
    Upsert (merge) a DataFrame into an existing Delta table in a Lakehouse.

    Uses the Delta Lake ``DeltaTable.forPath`` merge API.  When
    ``update_set`` and/or ``insert_set`` are ``None``, a ``whenMatchedUpdateAll``
    / ``whenNotMatchedInsertAll`` strategy is applied automatically.

    Parameters
    ----------
    source_df:
        New data to merge into the target table.
    lakehouse_name:
        Display name of the Lakehouse that holds the target table.
    relative_path:
        Path of the Delta table inside the Lakehouse.
    merge_condition:
        SQL expression that joins source and target rows
        (e.g. ``"src.id = tgt.id"``).
    update_set:
        Mapping of ``{target_col: source_expr}`` for matched rows.
        Pass ``None`` to update all columns automatically.
    insert_set:
        Mapping of ``{target_col: source_expr}`` for new rows.
        Pass ``None`` to insert all columns automatically.
    spark:
        Optional SparkSession.  When omitted the active session is used.

    Examples
    --------
    >>> merge_lakehouse(
    ...     new_df, "SilverLakehouse", "sales_clean",
    ...     merge_condition="src.id = tgt.id",
    ... )
    """
    from delta.tables import DeltaTable  # type: ignore[import-untyped]  # noqa: PLC0415

    _spark = spark or get_spark()
    base = get_lakehouse_abfs_path(lakehouse_name)
    full_path = f"{base}/{relative_path}"
    log(f"Merging into Lakehouse '{lakehouse_name}' → {full_path}")
    log(f"  Condition: {merge_condition}")

    target = DeltaTable.forPath(_spark, full_path)
    merge_builder = (
        target.alias("tgt")
        .merge(source_df.alias("src"), merge_condition)
    )

    if update_set is not None:
        merge_builder = merge_builder.whenMatchedUpdate(set=update_set)
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    if insert_set is not None:
        merge_builder = merge_builder.whenNotMatchedInsert(values=insert_set)
    else:
        merge_builder = merge_builder.whenNotMatchedInsertAll()

    merge_builder.execute()
    log("  Merge complete")
