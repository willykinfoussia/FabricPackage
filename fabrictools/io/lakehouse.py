"""Lakehouse I/O facade module."""

from __future__ import annotations

import re
from typing import Any, List, Optional

from pyspark.sql import DataFrame, SparkSession  # type: ignore[reportMissingImports]
from pyspark.sql.types import IntegralType  # type: ignore[reportMissingImports]

from fabrictools.core import log
from fabrictools.core import (
    build_lakehouse_read_path_candidates,
    build_lakehouse_write_path,
    get_lakehouse_abfs_path,
)
from fabrictools.core import get_spark
from fabrictools.io.discovery import list_lakehouse_tables

# ── Read ─────────────────────────────────────────────────────────────────────


def read_lakehouse(
    lakehouse_name: str,
    relative_path: str,
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """Read a dataset from a Fabric Lakehouse.

    Tries formats in order: **Delta → Parquet → CSV**. The first format that
    succeeds is used; the detected format is logged with the resulting shape.

    :param lakehouse_name: Display name of the Lakehouse (e.g. ``"BronzeLakehouse"``).
    :param relative_path: Path inside the Lakehouse root, relative to the ABFS base
        (e.g. ``"sales/2024"`` or ``"Tables/customers"``).
    :param spark: Optional ``SparkSession``; when omitted the active session is used.
    :type lakehouse_name: str
    :type relative_path: str
    :type spark: ~pyspark.sql.SparkSession | None

    :returns: Loaded dataframe.
    :rtype: ~pyspark.sql.DataFrame

    :raises RuntimeError: When none of the supported formats can be read from the path.

    .. rubric:: Example

    >>> df = read_lakehouse("BronzeLakehouse", "sales/2024")
    """
    _spark = spark or get_spark()
    base = get_lakehouse_abfs_path(lakehouse_name)
    candidate_relative_paths = build_lakehouse_read_path_candidates(relative_path)

    failures: list[str] = []
    for candidate_relative_path in candidate_relative_paths:
        full_path = f"{base}/{candidate_relative_path}"
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
    """Resolve the best candidate relative path for a Lakehouse read.

    If candidate generation yields a single path, return it directly. If multiple
    candidates exist, try each path and return the first readable one.

    :param lakehouse_name: Display name of the Lakehouse.
    :param relative_path: Logical path under the Lakehouse root.
    :param spark: Optional ``SparkSession``; when omitted the active session is used.
    :type lakehouse_name: str
    :type relative_path: str
    :type spark: ~pyspark.sql.SparkSession | None

    :returns: Relative path string that was verified readable.
    :rtype: str

    :raises RuntimeError: When no candidate path can be read.
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
    *,
    normalize_column_names: bool = True,
) -> None:
    """Write a ``DataFrame`` to a Fabric Lakehouse (default format: Delta).

    :param df: DataFrame to persist.
    :param lakehouse_name: Display name of the target Lakehouse.
    :param relative_path: Destination path inside the Lakehouse (e.g.
        ``"sales_clean"`` or ``"Tables/sales_clean"``).
    :param mode: Spark write mode: ``"overwrite"`` (default), ``"append"``,
        ``"ignore"``, or ``"error"``.
    :param partition_by: Optional column names to partition by. Each name is resolved
        like :py:func:`fabrictools.clean_data` /
        :py:func:`fabrictools.merge_dataframes` (physical name,
        normalized unique label, or snake_case). Auto-detected date partitions are
        appended when present on ``df``.
    :param format: ``"delta"`` (default), ``"parquet"``, or ``"csv"``.
    :param spark: Optional ``SparkSession``; when omitted the active session is used.
    :param normalize_column_names: If ``True`` (default), run
        :py:func:`fabrictools.rename_columns_normalized` before
        resolving ``partition_by`` and writing. If ``False``, keep physical column
        names unchanged.
    :type df: ~pyspark.sql.DataFrame
    :type lakehouse_name: str
    :type relative_path: str
    :type mode: str
    :type partition_by: list[str] | None
    :type format: str
    :type spark: ~pyspark.sql.SparkSession | None
    :type normalize_column_names: bool

    .. rubric:: Example

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

    # Lazy import: fabrictools.transform.columns → quality.clean → fabrictools.io
    # would otherwise create an import cycle while io.__init__ loads lakehouse.
    from fabrictools.transform.columns import (  # noqa: PLC0415
        _resolve_column_name,
        rename_columns_normalized,
    )

    if normalize_column_names:
        original_cols = list(df.columns)
        df = rename_columns_normalized(df)
        if list(df.columns) != original_cols:
            log("  Column names normalized (clean_data-style) before write")

    user_partitions = [
        _resolve_column_name(df, col, side="DataFrame")
        for col in (partition_by or [])
    ]
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
    """Upsert (merge) a ``DataFrame`` into an existing Delta table in a Lakehouse.

    Uses Delta Lake ``DeltaTable.forPath``. When ``update_set`` and/or ``insert_set``
    are ``None``, ``whenMatchedUpdateAll`` / ``whenNotMatchedInsertAll`` are used.

    :param source_df: Rows to merge into the target table.
    :param lakehouse_name: Lakehouse display name holding the target table.
    :param relative_path: Path of the Delta table inside the Lakehouse.
    :param merge_condition: SQL predicate joining source and target (e.g.
        ``"src.id = tgt.id"``).
    :param update_set: ``{target_col: source_expr}`` for matched rows, or ``None``
        to update all columns.
    :param insert_set: ``{target_col: source_expr}`` for new rows, or ``None`` to
        insert all columns.
    :param spark: Optional ``SparkSession``; when omitted the active session is used.
    :type source_df: ~pyspark.sql.DataFrame
    :type lakehouse_name: str
    :type relative_path: str
    :type merge_condition: str
    :type update_set: dict | None
    :type insert_set: dict | None
    :type spark: ~pyspark.sql.SparkSession | None

    .. rubric:: Example

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


def delete_all_lakehouse_tables(
    lakehouse_name: str,
    include_schemas: Optional[List[str]] = None,
    exclude_tables: Optional[List[str]] = None,
    continue_on_error: bool = False,
) -> dict[str, Any]:
    """Hard-delete all discovered Lakehouse table folders.

    Tables are discovered as ``Tables/<schema>/<table>`` and removed with
    ``notebookutils.fs.rm(<abfs>/Tables/<schema>/<table>, recurse=True)``.

    :param lakehouse_name: Lakehouse display name to purge.
    :param include_schemas: If set, only these schema names (case-insensitive).
    :param exclude_tables: Table or ``schema.table`` names to skip (case-insensitive).
    :param continue_on_error: If ``False`` (default), stop on first delete failure.
    :type lakehouse_name: str
    :type include_schemas: list[str] | None
    :type exclude_tables: list[str] | None
    :type continue_on_error: bool

    :returns: Summary with counts and per-table ``relative_path`` / errors.
    :rtype: dict

    :raises ValueError: When ``notebookutils`` is unavailable (not in Fabric).
    """
    try:
        import notebookutils  # type: ignore[import-untyped]  # noqa: PLC0415
    except ImportError as exc:
        raise ValueError(
            "notebookutils is not available — are you running inside "
            f"Microsoft Fabric? ({exc})"
        ) from exc

    base = get_lakehouse_abfs_path(lakehouse_name)
    table_paths = list_lakehouse_tables(
        lakehouse_name=lakehouse_name,
        include_schemas=include_schemas,
        exclude_tables=exclude_tables,
    )

    if not table_paths:
        log(
            f"No tables found in Lakehouse '{lakehouse_name}' for purge.",
            level="warning",
        )
        return {
            "total_tables": 0,
            "deleted_tables": 0,
            "failed_tables": 0,
            "tables": [],
            "failures": [],
        }

    deleted_entries: list[dict[str, str]] = []
    failure_entries: list[dict[str, str]] = []
    total_tables = len(table_paths)

    for index, table_relative_path in enumerate(table_paths, start=1):
        try:
            full_path = f"{base}/{table_relative_path}"
            log(f"[{index}/{total_tables}] Hard-deleting table path '{full_path}'...")
            notebookutils.fs.rm(full_path, recurse=True)
            deleted_entries.append(
                {
                    "relative_path": table_relative_path,
                    "path": full_path,
                }
            )
        except Exception as exc:
            failure_entries.append(
                {
                    "relative_path": table_relative_path,
                    "path": f"{base}/{table_relative_path}",
                    "error": str(exc),
                }
            )
            log(
                f"[{index}/{total_tables}] Failed to hard-delete '{table_relative_path}': {exc}",
                level="warning",
            )
            if not continue_on_error:
                raise

    return {
        "total_tables": total_tables,
        "deleted_tables": len(deleted_entries),
        "failed_tables": len(failure_entries),
        "tables": deleted_entries,
        "failures": failure_entries,
    }

__all__ = [
    "read_lakehouse",
    "resolve_lakehouse_read_candidate",
    "write_lakehouse",
    "merge_lakehouse",
    "delete_all_lakehouse_tables",
]

