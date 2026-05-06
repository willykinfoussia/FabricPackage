"""Prepared pipeline orchestration helpers."""

from __future__ import annotations

from typing import Any, Optional, Sequence

from pyspark.sql import SparkSession, DataFrame

from fabrictools.core.logging import log
from fabrictools.core.spark import get_spark
from fabrictools.io.discovery import list_lakehouse_tables_for_pipeline
from fabrictools.io import read_lakehouse
from fabrictools.prepare.schema import snapshot_source_schema
from fabrictools.prepare.resolve import resolve_columns
from fabrictools.prepare.transform import (
    DEFAULT_MAX_PARTITIONS_GUARD,
    transform_to_prepared,
    write_prepared_table,
)
from fabrictools.prepare.semantic import publish_semantic_model
from fabrictools.prepare.aggregations import generate_prepared_aggregations
from fabrictools.pipelines.config import (
    TableJobConfig,
    build_table_jobs_from_config,
    build_table_jobs_from_discovery,
)


def _build_jobs(
    *,
    source_lakehouse_name: str,
    mode: str,
    tables_config: Optional[list[dict[str, Any]]],
    include_schemas: Optional[list[str]],
    exclude_tables: Optional[list[str]],
    merge_condition: Optional[str],
    upsert_key_columns: Optional[list[str]],
) -> list[TableJobConfig]:
    if tables_config is not None:
        return build_table_jobs_from_config(
            tables_config=tables_config,
            default_mode=mode,
            supported_modes={
                "overwrite",
                "append",
                "ignore",
                "error",
                "merge",
                "upsert",
            },
            source_keys=(
                "source_relative_path",
                "source_path",
                "source_table",
                "bronze_path",
            ),
            target_keys=(
                "target_relative_path",
                "target_path",
                "target_table",
                "prepared_table",
                "silver_table",
            ),
            require_target=False,
            require_mode=False,
            allow_merge_condition=True,
        )
    mode_l = str(mode).strip().lower()
    merge_default = str(merge_condition).strip() if merge_condition else None
    key_default = [
        str(k).strip() for k in (upsert_key_columns or []) if str(k).strip()
    ] or None
    if key_default == []:
        key_default = None
    if mode_l in {"upsert", "merge"} and not merge_default and not key_default:
        key_default = ["id"]

    return build_table_jobs_from_discovery(
        source_lakehouse_name=source_lakehouse_name,
        discover_fn=list_lakehouse_tables_for_pipeline,
        include_schemas=include_schemas,
        exclude_tables=exclude_tables,
        mode=mode,
        partition_by=None,
        merge_condition=merge_default,
        upsert_key_columns=key_default,
    )


def prepare_and_write_data(
    source_lakehouse_name: str,
    source_relative_path: str,
    target_lakehouse_name: str,
    target_relative_path: str,
    mode: str = "upsert",
    sample_size: int = 500,
    profiling_confidence_threshold: float = 0.80,
    max_partitions_guard: int = DEFAULT_MAX_PARTITIONS_GUARD,
    vacuum_retention_hours: int = 168,
    enable_semantic_model_publish: bool = False,
    semantic_workspace: Optional[str] = None,
    semantic_model_name: str = "fabrictools_prepared_dataset",
    overwrite_semantic_model: bool = True,
    spark: Optional[SparkSession] = None,
    *,
    merge_condition: Optional[str] = None,
    upsert_key_columns: Optional[Sequence[str]] = None,
) -> DataFrame:
    """Run the full prepared pipeline for one source table (schema, resolve, transform, write).

    Optionally generates aggregations and publishes a semantic model when
    ``enable_semantic_model_publish`` is ``True`` (requires Fabric Semantic Link).

    :param source_lakehouse_name: Source Lakehouse name.
    :param source_relative_path: Source table path.
    :param target_lakehouse_name: Target Lakehouse for the prepared Delta table.
    :param target_relative_path: Target path for the prepared table.
    :param mode: Spark write mode for the prepared table.
    :param sample_size: Profiling sample size for :py:func:`fabrictools.resolve_columns`.
    :param profiling_confidence_threshold: Minimum confidence to trust profiling cache hits.
    :param max_partitions_guard: Upper bound for partition column selection (see :py:func:`fabrictools.write_prepared_table`).
    :param vacuum_retention_hours: Delta ``VACUUM`` retention when maintenance runs.
    :param enable_semantic_model_publish: If ``True``, call :py:func:`fabrictools.publish_semantic_model`.
    :param semantic_workspace: Fabric workspace for semantic publish (required when publish enabled).
    :param semantic_model_name: Model display name in the workspace.
    :param overwrite_semantic_model: Replace existing semantic model when publishing.
    :param spark: Optional ``SparkSession``.
    :type source_lakehouse_name: str
    :type source_relative_path: str
    :type target_lakehouse_name: str
    :type target_relative_path: str
    :type mode: str
    :type sample_size: int
    :type profiling_confidence_threshold: float
    :type max_partitions_guard: int
    :type vacuum_retention_hours: int
    :type enable_semantic_model_publish: bool
    :type semantic_workspace: str | None
    :type semantic_model_name: str
    :type overwrite_semantic_model: bool
    :type spark: ~pyspark.sql.SparkSession | None

    :returns: The prepared ``DataFrame`` that was written.
    :rtype: ~pyspark.sql.DataFrame

    .. rubric:: Example

    >>> prepared_df = prepare_and_write_data(  # doctest: +SKIP
    ...     "BronzeLakehouse",
    ...     "dbo.RawInvoices",
    ...     "GoldLakehouse",
    ...     "Tables/dbo/Prepared_Invoices",
    ...     mode="overwrite",
    ... )
    """
    _spark = spark or get_spark()
    source_df = read_lakehouse(
        source_lakehouse_name, source_relative_path, spark=_spark
    )
    schema_hash = snapshot_source_schema(
        source_lakehouse_name=source_lakehouse_name,
        source_relative_path=source_relative_path,
        spark=_spark,
    )
    resolved_mappings = resolve_columns(
        source_lakehouse_name=source_lakehouse_name,
        source_relative_path=source_relative_path,
        schema_hash=schema_hash,
        sample_size=sample_size,
        profiling_confidence_threshold=profiling_confidence_threshold,
        spark=_spark,
    )
    prepared_df = transform_to_prepared(
        source_lakehouse_name=source_lakehouse_name,
        source_relative_path=source_relative_path,
        resolved_mappings=resolved_mappings,
        spark=_spark,
    )
    write_prepared_table(
        df=prepared_df,
        resolved_mappings=resolved_mappings,
        target_lakehouse_name=target_lakehouse_name,
        target_relative_path=target_relative_path,
        mode=mode,
        max_partitions_guard=max_partitions_guard,
        vacuum_retention_hours=vacuum_retention_hours,
        merge_condition=merge_condition,
        upsert_key_columns=upsert_key_columns,
        spark=_spark,
    )
    agg_tables = generate_prepared_aggregations(
        source_lakehouse_name=source_lakehouse_name,
        target_lakehouse_name=target_lakehouse_name,
        target_relative_path=target_relative_path,
        resolved_mappings=resolved_mappings,
        spark=_spark,
    )
    if enable_semantic_model_publish:
        publish_semantic_model(
            target_lakehouse_name=target_lakehouse_name,
            agg_tables=agg_tables,
            resolved_mappings=resolved_mappings,
            semantic_workspace=semantic_workspace,
            semantic_model_name=semantic_model_name,
            overwrite_model=overwrite_semantic_model,
            spark=_spark,
        )
    return prepared_df


def prepare_and_write_all_tables(
    source_lakehouse_name: str,
    target_lakehouse_name: str,
    mode: str = "upsert",
    tables_config: Optional[list[dict[str, Any]]] = None,
    include_schemas: Optional[list[str]] = None,
    exclude_tables: Optional[list[str]] = None,
    sample_size: int = 500,
    profiling_confidence_threshold: float = 0.80,
    max_partitions_guard: int = DEFAULT_MAX_PARTITIONS_GUARD,
    vacuum_retention_hours: int = 168,
    enable_semantic_model_publish: bool = False,
    semantic_workspace: Optional[str] = None,
    semantic_model_name: str = "fabrictools_prepared_dataset",
    overwrite_semantic_model: bool = True,
    continue_on_error: bool = False,
    spark: Optional[SparkSession] = None,
    *,
    merge_condition: Optional[str] = None,
    upsert_key_columns: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Bulk prepared pipeline: discovery or ``tables_config``, then :py:func:`fabrictools.prepare_and_write_data` per job.

    :param source_lakehouse_name: Source Lakehouse.
    :param target_lakehouse_name: Target Lakehouse for prepared outputs.
    :param mode: Default write mode for discovered jobs.
    :param tables_config: Optional explicit job list (see :mod:`fabrictools.pipelines.config`).
    :param include_schemas: Discovery schema filter.
    :param exclude_tables: Discovery exclusion list.
    :param sample_size: Forwarded to each :py:func:`fabrictools.prepare_and_write_data` call.
    :param profiling_confidence_threshold: Forwarded to each call.
    :param max_partitions_guard: Forwarded to each call.
    :param vacuum_retention_hours: Forwarded to each call.
    :param enable_semantic_model_publish: Forwarded to each call.
    :param semantic_workspace: Forwarded to each call.
    :param semantic_model_name: Forwarded to each call.
    :param overwrite_semantic_model: Forwarded to each call.
    :param continue_on_error: If ``False``, abort on first table failure.
    :param spark: Optional ``SparkSession``.
    :type source_lakehouse_name: str
    :type target_lakehouse_name: str
    :type mode: str
    :type tables_config: list[dict] | None
    :type include_schemas: list[str] | None
    :type exclude_tables: list[str] | None
    :type sample_size: int
    :type profiling_confidence_threshold: float
    :type max_partitions_guard: int
    :type vacuum_retention_hours: int
    :type enable_semantic_model_publish: bool
    :type semantic_workspace: str | None
    :type semantic_model_name: str
    :type overwrite_semantic_model: bool
    :type continue_on_error: bool
    :type spark: ~pyspark.sql.SparkSession | None

    :returns: Summary dict with counts and per-table success/failure entries.
    :rtype: dict

    .. rubric:: Example

    >>> summary = prepare_and_write_all_tables(  # doctest: +SKIP
    ...     "BronzeLakehouse",
    ...     "GoldLakehouse",
    ...     mode="overwrite",
    ...     include_schemas=["dbo"],
    ... )
    """
    _spark = spark or get_spark()
    table_jobs = _build_jobs(
        source_lakehouse_name=source_lakehouse_name,
        mode=mode,
        tables_config=tables_config,
        include_schemas=include_schemas,
        exclude_tables=exclude_tables,
        merge_condition=merge_condition,
        upsert_key_columns=upsert_key_columns,
    )

    if not table_jobs:
        log(
            f"No tables found in Lakehouse '{source_lakehouse_name}' for prepare/write.",
            level="warning",
        )
        return {
            "total_tables": 0,
            "successful_tables": 0,
            "failed_tables": 0,
            "tables": [],
            "failures": [],
        }

    processed_tables: list[dict[str, str]] = []
    failures: list[dict[str, str]] = []
    total_tables = len(table_jobs)

    for index, table_job in enumerate(table_jobs, start=1):
        src = str(table_job["source_relative_path"])
        tgt = str(table_job["target_relative_path"])
        table_mode = str(table_job["mode"])
        job_merge = table_job.get("merge_condition")
        job_keys = table_job.get("upsert_key_columns")
        log(
            f"[{index}/{total_tables}] Preparing '{src}' -> '{tgt}' [mode={table_mode}]..."
        )
        try:
            prepare_and_write_data(
                source_lakehouse_name=source_lakehouse_name,
                source_relative_path=src,
                target_lakehouse_name=target_lakehouse_name,
                target_relative_path=tgt,
                mode=table_mode,
                sample_size=sample_size,
                profiling_confidence_threshold=profiling_confidence_threshold,
                max_partitions_guard=max_partitions_guard,
                vacuum_retention_hours=vacuum_retention_hours,
                enable_semantic_model_publish=enable_semantic_model_publish,
                semantic_workspace=semantic_workspace,
                semantic_model_name=semantic_model_name,
                overwrite_semantic_model=overwrite_semantic_model,
                merge_condition=job_merge,
                upsert_key_columns=job_keys,
                spark=_spark,
            )
            processed_tables.append(
                {
                    "source_relative_path": src,
                    "target_relative_path": tgt,
                    "mode": table_mode,
                }
            )
        except Exception as exc:
            failures.append(
                {
                    "source_relative_path": src,
                    "target_relative_path": tgt,
                    "mode": table_mode,
                    "error": str(exc),
                }
            )
            log(f"[{index}/{total_tables}] Failed for '{src}': {exc}", level="warning")
            if not continue_on_error:
                raise

    return {
        "total_tables": total_tables,
        "successful_tables": len(processed_tables),
        "failed_tables": len(failures),
        "tables": processed_tables,
        "failures": failures,
    }


__all__ = ["prepare_and_write_data", "prepare_and_write_all_tables"]
