"""Prepared pipeline orchestration helpers."""

from __future__ import annotations

from typing import Any, Optional

from pyspark.sql import SparkSession

from fabrictools.core.logging import log
from fabrictools.core.spark import get_spark
from fabrictools.io.discovery import list_lakehouse_tables
from fabrictools.pipelines.config import (
    TableJobConfig,
    build_table_jobs_from_config,
    build_table_jobs_from_discovery,
)
from fabrictools.prepare._legacy import get_legacy_module


def prepare_and_write_data(*args, **kwargs):
    """Delegate single-table prepare pipeline to legacy implementation."""
    return get_legacy_module().prepare_and_write_data(*args, **kwargs)


def _build_jobs(
    *,
    source_lakehouse_name: str,
    mode: str,
    tables_config: Optional[list[dict[str, Any]]],
    include_schemas: Optional[list[str]],
    exclude_tables: Optional[list[str]],
) -> list[TableJobConfig]:
    if tables_config is not None:
        return build_table_jobs_from_config(
            tables_config=tables_config,
            default_mode=mode,
            supported_modes={"overwrite", "append", "ignore", "error"},
            source_keys=("source_relative_path", "source_path", "source_table", "bronze_path"),
            target_keys=("target_relative_path", "target_path", "target_table", "prepared_table", "silver_table"),
            require_target=False,
            require_mode=False,
            allow_merge_condition=False,
        )
    return build_table_jobs_from_discovery(
        source_lakehouse_name=source_lakehouse_name,
        discover_fn=list_lakehouse_tables,
        include_schemas=include_schemas,
        exclude_tables=exclude_tables,
        mode=mode,
        partition_by=None,
    )


def prepare_and_write_all_tables(
    source_lakehouse_name: str,
    target_lakehouse_name: str,
    mode: str = "overwrite",
    tables_config: Optional[list[dict[str, Any]]] = None,
    include_schemas: Optional[list[str]] = None,
    exclude_tables: Optional[list[str]] = None,
    sample_size: int = 500,
    profiling_confidence_threshold: float = 0.80,
    max_partitions_guard: int = 500,
    vacuum_retention_hours: int = 168,
    enable_semantic_model_publish: bool = False,
    semantic_workspace: Optional[str] = None,
    semantic_model_name: str = "fabrictools_prepared_dataset",
    overwrite_semantic_model: bool = True,
    continue_on_error: bool = False,
    spark: Optional[SparkSession] = None,
) -> dict[str, Any]:
    """Bulk prepared pipeline orchestration with canonical config parsing."""
    _spark = spark or get_spark()
    table_jobs = _build_jobs(
        source_lakehouse_name=source_lakehouse_name,
        mode=mode,
        tables_config=tables_config,
        include_schemas=include_schemas,
        exclude_tables=exclude_tables,
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
        log(f"[{index}/{total_tables}] Preparing '{src}' -> '{tgt}' [mode={table_mode}]...")
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

