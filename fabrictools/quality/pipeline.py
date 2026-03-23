"""Quality pipeline orchestration helpers."""

from __future__ import annotations

from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

from fabrictools.core.logging import log
from fabrictools.core.spark import get_spark
from fabrictools.io import list_lakehouse_tables, merge_lakehouse, read_lakehouse, write_lakehouse
from fabrictools.pipelines.config import (
    TableJobConfig,
    build_table_jobs_from_config,
    build_table_jobs_from_discovery,
)
from fabrictools.quality.clean import add_silver_metadata, clean_data


def clean_and_write_data(
    source_lakehouse_name: str,
    source_relative_path: str,
    target_lakehouse_name: str,
    target_relative_path: str,
    mode: str = "overwrite",
    partition_by: Optional[list[str]] = None,
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """Read, clean, enrich metadata, and write one table."""
    _spark = spark or get_spark()
    source_df = read_lakehouse(source_lakehouse_name, source_relative_path, spark=_spark)
    cleaned_df = clean_data(source_df)
    silver_df = add_silver_metadata(
        cleaned_df,
        source_lakehouse_name=source_lakehouse_name,
        source_relative_path=source_relative_path,
        spark=_spark,
    )
    write_lakehouse(
        silver_df,
        lakehouse_name=target_lakehouse_name,
        relative_path=target_relative_path,
        mode=mode,
        partition_by=partition_by,
        spark=_spark,
    )
    return silver_df


def _build_jobs(
    *,
    source_lakehouse_name: str,
    mode: str,
    partition_by: Optional[list[str]],
    tables_config: Optional[list[dict[str, Any]]],
    include_schemas: Optional[list[str]],
    exclude_tables: Optional[list[str]],
) -> list[TableJobConfig]:
    if tables_config is not None:
        return build_table_jobs_from_config(
            tables_config=tables_config,
            default_mode=mode,
            default_partition_by=partition_by,
            supported_modes={"overwrite", "append", "merge"},
            source_keys=("source_relative_path", "source_path", "source_table", "bronze_path"),
            target_keys=("target_relative_path", "target_path", "target_table", "silver_table"),
            require_target=True,
            require_mode=True,
            allow_merge_condition=True,
        )
    return build_table_jobs_from_discovery(
        source_lakehouse_name=source_lakehouse_name,
        discover_fn=list_lakehouse_tables,
        include_schemas=include_schemas,
        exclude_tables=exclude_tables,
        mode=mode,
        partition_by=partition_by,
    )


def clean_and_write_all_tables(
    source_lakehouse_name: str,
    target_lakehouse_name: str,
    mode: str = "overwrite",
    partition_by: Optional[list[str]] = None,
    tables_config: Optional[list[dict[str, Any]]] = None,
    include_schemas: Optional[list[str]] = None,
    exclude_tables: Optional[list[str]] = None,
    continue_on_error: bool = False,
    spark: Optional[SparkSession] = None,
) -> dict[str, Any]:
    """Bulk clean/write orchestration with canonical table-job config parsing."""
    _spark = spark or get_spark()
    table_jobs = _build_jobs(
        source_lakehouse_name=source_lakehouse_name,
        mode=mode,
        partition_by=partition_by,
        tables_config=tables_config,
        include_schemas=include_schemas,
        exclude_tables=exclude_tables,
    )

    if not table_jobs:
        log(
            f"No tables found in Lakehouse '{source_lakehouse_name}' for bulk clean/write.",
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

    log(
        f"Bulk clean/write started: {total_tables} table(s) "
        f"from '{source_lakehouse_name}' to '{target_lakehouse_name}'."
    )

    for index, table_job in enumerate(table_jobs, start=1):
        src = str(table_job["source_relative_path"])
        tgt = str(table_job["target_relative_path"])
        table_mode = str(table_job["mode"])
        table_partition_by = table_job.get("partition_by")
        merge_condition = table_job.get("merge_condition")

        log(f"[{index}/{total_tables}] Processing '{src}' -> '{tgt}' [mode={table_mode}]...")
        try:
            if table_mode in {"overwrite", "append"}:
                clean_and_write_data(
                    source_lakehouse_name=source_lakehouse_name,
                    source_relative_path=src,
                    target_lakehouse_name=target_lakehouse_name,
                    target_relative_path=tgt,
                    mode=table_mode,
                    partition_by=table_partition_by,
                    spark=_spark,
                )
            else:
                source_df = read_lakehouse(source_lakehouse_name, src, spark=_spark)
                cleaned_df = clean_data(source_df)
                silver_df = add_silver_metadata(
                    cleaned_df,
                    source_lakehouse_name=source_lakehouse_name,
                    source_relative_path=src,
                    spark=_spark,
                )
                merge_lakehouse(
                    source_df=silver_df,
                    lakehouse_name=target_lakehouse_name,
                    relative_path=tgt,
                    merge_condition=str(merge_condition),
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


__all__ = ["clean_and_write_data", "clean_and_write_all_tables"]

