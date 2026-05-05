"""Quality pipeline orchestration helpers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from time import perf_counter
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

from fabrictools.core import get_lakehouse_abfs_path
from fabrictools.core.logging import log
from fabrictools.core.spark import configure_parquet_datetime_rebase, get_spark
from fabrictools.io import (
    list_lakehouse_tables_for_pipeline,
    merge_lakehouse,
    write_lakehouse,
)
from fabrictools.io.lakehouse import _read_lakehouse_from_base, _resolve_max_workers
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
    auto_partition: bool = True,
    auto_partition_threshold_bytes: int = 1_073_741_824,
    spark: Optional[SparkSession] = None,
    verbose: bool = True,
) -> DataFrame:
    """Read one Lakehouse path, clean, add Silver metadata, and write the target path.

    :param source_lakehouse_name: Bronze (or source) Lakehouse name.
    :param source_relative_path: Source ``Tables/...`` or logical path.
    :param target_lakehouse_name: Silver (or target) Lakehouse name.
    :param target_relative_path: Destination path for the write.
    :param mode: Spark write mode (e.g. ``overwrite``, ``append``).
    :param partition_by: Optional partition columns for :py:func:`fabrictools.write_lakehouse`.
    :param auto_partition: If ``True`` (default), automatically partition the data
        by detected date columns if they exist.
    :param auto_partition_threshold_bytes: Threshold in bytes to trigger auto-partitioning.
    :param spark: Optional ``SparkSession``.
    :type source_lakehouse_name: str
    :type source_relative_path: str
    :type target_lakehouse_name: str
    :type target_relative_path: str
    :type mode: str
    :type partition_by: list[str] | None
    :type spark: ~pyspark.sql.SparkSession | None

    :returns: The Silver dataframe that was written.
    :rtype: ~pyspark.sql.DataFrame

    .. rubric:: Example

    >>> silver_df = clean_and_write_data(  # doctest: +SKIP
    ...     "BronzeLakehouse",
    ...     "dbo.Orders",
    ...     "SilverLakehouse",
    ...     "Tables/dbo/Cleaned_Orders",
    ...     mode="overwrite",
    ...     partition_by=["ingestion_year", "ingestion_month"],
    ... )
    """
    _spark = configure_parquet_datetime_rebase(spark or get_spark())
    source_base_path = get_lakehouse_abfs_path(source_lakehouse_name)
    source_df, resolved_source_relative_path, _ = _read_lakehouse_from_base(
        lakehouse_name=source_lakehouse_name,
        relative_path=source_relative_path,
        base_path=source_base_path,
        spark=_spark,
    )
    cleaned_df = clean_data(source_df, verbose=verbose)
    silver_df = add_silver_metadata(
        cleaned_df,
        source_lakehouse_name=source_lakehouse_name,
        source_relative_path=source_relative_path,
        spark=_spark,
        verbose=verbose,
        resolved_source_relative_path=resolved_source_relative_path,
    )
    write_lakehouse(
        silver_df,
        lakehouse_name=target_lakehouse_name,
        relative_path=target_relative_path,
        mode=mode,
        partition_by=partition_by,
        auto_partition=auto_partition,
        auto_partition_threshold_bytes=auto_partition_threshold_bytes,
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
                "silver_table",
            ),
            require_target=True,
            require_mode=True,
            allow_merge_condition=True,
        )
    return build_table_jobs_from_discovery(
        source_lakehouse_name=source_lakehouse_name,
        discover_fn=list_lakehouse_tables_for_pipeline,
        include_schemas=include_schemas,
        exclude_tables=exclude_tables,
        mode=mode,
        partition_by=partition_by,
        cleaned_table_prefix=True,
    )


def _process_clean_table_job(
    *,
    table_job: TableJobConfig,
    index: int,
    total_tables: int,
    source_lakehouse_name: str,
    source_base_path: str,
    target_lakehouse_name: str,
    auto_partition: bool,
    auto_partition_threshold_bytes: int,
    auto_partition_when_partition_by_provided: bool,
    persist_intermediate: bool,
    spark: SparkSession,
    verbose: bool,
) -> dict[str, Any]:
    src = str(table_job["source_relative_path"])
    tgt = str(table_job["target_relative_path"])
    table_mode = str(table_job["mode"])
    table_partition_by = table_job.get("partition_by")
    merge_condition = table_job.get("merge_condition")
    started_at = perf_counter()
    silver_df: Optional[DataFrame] = None
    is_persisted = False

    if verbose:
        log(
            f"[{index}/{total_tables}] Processing '{src}' -> '{tgt}' [mode={table_mode}]..."
        )

    try:
        source_df, resolved_source_relative_path, _ = _read_lakehouse_from_base(
            lakehouse_name=source_lakehouse_name,
            relative_path=src,
            base_path=source_base_path,
            spark=spark,
        )
        cleaned_df = clean_data(source_df, verbose=verbose)
        silver_df = add_silver_metadata(
            cleaned_df,
            source_lakehouse_name=source_lakehouse_name,
            source_relative_path=src,
            spark=spark,
            verbose=verbose,
            resolved_source_relative_path=resolved_source_relative_path,
        )
        if persist_intermediate:
            silver_df = silver_df.persist()
            is_persisted = True

        if table_mode in {"overwrite", "append"}:
            effective_auto_partition = auto_partition and (
                auto_partition_when_partition_by_provided or not table_partition_by
            )
            write_lakehouse(
                silver_df,
                lakehouse_name=target_lakehouse_name,
                relative_path=tgt,
                mode=table_mode,
                partition_by=table_partition_by,
                auto_partition=effective_auto_partition,
                auto_partition_threshold_bytes=auto_partition_threshold_bytes,
                spark=spark,
            )
        else:
            merge_lakehouse(
                source_df=silver_df,
                lakehouse_name=target_lakehouse_name,
                relative_path=tgt,
                merge_condition=str(merge_condition),
                spark=spark,
            )

        duration_seconds = round(perf_counter() - started_at, 3)
        if verbose:
            log(
                f"[{index}/{total_tables}] Completed '{src}' in "
                f"{duration_seconds:.3f}s."
            )
        return {
            "ok": True,
            "index": index,
            "entry": {
                "source_relative_path": src,
                "target_relative_path": tgt,
                "mode": table_mode,
                "duration_seconds": duration_seconds,
            },
        }
    except Exception as exc:
        duration_seconds = round(perf_counter() - started_at, 3)
        if verbose:
            log(
                f"[{index}/{total_tables}] Failed for '{src}' "
                f"after {duration_seconds:.3f}s: {exc}",
                level="warning",
            )
        return {
            "ok": False,
            "index": index,
            "exception": exc,
            "entry": {
                "source_relative_path": src,
                "target_relative_path": tgt,
                "mode": table_mode,
                "error": str(exc),
                "duration_seconds": duration_seconds,
            },
        }
    finally:
        if is_persisted and silver_df is not None:
            silver_df.unpersist()


def clean_and_write_all_tables(
    source_lakehouse_name: str,
    target_lakehouse_name: str,
    mode: str = "overwrite",
    partition_by: Optional[list[str]] = None,
    auto_partition: bool = True,
    auto_partition_threshold_bytes: int = 1_073_741_824,
    tables_config: Optional[list[dict[str, Any]]] = None,
    include_schemas: Optional[list[str]] = None,
    exclude_tables: Optional[list[str]] = None,
    continue_on_error: bool = False,
    spark: Optional[SparkSession] = None,
    verbose: bool = True,
    *,
    max_workers: Optional[int] = None,
    auto_partition_when_partition_by_provided: bool = True,
    persist_intermediate: bool = False,
) -> dict[str, Any]:
    """Bulk clean/write (or merge) using discovery or an explicit ``tables_config``.

    When ``tables_config`` is omitted, jobs are built from
    :py:func:`fabrictools.io.discovery.list_lakehouse_tables_for_pipeline`; target
    paths use a ``Cleaned_`` leaf (PascalCase from the source table), e.g.
    ``Tables/dbo/projets table`` → ``Tables/dbo/Cleaned_ProjetsTable``.

    :param source_lakehouse_name: Lakehouse to read from.
    :param target_lakehouse_name: Lakehouse to write or merge into.
    :param mode: Default mode when not overridden per table (``overwrite``, ``append``, ``merge``).
    :param partition_by: Default partition columns for writes.
    :param auto_partition: If ``True`` (default), automatically partition the data
        by detected date columns if they exist.
    :param auto_partition_threshold_bytes: Threshold in bytes to trigger auto-partitioning.
    :param tables_config: Optional list of per-table job dicts (see ``pipelines.config``).
    :param include_schemas: Discovery filter: schema allow-list.
    :param exclude_tables: Discovery filter: table deny-list.
    :param continue_on_error: If ``False``, stop on first failure.
    :param max_workers: Maximum number of tables processed concurrently. When omitted,
        uses ``min(total_tables, 5)``. Pass ``1`` to force sequential behavior.
    :param auto_partition_when_partition_by_provided: If ``False``, skip automatic
        partition detection when a table already has explicit ``partition_by``.
    :param persist_intermediate: If ``True``, persist each cleaned Silver dataframe
        for the duration of its write/merge, then unpersist it.
    :param spark: Optional ``SparkSession``.
    :type source_lakehouse_name: str
    :type target_lakehouse_name: str
    :type mode: str
    :type partition_by: list[str] | None
    :type tables_config: list[dict] | None
    :type include_schemas: list[str] | None
    :type exclude_tables: list[str] | None
    :type continue_on_error: bool
    :type max_workers: int | None
    :type auto_partition_when_partition_by_provided: bool
    :type persist_intermediate: bool
    :type spark: ~pyspark.sql.SparkSession | None

    :returns: Summary dict with ``total_tables``, ``successful_tables``, ``failed_tables``,
        ``tables``, ``failures``.
    :rtype: dict

    .. rubric:: Example

    >>> summary = clean_and_write_all_tables(  # doctest: +SKIP
    ...     "BronzeLakehouse",
    ...     "SilverLakehouse",
    ...     mode="overwrite",
    ...     include_schemas=["dbo"],
    ...     exclude_tables=["dbo.LegacyArchive"],
    ... )
    >>> summary["successful_tables"]  # doctest: +SKIP
    8
    """
    _spark = configure_parquet_datetime_rebase(spark or get_spark())
    table_jobs = _build_jobs(
        source_lakehouse_name=source_lakehouse_name,
        mode=mode,
        partition_by=partition_by,
        tables_config=tables_config,
        include_schemas=include_schemas,
        exclude_tables=exclude_tables,
    )

    if not table_jobs:
        if verbose:
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

    processed_tables: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    total_tables = len(table_jobs)
    effective_max_workers = _resolve_max_workers(max_workers, total_tables)

    if verbose:
        log(
            f"Bulk clean/write started: {total_tables} table(s) "
            f"from '{source_lakehouse_name}' to '{target_lakehouse_name}' "
            f"with up to {effective_max_workers} concurrent task(s)."
        )

    source_base_path = get_lakehouse_abfs_path(source_lakehouse_name)

    def process(index: int, table_job: TableJobConfig) -> dict[str, Any]:
        auto_partition_with_explicit_partitions = (
            auto_partition_when_partition_by_provided
        )
        return _process_clean_table_job(
            table_job=table_job,
            index=index,
            total_tables=total_tables,
            source_lakehouse_name=source_lakehouse_name,
            source_base_path=source_base_path,
            target_lakehouse_name=target_lakehouse_name,
            auto_partition=auto_partition,
            auto_partition_threshold_bytes=auto_partition_threshold_bytes,
            auto_partition_when_partition_by_provided=(
                auto_partition_with_explicit_partitions
            ),
            persist_intermediate=persist_intermediate,
            spark=_spark,
            verbose=verbose,
        )

    results_by_index: dict[int, dict[str, Any]] = {}
    if effective_max_workers == 1:
        for index, table_job in enumerate(table_jobs, start=1):
            result = process(index, table_job)
            results_by_index[index] = result
            if not result["ok"] and not continue_on_error:
                exc = result.get("exception")
                if isinstance(exc, BaseException):
                    raise exc
                raise RuntimeError("Bulk clean/write failed without exception details.")
    else:
        with ThreadPoolExecutor(max_workers=effective_max_workers) as executor:
            future_to_index = {
                executor.submit(process, index, table_job): index
                for index, table_job in enumerate(table_jobs, start=1)
            }
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                result = future.result()
                results_by_index[index] = result
                if not result["ok"] and not continue_on_error:
                    exc = result.get("exception")
                    if isinstance(exc, BaseException):
                        raise exc
                    raise RuntimeError(
                        "Bulk clean/write failed without exception details."
                    )

    for index in range(1, total_tables + 1):
        result = results_by_index.get(index)
        if result is None:
            continue
        if result["ok"]:
            processed_tables.append(result["entry"])
        else:
            failures.append(result["entry"])

    return {
        "total_tables": total_tables,
        "successful_tables": len(processed_tables),
        "failed_tables": len(failures),
        "tables": processed_tables,
        "failures": failures,
    }


__all__ = ["clean_and_write_data", "clean_and_write_all_tables"]
