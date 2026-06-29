"""Write IFS JSON payloads to Fabric Lakehouses."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession  # type: ignore[reportMissingImports]

from fabrictools.integrations.ifs._logging import log_ifs
from fabrictools.integrations.ifs.dataframe import ifs_data_to_dataframe
from fabrictools.io.lakehouse import write_lakehouse, write_lakehouses

_WRITE_LAKEHOUSE_OPTIONAL_KEYS = (
    "merge_condition",
    "upsert_key_columns",
    "normalize_column_names",
    "enable_column_mapping",
    "auto_partition",
    "auto_partition_threshold_bytes",
)


def write_ifs_data_to_lakehouse(
    ifs_data: str,
    lakehouse_name: str,
    relative_path: str,
    *,
    mode: str = "overwrite",
    partition_by: Optional[list[str]] = None,
    format: str = "delta",
    spark: Optional[SparkSession] = None,
    merge_condition: Optional[str] = None,
    upsert_key_columns: Optional[Sequence[str]] = None,
    normalize_column_names: bool = True,
    enable_column_mapping: bool = False,
    auto_partition: bool = False,
    auto_partition_threshold_bytes: int = 1_073_741_824,
) -> DataFrame:
    """Convert an IFS JSON string to a Spark DataFrame and write it to a Lakehouse.

    :param ifs_data: JSON string — array of entities or OData ``{"value": [...]}``.
    :param lakehouse_name: Target Lakehouse display name.
    :param relative_path: Lakehouse relative path (e.g. ``Tables/dbo/ifs_customers``).
    :param mode: Lakehouse write mode (default ``overwrite``).
    :param partition_by: Optional partition columns for the write.
    :param format: Output format (default ``delta``).
    :param spark: Optional Spark session; active session is used when omitted.
    """
    log_ifs(
        f"write_ifs_data_to_lakehouse — démarrage "
        f"(lakehouse={lakehouse_name!r}, path={relative_path!r}, mode={mode!r})"
    )
    df = ifs_data_to_dataframe(ifs_data, spark=spark)
    log_ifs(f"Écriture Lakehouse — {lakehouse_name}/{relative_path} (mode={mode})")
    write_lakehouse(
        df,
        lakehouse_name,
        relative_path,
        mode=mode,
        partition_by=partition_by,
        format=format,
        spark=spark,
        merge_condition=merge_condition,
        upsert_key_columns=upsert_key_columns,
        normalize_column_names=normalize_column_names,
        enable_column_mapping=enable_column_mapping,
        auto_partition=auto_partition,
        auto_partition_threshold_bytes=auto_partition_threshold_bytes,
    )
    log_ifs(f"write_ifs_data_to_lakehouse — terminé ({df.count()} ligne(s) écrites)")
    return df


def write_ifs_data_to_lakehouses(
    requests: list[dict[str, Any]],
    *,
    max_workers: Optional[int] = None,
    continue_on_error: bool = False,
    spark: Optional[SparkSession] = None,
) -> dict[str, Any]:
    """Convert multiple IFS JSON strings to DataFrames and write them in parallel.

    Each request must contain ``ifs_data``, ``lakehouse_name`` and ``relative_path``.
    Optional keys mirror :py:func:`write_lakehouse`: ``mode``, ``partition_by``,
    ``format``, ``merge_condition``, ``upsert_key_columns``, ``name``,
    ``normalize_column_names``, ``enable_column_mapping``, ``auto_partition`` and
    ``auto_partition_threshold_bytes``.

    DataFrame conversion runs sequentially on the driver; Lakehouse writes run in
    parallel via :py:func:`write_lakehouses`.
    """
    if not requests:
        return {
            "total_tables": 0,
            "successful_tables": 0,
            "failed_tables": 0,
            "tables": [],
            "failures": [],
        }

    log_ifs(
        f"write_ifs_data_to_lakehouses — démarrage ({len(requests)} table(s))"
    )
    write_requests: list[dict[str, Any]] = []
    for index, request in enumerate(requests, start=1):
        if "ifs_data" not in request:
            raise ValueError(
                f"write_ifs_data_to_lakehouses requests[{index}] is missing required key 'ifs_data'."
            )
        if "lakehouse_name" not in request:
            raise ValueError(
                f"write_ifs_data_to_lakehouses requests[{index}] is missing required key 'lakehouse_name'."
            )
        if "relative_path" not in request:
            raise ValueError(
                f"write_ifs_data_to_lakehouses requests[{index}] is missing required key 'relative_path'."
            )

        write_request: dict[str, Any] = {
            "df": ifs_data_to_dataframe(str(request["ifs_data"]), spark=spark),
            "lakehouse_name": request["lakehouse_name"],
            "relative_path": request["relative_path"],
        }
        if request.get("name") is not None:
            write_request["name"] = request["name"]
        for key in ("mode", "partition_by", "format", *_WRITE_LAKEHOUSE_OPTIONAL_KEYS):
            if key in request:
                write_request[key] = request[key]
        write_requests.append(write_request)

    summary = write_lakehouses(
        write_requests,
        max_workers=max_workers,
        continue_on_error=continue_on_error,
        spark=spark,
    )
    log_ifs(
        f"write_ifs_data_to_lakehouses — terminé "
        f"({summary['successful_tables']}/{summary['total_tables']} table(s) écrites)"
    )
    return summary
