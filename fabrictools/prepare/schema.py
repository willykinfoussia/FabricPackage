"""Prepared schema snapshot helpers."""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame, functions as F
from typing import Any, Optional
import json
import hashlib
from datetime import datetime

from fabrictools.core import log
from fabrictools.core import get_spark
from fabrictools.io import read_lakehouse, write_lakehouse, resolve_lakehouse_read_candidate, get_lakehouse_abfs_path


CONFIG_SOURCE_SNAPSHOT_PATH = "schema_snapshot"


def _build_schema_hash(df: DataFrame) -> str:
    """Build a stable hash from column names and data types."""
    payload = "|".join(
        f"{field.name}:{field.dataType.simpleString()}"
        for field in df.schema.fields
    )
    return hashlib.md5(payload.encode("utf-8")).hexdigest()

def snapshot_source_schema(
    source_lakehouse_name: str,
    source_relative_path: str,
    spark: Optional[SparkSession] = None,
) -> str:
    """
    Snapshot source schema and profile stats into source config table.

    Returns the schema hash used for cache invalidation.
    """
    _spark = spark or get_spark()
    source_df = read_lakehouse(source_lakehouse_name, source_relative_path, spark=_spark)
    schema_hash = _build_schema_hash(source_df)
    total_rows = source_df.count()

    agg_exprs = []
    for col_name in source_df.columns:
        agg_exprs.extend(
            [
                F.min(F.col(col_name)).alias(f"{col_name}__min"),
                F.max(F.col(col_name)).alias(f"{col_name}__max"),
                F.sum(F.when(F.col(col_name).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(
                    f"{col_name}__nulls"
                ),
                F.avg(F.length(F.col(col_name).cast("string"))).alias(f"{col_name}__avg_len"),
                F.countDistinct(F.col(col_name)).alias(f"{col_name}__distinct"),
            ]
        )
    stats_row = source_df.agg(*agg_exprs).collect()[0].asDict()

    describe_detail_payload = ""
    describe_extended_payload = ""
    try:
        resolved = resolve_lakehouse_read_candidate(
            source_lakehouse_name,
            source_relative_path,
            spark=_spark,
        )
        abfs_base = get_lakehouse_abfs_path(source_lakehouse_name)
        full_path = f"{abfs_base}/{resolved}"
        describe_detail_payload = json.dumps(
            _spark.sql(f"DESCRIBE DETAIL delta.`{full_path}`").first().asDict(),
            default=str,
        )
        describe_extended_payload = json.dumps(
            [row.asDict() for row in _spark.sql(f"DESCRIBE EXTENDED delta.`{full_path}`").collect()],
            default=str,
        )
    except Exception as exc:
        log(f"Could not collect DESCRIBE DETAIL/EXTENDED: {exc}", level="warning")

    snapshot_rows: list[dict[str, Any]] = []
    for field in source_df.schema.fields:
        name = field.name
        distinct_count = int(stats_row.get(f"{name}__distinct") or 0)
        cardinality_ratio = (
            float(distinct_count) / float(total_rows) if total_rows else 0.0
        )
        snapshot_rows.append(
            {
                "snapshot_timestamp": datetime.utcnow().isoformat(),
                "source_relative_path": source_relative_path,
                "schema_hash": schema_hash,
                "col_source": name,
                "delta_type": field.dataType.simpleString(),
                "min_value": None if stats_row.get(f"{name}__min") is None else str(stats_row.get(f"{name}__min")),
                "max_value": None if stats_row.get(f"{name}__max") is None else str(stats_row.get(f"{name}__max")),
                "null_count": int(stats_row.get(f"{name}__nulls") or 0),
                "avg_len": float(stats_row.get(f"{name}__avg_len") or 0.0),
                "distinct_count": distinct_count,
                "total_count": int(total_rows),
                "cardinality_ratio": cardinality_ratio,
                "describe_detail": describe_detail_payload,
                "describe_extended": describe_extended_payload,
            }
        )

    snapshot_df = _spark.createDataFrame(snapshot_rows)
    ordered_snapshot_columns = ["col_source"] + [
        col_name for col_name in snapshot_df.columns if col_name != "col_source"
    ]
    snapshot_df = snapshot_df.select(*ordered_snapshot_columns)
    write_lakehouse(
        snapshot_df,
        lakehouse_name=source_lakehouse_name,
        relative_path=f"{source_relative_path}_{CONFIG_SOURCE_SNAPSHOT_PATH}",
        mode="overwrite",
        spark=_spark,
    )
    log(
        "Source schema snapshot written: "
        f"{len(snapshot_rows)} columns, schema_hash={schema_hash}"
    )
    return schema_hash


__all__ = ["snapshot_source_schema"]