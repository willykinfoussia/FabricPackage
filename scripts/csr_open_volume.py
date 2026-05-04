"""Open CSR volumes grouped by business state.

This script mirrors the ``scripts/CSR`` Power Query: it keeps active CSR
statuses, counts rows by status, then folds them into Support vs Commercial
open volumes.
"""

from __future__ import annotations

from dataclasses import dataclass

import fabrictools as ft
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from csr_spark import CSR_TYPE_MAP


OPEN_CSR_STATUSES = (
    "Request treated by Support",
    "Request validated by Support",
    "Request waiting for Commercial Treatment",
    "Waiting for Support acceptance",
    "Waiting for Support Treatment",
)

SUPPORT_CSR_STATUSES = (
    "Request treated by Support",
    "Request validated by Support",
    "Temporary solution waiting for customer validation",
    "Waiting for Support acceptance",
    "Waiting for Support Treatment",
)


@dataclass(frozen=True)
class CsrOpenVolumePaths:
    """Lakehouse relative paths used by this CSR derived output."""

    csr_data: str = "CSRData"
    output: str = "CSR"


def _require_column(df: DataFrame, name: str) -> str:
    resolved = ft.resolve_dataframe_column(df, name)
    if resolved is None:
        raise ValueError(f"Colonne introuvable: {name!r}")
    return resolved


def build_csr_open_volume(csr_df: DataFrame) -> DataFrame:
    """Build open CSR volumes by ``Etat CSR`` from the raw CSR dataframe."""

    typed = ft.cast_columns(csr_df, CSR_TYPE_MAP)
    status_col = _require_column(typed, "Status")

    filtered = typed.where(F.col(status_col).isin(*OPEN_CSR_STATUSES))
    counts_by_status = filtered.groupBy(status_col).agg(
        F.count(F.lit(1)).cast("bigint").alias("Nombre")
    )

    with_business_state = counts_by_status.withColumn(
        "Etat CSR",
        F.when(F.col(status_col).isin(*SUPPORT_CSR_STATUSES), F.lit("Open Support"))
        .otherwise(F.lit("Open Commercial")),
    )

    return with_business_state.groupBy("Etat CSR").agg(
        F.sum("Nombre").cast("bigint").alias("Volume")
    )


def build_csr_open_volume_from_lakehouse(
    *,
    lakehouse_name: str,
    paths: CsrOpenVolumePaths = CsrOpenVolumePaths(),
    spark: SparkSession | None = None,
) -> DataFrame:
    """Read CSR data from Lakehouse, then build open CSR volumes."""

    csr_df = ft.read_lakehouse(lakehouse_name, paths.csr_data, spark=spark)
    return build_csr_open_volume(csr_df)


def run_csr_open_volume_pipeline(
    *,
    lakehouse_name: str,
    paths: CsrOpenVolumePaths = CsrOpenVolumePaths(),
    mode: str = "overwrite",
    spark: SparkSession | None = None,
) -> DataFrame:
    """Build and write the CSR open-volume table, then return it."""

    output_df = build_csr_open_volume_from_lakehouse(
        lakehouse_name=lakehouse_name,
        paths=paths,
        spark=spark,
    )

    ft.write_lakehouse(
        output_df,
        lakehouse_name=lakehouse_name,
        relative_path=paths.output,
        mode=mode,
        format="delta",
        spark=spark,
        normalize_column_names=False,
        enable_column_mapping=True,
        auto_partition=False,
    )
    return output_df


__all__ = [
    "CsrOpenVolumePaths",
    "build_csr_open_volume",
    "build_csr_open_volume_from_lakehouse",
    "run_csr_open_volume_pipeline",
]
