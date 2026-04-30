"""Counts of active CSR rows by customer correspondent and company."""

from __future__ import annotations

from dataclasses import dataclass

import fabrictools as ft
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from csr_spark import CSR_TYPE_MAP


CSR_STATUSES = (
    "Request treated by Commercial Department",
    "Request treated by Support",
    "Request validated by Support",
    "Request waiting for Commercial Treatment",
    "Temporary solution waiting for customer validation",
    "Waiting for Support acceptance",
    "Waiting for Support Treatment",
)

NA_VALUE = "#N/A"


@dataclass(frozen=True)
class CsrCorrespondantCountsPaths:
    """Lakehouse relative paths used by this CSR derived output."""

    csr_data: str = "CSRData"
    correspondant_client: str = "Correspondant Client"
    output: str = "CSRCorrespondantCounts"


def _require_column(df: DataFrame, name: str) -> str:
    resolved = ft.resolve_dataframe_column(df, name)
    if resolved is None:
        raise ValueError(f"Colonne introuvable: {name!r}")
    return resolved


def _join_correspondant_client(
    csr_df: DataFrame,
    correspondant_client_df: DataFrame,
) -> DataFrame:
    company_col = _require_column(csr_df, "compagnie")
    trigramme_col = _require_column(correspondant_client_df, "Trigramme")
    correspondant_col = _require_column(correspondant_client_df, "Nom Correspondant Clt")

    lookup = correspondant_client_df.select(
        F.col(trigramme_col).alias("__correspondant_trigramme"),
        F.col(correspondant_col).alias("Nom Correspondant Clt"),
    )

    return csr_df.join(
        lookup,
        F.col(company_col) == F.col("__correspondant_trigramme"),
        "left",
    ).drop("__correspondant_trigramme")


def build_csr_correspondant_counts(
    *,
    csr_df: DataFrame,
    correspondant_client_df: DataFrame,
) -> DataFrame:
    """Count CSR rows by customer correspondent and company."""

    typed = ft.cast_columns(csr_df, CSR_TYPE_MAP)
    status_col = _require_column(typed, "Status")
    company_col = _require_column(typed, "compagnie")

    filtered = typed.where(F.col(status_col).isin(*CSR_STATUSES))
    joined = _join_correspondant_client(filtered, correspondant_client_df)
    correspondant_col = _require_column(joined, "Nom Correspondant Clt")

    cleaned = joined.withColumn(
        correspondant_col,
        F.when(F.col(correspondant_col) == F.lit(NA_VALUE), F.lit(None)).otherwise(
            F.col(correspondant_col)
        ),
    )
    with_correspondant = cleaned.where(F.col(correspondant_col).isNotNull())
    grouped = with_correspondant.groupBy(correspondant_col, company_col).agg(
        F.count(F.lit(1)).cast("bigint").alias("Nombre")
    )
    return grouped.orderBy(F.col("Nombre").desc())


def build_csr_correspondant_counts_from_lakehouse(
    *,
    lakehouse_name: str,
    paths: CsrCorrespondantCountsPaths = CsrCorrespondantCountsPaths(),
    spark: SparkSession | None = None,
) -> DataFrame:
    """Read CSR and correspondent tables from Lakehouse, then build counts."""

    csr_df = ft.read_lakehouse(lakehouse_name, paths.csr_data, spark=spark)
    correspondant_client_df = ft.read_lakehouse(
        lakehouse_name,
        paths.correspondant_client,
        spark=spark,
    )
    return build_csr_correspondant_counts(
        csr_df=csr_df,
        correspondant_client_df=correspondant_client_df,
    )


def run_csr_correspondant_counts_pipeline(
    *,
    lakehouse_name: str,
    paths: CsrCorrespondantCountsPaths = CsrCorrespondantCountsPaths(),
    mode: str = "overwrite",
    spark: SparkSession | None = None,
) -> DataFrame:
    """Build and write the CSR correspondent counts table, then return it."""

    output_df = build_csr_correspondant_counts_from_lakehouse(
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
    "CsrCorrespondantCountsPaths",
    "build_csr_correspondant_counts",
    "build_csr_correspondant_counts_from_lakehouse",
    "run_csr_correspondant_counts_pipeline",
]
