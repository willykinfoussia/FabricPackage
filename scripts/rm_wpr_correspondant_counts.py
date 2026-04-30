"""Counts by customer correspondent for RM WPR rows over 10 days."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import fabrictools as ft
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from rme_spark import RmePaths, build_rm_wpr


TARGET_CLASSIFICATION = "> 10j"


@dataclass(frozen=True)
class RmWprCorrespondantCountsPaths:
    """Lakehouse relative path used by this derived output."""

    output: str = "RMWPRCorrespondantCounts"


def _require_column(df: DataFrame, name: str) -> str:
    resolved = ft.resolve_dataframe_column(df, name)
    if resolved is None:
        raise ValueError(f"Colonne introuvable: {name!r}")
    return resolved


def build_rm_wpr_correspondant_counts(rm_wpr_df: DataFrame) -> DataFrame:
    """Filter RM WPR rows over 10 days, then count by customer correspondent."""

    classification_col = _require_column(rm_wpr_df, "Classement Nb Jours")
    correspondant_col = _require_column(rm_wpr_df, "Nom Correspondant Clt")

    filtered = rm_wpr_df.where(F.col(classification_col) == F.lit(TARGET_CLASSIFICATION))
    grouped = filtered.groupBy(correspondant_col).agg(
        F.count(F.lit(1)).cast("bigint").alias("Nombre")
    )
    return grouped.orderBy(F.col("Nombre").desc())


def build_rm_wpr_correspondant_counts_from_lakehouse(
    *,
    lakehouse_name: str,
    rme_paths: RmePaths = RmePaths(),
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build RM WPR, then return counts by customer correspondent."""

    rm_wpr_df = build_rm_wpr(
        lakehouse_name=lakehouse_name,
        paths=rme_paths,
        spark=spark,
        today=today,
    )
    return build_rm_wpr_correspondant_counts(rm_wpr_df)


def run_rm_wpr_correspondant_counts_pipeline(
    *,
    lakehouse_name: str,
    rme_paths: RmePaths = RmePaths(),
    counts_paths: RmWprCorrespondantCountsPaths = RmWprCorrespondantCountsPaths(),
    mode: str = "overwrite",
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build and write the derived counts table, then return it."""

    output_df = build_rm_wpr_correspondant_counts_from_lakehouse(
        lakehouse_name=lakehouse_name,
        rme_paths=rme_paths,
        spark=spark,
        today=today,
    )

    ft.write_lakehouse(
        output_df,
        lakehouse_name=lakehouse_name,
        relative_path=counts_paths.output,
        mode=mode,
        format="delta",
        spark=spark,
        normalize_column_names=False,
        enable_column_mapping=True,
        auto_partition=False,
    )
    return output_df


__all__ = [
    "RmWprCorrespondantCountsPaths",
    "build_rm_wpr_correspondant_counts",
    "build_rm_wpr_correspondant_counts_from_lakehouse",
    "run_rm_wpr_correspondant_counts_pipeline",
]
