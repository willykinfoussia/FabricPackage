"""Counts by customer correspondent for RM WPRC rows outside the < 30j bucket."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import fabrictools as ft
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from rme_spark import RmePaths, build_rm_wprc


EXCLUDED_CLASSIFICATION = "< 30j"


@dataclass(frozen=True)
class RmWprcCorrespondantCountsPaths:
    """Lakehouse relative path used by this derived output."""

    output: str = "RMWPRCCorrespondantCounts"


def _require_column(df: DataFrame, name: str) -> str:
    resolved = ft.resolve_dataframe_column(df, name)
    if resolved is None:
        raise ValueError(f"Colonne introuvable: {name!r}")
    return resolved


def build_rm_wprc_correspondant_counts(rm_wprc_df: DataFrame) -> DataFrame:
    """Filter RM WPRC rows, then count by customer correspondent."""

    classification_col = _require_column(rm_wprc_df, "Classement Nb Jours")
    correspondant_col = _require_column(rm_wprc_df, "Nom Correspondant Clt")

    filtered = rm_wprc_df.where(
        F.col(classification_col) != F.lit(EXCLUDED_CLASSIFICATION)
    )
    grouped = filtered.groupBy(correspondant_col).agg(
        F.count(F.lit(1)).cast("bigint").alias("Nombre")
    )
    without_null_names = grouped.where(F.col(correspondant_col).isNotNull())
    return without_null_names.orderBy(F.col("Nombre").desc())


def build_rm_wprc_correspondant_counts_from_lakehouse(
    *,
    lakehouse_name: str,
    rme_paths: RmePaths = RmePaths(),
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build RM WPRC, then return counts by customer correspondent."""

    rm_wprc_df = build_rm_wprc(
        lakehouse_name=lakehouse_name,
        paths=rme_paths,
        spark=spark,
        today=today,
    )
    return build_rm_wprc_correspondant_counts(rm_wprc_df)


def run_rm_wprc_correspondant_counts_pipeline(
    *,
    lakehouse_name: str,
    rme_paths: RmePaths = RmePaths(),
    counts_paths: RmWprcCorrespondantCountsPaths = RmWprcCorrespondantCountsPaths(),
    mode: str = "overwrite",
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build and write the derived counts table, then return it."""

    output_df = build_rm_wprc_correspondant_counts_from_lakehouse(
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
    "RmWprcCorrespondantCountsPaths",
    "build_rm_wprc_correspondant_counts",
    "build_rm_wprc_correspondant_counts_from_lakehouse",
    "run_rm_wprc_correspondant_counts_pipeline",
]
