"""Top companies from combined RM WPR and RM WPRC dataframes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import fabrictools as ft
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from rme_spark import RmePaths, build_rm_wpr, build_rm_wprc


EXCLUDED_CLASSIFICATIONS = ("< 10j", "< 30j")
LOAN_TYPE_RM = "Loan"


@dataclass(frozen=True)
class RmWaitingTopCompaniesPaths:
    """Lakehouse relative path used by this derived output."""

    output: str = "RMWaitingTopCompanies"


def _require_column(df: DataFrame, name: str) -> str:
    resolved = ft.resolve_dataframe_column(df, name)
    if resolved is None:
        raise ValueError(f"Colonne introuvable: {name!r}")
    return resolved


def build_rm_waiting_top_companies(
    *,
    rm_wpr_df: DataFrame,
    rm_wprc_df: DataFrame,
    limit: int = 10,
) -> DataFrame:
    """Combine RM WPR and RM WPRC dataframes, then return the top companies."""

    combined = rm_wpr_df.unionByName(rm_wprc_df, allowMissingColumns=True)
    classification_col = _require_column(combined, "Classement Nb Jours")
    type_rm_col = _require_column(combined, "Type RM")
    company_col = _require_column(combined, "compagnie")

    filtered = combined.where(
        (~F.col(classification_col).isin(*EXCLUDED_CLASSIFICATIONS))
        & (F.col(type_rm_col) != F.lit(LOAN_TYPE_RM))
    )
    grouped = filtered.groupBy(company_col).agg(F.count(F.lit(1)).cast("bigint").alias("Nombre"))
    return grouped.orderBy(F.col("Nombre").desc()).limit(limit)


def build_rm_waiting_top_companies_from_lakehouse(
    *,
    lakehouse_name: str,
    rme_paths: RmePaths = RmePaths(),
    spark: SparkSession | None = None,
    today: date | None = None,
    limit: int = 10,
) -> DataFrame:
    """Build RM WPR and RM WPRC, then return the top companies."""

    rm_wpr_df = build_rm_wpr(
        lakehouse_name=lakehouse_name,
        paths=rme_paths,
        spark=spark,
        today=today,
    )
    rm_wprc_df = build_rm_wprc(
        lakehouse_name=lakehouse_name,
        paths=rme_paths,
        spark=spark,
        today=today,
    )
    return build_rm_waiting_top_companies(
        rm_wpr_df=rm_wpr_df,
        rm_wprc_df=rm_wprc_df,
        limit=limit,
    )


def run_rm_waiting_top_companies_pipeline(
    *,
    lakehouse_name: str,
    rme_paths: RmePaths = RmePaths(),
    top_paths: RmWaitingTopCompaniesPaths = RmWaitingTopCompaniesPaths(),
    mode: str = "overwrite",
    spark: SparkSession | None = None,
    today: date | None = None,
    limit: int = 10,
) -> DataFrame:
    """Build and write the derived top-companies table, then return it."""

    output_df = build_rm_waiting_top_companies_from_lakehouse(
        lakehouse_name=lakehouse_name,
        rme_paths=rme_paths,
        spark=spark,
        today=today,
        limit=limit,
    )

    ft.write_lakehouse(
        output_df,
        lakehouse_name=lakehouse_name,
        relative_path=top_paths.output,
        mode=mode,
        format="delta",
        spark=spark,
        normalize_column_names=False,
        enable_column_mapping=True,
        auto_partition=False,
    )
    return output_df


__all__ = [
    "RmWaitingTopCompaniesPaths",
    "build_rm_waiting_top_companies",
    "build_rm_waiting_top_companies_from_lakehouse",
    "run_rm_waiting_top_companies_pipeline",
]
