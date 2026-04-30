"""Spark transformation derived from the RME resources-requested dataframe.

The source dataframe is the output of ``build_rme_resources_requested`` from
``rme_spark.py``. This script mirrors the Power Query that keeps delayed RM/PN
rows for follow-up comments.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import fabrictools as ft
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from rme_spark import RmePaths, build_rme_resources_requested


METRO_STILL_IN_TIME = "metro still in time"
LOAN_TYPE_RM = "Loan"

OUTPUT_COLUMNS = ("RM", "PN", "retard (j)", "Type RM", "Commentaires")

COLUMNS_TO_REMOVE = (
    "id",
    "CSR reference",
    "Creator",
    "Status",
    "Type",
    "Bench",
    "compagnie",
    "requirements_date",
    "designation_return_customer",
    "site",
    "purchase_order",
    "pn_sent_customer",
    "sn_sent_customer",
    "sn_return_customer",
    "cc",
    "ml",
    "warranty",
    "refurnish_stock",
    "stock",
    "AWB T&S",
    "AWB Customer",
    "priority",
    "used",
    "Did Standard Exchange solve your failure",
    "resource_received",
    "failure_description",
    "display_repair_report",
    "shipping_date_ts",
    "shipping_date_client",
    "return_date",
    "date_creation",
    "date_resource_shipping",
    "date_confirmation",
    "date_acknowledgment",
    "date_last_modified",
    "close",
    "loan_return_date",
    "packingsheetreference_ts",
    "packingsheetreference_client",
    "designation_sent_customer",
    "comment_close",
    "date_close",
    "Creator close",
    "LC Reason",
    "LC Purchase order reference",
    "LS CCML",
    "LS PO price",
    "LS PO currency",
    "LS Taken in charge by",
    "LS Bench type",
    "LS Final destination",
    "LS CER Ref",
    "LS Military customer",
    "LS Metrological conformity before intervention",
    "LS Comment",
    "LS Metrological follow-up",
    "LS CER Starting Date",
    "LS CER Ending Date",
    "CR complete",
    "CR Declarant",
    "CR File number",
    "CR AWB arrival",
    "CR Customs export declaration to be sold",
    "CR Information source",
    "CR Return packing sheet ref.",
    "CR Supply Origin",
    "CR Declared customs value",
    "CR Customs value currency",
    "CR Import regime",
    "CR Import value (euro)",
    "CR Customs information date",
    "CR Physical reception date",
    "reason",
    "comment_rm",
    "comment_shipping",
    "comment_return_confirmation",
    "comment_acknowledgment",
    "comment_repair_report",
    "end_date_validity_customer",
    "end_date_validity_sent",
    "RM / PN",
)


@dataclass(frozen=True)
class RmeResourcesRequestedDelayPaths:
    """Lakehouse relative path used by this derived output."""

    output: str = "RMEResourcesRequestedDelay"


def _require_column(df: DataFrame, name: str) -> str:
    resolved = ft.resolve_dataframe_column(df, name)
    if resolved is None:
        raise ValueError(f"Colonne introuvable: {name!r}")
    return resolved


def build_rme_resources_requested_delay(source_df: DataFrame) -> DataFrame:
    """Transform the output of ``build_rme_resources_requested``."""

    class_col = _require_column(source_df, "Classement Nb jours")
    type_rm_col = _require_column(source_df, "Type RM")
    nb_days_col = _require_column(source_df, "Nb Jours")

    cleaned = ft.remove_columns(source_df, *COLUMNS_TO_REMOVE)
    filtered = cleaned.where(
        (F.col(class_col) != F.lit(METRO_STILL_IN_TIME))
        & (F.col(type_rm_col) != F.lit(LOAN_TYPE_RM))
    )
    ordered = filtered.orderBy(F.col(nb_days_col).desc())
    without_class = ft.remove_columns(ordered, "Classement Nb jours")

    renamed = (
        without_class.withColumnRenamed(_require_column(without_class, "pn_return_customer"), "PN")
        .withColumnRenamed(_require_column(without_class, "Reference"), "RM")
        .withColumnRenamed(_require_column(without_class, "Nb Jours"), "retard (j)")
    )

    return renamed.select("RM", "PN", "retard (j)", "Type RM").withColumn(
        "Commentaires",
        F.lit(""),
    )


def build_rme_resources_requested_delay_from_lakehouse(
    *,
    lakehouse_name: str,
    rme_paths: RmePaths = RmePaths(),
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build the source RME dataframe, then apply the delay transformation."""

    source_df = build_rme_resources_requested(
        lakehouse_name=lakehouse_name,
        paths=rme_paths,
        spark=spark,
        today=today,
    )
    return build_rme_resources_requested_delay(source_df)


def run_rme_resources_requested_delay_pipeline(
    *,
    lakehouse_name: str,
    rme_paths: RmePaths = RmePaths(),
    delay_paths: RmeResourcesRequestedDelayPaths = RmeResourcesRequestedDelayPaths(),
    mode: str = "overwrite",
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build and write the derived delay table, then return it."""

    output_df = build_rme_resources_requested_delay_from_lakehouse(
        lakehouse_name=lakehouse_name,
        rme_paths=rme_paths,
        spark=spark,
        today=today,
    )

    ft.write_lakehouse(
        output_df,
        lakehouse_name=lakehouse_name,
        relative_path=delay_paths.output,
        mode=mode,
        format="delta",
        spark=spark,
        normalize_column_names=False,
        enable_column_mapping=True,
        auto_partition=False,
    )
    return output_df


__all__ = [
    "RmeResourcesRequestedDelayPaths",
    "build_rme_resources_requested_delay",
    "build_rme_resources_requested_delay_from_lakehouse",
    "run_rme_resources_requested_delay_pipeline",
]
