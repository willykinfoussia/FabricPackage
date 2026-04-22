"""Business readiness helpers for Gold layer."""

from __future__ import annotations

import re
from typing import Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from fabrictools.core.logging import log
from fabrictools.core.spark import get_spark
from fabrictools.io.lakehouse import read_lakehouse, write_lakehouse
from fabrictools.prepare.french_column_tokens import FRENCH_COLUMN_TOKEN_MAP


def _format_column_token(token: str) -> str:
    """Format one token with French mapping and safe special cases."""
    normalized = token.strip().lower()
    if not normalized:
        return ""
    if normalized == "n":
        return "N°"
    return FRENCH_COLUMN_TOKEN_MAP.get(normalized, normalized.capitalize())


def _to_pascal_case(name: str) -> str:
    """Remove Cleaned/Processed prefixes and convert to PascalCase.

    Example: 'Cleaned_clients_table' -> 'ClientsTable'
    """
    name = re.sub(r"^(Cleaned|Processed)_?", "", name, flags=re.IGNORECASE)
    parts = re.split(r"[^a-zA-Z0-9]+", name)
    return "".join(p[0].upper() + p[1:] for p in parts if p)


def _to_normal_case(name: str) -> str:
    """Convert snake_case column name to Normal Case.

    Uses token-level mapping to restore common French accents/abbreviations.

    Examples:
    - 'client_description' -> 'Client Description'
    - 'annee_cree' -> 'Année Créée'
    - 'n_element' -> 'N° Élément'
    - 'qte_entree' -> 'Qté Entrée'
    """
    parts = re.split(r"_", name)
    return " ".join(_format_column_token(part) for part in parts if part)


def _resolve_target_path(source_path: str, custom_name: Optional[str]) -> str:
    """Resolve the target relative path for a table."""
    parts = source_path.replace("\\", "/").split("/")
    if custom_name:
        parts[-1] = custom_name
    else:
        parts[-1] = _to_pascal_case(parts[-1])
    return "/".join(parts)


def _infer_layer_name(lakehouse_name: str) -> str:
    """Infer a layer label from a lakehouse name."""
    normalized = lakehouse_name.strip().lower()
    if normalized.endswith("lakehouse"):
        normalized = normalized[: -len("lakehouse")]
    return normalized or lakehouse_name


def make_business_ready(
    source_lakehouse_name: str,
    target_lakehouse_name: str,
    tables: list[str],
    custom_table_names: Optional[dict[str, str]] = None,
    mode: str = "overwrite",
    ingestion_timestamp_col: str = "ingestion_timestamp",
    source_layer_col: str = "ingestion_source_layer",
    source_path_col: str = "ingestion_source_path",
    year_col: str = "ingestion_year",
    month_col: str = "ingestion_month",
    day_col: str = "ingestion_day",
    source_layer: Optional[str] = None,
    target_layer: str = "gold",
    spark: Optional[SparkSession] = None,
    verbose: bool = False,
) -> dict[str, Any]:
    """Transform Silver tables to Business Ready (Gold) tables.

    Reads each table, updates ingestion metadata, renames columns from
    snake_case to Normal Case, renames tables (PascalCase, removing
    Cleaned/Processed), and writes them to the target Lakehouse.

    :param source_lakehouse_name: Source Lakehouse (e.g. Silver).
    :param target_lakehouse_name: Target Lakehouse (e.g. Gold).
    :param tables: List of relative paths for tables to process.
    :param custom_table_names: Optional mapping from source table path to
        exact target table name (leaf only or full relative path leaf).
    :param mode: Write mode (default: 'overwrite').
    :param ingestion_timestamp_col: Column name for the ingestion timestamp.
    :param source_layer_col: Column name for the ingestion layer.
    :param source_path_col: Column name for the source table path.
    :param year_col: Column name for ingestion year.
    :param month_col: Column name for ingestion month.
    :param day_col: Column name for ingestion day.
    :param source_layer: Optional source layer label override.
    :param target_layer: Name of the target layer (default: 'gold').
    :param spark: Optional SparkSession.
    :param verbose: Print processing details.

    :returns: Summary dictionary.
    :rtype: dict

    .. rubric:: Example

    >>> summary = make_business_ready(  # doctest: +SKIP
    ...     source_lakehouse_name="SilverLakehouse",
    ...     target_lakehouse_name="GoldLakehouse",
    ...     tables=["Tables/dbo/Cleaned_orders", "Tables/dbo/Processed_clients"],
    ...     custom_table_names={"Tables/dbo/Cleaned_orders": "CommandesBusiness"},
    ... )
    """
    _spark = spark or get_spark()
    custom_mapping = custom_table_names or {}
    source_layer_value = source_layer or _infer_layer_name(source_lakehouse_name)

    processed_tables: list[dict[str, str]] = []
    failures: list[dict[str, str]] = []
    total_tables = len(tables)

    if verbose:
        log(f"Starting make_business_ready for {total_tables} tables.")

    for index, src_table in enumerate(tables, start=1):
        try:
            custom_target = custom_mapping.get(src_table)
            tgt_table = _resolve_target_path(src_table, custom_target)

            if verbose:
                log(
                    f"[{index}/{total_tables}] Processing '{src_table}' -> '{tgt_table}' "
                    f"(target layer: {target_layer})..."
                )

            df = read_lakehouse(source_lakehouse_name, src_table, spark=_spark)

            # Refresh ingestion metadata from the current run context.
            current_date_expr = F.current_date()
            df = (
                df.withColumn(ingestion_timestamp_col, F.current_timestamp())
                .withColumn(source_layer_col, F.lit(source_layer_value))
                .withColumn(source_path_col, F.lit(src_table))
                .withColumn(year_col, F.year(current_date_expr))
                .withColumn(month_col, F.month(current_date_expr))
                .withColumn(day_col, F.dayofmonth(current_date_expr))
            )

            # Rename all columns to Normal Case in one shot.
            # Using toDF avoids case-only rename issues with Spark.
            new_columns = [_to_normal_case(col_name) for col_name in df.columns]
            df = df.toDF(*new_columns)

            # Write to Gold
            write_lakehouse(
                df,
                lakehouse_name=target_lakehouse_name,
                relative_path=tgt_table,
                mode=mode,
                spark=_spark,
                normalize_column_names=False,
                enable_column_mapping=True,
            )

            processed_tables.append(
                {
                    "source_relative_path": src_table,
                    "target_relative_path": tgt_table,
                    "mode": mode,
                }
            )

        except Exception as exc:
            if verbose:
                log(
                    f"[{index}/{total_tables}] Failed for '{src_table}': {exc}",
                    level="warning",
                )
            failures.append({"source_relative_path": src_table, "error": str(exc)})

    return {
        "total_tables": total_tables,
        "successful_tables": len(processed_tables),
        "failed_tables": len(failures),
        "tables": processed_tables,
        "failures": failures,
    }


__all__ = ["make_business_ready"]
