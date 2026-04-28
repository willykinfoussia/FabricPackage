"""Spark conversion of the RME Power Query pipeline.

The script reads the Power Query source tables from a Fabric Lakehouse through
fabrictools, applies the RME business rules, and optionally writes the result
back to a Lakehouse table.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

import fabrictools as ft
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


CONFIG_KEY_LOW_DAYS = "Seuil_bas_jours_resources_requested"
CONFIG_KEY_HIGH_DAYS = "Seuil_haut_jours_resources_requested"
CONFIG_KEY_WPRC_LOW_DAYS = "Seuil_bas_jours_waiting_part_return_confirmation"
CONFIG_KEY_WPRC_HIGH_DAYS = "Seuil_haut_jours_waiting_part_return_confirmation"
CONFIG_KEY_WPR_DAYS = "Seuil_jours_waiting_for_part_reception"

CONFIG_KEY_CANDIDATES = ("Name", "name", "Key", "key", "Cle", "Clé")
CONFIG_VALUE_CANDIDATES = ("Value", "value", "Valeur", "valeur")

ALLOWED_STATUSES = ("Not created", "Resource requested")
WPRC_STATUS = "Waiting for part return confirmation"
WPR_STATUS = "Waiting for part reception"
EXCLUDED_COMPANIES = ("EADS_TS", "EADS_TS_UK")
EXCLUDED_CSR_REFERENCE_PREFIX = "EADS_TS_"
METRO_STANDARD_EXCHANGE_TYPE = "Metrological Verification by Standard Exchange"
METRO_STILL_IN_TIME = "metro still in time"

RME_TYPE_MAP = {
    "id": "bigint",
    "CSR reference": "string",
    "Creator": "string",
    "Status": "string",
    "Type": "string",
    "Type RM": "string",
    "Bench": "string",
    "Reference": "string",
    "compagnie": "string",
    "requirements_date": "timestamp",
    "designation_return_customer": "string",
    "site": "string",
    "cc": "bigint",
    "ml": "bigint",
    "warranty": "string",
    "refurnish_stock": "string",
    "stock": "string",
    "AWB Customer": "bigint",
    "priority": "string",
    "used": "string",
    "Did Standard Exchange solve your failure": "string",
    "resource_received": "string",
    "failure_description": "string",
    "display_repair_report": "string",
    "shipping_date_ts": "timestamp",
    "date_creation": "timestamp",
    "date_resource_shipping": "timestamp",
    "date_confirmation": "timestamp",
    "date_acknowledgment": "timestamp",
    "date_last_modified": "timestamp",
    "close": "string",
    "loan_return_date": "timestamp",
    "packingsheetreference_client": "string",
    "designation_sent_customer": "string",
    "comment_close": "string",
    "date_close": "timestamp",
    "Creator close": "bigint",
    "LC Reason": "bigint",
    "LS CCML": "string",
    "LS PO currency": "string",
    "LS Taken in charge by": "string",
    "LS Bench type": "string",
    "LS Final destination": "string",
    "LS CER Ref": "string",
    "LS Military customer": "bigint",
    "LS Metrological conformity before intervention": "bigint",
    "LS Comment": "string",
    "LS Metrological follow-up": "bigint",
    "LS CER Starting Date": "timestamp",
    "LS CER Ending Date": "timestamp",
    "CR complete": "string",
    "CR Declarant": "string",
    "CR File number": "string",
    "CR AWB arrival": "bigint",
    "CR Information source": "string",
    "CR Return packing sheet ref.": "string",
    "CR Supply Origin": "string",
    "CR Declared customs value": "double",
    "CR Customs value currency": "string",
    "CR Import regime": "string",
    "CR Import value (euro)": "bigint",
    "CR Customs information date": "timestamp",
    "CR Physical reception date": "timestamp",
    "comment_rm": "string",
    "comment_shipping": "string",
    "comment_return_confirmation": "string",
    "comment_acknowledgment": "string",
    "end_date_validity_customer": "timestamp",
    "end_date_validity_sent": "timestamp",
}

WPRC_COLUMNS_TO_REMOVE = (
    "Type",
    "requirements_date",
    "site",
    "purchase_order",
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
    "shipping_date_client",
    "return_date",
    "date_acknowledgment",
    "date_last_modified",
    "close",
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
)

WPRC_OUTPUT_COLUMNS = (
    "Reference",
    "CSR reference",
    "Creator",
    "Status",
    "Type RM",
    "Bench",
    "compagnie",
    "designation_return_customer",
    "pn_sent_customer",
    "pn_return_customer",
    "sn_sent_customer",
    "sn_return_customer",
    "shipping_date_ts",
    "date_creation",
    "date_resource_shipping",
    "date_confirmation",
    "loan_return_date",
    "end_date_validity_customer",
    "end_date_validity_sent",
    "Nb Jours",
    "Classement Nb Jours",
    "Nom Correspondant Clt",
    "LienInternet",
)

WPR_COLUMNS_TO_REMOVE = tuple(
    column for column in WPRC_COLUMNS_TO_REMOVE if column != "requirements_date"
)

WPR_OUTPUT_COLUMNS = (
    "Reference",
    "CSR reference",
    "Creator",
    "Status",
    "Type RM",
    "Bench",
    "compagnie",
    "requirements_date",
    "designation_return_customer",
    "pn_sent_customer",
    "pn_return_customer",
    "sn_sent_customer",
    "sn_return_customer",
    "shipping_date_ts",
    "date_creation",
    "date_resource_shipping",
    "date_confirmation",
    "loan_return_date",
    "end_date_validity_customer",
    "end_date_validity_sent",
    "Nb Jours",
    "Classement Nb Jours",
    "Nom Correspondant Clt",
    "LienInternet",
)


@dataclass(frozen=True)
class RmePaths:
    """Lakehouse relative paths used by the RME pipeline."""

    config: str = "Config"
    rme: str = "RME"
    correspondant_client: str = "Correspondant Client"
    output: str = "RMEResourcesRequested"
    wprc_output: str = "RMWPRC"
    wpr_output: str = "RMWPR"


def _resolve_column_from_candidates(
    df: DataFrame,
    candidates: tuple[str, ...],
) -> str | None:
    for candidate in candidates:
        resolved = ft.resolve_dataframe_column(df, candidate)
        if resolved is not None:
            return resolved
    return None


def _require_column(df: DataFrame, *candidates: str) -> str:
    resolved = _resolve_column_from_candidates(df, tuple(candidates))
    if resolved is None:
        raise ValueError(
            "Colonne introuvable. Candidats: " + ", ".join(repr(c) for c in candidates)
        )
    return resolved


def _load_config(config_df: DataFrame) -> dict[str, str]:
    key_col = _resolve_column_from_candidates(config_df, CONFIG_KEY_CANDIDATES)
    value_col = _resolve_column_from_candidates(config_df, CONFIG_VALUE_CANDIDATES)

    if key_col is None or value_col is None:
        if len(config_df.columns) < 2:
            raise ValueError(
                "La table Config doit contenir au moins deux colonnes cle/valeur."
            )
        key_col = config_df.columns[0]
        value_col = config_df.columns[1]

    rows = (
        config_df.select(
            F.col(key_col).cast("string").alias("cfg_key"),
            F.col(value_col).cast("string").alias("cfg_value"),
        )
        .where(F.col("cfg_key").isNotNull())
        .collect()
    )
    return {
        row["cfg_key"]: row["cfg_value"]
        for row in rows
        if row["cfg_value"] is not None
    }


def _parse_config_number(config: dict[str, str], key: str) -> Decimal:
    raw_value = config.get(key)
    if raw_value is None:
        raise ValueError(f"Cle manquante dans la table Config: {key}")

    normalized = raw_value.strip().replace(",", ".")
    try:
        return Decimal(normalized)
    except InvalidOperation as exc:
        raise ValueError(
            f"La valeur de configuration {key!r} doit etre numerique: {raw_value!r}"
        ) from exc


def _format_config_number(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral_value():
        return str(normalized.quantize(Decimal("1")))
    return format(normalized, "f")


def _apply_schema(rme_df: DataFrame) -> DataFrame:
    return ft.cast_columns(rme_df, RME_TYPE_MAP)


def _filter_rme(rme_df: DataFrame) -> DataFrame:
    status_col = _require_column(rme_df, "Status")
    csr_reference_col = _require_column(rme_df, "CSR reference")

    return rme_df.where(F.col(status_col).isin(*ALLOWED_STATUSES)).where(
        ~F.col(csr_reference_col).startswith(EXCLUDED_CSR_REFERENCE_PREFIX)
    )


def _filter_wprc(rme_df: DataFrame) -> DataFrame:
    status_col = _require_column(rme_df, "Status")
    company_col = _require_column(rme_df, "compagnie")

    filtered = rme_df.where(F.col(status_col) == F.lit(WPRC_STATUS))
    return ft.filter_column_by_values(
        filtered,
        company_col,
        EXCLUDED_COMPANIES,
        exclude=True,
    )


def _filter_wpr(rme_df: DataFrame) -> DataFrame:
    status_col = _require_column(rme_df, "Status")
    company_col = _require_column(rme_df, "compagnie")

    filtered = rme_df.where(F.col(status_col) == F.lit(WPR_STATUS))
    return ft.filter_column_by_values(
        filtered,
        company_col,
        EXCLUDED_COMPANIES,
        exclude=True,
    )


def _add_day_classification(
    rme_df: DataFrame,
    *,
    low_days: Decimal,
    high_days: Decimal,
    today: date | None = None,
) -> DataFrame:
    requirements_date_col = _require_column(rme_df, "requirements_date")
    type_rm_col = _require_column(rme_df, "Type RM")
    validity_date_col = _require_column(rme_df, "end_date_validity_customer")
    low_value = float(low_days)
    high_value = float(high_days)
    low_text = _format_config_number(low_days)
    high_text = _format_config_number(high_days)
    current_day = F.lit(today.isoformat()).cast("date") if today else F.current_date()

    with_dates = (
        rme_df.withColumn(
            requirements_date_col,
            F.to_date(F.col(requirements_date_col)),
        )
        .withColumn(
            validity_date_col,
            F.to_date(F.col(validity_date_col)),
        )
        .withColumn(
            "Nb Jours",
            F.datediff(current_day, F.col(requirements_date_col)).cast("bigint"),
        )
    )

    base_classification = (
        F.when(F.col("Nb Jours") <= F.lit(low_value), F.lit(f"< {low_text}j"))
        .when(
            F.col("Nb Jours") <= F.lit(high_value),
            F.lit(f"{low_text}j < x < {high_text}j"),
        )
        .otherwise(F.lit(f"> {high_text}j"))
    )

    metro_classification = (
        F.when(
            F.col(validity_date_col).isNull()
            & (F.col(requirements_date_col) > current_day),
            F.lit(METRO_STILL_IN_TIME),
        )
        .when(
            F.col(validity_date_col).isNotNull()
            & (F.col(validity_date_col) > current_day),
            F.lit(METRO_STILL_IN_TIME),
        )
        .otherwise(F.col("Classement Nb jours"))
    )

    return (
        with_dates.withColumn("Classement Nb jours", base_classification)
        .withColumn(
            "metro",
            F.when(
                F.col(type_rm_col) == F.lit(METRO_STANDARD_EXCHANGE_TYPE),
                metro_classification,
            ).otherwise(F.col("Classement Nb jours")),
        )
        .drop("Classement Nb jours")
        .withColumnRenamed("metro", "Classement Nb jours")
    )


def _add_wprc_day_classification(
    rme_df: DataFrame,
    *,
    low_days: Decimal,
    high_days: Decimal,
    today: date | None = None,
) -> DataFrame:
    status_col = _require_column(rme_df, "Status")
    resource_shipping_col = _require_column(rme_df, "date_resource_shipping")
    confirmation_col = _require_column(rme_df, "date_confirmation")
    low_value = float(low_days)
    high_value = float(high_days)
    low_text = _format_config_number(low_days)
    high_text = _format_config_number(high_days)
    current_day = F.lit(today.isoformat()).cast("date") if today else F.current_date()

    with_dates = (
        rme_df.withColumn(
            confirmation_col,
            F.to_date(F.col(confirmation_col)),
        )
        .withColumn(
            resource_shipping_col,
            F.to_date(F.col(resource_shipping_col)),
        )
    )
    with_nb_days = with_dates.withColumn(
        "Nb Jours",
        F.when(
            F.col(status_col) == F.lit(WPRC_STATUS),
            F.datediff(current_day, F.col(resource_shipping_col)),
        )
        .otherwise(F.datediff(current_day, F.col(confirmation_col)))
        .cast("bigint"),
    )

    return with_nb_days.withColumn(
        "Classement Nb Jours",
        F.when(F.col("Nb Jours").isNull(), F.lit(None).cast("string"))
        .when(F.col("Nb Jours") < F.lit(low_value), F.lit(f"< {low_text}j"))
        .when(
            F.col("Nb Jours") < F.lit(high_value),
            F.lit(f"{low_text}j < x < {high_text}j"),
        )
        .otherwise(F.lit(f"> {high_text}j")),
    )


def _add_wpr_day_classification(
    rme_df: DataFrame,
    *,
    threshold_days: Decimal,
    today: date | None = None,
) -> DataFrame:
    status_col = _require_column(rme_df, "Status")
    resource_shipping_col = _require_column(rme_df, "date_resource_shipping")
    confirmation_col = _require_column(rme_df, "date_confirmation")
    threshold_value = float(threshold_days)
    threshold_text = _format_config_number(threshold_days)
    current_day = F.lit(today.isoformat()).cast("date") if today else F.current_date()

    with_dates = (
        rme_df.withColumn(
            confirmation_col,
            F.to_date(F.col(confirmation_col)),
        )
        .withColumn(
            resource_shipping_col,
            F.to_date(F.col(resource_shipping_col)),
        )
    )
    with_nb_days = with_dates.withColumn(
        "Nb Jours",
        F.when(
            F.col(status_col) == F.lit(WPRC_STATUS),
            F.datediff(current_day, F.col(resource_shipping_col)),
        )
        .otherwise(F.datediff(current_day, F.col(confirmation_col)))
        .cast("bigint"),
    )

    return with_nb_days.withColumn(
        "Classement Nb Jours",
        F.when(
            F.col("Nb Jours") <= F.lit(threshold_value),
            F.lit(f"< {threshold_text}j"),
        ).otherwise(F.lit(f"> {threshold_text}j")),
    )


def _join_correspondant_client(
    rme_df: DataFrame,
    correspondant_client_df: DataFrame,
) -> DataFrame:
    company_col = _require_column(rme_df, "compagnie")
    trigramme_col = _require_column(correspondant_client_df, "Trigramme")
    name_col = _require_column(correspondant_client_df, "Nom Correspondant Clt")

    lookup = correspondant_client_df.select(
        F.col(trigramme_col).alias("__correspondant_trigramme"),
        F.col(name_col).alias("Nom Correspondant Clt"),
    ).dropDuplicates(["__correspondant_trigramme"])

    return rme_df.join(
        lookup,
        F.col(company_col) == F.col("__correspondant_trigramme"),
        "left",
    ).drop("__correspondant_trigramme")


def _finalize_wprc_output(
    rme_df: DataFrame,
    correspondant_client_df: DataFrame,
) -> DataFrame:
    id_col = _require_column(rme_df, "id")
    cleaned = ft.remove_columns(rme_df, *WPRC_COLUMNS_TO_REMOVE)
    with_client = _join_correspondant_client(cleaned, correspondant_client_df)
    with_link = with_client.withColumn(
        "LienInternet",
        F.concat(F.lit("https://myfdt-ts.com/rm/"), F.col(id_col).cast("string"), F.lit("/detail")),
    )
    without_id = ft.remove_columns(with_link, "id")
    selected = without_id.select(*(_require_column(without_id, column) for column in WPRC_OUTPUT_COLUMNS))
    return selected.where(F.col("Classement Nb Jours").isNotNull())


def _finalize_wpr_output(
    rme_df: DataFrame,
    correspondant_client_df: DataFrame,
) -> DataFrame:
    id_col = _require_column(rme_df, "id")
    cleaned = ft.remove_columns(rme_df, *WPR_COLUMNS_TO_REMOVE)
    with_client = _join_correspondant_client(cleaned, correspondant_client_df)
    with_link = with_client.withColumn(
        "LienInternet",
        F.concat(
            F.lit("https://myfdt-ts.com/rm/"),
            F.col(id_col).cast("string"),
            F.lit("/detail"),
        ),
    )
    without_id = ft.remove_columns(with_link, "id")
    return without_id.select(
        *(_require_column(without_id, column) for column in WPR_OUTPUT_COLUMNS)
    )


def _add_rm_pn(rme_df: DataFrame) -> DataFrame:
    reference_col = _require_column(rme_df, "Reference")
    pn_return_customer_col = _require_column(rme_df, "pn_return_customer")

    return rme_df.withColumn(
        "RM / PN",
        F.concat(
            F.col(reference_col).cast("string"),
            F.lit(" | "),
            F.col(pn_return_customer_col).cast("string"),
        ),
    )


def build_rme_resources_requested(
    *,
    lakehouse_name: str,
    paths: RmePaths = RmePaths(),
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build the RME resources-requested output DataFrame from Lakehouse tables."""

    config_df = ft.read_lakehouse(lakehouse_name, paths.config, spark=spark)
    rme_df = ft.read_lakehouse(lakehouse_name, paths.rme, spark=spark)

    config = _load_config(config_df)
    low_days = _parse_config_number(config, CONFIG_KEY_LOW_DAYS)
    high_days = _parse_config_number(config, CONFIG_KEY_HIGH_DAYS)

    typed = _apply_schema(rme_df)
    filtered = _filter_rme(typed)
    classified = _add_day_classification(
        filtered,
        low_days=low_days,
        high_days=high_days,
        today=today,
    )
    return _add_rm_pn(classified)


def build_rm_wprc(
    *,
    lakehouse_name: str,
    paths: RmePaths = RmePaths(),
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build the RM WPRC output DataFrame from Lakehouse tables."""

    config_df = ft.read_lakehouse(lakehouse_name, paths.config, spark=spark)
    rme_df = ft.read_lakehouse(lakehouse_name, paths.rme, spark=spark)
    correspondant_client_df = ft.read_lakehouse(
        lakehouse_name,
        paths.correspondant_client,
        spark=spark,
    )

    config = _load_config(config_df)
    low_days = _parse_config_number(config, CONFIG_KEY_WPRC_LOW_DAYS)
    high_days = _parse_config_number(config, CONFIG_KEY_WPRC_HIGH_DAYS)

    typed = _apply_schema(rme_df)
    filtered = _filter_wprc(typed)
    classified = _add_wprc_day_classification(
        filtered,
        low_days=low_days,
        high_days=high_days,
        today=today,
    )
    return _finalize_wprc_output(classified, correspondant_client_df)


def build_rm_wpr(
    *,
    lakehouse_name: str,
    paths: RmePaths = RmePaths(),
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build the RM WPR output DataFrame from Lakehouse tables."""

    config_df = ft.read_lakehouse(lakehouse_name, paths.config, spark=spark)
    rme_df = ft.read_lakehouse(lakehouse_name, paths.rme, spark=spark)
    correspondant_client_df = ft.read_lakehouse(
        lakehouse_name,
        paths.correspondant_client,
        spark=spark,
    )

    config = _load_config(config_df)
    threshold_days = _parse_config_number(config, CONFIG_KEY_WPR_DAYS)

    typed = _apply_schema(rme_df)
    filtered = _filter_wpr(typed)
    classified = _add_wpr_day_classification(
        filtered,
        threshold_days=threshold_days,
        today=today,
    )
    return _finalize_wpr_output(classified, correspondant_client_df)


def run_rme_resources_requested_pipeline(
    *,
    lakehouse_name: str,
    paths: RmePaths = RmePaths(),
    mode: str = "overwrite",
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build and write the RME resources-requested table, then return it."""

    output_df = build_rme_resources_requested(
        lakehouse_name=lakehouse_name,
        paths=paths,
        spark=spark,
        today=today,
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


def run_rm_wprc_pipeline(
    *,
    lakehouse_name: str,
    paths: RmePaths = RmePaths(),
    mode: str = "overwrite",
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build and write the RM WPRC table, then return it."""

    output_df = build_rm_wprc(
        lakehouse_name=lakehouse_name,
        paths=paths,
        spark=spark,
        today=today,
    )

    ft.write_lakehouse(
        output_df,
        lakehouse_name=lakehouse_name,
        relative_path=paths.wprc_output,
        mode=mode,
        format="delta",
        spark=spark,
        normalize_column_names=False,
        enable_column_mapping=True,
        auto_partition=False,
    )
    return output_df


def run_rm_wpr_pipeline(
    *,
    lakehouse_name: str,
    paths: RmePaths = RmePaths(),
    mode: str = "overwrite",
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build and write the RM WPR table, then return it."""

    output_df = build_rm_wpr(
        lakehouse_name=lakehouse_name,
        paths=paths,
        spark=spark,
        today=today,
    )

    ft.write_lakehouse(
        output_df,
        lakehouse_name=lakehouse_name,
        relative_path=paths.wpr_output,
        mode=mode,
        format="delta",
        spark=spark,
        normalize_column_names=False,
        enable_column_mapping=True,
        auto_partition=False,
    )
    return output_df


__all__ = [
    "RmePaths",
    "build_rm_wpr",
    "build_rm_wprc",
    "build_rme_resources_requested",
    "run_rm_wpr_pipeline",
    "run_rm_wprc_pipeline",
    "run_rme_resources_requested_pipeline",
]
