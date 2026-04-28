"""Spark conversion of the CSR Power Query pipeline.

The script reads the Power Query source tables from a Fabric Lakehouse through
fabrictools, applies the CSR business rules, and optionally writes the result
back to a Lakehouse table.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

import fabrictools as ft
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column
from pyspark.sql import functions as F


CONFIG_KEY_ACCEPTANCE_HOURS = "Seuil_horaire_acceptation_CSR"
CONFIG_KEY_SLIDING_DAYS = "Nb_jours_suivi glissant_CSR"
CONFIG_KEY_COMMERCIAL_LOW_DAYS = "Seuil_bas_jours_waiting_commercial_treatment"
CONFIG_KEY_COMMERCIAL_HIGH_DAYS = "Seuil_haut_jours_waiting_commercial_treatment"

CONFIG_KEY_CANDIDATES = ("Name", "name", "Key", "key", "Cle", "Clé")
CONFIG_VALUE_CANDIDATES = ("Value", "value", "Valeur", "valeur")

ALLOWED_STATUSES = (
    "Request treated by Commercial Department",
    "Request treated by Support",
    "Request validated by Support",
    "Request waiting for Commercial Treatment",
    "Waiting for Support Treatment",
    "Definitive solution waiting for customer validation",
    "Request closed by a definitive solution",
)

EXCLUDED_COMPANIES = ("AID", "EADS_TS", "EADS_TS_UK")
COMMERCIAL_TREATED_STATUSES = (
    "Request treated by Commercial Department",
    "Request waiting for Commercial Treatment",
)
COMMERCIAL_TREATED_FINAL_STATUS = "Request treated by Commercial Department"
COMMERCIAL_WAITING_FINAL_STATUS = "Request waiting for Commercial Treatment"
COMMERCIAL_TREATED_EXCLUDED_COMPANIES = ("EADS_TS", "EADS_TS_UK")

CSR_TYPE_MAP = {
    "id": "bigint",
    "Reference FD": "string",
    "compagnie": "string",
    "Creator FD": "string",
    "Customer FD": "string",
    "Bench": "string",
    "Bench client": "string",
    "Object": "string",
    "TUA": "string",
    "Failure consequence": "string",
    "Service requested define": "string",
    "Service requested define client": "string",
    "When was the failure identified": "string",
    "Description": "string",
    "Status": "string",
    "Reject Description": "string",
    "Technical TS": "string",
    "Commercial Request": "string",
    "Commercial Acceptance": "string",
    "Commercial Reject Comment": "string",
    "Created by TS": "string",
    "Date creation": "timestamp",
    "Date acceptance": "timestamp",
    "Date last modified": "timestamp",
    "date_last_modified_client": "timestamp",
    "Deleted": "bigint",
    "FD Type": "string",
    "FD Subtype": "string",
    "Faulty resource": "string",
    "FD Source": "string",
    "Unavailable time": "bigint",
    "Operations delayed time": "bigint",
    "Standard Exchange on-site reception date": "timestamp",
    "FTU": "bigint",
    "close_date": "timestamp",
    "date_creation_solution_definitive": "timestamp",
    "date_creation_solution_temp": "timestamp",
    "bench_type": "string",
}

FINAL_COLUMNS_TO_REMOVE = (
    "id",
    "Creator FD",
    "Customer FD",
    "Bench client",
    "TUA",
    "Service requested define",
    "Service requested define client",
    "When was the failure identified",
    "FD Validate",
    "Reject Description",
    "Commercial Reject Comment",
    "Comments",
    "Created by TS",
    "Date last modified",
    "date_last_modified_client",
    "Deleted",
    "FD Type",
    "FD Subtype",
    "Faulty resource",
    "FD Source",
    "Unavailable time",
    "Acknowledgment time",
    "Operations delayed time",
    "Back Into Service Date",
    "Intervention Starting Date",
    "Intervention Starting End",
    "Standard Exchange on-site reception date",
    "FTU",
    "close_date",
    "date_creation_solution_definitive",
    "date_response_solution_definitive",
    "date_creation_solution_temp",
    "date_response_solution_temp",
    "bench_type",
    "10j glissants",
)

COMMERCIAL_TREATED_COLUMNS_TO_REMOVE = tuple(
    column for column in FINAL_COLUMNS_TO_REMOVE if column != "10j glissants"
)


@dataclass(frozen=True)
class CsrPaths:
    """Lakehouse relative paths used by the CSR pipeline."""

    config: str = "Config"
    csr_data: str = "CSRData"
    output: str = "CSR"
    commercial_treated_output: str = "CSRCommercialTreated"
    commercial_waiting_output: str = "CSRCommercialWaiting"


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


def _to_timestamp_on_date(date_col: Column, time_text: str) -> Column:
    return F.to_timestamp(
        F.concat_ws(" ", F.date_format(date_col, "yyyy-MM-dd"), F.lit(time_text))
    )


def _adjust_acceptance_datetime(acceptance_col: Column) -> Column:
    time_text = F.date_format(acceptance_col, "HH:mm:ss")
    acceptance_date = F.to_date(acceptance_col)

    return (
        F.when(acceptance_col.isNull(), F.lit(None).cast("timestamp"))
        .when(
            time_text < F.lit("08:00:00"),
            _to_timestamp_on_date(F.date_sub(acceptance_date, 1), "18:00:00"),
        )
        .when(
            time_text > F.lit("18:00:00"),
            _to_timestamp_on_date(acceptance_date, "18:00:00"),
        )
        .otherwise(acceptance_col)
    )


def _adjust_creation_datetime(creation_col: Column) -> Column:
    time_text = F.date_format(creation_col, "HH:mm:ss")
    creation_date = F.to_date(creation_col)

    return (
        F.when(creation_col.isNull(), F.lit(None).cast("timestamp"))
        .when(
            time_text < F.lit("08:00:00"),
            _to_timestamp_on_date(creation_date, "08:00:00"),
        )
        .when(
            time_text > F.lit("18:00:00"),
            _to_timestamp_on_date(F.date_add(creation_date, 1), "08:00:00"),
        )
        .otherwise(creation_col)
    )


def _weekend_hours_between(start_col: Column, end_col: Column) -> Column:
    start_date = F.to_date(start_col)
    end_date = F.to_date(end_col)
    contains_weekend = F.exists(
        F.sequence(start_date, end_date),
        lambda current_date: F.dayofweek(current_date).isin(1, 7),
    )
    return F.when(
        start_date.isNotNull() & end_date.isNotNull() & contains_weekend,
        F.lit(62),
    ).otherwise(F.lit(0))


def _apply_schema(csr_df: DataFrame) -> DataFrame:
    return ft.cast_columns(csr_df, CSR_TYPE_MAP)


def _filter_csr(csr_df: DataFrame) -> DataFrame:
    status_col = _require_column(csr_df, "Status")
    company_col = _require_column(csr_df, "compagnie")

    filtered = ft.filter_column_by_values(
        csr_df,
        status_col,
        ALLOWED_STATUSES,
        exclude=False,
    )
    return ft.filter_column_by_values(
        filtered,
        company_col,
        EXCLUDED_COMPANIES,
        exclude=True,
    )


def _filter_csr_commercial_treated_base(csr_df: DataFrame) -> DataFrame:
    status_col = _require_column(csr_df, "Status")
    company_col = _require_column(csr_df, "compagnie")

    filtered = ft.filter_column_by_values(
        csr_df,
        status_col,
        COMMERCIAL_TREATED_STATUSES,
        exclude=False,
    )
    return ft.filter_column_by_values(
        filtered,
        company_col,
        COMMERCIAL_TREATED_EXCLUDED_COMPANIES,
        exclude=True,
    )


def _add_acceptance_metrics(
    csr_df: DataFrame,
    *,
    acceptance_threshold_hours: Decimal,
) -> DataFrame:
    creation_col = _require_column(csr_df, "Date creation")
    acceptance_col = _require_column(csr_df, "Date acceptance")
    threshold_value = float(acceptance_threshold_hours)
    threshold_text = _format_config_number(acceptance_threshold_hours)

    with_adjusted_dates = (
        csr_df.withColumn(
            "AdjustedAcceptanceDateTime",
            _adjust_acceptance_datetime(F.col(acceptance_col)),
        )
        .withColumn(
            "AdjustedCreationDateTime",
            _adjust_creation_datetime(F.col(creation_col)),
        )
        .withColumn(
            "Temps Acceptation",
            (
                F.unix_timestamp("AdjustedAcceptanceDateTime")
                - F.unix_timestamp("AdjustedCreationDateTime")
            )
            / F.lit(3600.0),
        )
        .withColumn(
            "WeekendHours",
            _weekend_hours_between(
                F.col("AdjustedCreationDateTime"),
                F.col("AdjustedAcceptanceDateTime"),
            ),
        )
        .withColumn(
            "Temps Acceptation",
            F.col("Temps Acceptation") - F.col("WeekendHours"),
        )
        .drop("WeekendHours")
    )

    return with_adjusted_dates.withColumn(
        "classement durée acceptation",
        F.when(
            F.col("Temps Acceptation") > F.lit(threshold_value),
            F.lit(f"> {threshold_text}h"),
        ).otherwise(F.lit(f"< {threshold_text}h")),
    )


def _add_commercial_treated_classification(
    csr_df: DataFrame,
    *,
    low_days: Decimal,
    high_days: Decimal,
    today: date | None = None,
) -> DataFrame:
    creation_col = _require_column(csr_df, "Date creation")
    low_value = float(low_days)
    high_value = float(high_days)
    low_text = _format_config_number(low_days)
    high_text = _format_config_number(high_days)
    current_day = F.lit(today.isoformat()).cast("date") if today else F.current_date()

    with_creation_date = csr_df.withColumn(creation_col, F.to_date(F.col(creation_col)))
    with_days = with_creation_date.withColumn(
        "Temps Acceptation",
        F.datediff(current_day, F.col(creation_col)).cast("bigint"),
    )

    return with_days.withColumn(
        "classement duréee acceptation",
        F.when(F.col("Temps Acceptation") < F.lit(low_value), F.lit(f"< {low_text}j"))
        .when(
            F.col("Temps Acceptation") < F.lit(high_value),
            F.lit(f"{low_text}j < x < {high_text}j"),
        )
        .otherwise(F.lit(f"> {high_text}j")),
    )


def _apply_sliding_window(
    csr_df: DataFrame,
    *,
    sliding_days: int,
    today: date | None = None,
) -> DataFrame:
    creation_col = _require_column(csr_df, "Date creation")
    current_day = F.lit(today.isoformat()).cast("date") if today else F.current_date()

    with_date_creation = csr_df.withColumn(creation_col, F.to_date(F.col(creation_col)))
    return (
        with_date_creation.withColumn(
            "10j glissants",
            F.when(
                F.datediff(current_day, F.col(creation_col)) <= F.lit(sliding_days),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .where(F.col("10j glissants") == F.lit(1))
    )


def _finalize_output(csr_df: DataFrame) -> DataFrame:
    trimmed = ft.remove_columns(csr_df, *FINAL_COLUMNS_TO_REMOVE)
    return trimmed.select(
        *(F.col(column).cast("string").alias(column) for column in trimmed.columns)
    )


def _finalize_commercial_treated_output(csr_df: DataFrame) -> DataFrame:
    status_col = _require_column(csr_df, "Status")
    trimmed = ft.remove_columns(csr_df, *COMMERCIAL_TREATED_COLUMNS_TO_REMOVE)
    filtered = trimmed.where(F.col(status_col) == F.lit(COMMERCIAL_TREATED_FINAL_STATUS))
    return filtered.withColumn(
        "Temps Acceptation",
        F.col("Temps Acceptation").cast("string"),
    ).withColumn(
        "classement duréee acceptation",
        F.col("classement duréee acceptation").cast("string"),
    )


def _finalize_commercial_waiting_output(csr_df: DataFrame) -> DataFrame:
    status_col = _require_column(csr_df, "Status")
    trimmed = ft.remove_columns(csr_df, *COMMERCIAL_TREATED_COLUMNS_TO_REMOVE)
    filtered = trimmed.where(F.col(status_col) == F.lit(COMMERCIAL_WAITING_FINAL_STATUS))
    return filtered.select(
        *(F.col(column).cast("string").alias(column) for column in filtered.columns)
    )


def build_csr(
    *,
    lakehouse_name: str,
    paths: CsrPaths = CsrPaths(),
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build the CSR output DataFrame from Lakehouse tables."""

    config_df = ft.read_lakehouse(lakehouse_name, paths.config, spark=spark)
    csr_df = ft.read_lakehouse(lakehouse_name, paths.csr_data, spark=spark)

    config = _load_config(config_df)
    acceptance_threshold_hours = _parse_config_number(
        config,
        CONFIG_KEY_ACCEPTANCE_HOURS,
    )
    sliding_days = int(_parse_config_number(config, CONFIG_KEY_SLIDING_DAYS))

    typed = _apply_schema(csr_df)
    filtered = _filter_csr(typed)
    with_acceptance_metrics = _add_acceptance_metrics(
        filtered,
        acceptance_threshold_hours=acceptance_threshold_hours,
    )
    current_window = _apply_sliding_window(
        with_acceptance_metrics,
        sliding_days=sliding_days,
        today=today,
    )
    return _finalize_output(current_window)


def build_csr_commercial_treated(
    *,
    lakehouse_name: str,
    paths: CsrPaths = CsrPaths(),
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build the CSR commercial-treated output DataFrame from Lakehouse tables."""

    config_df = ft.read_lakehouse(lakehouse_name, paths.config, spark=spark)
    csr_df = ft.read_lakehouse(lakehouse_name, paths.csr_data, spark=spark)

    config = _load_config(config_df)
    low_days = _parse_config_number(config, CONFIG_KEY_COMMERCIAL_LOW_DAYS)
    high_days = _parse_config_number(config, CONFIG_KEY_COMMERCIAL_HIGH_DAYS)

    typed = _apply_schema(csr_df)
    filtered = _filter_csr_commercial_treated_base(typed)
    classified = _add_commercial_treated_classification(
        filtered,
        low_days=low_days,
        high_days=high_days,
        today=today,
    )
    return _finalize_commercial_treated_output(classified)


def build_csr_commercial_waiting(
    *,
    lakehouse_name: str,
    paths: CsrPaths = CsrPaths(),
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build the CSR commercial-waiting output DataFrame from Lakehouse tables."""

    config_df = ft.read_lakehouse(lakehouse_name, paths.config, spark=spark)
    csr_df = ft.read_lakehouse(lakehouse_name, paths.csr_data, spark=spark)

    config = _load_config(config_df)
    low_days = _parse_config_number(config, CONFIG_KEY_COMMERCIAL_LOW_DAYS)
    high_days = _parse_config_number(config, CONFIG_KEY_COMMERCIAL_HIGH_DAYS)

    typed = _apply_schema(csr_df)
    filtered = _filter_csr_commercial_treated_base(typed)
    classified = _add_commercial_treated_classification(
        filtered,
        low_days=low_days,
        high_days=high_days,
        today=today,
    )
    return _finalize_commercial_waiting_output(classified)


def run_csr_pipeline(
    *,
    lakehouse_name: str,
    paths: CsrPaths = CsrPaths(),
    mode: str = "overwrite",
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build and write the CSR table, then return the written DataFrame."""

    output_df = build_csr(
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


def run_csr_commercial_treated_pipeline(
    *,
    lakehouse_name: str,
    paths: CsrPaths = CsrPaths(),
    mode: str = "overwrite",
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build and write the CSR commercial-treated table, then return it."""

    output_df = build_csr_commercial_treated(
        lakehouse_name=lakehouse_name,
        paths=paths,
        spark=spark,
        today=today,
    )

    ft.write_lakehouse(
        output_df,
        lakehouse_name=lakehouse_name,
        relative_path=paths.commercial_treated_output,
        mode=mode,
        format="delta",
        spark=spark,
        normalize_column_names=False,
        enable_column_mapping=True,
        auto_partition=False,
    )
    return output_df


def run_csr_commercial_waiting_pipeline(
    *,
    lakehouse_name: str,
    paths: CsrPaths = CsrPaths(),
    mode: str = "overwrite",
    spark: SparkSession | None = None,
    today: date | None = None,
) -> DataFrame:
    """Build and write the CSR commercial-waiting table, then return it."""

    output_df = build_csr_commercial_waiting(
        lakehouse_name=lakehouse_name,
        paths=paths,
        spark=spark,
        today=today,
    )

    ft.write_lakehouse(
        output_df,
        lakehouse_name=lakehouse_name,
        relative_path=paths.commercial_waiting_output,
        mode=mode,
        format="delta",
        spark=spark,
        normalize_column_names=False,
        enable_column_mapping=True,
        auto_partition=False,
    )
    return output_df


__all__ = [
    "CsrPaths",
    "build_csr",
    "build_csr_commercial_treated",
    "build_csr_commercial_waiting",
    "run_csr_pipeline",
    "run_csr_commercial_treated_pipeline",
    "run_csr_commercial_waiting_pipeline",
]
