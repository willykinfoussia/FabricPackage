"""Build the `Suivi` snapshot from Silver Lakehouse TCD-like tables.

This script reproduces the VBA `EnregistrerValeurs` logic with PySpark:
- reads thresholds/volumes from a `Configuration` table,
- reads each TCD source table from the Lakehouse,
- computes the 21 ordered metrics,
- appends one row for `current_date` into `Suivi` (if not already present).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F


@dataclass(frozen=True)
class TcdTableNames:
    """Physical table names in the Silver Lakehouse."""

    configuration: str = "Configuration"
    acceptation_csr: str = "Acceptation_CSR"
    resources_not_created: str = "resources_not_created"
    resources_requested: str = "Resources_requested"
    rm_wprc: str = "RM WPRC"
    rm_wpr: str = "RM WPR"
    treated_by_commercial: str = "Treated_by_commercial"
    waiting_commercial_treatment: str = "Waiting_commercial_treatment"
    suivi: str = "Suivi"


def _quote_identifier(identifier: str) -> str:
    parts = [part for part in identifier.split(".") if part]
    return ".".join(f"`{part.replace('`', '``')}`" for part in parts)


def _read_table(spark: SparkSession, table_name: str) -> DataFrame:
    return spark.table(_quote_identifier(table_name))


def _normalize_name(name: str) -> str:
    return " ".join(name.strip().lower().split())


def _resolve_column_name(df: DataFrame, candidates: list[str]) -> str | None:
    by_normalized = {_normalize_name(col): col for col in df.columns}
    for candidate in candidates:
        found = by_normalized.get(_normalize_name(candidate))
        if found:
            return found
    return None


def _coerce_to_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except Exception:
        return 0


def _load_configuration(
    configuration_df: DataFrame,
    *,
    key_column_candidates: list[str] | None = None,
    value_column_candidates: list[str] | None = None,
) -> dict[str, str]:
    key_column_candidates = key_column_candidates or ["Libelle", "Label", "Key", "A"]
    value_column_candidates = value_column_candidates or ["Valeur", "Value", "B"]

    key_col = _resolve_column_name(configuration_df, key_column_candidates)
    value_col = _resolve_column_name(configuration_df, value_column_candidates)

    if key_col is None or value_col is None:
        # VBA reads configuration as columns A/B. Fallback to first two columns.
        if len(configuration_df.columns) < 2:
            raise ValueError(
                "La table Configuration doit contenir au moins 2 colonnes (cle/valeur)."
            )
        key_col = configuration_df.columns[0]
        value_col = configuration_df.columns[1]

    pairs = configuration_df.select(
        F.col(key_col).cast("string").alias("cfg_key"),
        F.col(value_col).cast("string").alias("cfg_value"),
    ).collect()
    return {
        row["cfg_key"]: row["cfg_value"]
        for row in pairs
        if row["cfg_key"] is not None and row["cfg_value"] is not None
    }


def _metric_count(
    df: DataFrame,
    *,
    class_col_candidates: list[str],
    metric_col_candidates: list[str],
    class_value: str,
) -> int:
    class_col = _resolve_column_name(df, class_col_candidates)
    metric_col = _resolve_column_name(df, metric_col_candidates)
    if class_col is None or metric_col is None:
        return 0

    row = (
        df.where(F.col(class_col) == F.lit(class_value))
        .agg(F.sum(F.col(metric_col).cast("double")).alias("metric_value"))
        .first()
    )
    if row is None:
        return 0
    return _coerce_to_int(row["metric_value"])


def _metric_not_created(
    df: DataFrame,
    *,
    class_value: str = "Not created",
) -> int:
    # The VBA uses a pivot-field setup with some ambiguity in naming.
    # We support the most common schemas and fallback safely to 0.
    class_col = _resolve_column_name(df, ["Status", "Nombre", "Classement", "Category"])
    metric_col = _resolve_column_name(
        df,
        [
            "Nombre",
            "Nombre de Reference",
            "Nombre de CSR reference",
            "Nombre de Reference FD",
            "Count",
        ],
    )
    if class_col is None or metric_col is None:
        return 0

    row = (
        df.where(F.col(class_col) == F.lit(class_value))
        .agg(F.sum(F.col(metric_col).cast("double")).alias("metric_value"))
        .first()
    )
    if row is None:
        return 0
    return _coerce_to_int(row["metric_value"])


def _build_suivi_ordered_values(
    *,
    cfg: dict[str, str],
    tcd: dict[str, DataFrame],
    current_day: date,
) -> list[Any]:
    seuil_h_accept = cfg["Seuil_horaire_acceptation_CSR"]
    seuil_wct_low = cfg["Seuil_bas_jours_waiting_commercial_treatment"]
    seuil_wct_high = cfg["Seuil_haut_jours_waiting_commercial_treatment"]
    seuil_rr_low = cfg["Seuil_bas_jours_resources_requested"]
    seuil_rr_high = cfg["Seuil_haut_jours_resources_requested"]
    seuil_wprc_low = cfg["Seuil_bas_jours_waiting_part_return_confirmation"]
    seuil_wprc_high = cfg["Seuil_haut_jours_waiting_part_return_confirmation"]
    seuil_wpr = cfg["Seuil_jours_waiting_for_part_reception"]

    volume_1 = _coerce_to_int(cfg.get("Volume_CSR_B2", "0"))
    volume_2 = _coerce_to_int(cfg.get("Volume_CSR_B3", "0"))

    label_acc_lt = f"< {seuil_h_accept}h"
    label_acc_gt = f"> {seuil_h_accept}h"

    label_rr_lt = f"< {seuil_rr_low}j"
    label_rr_mid = f"{seuil_rr_low}j < x < {seuil_rr_high}j"
    label_rr_gt = f"> {seuil_rr_high}j"

    label_wprc_lt = f"< {seuil_wprc_low}j"
    label_wprc_mid = f"{seuil_wprc_low}j < x < {seuil_wprc_high}j"
    label_wprc_gt = f"> {seuil_wprc_high}j"

    label_wpr_lt = f"< {seuil_wpr}j"
    label_wpr_gt = f"> {seuil_wpr}j"

    label_wct_lt = f"< {seuil_wct_low}j"
    label_wct_mid = f"{seuil_wct_low}j < x < {seuil_wct_high}j"
    label_wct_gt = f"> {seuil_wct_high}j"

    acceptation_class_cols = [
        "classement durée acceptation",
        "classement duréee acceptation",
        "classement duree acceptation",
    ]
    nb_jours_class_cols = ["Classement Nb jours", "Classement Nb Jours"]

    values: list[Any] = [current_day]
    values.extend(
        [
            _metric_count(
                tcd["acceptation_csr"],
                class_col_candidates=acceptation_class_cols,
                metric_col_candidates=["Nombre de Reference FD"],
                class_value=label_acc_lt,
            ),
            _metric_count(
                tcd["acceptation_csr"],
                class_col_candidates=acceptation_class_cols,
                metric_col_candidates=["Nombre de Reference FD"],
                class_value=label_acc_gt,
            ),
            _metric_not_created(tcd["resources_not_created"], class_value="Not created"),
            _metric_count(
                tcd["resources_requested"],
                class_col_candidates=nb_jours_class_cols,
                metric_col_candidates=["Nombre de CSR reference"],
                class_value=label_rr_lt,
            ),
            _metric_count(
                tcd["resources_requested"],
                class_col_candidates=nb_jours_class_cols,
                metric_col_candidates=["Nombre de CSR reference"],
                class_value=label_rr_mid,
            ),
            _metric_count(
                tcd["resources_requested"],
                class_col_candidates=nb_jours_class_cols,
                metric_col_candidates=["Nombre de CSR reference"],
                class_value=label_rr_gt,
            ),
            _metric_count(
                tcd["rm_wprc"],
                class_col_candidates=nb_jours_class_cols,
                metric_col_candidates=["Nombre de Reference"],
                class_value=label_wprc_lt,
            ),
            _metric_count(
                tcd["rm_wprc"],
                class_col_candidates=nb_jours_class_cols,
                metric_col_candidates=["Nombre de Reference"],
                class_value=label_wprc_mid,
            ),
            _metric_count(
                tcd["rm_wprc"],
                class_col_candidates=nb_jours_class_cols,
                metric_col_candidates=["Nombre de Reference"],
                class_value=label_wprc_gt,
            ),
            _metric_count(
                tcd["rm_wpr"],
                class_col_candidates=nb_jours_class_cols,
                metric_col_candidates=["Nombre de Reference"],
                class_value=label_wpr_lt,
            ),
            _metric_count(
                tcd["rm_wpr"],
                class_col_candidates=nb_jours_class_cols,
                metric_col_candidates=["Nombre de Reference"],
                class_value=label_wpr_gt,
            ),
            _metric_count(
                tcd["treated_by_commercial"],
                class_col_candidates=acceptation_class_cols,
                metric_col_candidates=["Nombre de Reference FD"],
                class_value=label_wct_lt,
            ),
            _metric_count(
                tcd["treated_by_commercial"],
                class_col_candidates=acceptation_class_cols,
                metric_col_candidates=["Nombre de Reference FD"],
                class_value=label_wct_mid,
            ),
            _metric_count(
                tcd["treated_by_commercial"],
                class_col_candidates=acceptation_class_cols,
                metric_col_candidates=["Nombre de Reference FD"],
                class_value=label_wct_gt,
            ),
            _metric_count(
                tcd["waiting_commercial_treatment"],
                class_col_candidates=acceptation_class_cols,
                metric_col_candidates=["Nombre de Reference FD"],
                class_value=label_wct_lt,
            ),
            _metric_count(
                tcd["waiting_commercial_treatment"],
                class_col_candidates=acceptation_class_cols,
                metric_col_candidates=["Nombre de Reference FD"],
                class_value=label_wct_mid,
            ),
            _metric_count(
                tcd["waiting_commercial_treatment"],
                class_col_candidates=acceptation_class_cols,
                metric_col_candidates=["Nombre de Reference FD"],
                class_value=label_wct_gt,
            ),
            volume_1,
            volume_2,
            _metric_count(
                tcd["resources_requested"],
                class_col_candidates=nb_jours_class_cols,
                metric_col_candidates=["Nombre de CSR reference"],
                class_value="metro still in time",
            ),
        ]
    )

    if len(values) != 21:
        raise RuntimeError(f"La ligne Suivi doit contenir 21 valeurs, obtenu={len(values)}.")
    return values


def update_suivi_from_tcd(
    *,
    spark: SparkSession,
    tables: TcdTableNames = TcdTableNames(),
    config_key_column_candidates: list[str] | None = None,
    config_value_column_candidates: list[str] | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    """Append one daily `Suivi` row (VBA-equivalent) from Silver TCD tables.

    Returns a small status payload with whether a row was written.
    """
    current_day = today or date.today()

    configuration_df = _read_table(spark, tables.configuration)
    cfg = _load_configuration(
        configuration_df,
        key_column_candidates=config_key_column_candidates,
        value_column_candidates=config_value_column_candidates,
    )

    required_config = [
        "Seuil_horaire_acceptation_CSR",
        "Seuil_bas_jours_waiting_commercial_treatment",
        "Seuil_haut_jours_waiting_commercial_treatment",
        "Seuil_bas_jours_resources_requested",
        "Seuil_haut_jours_resources_requested",
        "Seuil_bas_jours_waiting_part_return_confirmation",
        "Seuil_haut_jours_waiting_part_return_confirmation",
        "Seuil_jours_waiting_for_part_reception",
    ]
    missing = [key for key in required_config if key not in cfg]
    if missing:
        raise ValueError(
            "Clés manquantes dans la table Configuration: " + ", ".join(missing)
        )

    tcd_frames = {
        "acceptation_csr": _read_table(spark, tables.acceptation_csr),
        "resources_not_created": _read_table(spark, tables.resources_not_created),
        "resources_requested": _read_table(spark, tables.resources_requested),
        "rm_wprc": _read_table(spark, tables.rm_wprc),
        "rm_wpr": _read_table(spark, tables.rm_wpr),
        "treated_by_commercial": _read_table(spark, tables.treated_by_commercial),
        "waiting_commercial_treatment": _read_table(
            spark, tables.waiting_commercial_treatment
        ),
    }

    suivi_df = _read_table(spark, tables.suivi)
    if len(suivi_df.columns) < 21:
        raise ValueError(
            "La table Suivi doit avoir au moins 21 colonnes (comme le VBA)."
        )

    first_col = suivi_df.columns[0]
    already_exists = (
        suivi_df.where(F.to_date(F.col(first_col)) == F.lit(current_day)).limit(1).count()
        > 0
    )
    if already_exists:
        return {"written": False, "reason": "date_already_exists", "date": str(current_day)}

    ordered_values = _build_suivi_ordered_values(
        cfg=cfg,
        tcd=tcd_frames,
        current_day=current_day,
    )

    output_columns = list(suivi_df.columns)
    row_payload = {column: None for column in output_columns}
    for index, value in enumerate(ordered_values):
        row_payload[output_columns[index]] = value

    append_df = spark.createDataFrame([row_payload], schema=suivi_df.schema)
    append_df.write.mode("append").saveAsTable(_quote_identifier(tables.suivi))

    return {"written": True, "date": str(current_day), "table": tables.suivi}


# Fabric notebook usage:
# result = update_suivi_from_tcd(spark=spark)
# display(result)
