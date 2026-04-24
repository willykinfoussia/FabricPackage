@dataclass(frozen=True)
class TcdRelativePaths:
    configuration: str = PATH_TABLE_CONFIG
    acceptation_csr: str = PATH_ACCEPTATION_CSR
    resources_not_created: str = PATH_RESOURCES_NOT_CREATED
    resources_requested: str = PATH_RESOURCES_REQUESTED
    rm_wprc: str = PATH_TABLE_RM_WPRC
    rm_wpr: str = PATH_TABLE_RM_WPR
    treated_by_commercial: str = PATH_TREATED_BY_COMMERCIAL
    waiting_commercial_treatment: str = PATH_WAITING_COMMERCIAL_TREATMENT
    suivi: str = PATH_SUIVI


# Helpers


def _resolve_column_from_candidates(df: DataFrame, candidates: list[str]) -> str | None:
    for candidate in candidates:
        resolved = ft.resolve_dataframe_column(df, candidate)
        if resolved is not None:
            return resolved
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
    key_column_candidates = key_column_candidates or CFG_KEY_CANDIDATES
    value_column_candidates = value_column_candidates or CFG_VALUE_CANDIDATES

    key_col = _resolve_column_from_candidates(configuration_df, key_column_candidates)
    value_col = _resolve_column_from_candidates(
        configuration_df, value_column_candidates
    )

    if key_col is None or value_col is None:
        if len(configuration_df.columns) < 2:
            raise ValueError(
                "La table Configuration doit contenir au moins 2 colonnes (cle/valeur)."
            )
        key_col = configuration_df.columns[0]
        value_col = configuration_df.columns[1]

    rows = configuration_df.select(
        F.col(key_col).cast("string").alias("cfg_key"),
        F.col(value_col).cast("string").alias("cfg_value"),
    ).collect()
    return {
        row["cfg_key"]: row["cfg_value"]
        for row in rows
        if row["cfg_key"] is not None and row["cfg_value"] is not None
    }


def _metric_count(
    df: DataFrame,
    *,
    class_col_candidates: list[str],
    metric_col_candidates: list[str],
    class_value: str,
) -> int:
    class_col = _resolve_column_from_candidates(df, class_col_candidates)
    metric_col = _resolve_column_from_candidates(df, metric_col_candidates)
    if class_col is None or metric_col is None:
        return 0

    row = (
        df.where(F.col(class_col) == F.lit(class_value))
        .agg(F.sum(F.col(metric_col).cast("double")).alias("metric_value"))
        .first()
    )
    return 0 if row is None else _coerce_to_int(row["metric_value"])


def _metric_not_created(df: DataFrame, *, class_value: str = "Not created") -> int:
    return _metric_count(
        df,
        class_col_candidates=["Status", "Nombre", "Classement", "Category"],
        metric_col_candidates=[
            "Nombre",
            "Nombre de Reference",
            "Nombre de CSR reference",
            "Nombre de Reference FD",
            "Count",
        ],
        class_value=class_value,
    )


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

    volume_1 = _coerce_to_int(cfg.get(CFG_VOLUME_KEY_1, "0"))
    volume_2 = _coerce_to_int(cfg.get(CFG_VOLUME_KEY_2, "0"))

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
            _metric_not_created(
                tcd["resources_not_created"], class_value="Not created"
            ),
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
        raise RuntimeError(
            f"La ligne Suivi doit contenir 21 valeurs, obtenu={len(values)}."
        )
    return values


# Main function


def update_suivi_from_tcd(
    *,
    spark: SparkSession,
    lakehouse_name: str = SILVER_LAKEHOUSE,
    paths: TcdRelativePaths = TcdRelativePaths(),
    config_key_column_candidates: list[str] | None = None,
    config_value_column_candidates: list[str] | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    print("Update process started")
    current_day = today or date.today()

    configuration_df = ft.read_lakehouse(
        lakehouse_name, paths.configuration, spark=spark
    )
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
        "acceptation_csr": ft.read_lakehouse(
            lakehouse_name, paths.acceptation_csr, spark=spark
        ),
        "resources_not_created": ft.read_lakehouse(
            lakehouse_name, paths.resources_not_created, spark=spark
        ),
        "resources_requested": ft.read_lakehouse(
            lakehouse_name, paths.resources_requested, spark=spark
        ),
        "rm_wprc": ft.read_lakehouse(lakehouse_name, paths.rm_wprc, spark=spark),
        "rm_wpr": ft.read_lakehouse(lakehouse_name, paths.rm_wpr, spark=spark),
        "treated_by_commercial": ft.read_lakehouse(
            lakehouse_name, paths.treated_by_commercial, spark=spark
        ),
        "waiting_commercial_treatment": ft.read_lakehouse(
            lakehouse_name, paths.waiting_commercial_treatment, spark=spark
        ),
    }

    suivi_df = ft.read_lakehouse(lakehouse_name, paths.suivi, spark=spark)
    if len(suivi_df.columns) < 21:
        raise ValueError(
            "La table Suivi doit avoir au moins 21 colonnes (comme le VBA)."
        )

    first_col = suivi_df.columns[0]
    already_exists = (
        suivi_df.where(F.to_date(F.col(first_col)) == F.lit(current_day))
        .limit(1)
        .count()
        > 0
    )
    if already_exists:
        return {
            "written": False,
            "reason": "date_already_exists",
            "date": str(current_day),
        }

    ordered_values = _build_suivi_ordered_values(
        cfg=cfg, tcd=tcd_frames, current_day=current_day
    )

    output_columns = list(suivi_df.columns)
    row_payload = {column: None for column in output_columns}
    for index, value in enumerate(ordered_values):
        row_payload[output_columns[index]] = value

    append_df = spark.createDataFrame([row_payload], schema=suivi_df.schema)
    print("Appending new row to suivi table")
    ft.write_lakehouse(
        append_df,
        lakehouse_name=lakehouse_name,
        relative_path=paths.suivi,
        mode="append",
        format="delta",
        spark=spark,
        normalize_column_names=False,
        auto_partition=False,
    )

    return {"written": True, "date": str(current_day), "table": paths.suivi}
