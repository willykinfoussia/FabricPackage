
def filter_acceptance_for_date(df, target_date=None):
    day = target_date or date.today()
    return df.filter(to_date(col("date_acceptance")) == F.lit(day))

def filter_today_acceptance(df):
    return filter_acceptance_for_date(df)

def filter_creation_for_date(df, target_date=None):
    day = target_date or date.today()
    return df.filter(to_date(col("date_creation")) == F.lit(day))

def filter_today_creation(df):
    return filter_creation_for_date(df)

def build_tcd_frames_for_day(target_day: date | None = None) -> dict[str, DataFrame]:
    """Construit les TCD Suivi pour un jour donné (acceptation filtrée sur ce jour)."""
    day_csr_df = filter_acceptance_for_date(csr_df, target_day)
    volume_csr = build_csr_open_volume(filter_acceptance_for_date(extract_csr_df, target_day))
    return {
        "acceptation_csr": ft.build_tcd(
            day_csr_df,
            rows="classement_duree_acceptation",
            values="reference_fd",
        ),
        "resources_not_created": ft.build_tcd(
            nb_jours_df,
            values={"id": "count"},
            rows="status",
            custom_columns_names=["Status", "Nombre"],
        ),
        "resources_requested": ft.build_tcd(
            nb_jours_df,
            rows="RME Metro Classement Nb Jours",
            values="csr_reference",
            filters="status = 'Resource requested'",
        ),
        "rm_wprc": ft.build_tcd(
            rm_wprc_df,
            rows="Classement Nb Jours",
            values="Reference",
        ),
        "rm_wpr": ft.build_tcd(
            rm_wpr_df,
            rows="Classement Nb Jours",
            values="Reference",
        ),
        "treated_by_commercial": ft.build_tcd(
            csr_commercial_treated_df,
            rows="commercial_classement_duree_acceptation",
            values="reference_fd",
        ),
        "waiting_commercial_treatment": ft.build_tcd(
            csr_commercial_waiting_df,
            rows="commercial_classement_duree_acceptation",
            values="reference_fd",
        ),
        "volume_csr": volume_csr,
    }

_tcd_today = build_tcd_frames_for_day()
acceptation_csr = _tcd_today["acceptation_csr"]
resources_not_created = _tcd_today["resources_not_created"]
resources_requested = _tcd_today["resources_requested"]
rm_wprc = _tcd_today["rm_wprc"]
rm_wpr = _tcd_today["rm_wpr"]
treated_by_commercial = _tcd_today["treated_by_commercial"]
waiting_commercial_treatment = _tcd_today["waiting_commercial_treatment"]
volume_csr_df = _tcd_today["volume_csr"]

today_csr_df = filter_today_acceptance(csr_df)
# Helpers

from datetime import timedelta

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
    value_col = _resolve_column_from_candidates(configuration_df, value_column_candidates)

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


def _build_suivi_ordered_values(
    *,
    param_resolver: ft.ParamResolver,
    tcd: dict[str, DataFrame],
    current_day: date,
) -> list[Any]:
    seuil_h_accept = param_resolver.get("Seuil_horaire_acceptation_CSR")
    seuil_wct_low = param_resolver.get("Seuil_bas_jours_waiting_commercial_treatment")
    seuil_wct_high = param_resolver.get("Seuil_haut_jours_waiting_commercial_treatment")
    seuil_rr_low = param_resolver.get("Seuil_bas_jours_resources_requested")
    seuil_rr_high = param_resolver.get("Seuil_haut_jours_resources_requested")
    seuil_wprc_low = param_resolver.get("Seuil_bas_jours_waiting_part_return_confirmation")
    seuil_wprc_high = param_resolver.get("Seuil_haut_jours_waiting_part_return_confirmation")
    seuil_wpr = param_resolver.get("Seuil_jours_waiting_for_part_reception")

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
        "commercial classement durée acceptation",
    ]
    nb_jours_class_cols = ["Classement Nb jours", "Nb Jours", "RME Metro Classement Nb Jours"]

    values: list[Any] = [current_day]
    values.extend(
        [
            ft.metric_value_for_class(tcd["acceptation_csr"], class_col_candidates=acceptation_class_cols, metric_col_candidates=["Reference FD"], class_value=label_acc_lt, missing=0),
            ft.metric_value_for_class(tcd["acceptation_csr"], class_col_candidates=acceptation_class_cols, metric_col_candidates=["Reference FD"], class_value=label_acc_gt, missing=0),
            ft.metric_value_for_class(tcd["resources_not_created"], class_col_candidates=["Status"], metric_col_candidates=["Nombre"], class_value="Not created", missing=0),
            ft.metric_value_for_class(tcd["resources_requested"], class_col_candidates=nb_jours_class_cols, metric_col_candidates=["CSR reference"], class_value=label_rr_lt, missing=0),
            ft.metric_value_for_class(tcd["resources_requested"], class_col_candidates=nb_jours_class_cols, metric_col_candidates=["CSR reference"], class_value=label_rr_mid, missing=0),
            ft.metric_value_for_class(tcd["resources_requested"], class_col_candidates=nb_jours_class_cols, metric_col_candidates=["CSR reference"], class_value=label_rr_gt, missing=0),
            ft.metric_value_for_class(tcd["rm_wprc"], class_col_candidates=nb_jours_class_cols, metric_col_candidates=["Reference"], class_value=label_wprc_lt, missing=0),
            ft.metric_value_for_class(tcd["rm_wprc"], class_col_candidates=nb_jours_class_cols, metric_col_candidates=["Reference"], class_value=label_wprc_mid, missing=0),
            ft.metric_value_for_class(tcd["rm_wprc"], class_col_candidates=nb_jours_class_cols, metric_col_candidates=["Reference"], class_value=label_wprc_gt, missing=0),
            ft.metric_value_for_class(tcd["rm_wpr"], class_col_candidates=nb_jours_class_cols, metric_col_candidates=["Reference"], class_value=label_wpr_lt, missing=0),
            ft.metric_value_for_class(tcd["rm_wpr"], class_col_candidates=nb_jours_class_cols, metric_col_candidates=["Reference"], class_value=label_wpr_gt, missing=0),
            ft.metric_value_for_class(tcd["treated_by_commercial"], class_col_candidates=acceptation_class_cols, metric_col_candidates=["Reference FD"], class_value=label_wct_lt, missing=0),
            ft.metric_value_for_class(tcd["treated_by_commercial"], class_col_candidates=acceptation_class_cols, metric_col_candidates=["Reference FD"], class_value=label_wct_mid, missing=0),
            ft.metric_value_for_class(tcd["treated_by_commercial"], class_col_candidates=acceptation_class_cols, metric_col_candidates=["Reference FD"], class_value=label_wct_gt, missing=0),
            ft.metric_value_for_class(tcd["waiting_commercial_treatment"], class_col_candidates=acceptation_class_cols, metric_col_candidates=["Reference FD"], class_value=label_wct_lt, missing=0),
            ft.metric_value_for_class(tcd["waiting_commercial_treatment"], class_col_candidates=acceptation_class_cols, metric_col_candidates=["Reference FD"], class_value=label_wct_mid, missing=0),
            ft.metric_value_for_class(tcd["waiting_commercial_treatment"], class_col_candidates=acceptation_class_cols, metric_col_candidates=["Reference FD"], class_value=label_wct_gt, missing=0),
            ft.metric_value_for_class(tcd["volume_csr"], class_col_candidates=["Etat CSR"], metric_col_candidates=["Volume"], class_value="Open Support", missing=0),
            ft.metric_value_for_class(tcd["volume_csr"], class_col_candidates=["Etat CSR"], metric_col_candidates=["Volume"], class_value="Open Commercial", missing=0),
            ft.metric_value_for_class(tcd["resources_requested"], class_col_candidates=nb_jours_class_cols, metric_col_candidates=["CSR reference"], class_value="metro still in time", missing=0),
        ]
    )

    display(values)

    if len(values) != 21:
        raise RuntimeError(f"La ligne Suivi doit contenir 21 valeurs, obtenu={len(values)}.")
    return values


def _sql_date_equals(column: str, day: date) -> str:
    """Build a Delta delete predicate for rows whose date column equals ``day``."""
    escaped = column.replace("`", "``")
    return f"to_date(`{escaped}`) = '{day.isoformat()}'"


def _sql_date_between(column: str, start: date, end: date) -> str:
    """Build a Delta delete predicate for rows whose date column is in ``[start, end]``."""
    escaped = column.replace("`", "``")
    return (
        f"to_date(`{escaped}`) >= '{start.isoformat()}' "
        f"AND to_date(`{escaped}`) <= '{end.isoformat()}'"
    )


DEFAULT_REBUILD_DAYS = 30


def _date_range(end: date, days: int) -> list[date]:
    if days < 1:
        raise ValueError(f"days must be >= 1, got {days}")
    start = end - timedelta(days=days - 1)
    return [start + timedelta(days=offset) for offset in range(days)]


def delete_suivi_last_days(
    *,
    lakehouse_name: str,
    relative_path: str,
    date_column: str,
    days: int = DEFAULT_REBUILD_DAYS,
    end_date: date | None = None,
    spark: SparkSession | None = None,
) -> dict[str, Any]:
    """Supprime les lignes Suivi sur les ``days`` derniers jours (inclus)."""
    end = end_date or date.today()
    start = end - timedelta(days=days - 1)
    condition = _sql_date_between(date_column, start, end)
    ft.delete_lakehouse(
        lakehouse_name=lakehouse_name,
        relative_path=relative_path,
        condition=condition,
        spark=spark,
    )
    return {
        "deleted_from": str(start),
        "deleted_to": str(end),
        "days": days,
    }


def rebuild_suivi_last_days(
    *,
    spark: SparkSession,
    param_resolver: ft.ParamResolver,
    lakehouse_name: str = SILVER_LAKEHOUSE,
    relative_path: str = PATH_SUIVI,
    days: int = DEFAULT_REBUILD_DAYS,
    end_date: date | None = None,
    config_key_column_candidates: list[str] | None = None,
    config_value_column_candidates: list[str] | None = None,
) -> dict[str, Any]:
    """Override Suivi : supprime puis reconstruit les ``days`` derniers jours."""
    end = end_date or date.today()
    start = end - timedelta(days=days - 1)

    first_col = suivi_df.columns[0]
    delete_suivi_last_days(
        lakehouse_name=lakehouse_name,
        relative_path=relative_path,
        date_column=first_col,
        days=days,
        end_date=end,
        spark=spark,
    )

    results: list[dict[str, Any]] = []
    for target_day in _date_range(end, days):
        results.append(
            update_suivi_from_tcd(
                spark=spark,
                param_resolver=param_resolver,
                lakehouse_name=lakehouse_name,
                relative_path=relative_path,
                config_key_column_candidates=config_key_column_candidates,
                config_value_column_candidates=config_value_column_candidates,
                today=target_day,
                overwrite_today=False,
            )
        )

    return {
        "rebuilt_days": days,
        "from": str(start),
        "to": str(end),
        "results": results,
    }

# Main function

def update_suivi_from_tcd(
    *,
    spark: SparkSession,
    param_resolver: ft.ParamResolver,
    lakehouse_name: str = SILVER_LAKEHOUSE,
    relative_path: str = PATH_SUIVI,
    config_key_column_candidates: list[str] | None = None,
    config_value_column_candidates: list[str] | None = None,
    today: date | None = None,
    overwrite_today: bool = True,
    tcd_frames: dict[str, DataFrame] | None = None,
) -> dict[str, Any]:
    current_day = today or date.today()

    tcd = tcd_frames or build_tcd_frames_for_day(current_day)

    if len(suivi_df.columns) < 21:
        raise ValueError("La table Suivi doit avoir au moins 21 colonnes (comme le VBA).")

    first_col = suivi_df.columns[0]

    ordered_values = _build_suivi_ordered_values(
        param_resolver=param_resolver,
        tcd=tcd,
        current_day=current_day
    )

    output_columns = list(suivi_df.columns)
    row_payload = {column: None for column in output_columns}
    for index, value in enumerate(ordered_values):
        row_payload[output_columns[index]] = value

    upsert_df = spark.createDataFrame([row_payload], schema=suivi_df.schema)

    # --- DELETE des lignes du jour si overwrite_today = True
    if overwrite_today:
        condition = _sql_date_equals(first_col, current_day)
        ft.delete_lakehouse(
            lakehouse_name=lakehouse_name,
            relative_path=relative_path,
            condition=condition,
        )

    # --- APPEND de la nouvelle ligne
    ft.write_lakehouse(
        upsert_df,
        lakehouse_name=lakehouse_name,
        relative_path=relative_path,
        mode="append",
        format="delta",
        spark=spark,
        normalize_column_names=False,
        auto_partition=False,
    )

    return {
        "written": True,
        "date": str(current_day),
        "overwrite_today": overwrite_today
    }