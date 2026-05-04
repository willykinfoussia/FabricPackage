def _filter_rme(rme_df: DataFrame) -> DataFrame:
    return rme_df.where(_rme_filter_condition(rme_df))

def _rme_filter_condition(rme_df: DataFrame):
    status_col = ft.resolve_dataframe_column(rme_df, "Status")
    csr_reference_col = ft.resolve_dataframe_column(rme_df, "CSR reference")

    return F.col(status_col).isin(*ALLOWED_STATUSES) & (
        ~F.col(csr_reference_col).startswith(EXCLUDED_CSR_REFERENCE_PREFIX)
    )

def _filter_wprc(rme_df: DataFrame) -> DataFrame:
    return rme_df.where(_wprc_filter_condition(rme_df))

def _wprc_filter_condition(rme_df: DataFrame):
    status_col = ft.resolve_dataframe_column(rme_df, "Status")
    company_col = ft.resolve_dataframe_column(rme_df, "compagnie")

    return (F.col(status_col) == F.lit(WPRC_STATUS)) & (
        ~F.trim(F.col(company_col)).isin(*EXCLUDED_COMPANIES)
    )

def _filter_wpr(rme_df: DataFrame) -> DataFrame:
    return rme_df.where(_wpr_filter_condition(rme_df))

def _wpr_filter_condition(rme_df: DataFrame):
    status_col = ft.resolve_dataframe_column(rme_df, "Status")
    company_col = ft.resolve_dataframe_column(rme_df, "compagnie")

    return (F.col(status_col) == F.lit(WPR_STATUS)) & (
        ~F.trim(F.col(company_col)).isin(*EXCLUDED_COMPANIES)
    )

def _existing_column_or_null(rme_df: DataFrame, column: str, data_type: str):
    return F.col(column) if column in rme_df.columns else F.lit(None).cast(data_type)

def _add_day_classification(
    rme_df: DataFrame,
    *,
    low_days: Decimal,
    high_days: Decimal,
    today: date | None = None,
    apply_when=None,
) -> DataFrame:
    requirements_date_col = ft.resolve_dataframe_column(rme_df, "requirements_date")
    type_rm_col = ft.resolve_dataframe_column(rme_df, "Type RM")
    validity_date_col = ft.resolve_dataframe_column(rme_df, "end_date_validity_customer")
    low_value = float(low_days)
    high_value = float(high_days)
    current_day = F.lit(today.isoformat()).cast("date") if today else F.current_date()
    active_condition = apply_when if apply_when is not None else F.lit(True)
    requirements_date = F.to_date(F.col(requirements_date_col))
    validity_date = F.to_date(F.col(validity_date_col))
    previous_nb_days = _existing_column_or_null(rme_df, "Nb Jours", "bigint")
    previous_rme_classification = _existing_column_or_null(
        rme_df,
        "RME Metro Classement Nb Jours",
        "string",
    )

    with_days = rme_df.withColumn(
        "Nb Jours",
        F.when(
            active_condition,
            F.datediff(current_day, requirements_date),
        )
        .otherwise(previous_nb_days)
        .cast("bigint"),
    )

    base_classification = (
        F.when(F.col("Nb Jours").isNull(), F.lit(None).cast("string"))
        .when(F.col("Nb Jours") <= F.lit(low_value), F.lit(f"< {low_days}j"))
        .when(
            F.col("Nb Jours") <= F.lit(high_value),
            F.lit(f"{low_days}j < x < {high_days}j"),
        )
        .otherwise(F.lit(f"> {high_days}j"))
    )

    metro_classification = (
        F.when(
            validity_date.isNull()
            & (requirements_date > current_day),
            F.lit(METRO_STILL_IN_TIME),
        )
        .when(
            validity_date.isNotNull()
            & (validity_date > current_day),
            F.lit(METRO_STILL_IN_TIME),
        )
        .otherwise(base_classification)
    )

    return with_days.withColumn(
        "RME Metro Classement Nb Jours",
        F.when(
            active_condition
            & (F.col(type_rm_col) == F.lit(METRO_STANDARD_EXCHANGE_TYPE)),
            metro_classification,
        )
        .when(active_condition, base_classification)
        .otherwise(previous_rme_classification),
    )

def _add_rm_pn(rme_df: DataFrame, apply_when=None) -> DataFrame:
    reference_col = ft.resolve_dataframe_column(rme_df, "Reference")
    pn_return_customer_col = ft.resolve_dataframe_column(rme_df, "pn_return_customer")
    active_condition = apply_when if apply_when is not None else F.lit(True)
    previous_rm_pn = _existing_column_or_null(rme_df, "RM / PN", "string")

    return rme_df.withColumn(
        "RM / PN",
        F.when(
            active_condition,
            F.concat(
                F.col(reference_col).cast("string"),
                F.lit(" | "),
                F.col(pn_return_customer_col).cast("string"),
            ),
        ).otherwise(previous_rm_pn),
    )

def _add_wprc_day_classification(
    rme_df: DataFrame,
    *,
    low_days: Decimal,
    high_days: Decimal,
    today: date | None = None,
    apply_when=None,
) -> DataFrame:
    status_col = ft.resolve_dataframe_column(rme_df, "Status")
    resource_shipping_col = ft.resolve_dataframe_column(rme_df, "date_resource_shipping")
    confirmation_col = ft.resolve_dataframe_column(rme_df, "date_confirmation")
    low_value = float(low_days)
    high_value = float(high_days)
    current_day = F.lit(today.isoformat()).cast("date") if today else F.current_date()
    active_condition = apply_when if apply_when is not None else F.lit(True)
    resource_shipping_date = F.to_date(F.col(resource_shipping_col))
    confirmation_date = F.to_date(F.col(confirmation_col))
    previous_nb_days = _existing_column_or_null(rme_df, "Nb Jours", "bigint")
    previous_classification = _existing_column_or_null(
        rme_df,
        "Classement Nb Jours",
        "string",
    )

    nb_days = (
        F.when(
            F.col(status_col) == F.lit(WPRC_STATUS),
            F.datediff(current_day, resource_shipping_date),
        )
        .otherwise(F.datediff(current_day, confirmation_date))
        .cast("bigint")
    )
    with_nb_days = rme_df.withColumn(
        "Nb Jours",
        F.when(active_condition, nb_days).otherwise(previous_nb_days).cast("bigint"),
    )

    classification = (
        F.when(F.col("Nb Jours").isNull(), F.lit(None).cast("string"))
        .when(F.col("Nb Jours") < F.lit(low_value), F.lit(f"< {low_days}j"))
        .when(
            F.col("Nb Jours") < F.lit(high_value),
            F.lit(f"{low_days}j < x < {high_days}j"),
        )
        .otherwise(F.lit(f"> {high_days}j"))
    )

    return with_nb_days.withColumn(
        "Classement Nb Jours",
        F.when(active_condition, classification).otherwise(previous_classification),
    )

def _add_wpr_day_classification(
    rme_df: DataFrame,
    *,
    threshold_days: Decimal,
    today: date | None = None,
    apply_when=None,
) -> DataFrame:
    status_col = ft.resolve_dataframe_column(rme_df, "Status")
    resource_shipping_col = ft.resolve_dataframe_column(rme_df, "date_resource_shipping")
    confirmation_col = ft.resolve_dataframe_column(rme_df, "date_confirmation")
    threshold_value = float(threshold_days)
    current_day = F.lit(today.isoformat()).cast("date") if today else F.current_date()
    active_condition = apply_when if apply_when is not None else F.lit(True)
    resource_shipping_date = F.to_date(F.col(resource_shipping_col))
    confirmation_date = F.to_date(F.col(confirmation_col))
    previous_nb_days = _existing_column_or_null(rme_df, "Nb Jours", "bigint")
    previous_classification = _existing_column_or_null(
        rme_df,
        "Classement Nb Jours",
        "string",
    )

    nb_days = (
        F.when(
            F.col(status_col) == F.lit(WPRC_STATUS),
            F.datediff(current_day, resource_shipping_date),
        )
        .otherwise(F.datediff(current_day, confirmation_date))
        .cast("bigint")
    )
    with_nb_days = rme_df.withColumn(
        "Nb Jours",
        F.when(active_condition, nb_days).otherwise(previous_nb_days).cast("bigint"),
    )

    classification = (
        F.when(F.col("Nb Jours").isNull(), F.lit(None).cast("string"))
        .when(
            F.col("Nb Jours") <= F.lit(threshold_value),
            F.lit(f"< {threshold_days}j"),
        )
        .otherwise(F.lit(f"> {threshold_days}j"))
    )

    return with_nb_days.withColumn(
        "Classement Nb Jours",
        F.when(active_condition, classification).otherwise(previous_classification),
    )

def _join_correspondant_client(
    rme_df: DataFrame,
    correspondant_client_df: DataFrame,
) -> DataFrame:
    company_col = ft.resolve_dataframe_column(rme_df, "compagnie")
    trigramme_col = ft.resolve_dataframe_column(correspondant_client_df, "Trigramme")
    name_col = ft.resolve_dataframe_column(correspondant_client_df, "Nom Correspondant Clt")

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
    id_col = ft.resolve_dataframe_column(rme_df, "id")
    cleaned = ft.remove_columns(rme_df, *WPRC_COLUMNS_TO_REMOVE)
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
    selected = without_id.select(
        *(ft.resolve_dataframe_column(without_id, column) for column in WPRC_OUTPUT_COLUMNS)
    )
    return selected.where(F.col("Classement Nb Jours").isNotNull())

def _finalize_wpr_output(
    rme_df: DataFrame,
    correspondant_client_df: DataFrame,
) -> DataFrame:
    id_col = ft.resolve_dataframe_column(rme_df, "id")
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
        *(ft.resolve_dataframe_column(without_id, column) for column in WPR_OUTPUT_COLUMNS)
    )


def build_rme(
    param_resolver: ft.ParamResolver,
    rme_df: DataFrame,
) -> DataFrame:
    """Build the common enriched RME DataFrame without filtering rows out."""

    low_days = param_resolver.get(CONFIG_KEY_LOW_DAYS)
    high_days = param_resolver.get(CONFIG_KEY_HIGH_DAYS)
    wprc_low_days = param_resolver.get(CONFIG_KEY_WPRC_LOW_DAYS)
    wprc_high_days = param_resolver.get(CONFIG_KEY_WPRC_HIGH_DAYS)
    wpr_threshold_days = param_resolver.get(CONFIG_KEY_WPR_DAYS)
    current_day = date.today()

    rme_condition = _rme_filter_condition(rme_df)
    wprc_condition = _wprc_filter_condition(rme_df)
    wpr_condition = _wpr_filter_condition(rme_df)

    rme_df = _add_day_classification(
        rme_df,
        low_days=low_days,
        high_days=high_days,
        today=current_day,
        apply_when=rme_condition,
    )
    rme_df = _add_rm_pn(rme_df, apply_when=rme_condition)
    rme_df = _add_wprc_day_classification(
        rme_df,
        low_days=wprc_low_days,
        high_days=wprc_high_days,
        today=current_day,
        apply_when=wprc_condition,
    )
    return _add_wpr_day_classification(
        rme_df,
        threshold_days=wpr_threshold_days,
        today=current_day,
        apply_when=wpr_condition,
    )


def build_rme_resources_requested(
    param_resolver: ft.ParamResolver, 
    rme_df: DataFrame
) -> DataFrame:
    """Build the RME resources-requested output DataFrame from Lakehouse tables."""

    return _filter_rme(build_rme(param_resolver, rme_df))

def build_rm_wprc(
    param_resolver: ft.ParamResolver, 
    rme_df: DataFrame,
    correspondant_client_df: DataFrame,
) -> DataFrame:
    """Build the RM WPRC output DataFrame from Lakehouse tables."""

    built = build_rme(param_resolver, rme_df)
    return _finalize_wprc_output(_filter_wprc(built), correspondant_client_df)

def build_rm_wpr(
    param_resolver: ft.ParamResolver, 
    rme_df: DataFrame,
    correspondant_client_df: DataFrame,
) -> DataFrame:
    """Build the RM WPR output DataFrame from Lakehouse tables."""

    built = build_rme(param_resolver, rme_df)
    return _finalize_wpr_output(_filter_wpr(built), correspondant_client_df)
