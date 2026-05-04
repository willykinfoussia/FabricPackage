from __future__ import annotations

from datetime import date
from decimal import Decimal

import fabrictools as ft
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from csr_open_volume import (
    COMMERCIAL_CSR_STATUSES,
    OPEN_CSR_STATUSES,
    SUPPORT_CSR_STATUSES,
)
from csr_spark import (
    ALLOWED_STATUSES,
    COMMERCIAL_TREATED_COLUMNS_TO_REMOVE,
    COMMERCIAL_TREATED_EXCLUDED_COMPANIES,
    COMMERCIAL_TREATED_FINAL_STATUS,
    COMMERCIAL_TREATED_STATUSES,
    COMMERCIAL_WAITING_FINAL_STATUS,
    CONFIG_KEY_ACCEPTANCE_HOURS,
    CONFIG_KEY_COMMERCIAL_HIGH_DAYS,
    CONFIG_KEY_COMMERCIAL_LOW_DAYS,
    CONFIG_KEY_SLIDING_DAYS,
    EXCLUDED_COMPANIES,
    FINAL_COLUMNS_TO_REMOVE,
)

CSR_ACCEPTANCE_TIME_COL = "CSR Temps Acceptation"
CSR_ACCEPTANCE_CLASS_COL = "CSR classement durée acceptation"
COMMERCIAL_ACCEPTANCE_TIME_COL = "Commercial Temps Acceptation"
COMMON_COLUMNS_TO_REMOVE = (
    CSR_ACCEPTANCE_TIME_COL,
    CSR_ACCEPTANCE_CLASS_COL,
    COMMERCIAL_ACCEPTANCE_TIME_COL,
)


def _to_decimal(value) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value).strip().replace(",", "."))


def _format_config_number(value) -> str:
    normalized = _to_decimal(value).normalize()
    if normalized == normalized.to_integral_value():
        return str(normalized.quantize(Decimal("1")))
    return format(normalized, "f")


def _existing_column_or_null(df: DataFrame, column: str, data_type: str) -> Column:
    return F.col(column) if column in df.columns else F.lit(None).cast(data_type)


def _exclude_companies_condition(df: DataFrame, companies: tuple[str, ...]) -> Column:
    company_col = ft.resolve_dataframe_column(df, "compagnie")
    return ~F.trim(F.col(company_col)).isin(*companies)


def _filter_csr(csr_df: DataFrame) -> DataFrame:
    return csr_df.where(_csr_filter_condition(csr_df))


def _csr_filter_condition(csr_df: DataFrame) -> Column:
    status_col = ft.resolve_dataframe_column(csr_df, "Status")
    return F.col(status_col).isin(*ALLOWED_STATUSES) & _exclude_companies_condition(
        csr_df,
        EXCLUDED_COMPANIES,
    )


def _filter_csr_commercial_treated(csr_df: DataFrame) -> DataFrame:
    return csr_df.where(_csr_commercial_treated_filter_condition(csr_df))


def _csr_commercial_treated_filter_condition(csr_df: DataFrame) -> Column:
    status_col = ft.resolve_dataframe_column(csr_df, "Status")
    return _csr_commercial_base_filter_condition(csr_df) & (
        F.col(status_col) == F.lit(COMMERCIAL_TREATED_FINAL_STATUS)
    )


def _filter_csr_commercial_waiting(csr_df: DataFrame) -> DataFrame:
    return csr_df.where(_csr_commercial_waiting_filter_condition(csr_df))


def _csr_commercial_waiting_filter_condition(csr_df: DataFrame) -> Column:
    status_col = ft.resolve_dataframe_column(csr_df, "Status")
    return _csr_commercial_base_filter_condition(csr_df) & (
        F.col(status_col) == F.lit(COMMERCIAL_WAITING_FINAL_STATUS)
    )


def _filter_csr_open_volume(csr_df: DataFrame) -> DataFrame:
    status_col = ft.resolve_dataframe_column(csr_df, "Status")
    return csr_df.where(F.col(status_col).isin(*OPEN_CSR_STATUSES))


def _csr_commercial_base_filter_condition(csr_df: DataFrame) -> Column:
    status_col = ft.resolve_dataframe_column(csr_df, "Status")
    return F.col(status_col).isin(
        *COMMERCIAL_TREATED_STATUSES
    ) & _exclude_companies_condition(
        csr_df,
        COMMERCIAL_TREATED_EXCLUDED_COMPANIES,
    )


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


def _add_acceptance_metrics(
    csr_df: DataFrame,
    *,
    acceptance_threshold_hours: Decimal,
    apply_when: Column,
) -> DataFrame:
    creation_col = ft.resolve_dataframe_column(csr_df, "Date creation")
    acceptance_col = ft.resolve_dataframe_column(csr_df, "Date acceptance")
    threshold_value = float(_to_decimal(acceptance_threshold_hours))
    threshold_text = _format_config_number(acceptance_threshold_hours)
    previous_time = _existing_column_or_null(csr_df, CSR_ACCEPTANCE_TIME_COL, "double")
    previous_class = _existing_column_or_null(csr_df, CSR_ACCEPTANCE_CLASS_COL, "string")

    with_adjusted_dates = (
        csr_df.withColumn(
            "Adjusted_Acceptance_DateTime",
            F.when(
                apply_when,
                _adjust_acceptance_datetime(F.col(acceptance_col)),
            ).otherwise(
                _existing_column_or_null(
                    csr_df,
                    "Adjusted_Acceptance_DateTime",
                    "timestamp",
                )
            ),
        )
        .withColumn(
            "Adjusted_Creation_DateTime",
            F.when(
                apply_when,
                _adjust_creation_datetime(F.col(creation_col)),
            ).otherwise(
                _existing_column_or_null(
                    csr_df,
                    "Adjusted_Creation_DateTime",
                    "timestamp",
                )
            ),
        )
        .withColumn(
            CSR_ACCEPTANCE_TIME_COL,
            F.when(
                apply_when,
                (
                    F.unix_timestamp("Adjusted_Acceptance_DateTime")
                    - F.unix_timestamp("Adjusted_Creation_DateTime")
                )
                / F.lit(3600.0),
            ).otherwise(previous_time),
        )
        .withColumn(
            "WeekendHours",
            F.when(
                apply_when,
                _weekend_hours_between(
                    F.col("Adjusted_Creation_DateTime"),
                    F.col("Adjusted_Acceptance_DateTime"),
                ),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            CSR_ACCEPTANCE_TIME_COL,
            F.when(
                apply_when,
                F.col(CSR_ACCEPTANCE_TIME_COL) - F.col("WeekendHours"),
            ).otherwise(previous_time),
        )
        .drop("WeekendHours")
    )

    return (
        with_adjusted_dates.withColumn(
            "Adjusted_Acceptance_Date",
            F.when(
                apply_when,
                F.col("Adjusted_Acceptance_DateTime").cast("date"),
            ).otherwise(
                _existing_column_or_null(
                    with_adjusted_dates,
                    "Adjusted_Acceptance_Date",
                    "date",
                )
            ),
        )
        .withColumn(
            CSR_ACCEPTANCE_CLASS_COL,
            F.when(
                apply_when & (F.col(CSR_ACCEPTANCE_TIME_COL) > F.lit(threshold_value)),
                F.lit(f"> {threshold_text}h"),
            )
            .when(apply_when, F.lit(f"< {threshold_text}h"))
            .otherwise(previous_class),
        )
    )


def _add_sliding_window(
    csr_df: DataFrame,
    *,
    sliding_days: int,
    today: date | None = None,
    apply_when: Column,
) -> DataFrame:
    creation_col = ft.resolve_dataframe_column(csr_df, "Date creation")
    current_day = F.lit(today.isoformat()).cast("date") if today else F.current_date()
    creation_date = F.to_date(F.col(creation_col))
    previous_window = _existing_column_or_null(csr_df, "10j glissants", "bigint")

    return csr_df.withColumn(
        "10j glissants",
        F.when(
            apply_when,
            F.when(
                F.datediff(current_day, creation_date) <= F.lit(sliding_days),
                F.lit(1),
            ).otherwise(F.lit(0)),
        ).otherwise(previous_window),
    )


def _add_commercial_treated_classification(
    csr_df: DataFrame,
    *,
    low_days: Decimal,
    high_days: Decimal,
    today: date | None = None,
    apply_when: Column,
) -> DataFrame:
    creation_col = ft.resolve_dataframe_column(csr_df, "Date creation")
    low_value = float(_to_decimal(low_days))
    high_value = float(_to_decimal(high_days))
    low_text = _format_config_number(low_days)
    high_text = _format_config_number(high_days)
    current_day = F.lit(today.isoformat()).cast("date") if today else F.current_date()
    creation_date = F.to_date(F.col(creation_col))
    previous_time = _existing_column_or_null(
        csr_df,
        COMMERCIAL_ACCEPTANCE_TIME_COL,
        "bigint",
    )
    previous_class = _existing_column_or_null(
        csr_df,
        "classement duréee acceptation",
        "string",
    )

    with_days = csr_df.withColumn(
        COMMERCIAL_ACCEPTANCE_TIME_COL,
        F.when(
            apply_when,
            F.datediff(current_day, creation_date),
        )
        .otherwise(previous_time)
        .cast("bigint"),
    )

    classification = (
        F.when(
            F.col(COMMERCIAL_ACCEPTANCE_TIME_COL) < F.lit(low_value),
            F.lit(f"< {low_text}j"),
        )
        .when(
            F.col(COMMERCIAL_ACCEPTANCE_TIME_COL) < F.lit(high_value),
            F.lit(f"{low_text}j < x < {high_text}j"),
        )
        .otherwise(F.lit(f"> {high_text}j"))
    )

    return with_days.withColumn(
        "classement duréee acceptation",
        F.when(
            apply_when & F.col(COMMERCIAL_ACCEPTANCE_TIME_COL).isNotNull(),
            classification,
        ).otherwise(previous_class),
    )


def build_csr_common(
    param_resolver: ft.ParamResolver,
    extract_csr_df: DataFrame,
    *,
    today: date | None = None,
) -> DataFrame:
    """Build the common CSR dataframe with all derived columns."""

    acceptance_threshold_hours = param_resolver.get(CONFIG_KEY_ACCEPTANCE_HOURS)
    sliding_days = int(_to_decimal(param_resolver.get(CONFIG_KEY_SLIDING_DAYS)))
    commercial_low_days = param_resolver.get(CONFIG_KEY_COMMERCIAL_LOW_DAYS)
    commercial_high_days = param_resolver.get(CONFIG_KEY_COMMERCIAL_HIGH_DAYS)
    current_day = today or date.today()

    csr_condition = _csr_filter_condition(extract_csr_df)
    commercial_condition = _csr_commercial_base_filter_condition(extract_csr_df)

    csr_df = _add_acceptance_metrics(
        extract_csr_df,
        acceptance_threshold_hours=acceptance_threshold_hours,
        apply_when=csr_condition,
    )
    csr_df = _add_sliding_window(
        csr_df,
        sliding_days=sliding_days,
        today=current_day,
        apply_when=csr_condition,
    )
    return _add_commercial_treated_classification(
        csr_df,
        low_days=commercial_low_days,
        high_days=commercial_high_days,
        today=current_day,
        apply_when=commercial_condition,
    )


def _finalize_csr_output(csr_df: DataFrame) -> DataFrame:
    current_window = _filter_csr(csr_df).where(F.col("10j glissants") == F.lit(1))
    renamed = current_window.withColumn(
        "Temps Acceptation",
        F.col(CSR_ACCEPTANCE_TIME_COL),
    ).withColumn(
        "classement durée acceptation",
        F.col(CSR_ACCEPTANCE_CLASS_COL),
    )
    trimmed = ft.remove_columns(
        renamed,
        *FINAL_COLUMNS_TO_REMOVE,
        *COMMON_COLUMNS_TO_REMOVE,
    )
    return trimmed


def _finalize_commercial_treated_output(csr_df: DataFrame) -> DataFrame:
    filtered = _filter_csr_commercial_treated(csr_df).withColumn(
        "Temps Acceptation",
        F.col(COMMERCIAL_ACCEPTANCE_TIME_COL).cast("string"),
    ).withColumn(
        "classement duréee acceptation",
        F.col("classement duréee acceptation").cast("string"),
    )
    return ft.remove_columns(
        filtered,
        *COMMERCIAL_TREATED_COLUMNS_TO_REMOVE,
        *COMMON_COLUMNS_TO_REMOVE,
    )


def _finalize_commercial_waiting_output(csr_df: DataFrame) -> DataFrame:
    filtered = _filter_csr_commercial_waiting(csr_df).withColumn(
        "Temps Acceptation",
        F.col(COMMERCIAL_ACCEPTANCE_TIME_COL),
    )
    trimmed = ft.remove_columns(
        filtered,
        *COMMERCIAL_TREATED_COLUMNS_TO_REMOVE,
        *COMMON_COLUMNS_TO_REMOVE,
    )
    return trimmed.select(
        *(F.col(column).cast("string").alias(column) for column in trimmed.columns)
    )


def build_csr(
    param_resolver: ft.ParamResolver,
    extract_csr_df: DataFrame,
) -> DataFrame:
    """Build the CSR output DataFrame from an already loaded CSR dataframe."""

    return _finalize_csr_output(build_csr_common(param_resolver, extract_csr_df))


def build_csr_commercial_treated(
    param_resolver: ft.ParamResolver,
    extract_csr_df: DataFrame,
) -> DataFrame:
    """Build the CSR commercial-treated output DataFrame."""

    return _finalize_commercial_treated_output(
        build_csr_common(param_resolver, extract_csr_df)
    )


def build_csr_commercial_waiting(
    param_resolver: ft.ParamResolver,
    extract_csr_df: DataFrame,
) -> DataFrame:
    """Build the CSR commercial-waiting output DataFrame."""

    return _finalize_commercial_waiting_output(
        build_csr_common(param_resolver, extract_csr_df)
    )


def build_csr_open_volume(csr_df: DataFrame) -> DataFrame:
    """Build open CSR volumes by ``Etat CSR`` from the raw CSR dataframe."""

    status_col = ft.resolve_dataframe_column(csr_df, "Status")
    filtered = _filter_csr_open_volume(csr_df)
    counts_by_status = filtered.groupBy(status_col).agg(
        F.count(F.lit(1)).cast("bigint").alias("Nombre")
    )

    with_business_state = counts_by_status.withColumn(
        "Etat CSR",
        F.when(F.col(status_col).isin(*SUPPORT_CSR_STATUSES), F.lit("Open Support"))
        .when(F.col(status_col).isin(*COMMERCIAL_CSR_STATUSES), F.lit("Open Commercial"))
        .otherwise(F.lit(None)),
    )

    return with_business_state.groupBy("Etat CSR").agg(
        F.sum("Nombre").cast("bigint").alias("Volume")
    )


__all__ = [
    "build_csr_common",
    "build_csr",
    "build_csr_commercial_treated",
    "build_csr_commercial_waiting",
    "build_csr_open_volume",
]
