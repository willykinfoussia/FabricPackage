"""Date dimension builders."""

from __future__ import annotations

import datetime as dt
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import BooleanType, DateType

from fabrictools.core import log
from fabrictools.core import get_spark
from fabrictools.dimensions._targets import _write_dimension_targets


def _default_date_bounds() -> tuple[str, str]:
    today = dt.date.today()
    start_date = dt.date(today.year - (today.year % 100), 1, 1).isoformat()
    end_date = dt.date(today.year + 4, 12, 31).isoformat()
    return start_date, end_date


def build_dimension_date(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    fiscal_year_start_month: int = 1,
    lakehouse_name: Optional[str] = None,
    lakehouse_relative_path: Optional[str] = None,
    warehouse_name: Optional[str] = None,
    warehouse_table: Optional[str] = None,
    default_relative_path: str = "Dimension_Date",
    mode: str = "overwrite",
    batch_size: int = 10000,
    spark: Optional[SparkSession] = None,
    *,
    merge_condition: Optional[str] = None,
    upsert_key_columns: Optional[list[str]] = None,
) -> DataFrame:
    """Build a calendar date dimension (keys, labels, fiscal attributes, weekend flag,
    rolling last-N-day flags vs. the job's current date).

    Columns ``is_last_7days``, ``is_last_30days``, and ``is_last_90days`` are integers
    0 or 1. A row is 1 when its calendar ``date`` falls in the inclusive window from
    ``current_date() - N`` through ``current_date()`` in the Spark session at execution
    time (not the client/report "today"). Re-run the dimension job when those flags
    must stay aligned with the operational calendar day.

    Default inclusive range when ``start_date`` / ``end_date`` are omitted: from
    January 1st of ``current_year - (current_year % 100)`` through December 31st
    of ``current_year + 4``.

    :param start_date: Inclusive lower bound ``yyyy-MM-dd``, or ``None`` for default.
    :param end_date: Inclusive upper bound ``yyyy-MM-dd``, or ``None`` for default.
    :param fiscal_year_start_month: First fiscal month (1–12).
    :param lakehouse_name: If set with ``lakehouse_relative_path``, write Delta there.
    :param lakehouse_relative_path: Path under the Lakehouse for the dimension table.
    :param warehouse_name: If set with ``warehouse_table``, JDBC-write to Warehouse.
    :param warehouse_table: Fully qualified warehouse table name.
    :param default_relative_path: Fallback Lakehouse path segment when none given.
    :param mode: Spark write mode for persistence.
    :param batch_size: JDBC batch size when writing the warehouse.
    :param spark: Optional ``SparkSession``.
    :type start_date: str | None
    :type end_date: str | None
    :type fiscal_year_start_month: int
    :type lakehouse_name: str | None
    :type lakehouse_relative_path: str | None
    :type warehouse_name: str | None
    :type warehouse_table: str | None
    :type default_relative_path: str
    :type mode: str
    :type batch_size: int
    :type spark: ~pyspark.sql.SparkSession | None

    :returns: Ordered date dimension dataframe.
    :rtype: ~pyspark.sql.DataFrame

    :raises ValueError: If ``fiscal_year_start_month`` is outside 1..12.

    .. rubric:: Example

    >>> dim_date = build_dimension_date(  # doctest: +SKIP
    ...     start_date="2024-01-01",
    ...     end_date="2024-12-31",
    ...     lakehouse_name="GoldLakehouse",
    ...     lakehouse_relative_path="dimension_date",
    ... )
    """
    _spark = spark or get_spark()
    if fiscal_year_start_month < 1 or fiscal_year_start_month > 12:
        raise ValueError("fiscal_year_start_month must be between 1 and 12.")

    if start_date is None or end_date is None:
        default_start, default_end = _default_date_bounds()
        start_date = start_date or default_start
        end_date = end_date or default_end

    log(f"Building dimension_date for range {start_date} -> {end_date}")
    df = _spark.sql(
        "SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date".format(
            start=start_date,
            end=end_date,
        )
    )
    fiscal_month_expr = (
        (F.month("date") - F.lit(fiscal_year_start_month) + F.lit(12)) % F.lit(12)
    ) + F.lit(1)
    fiscal_year_expr = F.when(
        F.month("date") >= F.lit(fiscal_year_start_month),
        F.year("date"),
    ).otherwise(F.year("date") - F.lit(1))

    ref_date = F.current_date()

    date_df = df.select(
        F.date_format(F.col("date"), "yyyyMMdd").cast("int").alias("date_key"),
        F.col("date").cast(DateType()).alias("date"),
        F.year("date").alias("Année"),
        F.concat(F.lit("Q"), F.quarter("date").cast("string")).alias("quarter"),
        F.element_at(
            F.array(
                F.lit("janvier"),
                F.lit("février"),
                F.lit("mars"),
                F.lit("avril"),
                F.lit("mai"),
                F.lit("juin"),
                F.lit("juillet"),
                F.lit("août"),
                F.lit("septembre"),
                F.lit("octobre"),
                F.lit("novembre"),
                F.lit("décembre"),
            ),
            F.month("date"),
        ).alias("Mois"),
        F.concat(F.lit("S"), F.weekofyear("date").cast("string")).alias("week"),
        F.dayofmonth("date").alias("Jour"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format(F.col("date"), "MMM").alias("short_month"),
        F.year("date").alias("calendar_year"),
        F.month("date").alias("calendar_month"),
        fiscal_year_expr.alias("fiscal_year"),
        fiscal_month_expr.alias("fiscal_month"),
        F.concat(F.lit("CY"), F.year("date").cast("string")).alias(
            "calendar_year_label"
        ),
        F.concat(
            F.lit("CY"),
            F.year("date").cast("string"),
            F.lit("-"),
            F.date_format(F.col("date"), "MMM"),
        ).alias("calendar_month_label"),
        F.concat(F.lit("FY"), fiscal_year_expr.cast("string")).alias(
            "fiscal_year_label"
        ),
        F.concat(
            F.lit("FY"),
            fiscal_year_expr.cast("string"),
            F.lit("-"),
            F.date_format(F.col("date"), "MMM"),
        ).alias("fiscal_month_label"),
        F.weekofyear("date").alias("iso_week_number"),
        F.when(F.dayofweek("date").isin(1, 7), F.lit(True))
        .otherwise(F.lit(False))
        .cast(BooleanType())
        .alias("is_weekend"),
        F.col("date")
        .between(F.date_sub(ref_date, 7), ref_date)
        .cast("int")
        .alias("is_last_7days"),
        F.col("date")
        .between(F.date_sub(ref_date, 30), ref_date)
        .cast("int")
        .alias("is_last_30days"),
        F.col("date")
        .between(F.date_sub(ref_date, 90), ref_date)
        .cast("int")
        .alias("is_last_90days"),
    ).orderBy("date_key")
    lakehouse_keys = upsert_key_columns
    if lakehouse_keys is None and str(mode).strip().lower() in ("upsert", "merge"):
        lakehouse_keys = ["date_key"]
    _write_dimension_targets(
        df=date_df,
        lakehouse_name=lakehouse_name,
        lakehouse_relative_path=lakehouse_relative_path,
        warehouse_name=warehouse_name,
        warehouse_table=warehouse_table,
        default_relative_path=default_relative_path,
        mode=mode,
        batch_size=batch_size,
        spark=_spark,
        merge_condition=merge_condition,
        upsert_key_columns=lakehouse_keys,
    )
    return date_df


__all__ = ["build_dimension_date", "_default_date_bounds"]
