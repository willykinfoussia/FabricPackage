"""Dimensions pipeline orchestration."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from typing import Optional

from fabrictools.core import get_spark
from fabrictools.core import log
from fabrictools.dimensions._targets import _write_dimension_targets
from fabrictools.dimensions.date import build_dimension_date
from fabrictools.dimensions.geo import build_dimension_country, build_dimension_city


def generate_dimensions(
    lakehouse_name: Optional[str] = None,
    warehouse_name: Optional[str] = None,
    include_date: bool = True,
    include_country: bool = True,
    include_city: bool = True,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    fiscal_year_start_month: int = 1,
    countries_limit: Optional[int] = None,
    include_states_metadata: bool = True,
    fail_on_source_error: bool = True,
    city_regions: Optional[list[str]] = None,
    city_subregions: Optional[list[str]] = None,
    city_countries: Optional[list[str]] = None,
    mode: str = "upsert",
    batch_size: int = 10_000,
    date_relative_path: str = "Dimension_Date",
    country_relative_path: str = "Dimension_Country",
    city_relative_path: str = "Dimension_City",
    date_warehouse_table: str = "dbo.Dimension_Date",
    country_warehouse_table: str = "dbo.Dimension_Country",
    city_warehouse_table: str = "dbo.Dimension_City",
    spark: Optional[SparkSession] = None,
) -> dict[str, DataFrame]:
    """Build enabled dimensions and persist each to the configured Lakehouse and/or Warehouse.

    Keys in the returned map mirror the chosen relative path (or warehouse table name).

    :param lakehouse_name: Optional Lakehouse for all dimension writes.
    :param warehouse_name: Optional Warehouse for JDBC writes.
    :param include_date: Build date dimension when ``True``.
    :param include_country: Build country dimension when ``True``.
    :param include_city: Build city dimension when ``True``.
    :param start_date: Passed to :py:func:`fabrictools.build_dimension_date`.
    :param end_date: Passed to ``build_dimension_date``.
    :param fiscal_year_start_month: Passed to ``build_dimension_date``.
    :param countries_limit: Passed to geo builders.
    :param include_states_metadata: Passed to :py:func:`fabrictools.build_dimension_city`.
    :param fail_on_source_error: Passed to geo builders.
    :param city_regions: Passed as ``regions`` to ``build_dimension_city``.
    :param city_subregions: Passed as ``subregions`` to ``build_dimension_city``.
    :param city_countries: Passed as ``countries`` to ``build_dimension_city``.
    :param mode: Write mode for all targets.
    :param batch_size: JDBC batch size for warehouse writes.
    :param date_relative_path: Lakehouse path for the date table.
    :param country_relative_path: Lakehouse path for the country table.
    :param city_relative_path: Lakehouse path for the city table.
    :param date_warehouse_table: Warehouse table for date dimension.
    :param country_warehouse_table: Warehouse table for country dimension.
    :param city_warehouse_table: Warehouse table for city dimension.
    :param spark: Optional ``SparkSession``.
    :type lakehouse_name: str | None
    :type warehouse_name: str | None
    :type include_date: bool
    :type include_country: bool
    :type include_city: bool
    :type start_date: str | None
    :type end_date: str | None
    :type fiscal_year_start_month: int
    :type countries_limit: int | None
    :type include_states_metadata: bool
    :type fail_on_source_error: bool
    :type city_regions: list[str] | None
    :type city_subregions: list[str] | None
    :type city_countries: list[str] | None
    :type mode: str
    :type batch_size: int
    :type date_relative_path: str
    :type country_relative_path: str
    :type city_relative_path: str
    :type date_warehouse_table: str
    :type country_warehouse_table: str
    :type city_warehouse_table: str
    :type spark: ~pyspark.sql.SparkSession | None

    :returns: Map of dimension key to dataframe.
    :rtype: dict[str, ~pyspark.sql.DataFrame]

    :raises ValueError: If all dimension flags are ``False``.

    .. rubric:: Example

    >>> dims = generate_dimensions(  # doctest: +SKIP
    ...     lakehouse_name="GoldLakehouse",
    ...     include_date=True,
    ...     include_country=True,
    ...     include_city=False,
    ... )
    """
    _spark = spark or get_spark()
    generated: dict[str, DataFrame] = {}

    if not include_date and not include_country and not include_city:
        raise ValueError("At least one dimension must be enabled.")

    if include_date:
        date_df = build_dimension_date(
            start_date=start_date,
            end_date=end_date,
            fiscal_year_start_month=fiscal_year_start_month,
            lakehouse_name=lakehouse_name,
            lakehouse_relative_path=date_relative_path,
            warehouse_name=warehouse_name,
            warehouse_table=date_warehouse_table,
            default_relative_path=date_relative_path,
            mode=mode,
            batch_size=batch_size,
            spark=_spark,
        )
        generated[f"{date_relative_path or date_warehouse_table}"] = date_df

    if include_country:
        country_df = build_dimension_country(
            countries_limit=countries_limit,
            fail_on_source_error=fail_on_source_error,
            lakehouse_name=lakehouse_name,
            lakehouse_relative_path=country_relative_path,
            warehouse_name=warehouse_name,
            warehouse_table=country_warehouse_table,
            default_relative_path=country_relative_path,
            mode=mode,
            batch_size=batch_size,
            spark=_spark,
        )
        generated[f"{country_relative_path or country_warehouse_table}"] = country_df

    if include_city:
        city_df = build_dimension_city(
            countries_limit=countries_limit,
            include_states_metadata=include_states_metadata,
            fail_on_source_error=fail_on_source_error,
            regions=city_regions,
            subregions=city_subregions,
            countries=city_countries,
            lakehouse_name=lakehouse_name,
            lakehouse_relative_path=city_relative_path,
            warehouse_name=warehouse_name,
            warehouse_table=city_warehouse_table,
            default_relative_path=city_relative_path,
            mode=mode,
            batch_size=batch_size,
            spark=_spark,
        )
        generated[f"{city_relative_path or city_warehouse_table}"] = city_df

    log("Dimension generation completed")
    return generated


__all__ = ["generate_dimensions", "_write_dimension_targets"]
