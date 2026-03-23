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
    mode: str = "overwrite",
    batch_size: int = 10_000,
    date_relative_path: str = "dimension_date",
    country_relative_path: str = "dimension_country",
    city_relative_path: str = "dimension_city",
    date_warehouse_table: str = "dbo.dimension_date",
    country_warehouse_table: str = "dbo.dimension_country",
    city_warehouse_table: str = "dbo.dimension_city",
    spark: Optional[SparkSession] = None,
) -> dict[str, DataFrame]:
    """
    Build and write selected dimension tables to Lakehouse and Warehouse.

    Returns a dict of generated DataFrames keyed by dimension name.
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

