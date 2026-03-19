"""
Dimension table builders for Microsoft Fabric (Lakehouse + Warehouse).

This module provides:
- build_dimension_date(...)
- build_dimension_country(...)
- build_dimension_city(...)
- generate_dimensions(...)
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from fabrictools._logger import log
from fabrictools._spark import get_spark
from fabrictools.lakehouse import write_lakehouse
from fabrictools.warehouse import write_warehouse


def _country_schema() -> StructType:
    return StructType(
        [
            StructField("country_key", IntegerType(), True),
            StructField("country_code_2", StringType(), True),
            StructField("country_code_3", StringType(), True),
            StructField("country_name", StringType(), True),
            StructField("region", StringType(), True),
            StructField("subregion", StringType(), True),
        ]
    )


def _city_schema() -> StructType:
    return StructType(
        [
            StructField("city_key", IntegerType(), True),
            StructField("city_name", StringType(), True),
            StructField("state_name", StringType(), True),
            StructField("country_code_2", StringType(), True),
            StructField("country_code_3", StringType(), True),
            StructField("country_name", StringType(), True),
            StructField("country_key", IntegerType(), True),
            StructField("region", StringType(), True),
            StructField("subregion", StringType(), True),
        ]
    )


def _default_date_bounds() -> tuple[str, str]:
    today = dt.date.today()
    start_date = dt.date(today.year - (today.year % 100), 1, 1).isoformat()
    end_date = dt.date(today.year + 4, 12, 31).isoformat()
    return start_date, end_date


def _to_row_dict(item: Any) -> dict[str, Any]:
    if item is None:
        return {}
    if hasattr(item, "model_dump"):
        return item.model_dump()
    if hasattr(item, "dict"):
        return item.dict()
    return dict(item)


def _import_csc_package() -> tuple[Any, Any, Any]:
    try:
        from countrystatecity_countries import (  # type: ignore[import-not-found]
            get_cities_of_country,
            get_countries,
            get_states_of_country,
        )

        return get_countries, get_cities_of_country, get_states_of_country
    except Exception as exc:  # pragma: no cover - depends on runtime env
        raise ImportError(
            "Missing dependency 'countrystatecity-countries'. "
            "Install it with: pip install countrystatecity-countries"
        ) from exc


def _normalize_code(code: Any) -> Optional[str]:
    if code is None:
        return None
    normalized = str(code).strip().upper()
    return normalized or None


def build_dimension_date(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    fiscal_year_start_month: int = 1,
    lakehouse_name: Optional[str] = None,
    lakehouse_relative_path: Optional[str] = None,
    warehouse_name: Optional[str] = None,
    warehouse_table: Optional[str] = None,
    default_relative_path: str = "dimension_date",
    mode: str = "overwrite",
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """
    Build a date dimension DataFrame.

    Default range is rolling:
    - start: Jan 1st of current_year - 10
    - end: Dec 31st of current_year + 2
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

    date_df = (
        df.select(
            F.date_format(F.col("date"), "yyyyMMdd").cast("int").alias("date_key"),
            F.col("date").cast(DateType()).alias("date"),
            F.year("date").alias("year"),
            F.quarter("date").alias("quarter"),
            F.month("date").alias("month"),
            F.dayofmonth("date").alias("day"),
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
        )
        .orderBy("date_key")
    )
    _write_dimension_targets(
        df=date_df,
        lakehouse_name=lakehouse_name,
        lakehouse_relative_path=lakehouse_relative_path,
        warehouse_name=warehouse_name,
        warehouse_table=warehouse_table,
        default_relative_path=default_relative_path,
        mode=mode,
        spark=_spark,
    )
    return date_df


def build_dimension_country(
    countries_limit: Optional[int] = None,
    fail_on_source_error: bool = True,
    lakehouse_name: Optional[str] = None,
    lakehouse_relative_path: Optional[str] = None,
    warehouse_name: Optional[str] = None,
    warehouse_table: Optional[str] = None,
    default_relative_path: str = "dimension_country",
    mode: str = "overwrite",
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """Build `dimension_country` from `countrystatecity-countries`."""
    _spark = spark or get_spark()

    try:
        get_countries, _, _ = _import_csc_package()
        countries = list(get_countries())
        if countries_limit is not None:
            countries = countries[:countries_limit]

        rows: list[dict[str, Any]] = []
        for country in countries:
            payload = _to_row_dict(country)
            rows.append(
                {
                    "country_key": payload.get("id"),
                    "country_code_2": _normalize_code(payload.get("iso2")),
                    "country_code_3": _normalize_code(payload.get("iso3")),
                    "country_name": payload.get("name"),
                    "region": payload.get("region"),
                    "subregion": payload.get("subregion"),
                }
            )

        country_df = _spark.createDataFrame(rows, schema=_country_schema())
        country_df = country_df.filter(
            F.col("country_code_2").isNotNull() & F.col("country_name").isNotNull()
        )

        # Ensure deterministic numeric keys even if source IDs are missing.
        country_df = (
            country_df.withColumn(
                "country_key",
                F.coalesce(
                    F.col("country_key").cast("int"),
                    F.row_number().over(Window.orderBy("country_code_2")),
                ),
            )
            .dropDuplicates(["country_code_2"])
            .orderBy("country_key")
        )
        log(f"Built dimension_country ({country_df.count():,} rows)")
        _write_dimension_targets(
            df=country_df,
            lakehouse_name=lakehouse_name,
            lakehouse_relative_path=lakehouse_relative_path,
            warehouse_name=warehouse_name,
            warehouse_table=warehouse_table,
            default_relative_path=default_relative_path,
            mode=mode,
            spark=_spark,
        )
        return country_df
    except Exception as exc:
        if fail_on_source_error:
            raise RuntimeError(f"Could not build dimension_country: {exc}") from exc
        log(
            "dimension_country source error, returning empty DataFrame",
            level="warning",
        )
        empty_df = _spark.createDataFrame([], schema=_country_schema())
        _write_dimension_targets(
            df=empty_df,
            lakehouse_name=lakehouse_name,
            lakehouse_relative_path=lakehouse_relative_path,
            default_relative_path="dimension_country",
            mode=mode,
            spark=_spark,
        )
        return empty_df


def _normalize_filter_set(values: Optional[list[str]]) -> Optional[set[str]]:
    if values is None:
        return None
    return {v.strip().upper() for v in values if v and v.strip()}


def _country_matches_filter(
    payload: dict[str, Any],
    country_code_2: Optional[str],
    countries_filter: Optional[set[str]],
    regions_filter: Optional[set[str]],
    subregions_filter: Optional[set[str]],
) -> bool:
    if regions_filter is not None:
        region = (payload.get("region") or "").strip().upper()
        if region not in regions_filter:
            return False
    if subregions_filter is not None:
        subregion = (payload.get("subregion") or "").strip().upper()
        if subregion not in subregions_filter:
            return False
    if countries_filter is not None:
        identifiers = {
            (country_code_2 or ""),
            _normalize_code(payload.get("iso3")) or "",
            (payload.get("name") or "").strip().upper(),
        }
        if not identifiers & countries_filter:
            return False
    return True


def build_dimension_city(
    countries_limit: Optional[int] = None,
    include_states_metadata: bool = True,
    fail_on_source_error: bool = True,
    regions: Optional[list[str]] = None,
    subregions: Optional[list[str]] = None,
    countries: Optional[list[str]] = None,
    lakehouse_name: Optional[str] = None,
    lakehouse_relative_path: Optional[str] = None,
    warehouse_name: Optional[str] = None,
    warehouse_table: Optional[str] = None,
    default_relative_path: str = "dimension_city",
    mode: str = "overwrite",
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """Build `dimension_city` from `countrystatecity-countries`.

    Optional filters narrow down which countries (and thus cities) are
    included.  ``countries`` accepts any mix of ``country_code_2``,
    ``country_code_3`` or ``country_name`` values (case-insensitive).
    ``regions`` and ``subregions`` also accept case-insensitive values.
    All filters are combined with AND logic.
    """
    _spark = spark or get_spark()
    regions_filter = _normalize_filter_set(regions)
    subregions_filter = _normalize_filter_set(subregions)
    countries_filter = _normalize_filter_set(countries)

    try:
        get_countries, get_cities_of_country, get_states_of_country = _import_csc_package()
        all_countries = list(get_countries())
        if countries_limit is not None:
            all_countries = all_countries[:countries_limit]

        city_rows: list[dict[str, Any]] = []

        for country in all_countries:
            country_payload = _to_row_dict(country)
            country_code_2 = _normalize_code(country_payload.get("iso2"))
            country_code_3 = _normalize_code(country_payload.get("iso3"))
            country_name = country_payload.get("name")
            country_key = country_payload.get("id")
            region = country_payload.get("region")
            subregion = country_payload.get("subregion")
            if not country_code_2:
                continue

            if not _country_matches_filter(
                country_payload, country_code_2,
                countries_filter, regions_filter, subregions_filter,
            ):
                continue

            state_name_by_code: dict[str, str] = {}
            if include_states_metadata:
                try:
                    for state in get_states_of_country(country_code_2):
                        state_payload = _to_row_dict(state)
                        state_code = _normalize_code(state_payload.get("state_code"))
                        state_name = state_payload.get("name")
                        if state_code and state_name:
                            state_name_by_code[state_code] = state_name
                except Exception:
                    state_name_by_code = {}

            for city in get_cities_of_country(country_code_2):
                city_payload = _to_row_dict(city)
                city_name = city_payload.get("name")
                if not city_name:
                    continue

                state_code = _normalize_code(city_payload.get("state_code"))
                state_name = state_name_by_code.get(state_code) if state_code else None
                city_rows.append(
                    {
                        "city_key": city_payload.get("id"),
                        "city_name": city_name,
                        "state_name": state_name,
                        "country_code_2": country_code_2,
                        "country_code_3": country_code_3,
                        "country_name": country_name,
                        "country_key": country_key,
                        "region": region,
                        "subregion": subregion,
                    }
                )

        city_df = _spark.createDataFrame(city_rows, schema=_city_schema())
        city_df = city_df.filter(
            F.col("country_code_2").isNotNull() & F.col("city_name").isNotNull()
        )

        city_df = (
            city_df.withColumn(
                "city_key",
                F.coalesce(
                    F.col("city_key").cast("int"),
                    F.row_number().over(
                        Window.orderBy("country_code_2", "state_name", "city_name")
                    ),
                ),
            )
            .dropDuplicates(["country_code_2", "state_name", "city_name"])
            .orderBy("city_key")
        )
        log(f"Built dimension_city ({city_df.count():,} rows)")
        _write_dimension_targets(
            df=city_df,
            lakehouse_name=lakehouse_name,
            lakehouse_relative_path=lakehouse_relative_path,
            warehouse_name=warehouse_name,
            warehouse_table=warehouse_table,
            default_relative_path=default_relative_path,
            mode=mode,
            spark=_spark,
        )
        return city_df
    except Exception as exc:
        if fail_on_source_error:
            raise RuntimeError(f"Could not build dimension_city: {exc}") from exc
        log(
            "dimension_city source error, returning empty DataFrame",
            level="warning",
        )
        empty_df = _spark.createDataFrame([], schema=_city_schema())
        _write_dimension_targets(
            df=empty_df,
            lakehouse_name=lakehouse_name,
            lakehouse_relative_path=lakehouse_relative_path,
            warehouse_name=warehouse_name,
            warehouse_table=warehouse_table,
            default_relative_path=default_relative_path,
            mode=mode,
            spark=_spark,
        )
        return empty_df


def _write_dimension_targets(
    df: DataFrame,
    lakehouse_name: Optional[str],
    lakehouse_relative_path: Optional[str],
    warehouse_name: Optional[str],
    warehouse_table: Optional[str],
    default_relative_path: str,
    mode: str = "overwrite",
    batch_size: int = 10_000,
    spark: Optional[SparkSession] = None,
) -> None:
    _spark = spark or get_spark()
    if lakehouse_name:
        write_lakehouse(
            df=df,
            lakehouse_name=lakehouse_name,
            relative_path=lakehouse_relative_path or default_relative_path,
            mode=mode,
            spark=_spark,
        )
    if warehouse_name:
        write_warehouse(
            df=df,
            warehouse_name=warehouse_name,
            table=warehouse_table,
            mode=mode,
            batch_size=batch_size,
            spark=_spark,
        )


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

