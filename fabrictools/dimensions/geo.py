"""Geographic dimensions builders."""

from __future__ import annotations

from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from fabrictools.core import log
from fabrictools.core import get_spark
from fabrictools.dimensions.pipeline import _write_dimension_targets


def _country_schema() -> StructType:
    return StructType(
        [
            StructField("country_key", IntegerType(), True),
            StructField("country_code_2", StringType(), True),
            StructField("country_code_3", StringType(), True),
            StructField("country_name", StringType(), True),
            StructField("region", StringType(), True),
            StructField("subregion", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
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
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ]
    )

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

def _to_row_dict(item: Any) -> dict[str, Any]:
    if item is None:
        return {}
    if hasattr(item, "model_dump"):
        return item.model_dump()
    if hasattr(item, "dict"):
        return item.dict()
    return dict(item)

def _normalize_code(code: Any) -> Optional[str]:
    if code is None:
        return None
    normalized = str(code).strip().upper()
    return normalized or None

def _normalize_coordinate(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def _normalize_filter_set(values: Optional[list[str]]) -> Optional[set[str]]:
    if values is None:
        return None
    return {v.strip().upper() for v in values if v and v.strip()}

def build_dimension_country(
    countries_limit: Optional[int] = None,
    fail_on_source_error: bool = True,
    lakehouse_name: Optional[str] = None,
    lakehouse_relative_path: Optional[str] = None,
    warehouse_name: Optional[str] = None,
    warehouse_table: Optional[str] = None,
    default_relative_path: str = "dimension_country",
    mode: str = "overwrite",
    batch_size: int = 10000,
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
            batch_size=batch_size,
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
            warehouse_name=warehouse_name,
            warehouse_table=warehouse_table,
            default_relative_path=default_relative_path,
            mode=mode,
            batch_size=batch_size,
            spark=_spark,
        )
        return empty_df

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
    batch_size: int = 10000,
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
                        "latitude": _normalize_coordinate(city_payload.get("latitude")),
                        "longitude": _normalize_coordinate(city_payload.get("longitude")),
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
            batch_size=batch_size,
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
            batch_size=batch_size,
            spark=_spark,
        )
        return empty_df

__all__ = ["build_dimension_country", "build_dimension_city", "_import_csc_package"]