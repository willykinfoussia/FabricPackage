"""
Unit tests for fabrictools.dimensions.
"""

from __future__ import annotations

import re
from unittest.mock import MagicMock, patch

import pytest


class _FakeModel:
    def __init__(self, payload: dict):
        self._payload = payload

    def dict(self) -> dict:
        return dict(self._payload)


def _extract_select_aliases(select_args: tuple[object, ...]) -> set[str]:
    aliases: set[str] = set()
    for arg in select_args:
        match = re.search(r"\bAS\s+([a-zA-Z_][a-zA-Z0-9_]*)", str(arg))
        if match:
            aliases.add(match.group(1))
    return aliases


class TestBuildDimensionDate:
    @patch("fabrictools.dimensions.get_spark")
    @patch("fabrictools.dimensions._default_date_bounds", return_value=("2010-01-01", "2010-01-03"))
    def test_uses_default_rolling_range(self, mock_bounds, mock_get_spark):
        """When no bounds are provided, helper should use rolling defaults."""
        spark = MagicMock()
        exploded_df = MagicMock()
        projected_df = MagicMock()
        exploded_df.select.return_value = projected_df
        projected_df.orderBy.return_value = projected_df
        spark.sql.return_value = exploded_df
        mock_get_spark.return_value = spark

        from fabrictools.dimensions import build_dimension_date

        result = build_dimension_date()

        spark.sql.assert_called_once()
        sql_text = spark.sql.call_args.args[0]
        assert "2010-01-01" in sql_text
        assert "2010-01-03" in sql_text
        exploded_df.select.assert_called_once()
        projected_columns = _extract_select_aliases(exploded_df.select.call_args.args)
        assert {
            "short_month",
            "calendar_year",
            "calendar_month",
            "fiscal_year",
            "fiscal_month",
            "calendar_year_label",
            "calendar_month_label",
            "fiscal_year_label",
            "fiscal_month_label",
            "iso_week_number",
        }.issubset(projected_columns)
        projected_df.orderBy.assert_called_once_with("date_key")
        assert result is projected_df

    @patch("fabrictools.dimensions.get_spark")
    def test_rejects_invalid_fiscal_year_start_month(self, mock_get_spark):
        """Fiscal year start month must be in [1, 12]."""
        mock_get_spark.return_value = MagicMock()
        from fabrictools.dimensions import build_dimension_date

        with pytest.raises(ValueError, match="fiscal_year_start_month"):
            build_dimension_date(fiscal_year_start_month=0)
        with pytest.raises(ValueError, match="fiscal_year_start_month"):
            build_dimension_date(fiscal_year_start_month=13)

    @patch("fabrictools.dimensions.get_spark")
    def test_applies_configured_fiscal_year_start_month(self, mock_get_spark):
        """Projection should embed the configured fiscal year start month."""
        spark = MagicMock()
        exploded_df = MagicMock()
        projected_df = MagicMock()
        exploded_df.select.return_value = projected_df
        projected_df.orderBy.return_value = projected_df
        spark.sql.return_value = exploded_df
        mock_get_spark.return_value = spark

        from fabrictools.dimensions import build_dimension_date

        build_dimension_date(
            start_date="2020-01-01",
            end_date="2020-01-03",
            fiscal_year_start_month=4,
        )

        projected_columns = _extract_select_aliases(exploded_df.select.call_args.args)
        assert {"fiscal_year", "fiscal_month"}.issubset(projected_columns)
        projection_sql = " ".join(str(arg) for arg in exploded_df.select.call_args.args)
        assert "4" in projection_sql

    @patch("fabrictools.dimensions.write_lakehouse")
    @patch("fabrictools.dimensions.get_spark")
    @patch("fabrictools.dimensions._default_date_bounds", return_value=("2010-01-01", "2010-01-03"))
    def test_writes_date_dimension_to_lakehouse(
        self, mock_bounds, mock_get_spark, mock_write_lakehouse
    ):
        """When lakehouse params are provided, builder should persist the DataFrame."""
        spark = MagicMock()
        exploded_df = MagicMock()
        projected_df = MagicMock()
        exploded_df.select.return_value = projected_df
        projected_df.orderBy.return_value = projected_df
        spark.sql.return_value = exploded_df
        mock_get_spark.return_value = spark

        from fabrictools.dimensions import build_dimension_date

        result = build_dimension_date(
            lakehouse_name="MyLakehouse",
            lakehouse_relative_path="date_dim",
            mode="append",
        )

        mock_write_lakehouse.assert_called_once_with(
            df=projected_df,
            lakehouse_name="MyLakehouse",
            relative_path="date_dim",
            mode="append",
            spark=spark,
        )
        assert result is projected_df


class TestBuildDimensionCountry:
    @patch("fabrictools.dimensions.get_spark")
    @patch("fabrictools.dimensions._import_csc_package")
    def test_builds_country_rows_from_package(self, mock_import_pkg, mock_get_spark):
        """Country builder should map package payload fields to target schema."""
        spark = MagicMock()
        country_df = MagicMock()
        country_df.filter.return_value = country_df
        country_df.withColumn.return_value = country_df
        country_df.dropDuplicates.return_value = country_df
        country_df.orderBy.return_value = country_df
        country_df.count.return_value = 2
        spark.createDataFrame.return_value = country_df
        mock_get_spark.return_value = spark

        countries = [
            _FakeModel(
                {
                    "id": 1,
                    "iso2": "fr",
                    "iso3": "fra",
                    "name": "France",
                    "region": "Europe",
                    "subregion": "Western Europe",
                }
            ),
            _FakeModel(
                {
                    "id": 2,
                    "iso2": "us",
                    "iso3": "usa",
                    "name": "United States",
                    "region": "Americas",
                    "subregion": "Northern America",
                }
            ),
        ]
        mock_import_pkg.return_value = (lambda: countries, MagicMock(), MagicMock())

        from fabrictools.dimensions import build_dimension_country

        result = build_dimension_country()

        args, kwargs = spark.createDataFrame.call_args
        rows = args[0]
        assert rows[0]["country_code_2"] == "FR"
        assert rows[1]["country_code_3"] == "USA"
        assert kwargs["schema"] is not None
        country_df.filter.assert_called_once()
        country_df.dropDuplicates.assert_called_once_with(["country_code_2"])
        assert result is country_df

    @patch("fabrictools.dimensions.get_spark")
    @patch("fabrictools.dimensions._import_csc_package", side_effect=ImportError("missing"))
    def test_returns_empty_when_source_error_is_allowed(self, mock_import_pkg, mock_get_spark):
        """When fail_on_source_error=False, builder should return empty DataFrame."""
        spark = MagicMock()
        empty_df = MagicMock()
        spark.createDataFrame.return_value = empty_df
        mock_get_spark.return_value = spark

        from fabrictools.dimensions import build_dimension_country

        result = build_dimension_country(fail_on_source_error=False)

        spark.createDataFrame.assert_called_once()
        rows_arg = spark.createDataFrame.call_args.args[0]
        assert rows_arg == []
        assert result is empty_df

    @patch("fabrictools.dimensions.get_spark")
    @patch("fabrictools.dimensions._import_csc_package", side_effect=ImportError("missing"))
    def test_raises_when_source_error_is_not_allowed(self, mock_import_pkg, mock_get_spark):
        """When fail_on_source_error=True, builder should raise RuntimeError."""
        mock_get_spark.return_value = MagicMock()

        from fabrictools.dimensions import build_dimension_country

        with pytest.raises(RuntimeError, match="Could not build dimension_country"):
            build_dimension_country(fail_on_source_error=True)

    @patch("fabrictools.dimensions.write_lakehouse")
    @patch("fabrictools.dimensions.get_spark")
    @patch("fabrictools.dimensions._import_csc_package")
    def test_writes_country_dimension_to_lakehouse(
        self, mock_import_pkg, mock_get_spark, mock_write_lakehouse
    ):
        """Country builder should persist output when lakehouse params are provided."""
        spark = MagicMock()
        country_df = MagicMock()
        country_df.filter.return_value = country_df
        country_df.withColumn.return_value = country_df
        country_df.dropDuplicates.return_value = country_df
        country_df.orderBy.return_value = country_df
        country_df.count.return_value = 1
        spark.createDataFrame.return_value = country_df
        mock_get_spark.return_value = spark
        mock_import_pkg.return_value = (
            lambda: [_FakeModel({"id": 1, "iso2": "fr", "iso3": "fra", "name": "France"})],
            MagicMock(),
            MagicMock(),
        )

        from fabrictools.dimensions import build_dimension_country

        result = build_dimension_country(
            lakehouse_name="MyLakehouse",
            mode="append",
        )

        mock_write_lakehouse.assert_called_once_with(
            df=country_df,
            lakehouse_name="MyLakehouse",
            relative_path="dimension_country",
            mode="append",
            spark=spark,
        )
        assert result is country_df


class TestBuildDimensionCity:
    @patch("fabrictools.dimensions.get_spark")
    @patch("fabrictools.dimensions._import_csc_package")
    def test_builds_city_rows_with_state_mapping(self, mock_import_pkg, mock_get_spark):
        """City builder should enrich city rows with state and country metadata."""
        spark = MagicMock()
        city_df = MagicMock()
        city_df.filter.return_value = city_df
        city_df.withColumn.return_value = city_df
        city_df.dropDuplicates.return_value = city_df
        city_df.orderBy.return_value = city_df
        city_df.count.return_value = 3
        spark.createDataFrame.return_value = city_df
        mock_get_spark.return_value = spark

        countries = [
            _FakeModel({
                "id": 233, "iso2": "US", "iso3": "USA",
                "name": "United States", "region": "Americas",
                "subregion": "Northern America",
            }),
        ]
        states = [
            _FakeModel({"state_code": "CA", "name": "California"}),
            _FakeModel({"state_code": "NY", "name": "New York"}),
        ]
        cities = [
            _FakeModel({"id": 1, "name": "Los Angeles", "state_code": "CA"}),
            _FakeModel({"id": 2, "name": "New York City", "state_code": "NY"}),
            _FakeModel({"id": 3, "name": "Austin", "state_code": "TX"}),
            _FakeModel({"id": 4, "name": None, "state_code": "TX"}),
        ]

        mock_import_pkg.return_value = (
            lambda: countries,
            lambda _country_code: cities,
            lambda _country_code: states,
        )

        from fabrictools.dimensions import build_dimension_city

        result = build_dimension_city(include_states_metadata=True)

        args, kwargs = spark.createDataFrame.call_args
        rows = args[0]
        assert len(rows) == 3
        assert rows[0]["country_code_2"] == "US"
        assert rows[0]["country_code_3"] == "USA"
        assert rows[0]["country_key"] == 233
        assert rows[0]["region"] == "Americas"
        assert rows[0]["subregion"] == "Northern America"
        assert rows[0]["state_name"] == "California"
        assert rows[2]["state_name"] is None
        assert kwargs["schema"] is not None
        city_df.dropDuplicates.assert_called_once_with(["country_code_2", "state_name", "city_name"])
        assert result is city_df

    @patch("fabrictools.dimensions.get_spark")
    @patch("fabrictools.dimensions._import_csc_package", side_effect=ImportError("missing"))
    def test_returns_empty_when_city_source_error_is_allowed(self, mock_import_pkg, mock_get_spark):
        """When city source fails and errors are allowed, return an empty DataFrame."""
        spark = MagicMock()
        empty_df = MagicMock()
        spark.createDataFrame.return_value = empty_df
        mock_get_spark.return_value = spark

        from fabrictools.dimensions import build_dimension_city

        result = build_dimension_city(fail_on_source_error=False)

        spark.createDataFrame.assert_called_once()
        rows_arg = spark.createDataFrame.call_args.args[0]
        assert rows_arg == []
        assert result is empty_df

    @patch("fabrictools.dimensions.write_lakehouse")
    @patch("fabrictools.dimensions.get_spark")
    @patch("fabrictools.dimensions._import_csc_package")
    def test_writes_city_dimension_to_lakehouse(
        self, mock_import_pkg, mock_get_spark, mock_write_lakehouse
    ):
        """City builder should persist output when lakehouse params are provided."""
        spark = MagicMock()
        city_df = MagicMock()
        city_df.filter.return_value = city_df
        city_df.withColumn.return_value = city_df
        city_df.dropDuplicates.return_value = city_df
        city_df.orderBy.return_value = city_df
        city_df.count.return_value = 1
        spark.createDataFrame.return_value = city_df
        mock_get_spark.return_value = spark
        mock_import_pkg.return_value = (
            lambda: [_FakeModel({
                "id": 233, "iso2": "US", "iso3": "USA",
                "name": "United States", "region": "Americas",
                "subregion": "Northern America",
            })],
            lambda _country_code: [_FakeModel({"id": 1, "name": "Los Angeles", "state_code": "CA"})],
            lambda _country_code: [_FakeModel({"state_code": "CA", "name": "California"})],
        )

        from fabrictools.dimensions import build_dimension_city

        result = build_dimension_city(
            lakehouse_name="MyLakehouse",
            lakehouse_relative_path="Tables/custom_city_dim",
        )

        mock_write_lakehouse.assert_called_once_with(
            df=city_df,
            lakehouse_name="MyLakehouse",
            relative_path="Tables/custom_city_dim",
            mode="overwrite",
            spark=spark,
        )
        assert result is city_df

    @patch("fabrictools.dimensions.get_spark")
    @patch("fabrictools.dimensions._import_csc_package")
    def test_filters_by_region(self, mock_import_pkg, mock_get_spark):
        """City builder should only include countries matching the region filter."""
        spark = MagicMock()
        city_df = MagicMock()
        city_df.filter.return_value = city_df
        city_df.withColumn.return_value = city_df
        city_df.dropDuplicates.return_value = city_df
        city_df.orderBy.return_value = city_df
        city_df.count.return_value = 1
        spark.createDataFrame.return_value = city_df
        mock_get_spark.return_value = spark

        countries = [
            _FakeModel({
                "id": 1, "iso2": "FR", "iso3": "FRA",
                "name": "France", "region": "Europe",
                "subregion": "Western Europe",
            }),
            _FakeModel({
                "id": 233, "iso2": "US", "iso3": "USA",
                "name": "United States", "region": "Americas",
                "subregion": "Northern America",
            }),
        ]
        mock_import_pkg.return_value = (
            lambda: countries,
            lambda cc: [_FakeModel({"id": 10, "name": "Paris", "state_code": None})]
            if cc == "FR"
            else [_FakeModel({"id": 20, "name": "New York City", "state_code": None})],
            lambda _cc: [],
        )

        from fabrictools.dimensions import build_dimension_city

        build_dimension_city(regions=["Europe"])

        rows = spark.createDataFrame.call_args.args[0]
        assert len(rows) == 1
        assert rows[0]["country_code_2"] == "FR"
        assert rows[0]["region"] == "Europe"

    @patch("fabrictools.dimensions.get_spark")
    @patch("fabrictools.dimensions._import_csc_package")
    def test_filters_by_subregion(self, mock_import_pkg, mock_get_spark):
        """City builder should only include countries matching the subregion filter."""
        spark = MagicMock()
        city_df = MagicMock()
        city_df.filter.return_value = city_df
        city_df.withColumn.return_value = city_df
        city_df.dropDuplicates.return_value = city_df
        city_df.orderBy.return_value = city_df
        city_df.count.return_value = 1
        spark.createDataFrame.return_value = city_df
        mock_get_spark.return_value = spark

        countries = [
            _FakeModel({
                "id": 1, "iso2": "FR", "iso3": "FRA",
                "name": "France", "region": "Europe",
                "subregion": "Western Europe",
            }),
            _FakeModel({
                "id": 2, "iso2": "DE", "iso3": "DEU",
                "name": "Germany", "region": "Europe",
                "subregion": "Western Europe",
            }),
            _FakeModel({
                "id": 3, "iso2": "PL", "iso3": "POL",
                "name": "Poland", "region": "Europe",
                "subregion": "Eastern Europe",
            }),
        ]
        mock_import_pkg.return_value = (
            lambda: countries,
            lambda cc: [_FakeModel({"id": 10, "name": f"City-{cc}", "state_code": None})],
            lambda _cc: [],
        )

        from fabrictools.dimensions import build_dimension_city

        build_dimension_city(subregions=["Western Europe"])

        rows = spark.createDataFrame.call_args.args[0]
        assert len(rows) == 2
        codes = {r["country_code_2"] for r in rows}
        assert codes == {"FR", "DE"}

    @patch("fabrictools.dimensions.get_spark")
    @patch("fabrictools.dimensions._import_csc_package")
    def test_filters_by_country_multi_format(self, mock_import_pkg, mock_get_spark):
        """Country filter should accept code2, code3 and name (case-insensitive)."""
        spark = MagicMock()
        city_df = MagicMock()
        city_df.filter.return_value = city_df
        city_df.withColumn.return_value = city_df
        city_df.dropDuplicates.return_value = city_df
        city_df.orderBy.return_value = city_df
        city_df.count.return_value = 2
        spark.createDataFrame.return_value = city_df
        mock_get_spark.return_value = spark

        countries = [
            _FakeModel({
                "id": 1, "iso2": "FR", "iso3": "FRA",
                "name": "France", "region": "Europe",
                "subregion": "Western Europe",
            }),
            _FakeModel({
                "id": 233, "iso2": "US", "iso3": "USA",
                "name": "United States", "region": "Americas",
                "subregion": "Northern America",
            }),
            _FakeModel({
                "id": 2, "iso2": "DE", "iso3": "DEU",
                "name": "Germany", "region": "Europe",
                "subregion": "Western Europe",
            }),
        ]
        mock_import_pkg.return_value = (
            lambda: countries,
            lambda cc: [_FakeModel({"id": 10, "name": f"City-{cc}", "state_code": None})],
            lambda _cc: [],
        )

        from fabrictools.dimensions import build_dimension_city

        # Mix code2, code3, and name (case-insensitive)
        build_dimension_city(countries=["fr", "USA", "germany"])

        rows = spark.createDataFrame.call_args.args[0]
        assert len(rows) == 3
        codes = {r["country_code_2"] for r in rows}
        assert codes == {"FR", "US", "DE"}

    @patch("fabrictools.dimensions.get_spark")
    @patch("fabrictools.dimensions._import_csc_package")
    def test_filters_combined_and_logic(self, mock_import_pkg, mock_get_spark):
        """Region, subregion, and country filters are combined with AND logic."""
        spark = MagicMock()
        city_df = MagicMock()
        city_df.filter.return_value = city_df
        city_df.withColumn.return_value = city_df
        city_df.dropDuplicates.return_value = city_df
        city_df.orderBy.return_value = city_df
        city_df.count.return_value = 1
        spark.createDataFrame.return_value = city_df
        mock_get_spark.return_value = spark

        countries = [
            _FakeModel({
                "id": 1, "iso2": "FR", "iso3": "FRA",
                "name": "France", "region": "Europe",
                "subregion": "Western Europe",
            }),
            _FakeModel({
                "id": 2, "iso2": "DE", "iso3": "DEU",
                "name": "Germany", "region": "Europe",
                "subregion": "Western Europe",
            }),
        ]
        mock_import_pkg.return_value = (
            lambda: countries,
            lambda cc: [_FakeModel({"id": 10, "name": f"City-{cc}", "state_code": None})],
            lambda _cc: [],
        )

        from fabrictools.dimensions import build_dimension_city

        build_dimension_city(regions=["Europe"], countries=["FR"])

        rows = spark.createDataFrame.call_args.args[0]
        assert len(rows) == 1
        assert rows[0]["country_code_2"] == "FR"

    @patch("fabrictools.dimensions.get_spark")
    @patch("fabrictools.dimensions._import_csc_package")
    def test_no_filters_returns_all(self, mock_import_pkg, mock_get_spark):
        """Without filters, all countries and cities should be included."""
        spark = MagicMock()
        city_df = MagicMock()
        city_df.filter.return_value = city_df
        city_df.withColumn.return_value = city_df
        city_df.dropDuplicates.return_value = city_df
        city_df.orderBy.return_value = city_df
        city_df.count.return_value = 2
        spark.createDataFrame.return_value = city_df
        mock_get_spark.return_value = spark

        countries = [
            _FakeModel({
                "id": 1, "iso2": "FR", "iso3": "FRA",
                "name": "France", "region": "Europe",
                "subregion": "Western Europe",
            }),
            _FakeModel({
                "id": 233, "iso2": "US", "iso3": "USA",
                "name": "United States", "region": "Americas",
                "subregion": "Northern America",
            }),
        ]
        mock_import_pkg.return_value = (
            lambda: countries,
            lambda cc: [_FakeModel({"id": 10, "name": f"City-{cc}", "state_code": None})],
            lambda _cc: [],
        )

        from fabrictools.dimensions import build_dimension_city

        build_dimension_city()

        rows = spark.createDataFrame.call_args.args[0]
        assert len(rows) == 2

class TestGenerateDimensions:
    @patch("fabrictools.dimensions.get_spark")
    @patch("fabrictools.dimensions._write_dimension_targets")
    @patch("fabrictools.dimensions.build_dimension_city")
    @patch("fabrictools.dimensions.build_dimension_country")
    @patch("fabrictools.dimensions.build_dimension_date")
    def test_orchestrates_all_dimension_builds_and_writes(
        self,
        mock_build_date,
        mock_build_country,
        mock_build_city,
        mock_write_targets,
        mock_get_spark,
    ):
        """generate_dimensions should build and write each enabled dimension."""
        spark = MagicMock()
        mock_get_spark.return_value = spark
        date_df = MagicMock()
        country_df = MagicMock()
        city_df = MagicMock()
        mock_build_date.return_value = date_df
        mock_build_country.return_value = country_df
        mock_build_city.return_value = city_df

        from fabrictools.dimensions import generate_dimensions

        result = generate_dimensions(
            lakehouse_name="MyLakehouse",
            warehouse_name="MyWarehouse",
            include_date=True,
            include_country=True,
            include_city=True,
            start_date="2020-01-01",
            end_date="2020-12-31",
            fiscal_year_start_month=4,
            countries_limit=10,
            city_regions=["Europe"],
            city_subregions=["Western Europe"],
            city_countries=["FR"],
        )

        mock_build_date.assert_called_once_with(
            start_date="2020-01-01",
            end_date="2020-12-31",
            fiscal_year_start_month=4,
            spark=spark,
        )
        mock_build_country.assert_called_once_with(
            countries_limit=10,
            fail_on_source_error=True,
            spark=spark,
        )
        mock_build_city.assert_called_once_with(
            countries_limit=10,
            include_states_metadata=True,
            fail_on_source_error=True,
            regions=["Europe"],
            subregions=["Western Europe"],
            countries=["FR"],
            spark=spark,
        )
        assert mock_write_targets.call_count == 3
        assert result == {
            "dimension_date": date_df,
            "dimension_country": country_df,
            "dimension_city": city_df,
        }

    @patch("fabrictools.dimensions.get_spark")
    def test_raises_when_no_dimension_is_enabled(self, mock_get_spark):
        """At least one dimension must be enabled."""
        mock_get_spark.return_value = MagicMock()
        from fabrictools.dimensions import generate_dimensions

        with pytest.raises(ValueError, match="At least one dimension"):
            generate_dimensions(
                lakehouse_name="LH",
                warehouse_name="WH",
                include_date=False,
                include_country=False,
                include_city=False,
            )

