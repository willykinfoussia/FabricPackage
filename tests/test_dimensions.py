"""
Unit tests for fabrictools.dimensions.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class _FakeModel:
    def __init__(self, payload: dict):
        self._payload = payload

    def dict(self) -> dict:
        return dict(self._payload)


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
        projected_df.orderBy.assert_called_once_with("date_key")
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
            _FakeModel({"id": 233, "iso2": "US", "name": "United States"}),
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
            countries_limit=10,
        )

        mock_build_date.assert_called_once_with(
            start_date="2020-01-01",
            end_date="2020-12-31",
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

