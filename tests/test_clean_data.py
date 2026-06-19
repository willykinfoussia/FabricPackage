"""Tests for fabrictools.quality.clean helpers."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from fabrictools.quality.clean import _build_unique_column_names, clean_data


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("fabrictools_test_clean_data")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_build_unique_column_names_keeps_value_prefix_without_odata_context() -> None:
    columns = ["value.CompanyId", "Name"]
    assert _build_unique_column_names(columns) == ["value_companyid", "name"]


def test_build_unique_column_names_strips_value_prefix_with_odata_context() -> None:
    columns = ["@odata.context", "value.CompanyId", "value.Name"]
    assert _build_unique_column_names(columns) == [
        "odata_context",
        "companyid",
        "name",
    ]


def test_build_unique_column_names_strips_value_prefix_with_literal_odata_context() -> None:
    columns = ["odata_context", "value_CompanyId"]
    assert _build_unique_column_names(columns) == ["odata_context", "companyid"]


def test_build_unique_column_names_deduplicates_after_value_prefix_strip() -> None:
    columns = ["@odata.context", "value.Id", "Id"]
    assert _build_unique_column_names(columns) == ["odata_context", "id", "id_2"]


def test_build_unique_column_names_never_strips_odata_context() -> None:
    columns = ["@odata.context"]
    assert _build_unique_column_names(columns) == ["odata_context"]


def test_clean_data_strips_value_prefix_for_odata_payload(spark) -> None:
    df = spark.createDataFrame(
        [
            {
                "@odata.context": "https://ifs.example.com/$metadata#EntitySet",
                "value.CompanyId": "SPH",
                "value.Name": "ACME",
            }
        ]
    )

    cleaned = clean_data(df, drop_duplicates=False, drop_all_null_rows=False)

    assert cleaned.columns == ["odata_context", "companyid", "name"]
    row = cleaned.collect()[0]
    assert row["companyid"] == "SPH"
    assert row["name"] == "ACME"
