"""Tests for fabrictools.quality.clean helpers."""

from __future__ import annotations

from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, StringType, TimestampType

from fabrictools.quality.clean import (
    _build_unique_column_names,
    _candidate_date_format_indices,
    _candidate_timestamp_format_indices,
    clean_data,
    detect_and_cast_columns,
)


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

    cleaned = clean_data(df, drop_all_null_rows=False)

    assert cleaned.columns == ["odata_context", "companyid", "name"]
    row = cleaned.collect()[0]
    assert row["companyid"] == "SPH"
    assert row["name"] == "ACME"


def test_candidate_date_format_indices_iso_dash() -> None:
    assert _candidate_date_format_indices("2024-01-15") == [0]


def test_candidate_date_format_indices_european_slash() -> None:
    indices = _candidate_date_format_indices("15/01/2024")
    assert 6 in indices
    assert 7 in indices


def test_candidate_timestamp_format_indices_with_time() -> None:
    indices = _candidate_timestamp_format_indices("2024-01-15 10:30:00")
    assert 0 in indices


def test_detect_and_cast_columns_casts_iso_date(spark) -> None:
    df = spark.createDataFrame([("2024-01-15",), ("2024-02-20",)], ["order_date"])

    result = detect_and_cast_columns(df)

    assert isinstance(result.schema["order_date"].dataType, DateType)
    rows = result.collect()
    assert rows[0]["order_date"] == date(2024, 1, 15)
    assert rows[1]["order_date"] == date(2024, 2, 20)


def test_detect_and_cast_columns_casts_european_date(spark) -> None:
    df = spark.createDataFrame([("15/01/2024",), ("20/02/2024",)], ["order_date"])

    result = detect_and_cast_columns(df)

    assert isinstance(result.schema["order_date"].dataType, DateType)
    rows = result.collect()
    assert rows[0]["order_date"] == date(2024, 1, 15)
    assert rows[1]["order_date"] == date(2024, 2, 20)


def test_detect_and_cast_columns_casts_timestamp(spark) -> None:
    df = spark.createDataFrame(
        [("2024-01-15T10:30:00",), ("2024-02-20T14:45:00",)],
        ["event_time"],
    )

    result = detect_and_cast_columns(df)

    assert isinstance(result.schema["event_time"].dataType, TimestampType)
    rows = result.collect()
    assert rows[0]["event_time"] == datetime(2024, 1, 15, 10, 30, 0)
    assert rows[1]["event_time"] == datetime(2024, 2, 20, 14, 45, 0)


def test_clean_data_empty_strings_become_null(spark) -> None:
    df = spark.createDataFrame([("  ", "42"), ("ACME", "")], ["name", "amount"])

    cleaned = clean_data(df, drop_all_null_rows=False)

    row_blank_name = cleaned.collect()[0]
    row_blank_amount = cleaned.collect()[1]
    assert row_blank_name["name"] is None
    assert isinstance(cleaned.schema["amount"].dataType, IntegerType)
    assert row_blank_name["amount"] == 42
    assert row_blank_amount["name"] == "ACME"
    assert row_blank_amount["amount"] is None


def test_clean_data_normalize_strings_trims_whitespace(spark) -> None:
    df = spark.createDataFrame([("  ACME  ",)], ["name"])

    cleaned = clean_data(df, drop_all_null_rows=False)

    assert cleaned.collect()[0]["name"] == "ACME"
    assert isinstance(cleaned.schema["name"].dataType, StringType)
