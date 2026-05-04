from __future__ import annotations

from types import SimpleNamespace

import pytest

from fabrictools.prepare import business


class FakeDataFrame:
    def __init__(self, columns: list[str]) -> None:
        self.columns = list(columns)
        self.to_df_calls: list[list[str]] = []

    def withColumn(self, name: str, _value: object) -> "FakeDataFrame":
        if name not in self.columns:
            self.columns.append(name)
        return self

    def toDF(self, *columns: str) -> "FakeDataFrame":
        self.to_df_calls.append(list(columns))
        self.columns = list(columns)
        return self


def test_unique_normal_case_columns_disambiguates_collisions() -> None:
    assert business._to_unique_normal_case_columns(
        ["client_description", "client__description", "n_element", "id", "ID"]
    ) == [
        "Client Description",
        "Client Description 2",
        "N° Élément",
        "ID",
        "ID 2",
    ]


def test_build_business_table_plan_rejects_duplicate_targets() -> None:
    with pytest.raises(ValueError, match="same target path"):
        business._build_business_table_plan(
            ["Tables/dbo/Cleaned_clients", "Tables/dbo/Processed_clients"],
            {},
        )


def test_make_business_ready_uses_cached_bases_and_perf_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark = object()
    read_calls: list[dict[str, object]] = []
    write_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        business,
        "F",
        SimpleNamespace(
            current_date=lambda: "current_date",
            current_timestamp=lambda: "current_timestamp",
            lit=lambda value: ("lit", value),
            year=lambda value: ("year", value),
            month=lambda value: ("month", value),
            dayofmonth=lambda value: ("dayofmonth", value),
        ),
    )
    monkeypatch.setattr(
        business,
        "get_lakehouse_abfs_path",
        lambda lakehouse_name: f"abfs://{lakehouse_name}",
    )

    def fake_read(**kwargs: object) -> tuple[FakeDataFrame, str, str]:
        read_calls.append(kwargs)
        return (
            FakeDataFrame(["client_description"]),
            "Tables/dbo/Cleaned_clients",
            "abfs://SilverLakehouse/Tables/dbo/Cleaned_clients",
        )

    def fake_write(**kwargs: object) -> tuple[str, str]:
        write_calls.append(kwargs)
        return (
            "Tables/dbo/Clients",
            "abfs://GoldLakehouse/Tables/dbo/Clients",
        )

    monkeypatch.setattr(business, "_read_lakehouse_from_base", fake_read)
    monkeypatch.setattr(business, "_write_lakehouse_to_base", fake_write)

    summary = business.make_business_ready(
        source_lakehouse_name="SilverLakehouse",
        target_lakehouse_name="GoldLakehouse",
        tables=["Tables/dbo/Cleaned_clients"],
        mode="overwrite",
        source_format="delta",
        partition_by=["Ingestion Year"],
        auto_partition=False,
        auto_partition_threshold_bytes=42,
        spark=spark,
    )

    assert summary["successful_tables"] == 1
    assert summary["tables"][0]["resolved_source_relative_path"] == (
        "Tables/dbo/Cleaned_clients"
    )
    assert summary["tables"][0]["resolved_target_relative_path"] == (
        "Tables/dbo/Clients"
    )

    assert read_calls == [
        {
            "lakehouse_name": "SilverLakehouse",
            "relative_path": "Tables/dbo/Cleaned_clients",
            "base_path": "abfs://SilverLakehouse",
            "spark": spark,
            "format": "delta",
        }
    ]
    assert write_calls[0]["base_path"] == "abfs://GoldLakehouse"
    assert write_calls[0]["relative_path"] == "Tables/dbo/Clients"
    assert write_calls[0]["partition_by"] == ["Ingestion Year"]
    assert write_calls[0]["auto_partition"] is False
    assert write_calls[0]["auto_partition_threshold_bytes"] == 42
    assert write_calls[0]["df"].columns == [
        "Client Description",
        "Ingestion Timestamp",
        "Ingestion Source Layer",
        "Ingestion Source Path",
        "Ingestion Year",
        "Ingestion Month",
        "Ingestion Day",
    ]


def test_make_business_ready_keeps_per_table_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        business,
        "get_lakehouse_abfs_path",
        lambda lakehouse_name: f"abfs://{lakehouse_name}",
    )

    def fake_read(**_kwargs: object) -> tuple[FakeDataFrame, str, str]:
        raise RuntimeError("read failed")

    monkeypatch.setattr(business, "_read_lakehouse_from_base", fake_read)

    summary = business.make_business_ready(
        "SilverLakehouse",
        "GoldLakehouse",
        ["Tables/dbo/Cleaned_clients"],
        spark=object(),
    )

    assert summary["successful_tables"] == 0
    assert summary["failed_tables"] == 1
    assert summary["failures"] == [
        {
            "source_relative_path": "Tables/dbo/Cleaned_clients",
            "error": "read failed",
        }
    ]
