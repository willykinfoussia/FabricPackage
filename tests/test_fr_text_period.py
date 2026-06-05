"""Tests for French year/month extraction from free-form text labels."""

from __future__ import annotations

import pytest

from fabrictools.transform._fr_month_tokens import (
    month_num_from_fr_text_label,
    year_from_fr_text_label,
)
from fabrictools.transform.fr_text_period import (
    month_from_fr_text,
    with_year_month_from_fr_text,
    year_from_fr_text,
)

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("fabrictools_test_fr_text_period")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.mark.parametrize(
    "label,expected",
    [
        ("OIT fev 2026", 2026),
        ("2024", 2024),
        ("prefix 1999 suffix", 1999),
        ("no year here", None),
        ("", None),
    ],
)
def test_year_from_fr_text_label_python(label: str, expected: int | None) -> None:
    assert year_from_fr_text_label(label) == expected


@pytest.mark.parametrize(
    "label,expected",
    [
        ("févr", 2),
        ("fev", 2),
        ("OIT fev 2026", 2),
        ("janvier_2024", 1),
        ("mai", 5),
        ("unknown label", None),
    ],
)
def test_month_num_from_fr_text_label_python(label: str, expected: int | None) -> None:
    assert month_num_from_fr_text_label(label) == expected


def test_month_does_not_match_substring_python() -> None:
    assert month_num_from_fr_text_label("courtmai2024") is None


def test_year_from_fr_text_spark(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("OIT fev 2026",), ("no year",)],
        ["label"],
    )
    rows = df.withColumn("y", year_from_fr_text("label")).collect()
    assert rows[0]["y"] == 2026
    assert rows[1]["y"] is None


def test_month_from_fr_text_spark(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("févr",), ("OIT fev 2026",), ("n/a",)],
        ["label"],
    )
    rows = df.withColumn("m", month_from_fr_text("label")).collect()
    assert rows[0]["m"] == 2
    assert rows[1]["m"] == 2
    assert rows[2]["m"] is None


def test_with_year_month_from_fr_text_spark(spark: SparkSession) -> None:
    df = spark.createDataFrame([("OIT fev 2026",)], ["libelle_periode"])
    out = with_year_month_from_fr_text(df, "libelle_periode")
    row = out.collect()[0]
    assert row["annee"] == 2026
    assert row["mois"] == 2


def test_with_year_month_from_fr_text_unknown_column(spark: SparkSession) -> None:
    df = spark.createDataFrame([("x",)], ["a"])
    with pytest.raises(ValueError, match="does not resolve"):
        with_year_month_from_fr_text(df, "missing_col")
