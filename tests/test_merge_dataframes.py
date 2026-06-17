"""Tests for conditional prefix naming in merge_dataframes."""

from __future__ import annotations

import pytest

from fabrictools.transform.merge import merge_dataframes

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("fabrictools_test_merge_dataframes")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_merge_no_collision_uses_unprefixed_name(spark: SparkSession) -> None:
    main = spark.createDataFrame([(1, "a")], ["id", "label"])
    configuration = spark.createDataFrame([(1, "ok")], ["id", "statut"])

    out = merge_dataframes(
        main=main,
        join_df=configuration,
        join_columns=["statut"],
        keys=[("id", "id")],
        how="left",
    )

    assert "statut" in out.columns
    assert "configuration_statut" not in out.columns
    assert out.collect()[0]["statut"] == "ok"


def test_merge_collision_uses_inferred_prefix(spark: SparkSession) -> None:
    main = spark.createDataFrame([(1, "a", "left")], ["id", "label", "statut"])
    configuration = spark.createDataFrame([(1, "right")], ["id", "statut"])

    out = merge_dataframes(
        main=main,
        join_df=configuration,
        join_columns=["statut"],
        keys=[("id", "id")],
        how="left",
    )

    assert "statut" in out.columns
    assert "configuration_statut" in out.columns
    row = out.collect()[0]
    assert row["statut"] == "left"
    assert row["configuration_statut"] == "right"


def test_merge_mixed_collision_and_no_collision(spark: SparkSession) -> None:
    main = spark.createDataFrame([(1, "x")], ["id", "statut"])
    configuration = spark.createDataFrame([(1, "y", "z")], ["id", "statut", "commentaire"])

    out = merge_dataframes(
        main=main,
        join_df=configuration,
        join_columns=["statut", "commentaire"],
        keys=[("id", "id")],
        how="left",
    )

    assert "configuration_statut" in out.columns
    assert "commentaire" in out.columns
    assert "configuration_commentaire" not in out.columns


def test_merge_join_column_names_skips_prefix(spark: SparkSession) -> None:
    main = spark.createDataFrame([(1, "x")], ["id", "statut"])
    configuration = spark.createDataFrame([(1, "y")], ["id", "statut"])

    out = merge_dataframes(
        main=main,
        join_df=configuration,
        join_columns=["statut"],
        join_column_names=["statut_right"],
        keys=[("id", "id")],
        how="left",
    )

    assert "statut_right" in out.columns
    assert "configuration_statut" not in out.columns
    assert out.collect()[0]["statut_right"] == "y"


def test_merge_keys_not_reprojected(spark: SparkSession) -> None:
    main = spark.createDataFrame([(1,)], ["n_commande"])
    configuration = spark.createDataFrame([(1, 100.0)], ["n_commande", "montant"])

    out = merge_dataframes(
        main=main,
        join_df=configuration,
        join_columns=["montant"],
        keys=[("n_commande", "n_commande")],
        how="left",
    )

    assert out.columns == ["n_commande", "montant"]
    assert "configuration_montant" not in out.columns
