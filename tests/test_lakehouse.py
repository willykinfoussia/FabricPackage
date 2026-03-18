"""
Unit tests for fabrictools.lakehouse.

All Fabric-specific dependencies (notebookutils, SparkSession, DeltaTable)
are mocked so the tests run without a live Spark / Fabric environment.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import IntegerType, StringType

# ── Helpers ───────────────────────────────────────────────────────────────────

ABFS_BASE = "abfss://container@account.dfs.core.windows.net"
LH_NAME = "TestLakehouse"
REL_PATH = "sales/2024"
FULL_PATH = f"{ABFS_BASE}/{REL_PATH}"


def _make_df(rows: int = 5, cols: int = 3) -> MagicMock:
    """Return a mock DataFrame with .count() and .columns."""
    df = MagicMock()
    df.count.return_value = rows
    df.columns = [f"col{i}" for i in range(cols)]
    return df


# ── read_lakehouse ─────────────────────────────────────────────────────────────


class TestReadLakehouse:
    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.lakehouse.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_reads_delta_first(self, mock_path, mock_spark):
        """Delta is the preferred format and should be tried first."""
        mock_df = _make_df()
        spark = MagicMock()
        spark.read.format.return_value.load.return_value = mock_df
        mock_spark.return_value = spark

        from fabrictools.lakehouse import read_lakehouse

        result = read_lakehouse(LH_NAME, REL_PATH)

        spark.read.format.assert_called_once_with("delta")
        spark.read.format.return_value.load.assert_called_once_with(FULL_PATH)
        assert result is mock_df

    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.lakehouse.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_falls_back_to_parquet(self, mock_path, mock_spark):
        """When Delta read fails, Parquet should be attempted."""
        mock_df = _make_df()
        spark = MagicMock()
        delta_reader = MagicMock()
        delta_reader.load.side_effect = Exception("not delta")
        parquet_reader = MagicMock()
        parquet_reader.load.return_value = mock_df

        spark.read.format.side_effect = lambda fmt: (
            delta_reader if fmt == "delta" else parquet_reader
        )
        mock_spark.return_value = spark

        from fabrictools.lakehouse import read_lakehouse

        result = read_lakehouse(LH_NAME, REL_PATH)
        assert result is mock_df

    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.lakehouse.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_falls_back_to_csv(self, mock_path, mock_spark):
        """When both Delta and Parquet fail, CSV should be attempted."""
        mock_df = _make_df()
        spark = MagicMock()
        failing_reader = MagicMock()
        failing_reader.load.side_effect = Exception("unsupported format")

        csv_chain = MagicMock()
        csv_chain.option.return_value = csv_chain
        csv_chain.csv.return_value = mock_df
        spark.read.format.return_value = failing_reader
        spark.read.option.return_value = csv_chain

        mock_spark.return_value = spark

        from fabrictools.lakehouse import read_lakehouse

        result = read_lakehouse(LH_NAME, REL_PATH)
        assert result is mock_df

    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.lakehouse.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_raises_when_all_formats_fail(self, mock_path, mock_spark):
        """RuntimeError is raised when no format succeeds."""
        spark = MagicMock()
        failing_reader = MagicMock()
        failing_reader.load.side_effect = Exception("bad")
        spark.read.format.return_value = failing_reader

        csv_chain = MagicMock()
        csv_chain.option.return_value = csv_chain
        csv_chain.csv.side_effect = Exception("bad csv")
        spark.read.option.return_value = csv_chain

        mock_spark.return_value = spark

        from fabrictools.lakehouse import read_lakehouse

        with pytest.raises(RuntimeError, match="Delta, Parquet, or CSV"):
            read_lakehouse(LH_NAME, REL_PATH)

    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.lakehouse.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_accepts_explicit_spark(self, mock_path, mock_spark):
        """Passing `spark` explicitly should bypass get_spark()."""
        mock_df = _make_df()
        explicit_spark = MagicMock()
        explicit_spark.read.format.return_value.load.return_value = mock_df

        from fabrictools.lakehouse import read_lakehouse

        read_lakehouse(LH_NAME, REL_PATH, spark=explicit_spark)
        mock_spark.assert_not_called()


# ── write_lakehouse ────────────────────────────────────────────────────────────


class TestWriteLakehouse:
    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.lakehouse.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_writes_delta_by_default(self, mock_path, mock_spark):
        """Default format should be delta."""
        mock_df = _make_df()
        writer = MagicMock()
        writer.format.return_value = writer
        writer.mode.return_value = writer
        mock_df.write = writer

        from fabrictools.lakehouse import write_lakehouse

        write_lakehouse(mock_df, LH_NAME, "output/table")

        writer.format.assert_called_once_with("delta")
        writer.mode.assert_called_once_with("overwrite")
        writer.save.assert_called_once_with(f"{ABFS_BASE}/output/table")

    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.lakehouse.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_partitions_when_requested(self, mock_path, mock_spark):
        """partitionBy should be applied when partition_by is provided."""
        mock_df = _make_df()
        writer = MagicMock()
        writer.format.return_value = writer
        writer.mode.return_value = writer
        writer.partitionBy.return_value = writer
        mock_df.write = writer

        from fabrictools.lakehouse import write_lakehouse

        write_lakehouse(mock_df, LH_NAME, "output/table", partition_by=["year", "month"])

        writer.partitionBy.assert_called_once_with("year", "month")


# ── merge_lakehouse ────────────────────────────────────────────────────────────


class TestMergeLakehouse:
    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.lakehouse.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_merge_calls_execute(self, mock_path, mock_spark):
        """merge_lakehouse should call execute() on the merge builder."""
        source_df = _make_df()
        source_df.alias.return_value = source_df

        mock_delta_table = MagicMock()
        merge_builder = MagicMock()
        merge_builder.whenMatchedUpdateAll.return_value = merge_builder
        merge_builder.whenNotMatchedInsertAll.return_value = merge_builder
        mock_delta_table.alias.return_value.merge.return_value = merge_builder

        with patch("fabrictools.lakehouse.DeltaTable") as mock_dt_cls:
            mock_dt_cls.forPath.return_value = mock_delta_table

            from fabrictools.lakehouse import merge_lakehouse

            merge_lakehouse(
                source_df,
                LH_NAME,
                "sales_clean",
                merge_condition="src.id = tgt.id",
            )

            merge_builder.execute.assert_called_once()


# ── data_quality helpers ───────────────────────────────────────────────────────


class TestDataCleaningAndQuality:
    def test_clean_data_applies_standard_transforms(self):
        """clean_data should rename, trim/nullify strings, deduplicate, and drop all-null rows."""
        source_df = MagicMock()
        source_df.columns = ["Order ID", " Customer Name ", "Amount"]
        source_df.count.return_value = 15
        source_df.schema.fields = [
            SimpleNamespace(name="order_id", dataType=IntegerType()),
            SimpleNamespace(name="customer_name", dataType=StringType()),
            SimpleNamespace(name="amount", dataType=IntegerType()),
        ]

        cleaned_df = MagicMock()
        cleaned_df.columns = ["order_id", "customer_name", "amount"]
        cleaned_df.count.return_value = 12
        cleaned_df.schema.fields = source_df.schema.fields
        source_df.toDF.return_value = cleaned_df
        cleaned_df.withColumn.return_value = cleaned_df
        cleaned_df.dropDuplicates.return_value = cleaned_df
        cleaned_df.dropna.return_value = cleaned_df

        from fabrictools.data_quality import clean_data

        result = clean_data(source_df)

        source_df.toDF.assert_called_once_with("order_id", "customer_name", "amount")
        cleaned_df.withColumn.assert_called_once()
        cleaned_df.dropDuplicates.assert_called_once()
        cleaned_df.dropna.assert_called_once_with(how="all")
        assert result is cleaned_df

    def test_scan_data_errors_reports_counts_and_collisions(self):
        """scan_data_errors should report nulls, blanks, duplicates, and column-name collisions."""
        df = MagicMock()
        df.columns = ["Order ID", "order-id", "customer_name"]
        df.count.return_value = 10
        df.schema.fields = [
            SimpleNamespace(name="Order ID", dataType=IntegerType()),
            SimpleNamespace(name="order-id", dataType=StringType()),
            SimpleNamespace(name="customer_name", dataType=StringType()),
        ]

        null_counts_row = MagicMock()
        null_counts_row.asDict.return_value = {
            "Order ID": 1,
            "order-id": 0,
            "customer_name": 2,
        }
        blank_counts_row = MagicMock()
        blank_counts_row.asDict.return_value = {
            "order-id": 3,
            "customer_name": 0,
        }
        agg_null = MagicMock()
        agg_null.collect.return_value = [null_counts_row]
        agg_blank = MagicMock()
        agg_blank.collect.return_value = [blank_counts_row]
        df.agg.side_effect = [agg_null, agg_blank]

        distinct_df = MagicMock()
        distinct_df.count.return_value = 8
        df.distinct.return_value = distinct_df

        sample_row = MagicMock()
        sample_row.asDict.return_value = {"Order ID": 1, "order-id": "A-1", "customer_name": "Acme"}
        limited_df = MagicMock()
        limited_df.collect.return_value = [sample_row]
        df.limit.return_value = limited_df

        from fabrictools.data_quality import scan_data_errors

        report = scan_data_errors(df, include_samples=True)

        assert report["row_count"] == 10
        assert report["column_count"] == 3
        assert report["duplicate_row_count"] == 2
        assert report["null_counts"]["customer_name"] == 2
        assert report["blank_string_counts"]["order-id"] == 3
        assert report["normalized_name_collisions"] == {"order_id": ["Order ID", "order-id"]}
        assert len(report["sample_rows"]) == 1

    @patch("fabrictools.data_quality.write_lakehouse")
    @patch("fabrictools.data_quality.clean_data")
    @patch("fabrictools.data_quality.read_lakehouse")
    @patch("fabrictools.data_quality.get_spark")
    def test_clean_and_write_data_orchestration(
        self, mock_get_spark, mock_read, mock_clean, mock_write
    ):
        """clean_and_write_data should orchestrate read -> clean -> write."""
        spark = MagicMock()
        mock_get_spark.return_value = spark
        source_df = MagicMock()
        cleaned_df = MagicMock()
        mock_read.return_value = source_df
        mock_clean.return_value = cleaned_df

        from fabrictools.data_quality import clean_and_write_data

        result = clean_and_write_data(
            source_lakehouse_name="RawLakehouse",
            source_relative_path="sales/raw",
            target_lakehouse_name="CuratedLakehouse",
            target_relative_path="sales/clean",
            mode="append",
            partition_by=["year"],
        )

        mock_read.assert_called_once_with("RawLakehouse", "sales/raw", spark=spark)
        mock_clean.assert_called_once_with(source_df)
        mock_write.assert_called_once_with(
            cleaned_df,
            lakehouse_name="CuratedLakehouse",
            relative_path="sales/clean",
            mode="append",
            partition_by=["year"],
            spark=spark,
        )
        assert result is cleaned_df
