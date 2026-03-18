"""
Unit tests for fabrictools.lakehouse.

All Fabric-specific dependencies (notebookutils, SparkSession, DeltaTable)
are mocked so the tests run without a live Spark / Fabric environment.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

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
