"""
Unit tests for fabrictools.warehouse.

All Fabric-specific dependencies (notebookutils, SparkSession, JDBC) are
mocked so the tests run without a live Spark / Fabric environment.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

JDBC_URL = "jdbc:sqlserver://myserver.database.windows.net;database=MyWarehouse;encrypt=true;trustServerCertificate=false;loginTimeout=30;"
WH_NAME = "MyWarehouse"
QUERY = "SELECT * FROM dbo.sales"
TABLE = "dbo.sales_clean"


def _make_df(rows: int = 10, cols: int = 4) -> MagicMock:
    df = MagicMock()
    df.count.return_value = rows
    df.columns = [f"col{i}" for i in range(cols)]
    return df


# ── read_warehouse ─────────────────────────────────────────────────────────────


class TestReadWarehouse:
    @patch("fabrictools.warehouse.get_spark")
    @patch("fabrictools.warehouse.get_warehouse_jdbc_url", return_value=JDBC_URL)
    def test_reads_via_jdbc(self, mock_url, mock_spark):
        """read_warehouse should use the jdbc format with the resolved URL."""
        mock_df = _make_df()
        spark = MagicMock()
        reader = MagicMock()
        reader.format.return_value = reader
        reader.option.return_value = reader
        reader.load.return_value = mock_df
        spark.read = reader
        mock_spark.return_value = spark

        from fabrictools.warehouse import read_warehouse

        result = read_warehouse(WH_NAME, QUERY)

        reader.format.assert_called_once_with("jdbc")
        assert result is mock_df

    @patch("fabrictools.warehouse.get_spark")
    @patch("fabrictools.warehouse.get_warehouse_jdbc_url", return_value=JDBC_URL)
    def test_accepts_explicit_spark(self, mock_url, mock_spark):
        """Passing spark explicitly should bypass get_spark()."""
        mock_df = _make_df()
        explicit_spark = MagicMock()
        reader = MagicMock()
        reader.format.return_value = reader
        reader.option.return_value = reader
        reader.load.return_value = mock_df
        explicit_spark.read = reader

        from fabrictools.warehouse import read_warehouse

        read_warehouse(WH_NAME, QUERY, spark=explicit_spark)
        mock_spark.assert_not_called()


# ── write_warehouse ────────────────────────────────────────────────────────────


class TestWriteWarehouse:
    @patch("fabrictools.warehouse.get_spark")
    @patch("fabrictools.warehouse.get_warehouse_jdbc_url", return_value=JDBC_URL)
    def test_writes_via_jdbc(self, mock_url, mock_spark):
        """write_warehouse should use jdbc format with correct dbtable option."""
        mock_df = _make_df()
        writer = MagicMock()
        writer.format.return_value = writer
        writer.option.return_value = writer
        writer.mode.return_value = writer
        mock_df.write = writer
        mock_spark.return_value = MagicMock()

        from fabrictools.warehouse import write_warehouse

        write_warehouse(mock_df, WH_NAME, TABLE)

        writer.format.assert_called_once_with("jdbc")
        writer.save.assert_called_once()

    @patch("fabrictools.warehouse.get_spark")
    @patch("fabrictools.warehouse.get_warehouse_jdbc_url", return_value=JDBC_URL)
    def test_uses_overwrite_mode_by_default(self, mock_url, mock_spark):
        """Default mode should be overwrite."""
        mock_df = _make_df()
        writer = MagicMock()
        writer.format.return_value = writer
        writer.option.return_value = writer
        writer.mode.return_value = writer
        mock_df.write = writer
        mock_spark.return_value = MagicMock()

        from fabrictools.warehouse import write_warehouse

        write_warehouse(mock_df, WH_NAME, TABLE)

        writer.mode.assert_called_once_with("overwrite")

    @patch("fabrictools.warehouse.get_spark")
    @patch("fabrictools.warehouse.get_warehouse_jdbc_url", return_value=JDBC_URL)
    def test_append_mode(self, mock_url, mock_spark):
        """mode='append' should be forwarded to the Spark writer."""
        mock_df = _make_df()
        writer = MagicMock()
        writer.format.return_value = writer
        writer.option.return_value = writer
        writer.mode.return_value = writer
        mock_df.write = writer
        mock_spark.return_value = MagicMock()

        from fabrictools.warehouse import write_warehouse

        write_warehouse(mock_df, WH_NAME, TABLE, mode="append")

        writer.mode.assert_called_once_with("append")
