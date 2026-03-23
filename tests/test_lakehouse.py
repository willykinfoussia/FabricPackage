"""
Unit tests for fabrictools.lakehouse.

All Fabric-specific dependencies (notebookutils, SparkSession, DeltaTable)
are mocked so the tests run without a live Spark / Fabric environment.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

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
    @patch("fabrictools.lakehouse._try_read_formats")
    @patch("fabrictools.lakehouse.build_lakehouse_read_path_candidates", return_value=["sales/raw"])
    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    @patch("fabrictools.lakehouse.get_spark")
    def test_resolve_candidate_single_path_returns_directly(
        self, mock_get_spark, mock_path, mock_candidates, mock_try_read
    ):
        """Single candidate should be returned directly without probing formats."""
        from fabrictools.lakehouse import resolve_lakehouse_read_candidate

        resolved = resolve_lakehouse_read_candidate(LH_NAME, "sales/raw")

        assert resolved == "sales/raw"
        mock_try_read.assert_not_called()

    @patch("fabrictools.lakehouse._try_read_formats")
    @patch(
        "fabrictools.lakehouse.build_lakehouse_read_path_candidates",
        return_value=["sales/raw", "Tables/dbo/sales/raw", "Files/sales/raw"],
    )
    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    @patch("fabrictools.lakehouse.get_spark")
    def test_resolve_candidate_fallbacks_to_first_readable(
        self, mock_get_spark, mock_path, mock_candidates, mock_try_read
    ):
        """Multiple candidates should be tried until the first readable path."""
        spark = MagicMock()
        mock_get_spark.return_value = spark
        mock_try_read.side_effect = [RuntimeError("missing"), _make_df()]

        from fabrictools.lakehouse import resolve_lakehouse_read_candidate

        resolved = resolve_lakehouse_read_candidate(LH_NAME, "sales/raw")

        assert resolved == "Tables/dbo/sales/raw"
        assert mock_try_read.call_count == 2
        assert mock_try_read.call_args_list[0].args == (spark, f"{ABFS_BASE}/sales/raw")
        assert mock_try_read.call_args_list[1].args == (
            spark,
            f"{ABFS_BASE}/Tables/dbo/sales/raw",
        )

    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_bare_name_prefers_tables_dbo_before_files(self, mock_path, mock_spark):
        """For ambiguous names, Tables/dbo should be attempted before Files."""
        mock_df = _make_df()
        spark = MagicMock()
        attempts = []

        def format_reader(fmt):
            reader = MagicMock()

            def load(path):
                attempts.append((fmt, path))
                if path.endswith("/Tables/dbo/customers") and fmt == "delta":
                    return mock_df
                raise Exception("not found")

            reader.load.side_effect = load
            return reader

        csv_chain = MagicMock()
        csv_chain.option.return_value = csv_chain
        csv_chain.csv.side_effect = Exception("not csv")

        spark.read.format.side_effect = format_reader
        spark.read.option.return_value = csv_chain
        mock_spark.return_value = spark

        from fabrictools.lakehouse import read_lakehouse

        result = read_lakehouse(LH_NAME, "customers")

        assert result is mock_df
        assert attempts[0][1].endswith("/customers")
        assert attempts[1][1].endswith("/customers")
        assert attempts[2][1].endswith("/Tables/dbo/customers")
        assert not any(path.endswith("/Files/customers") for _, path in attempts)

    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_dbo_prefix_is_corrected_to_tables_dbo(self, mock_path, mock_spark):
        """dbo/<name> should be corrected to Tables/dbo/<name> when needed."""
        mock_df = _make_df()
        spark = MagicMock()
        attempts = []

        def format_reader(fmt):
            reader = MagicMock()

            def load(path):
                attempts.append((fmt, path))
                if path.endswith("/Tables/dbo/customers") and fmt == "delta":
                    return mock_df
                raise Exception("not found")

            reader.load.side_effect = load
            return reader

        csv_chain = MagicMock()
        csv_chain.option.return_value = csv_chain
        csv_chain.csv.side_effect = Exception("not csv")

        spark.read.format.side_effect = format_reader
        spark.read.option.return_value = csv_chain
        mock_spark.return_value = spark

        from fabrictools.lakehouse import read_lakehouse

        result = read_lakehouse(LH_NAME, "dbo/customers")

        assert result is mock_df
        assert any(path.endswith("/dbo/customers") for _, path in attempts)
        assert any(path.endswith("/Tables/dbo/customers") for _, path in attempts)

    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.lakehouse.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_explicit_tables_path_still_works_directly(self, mock_path, mock_spark):
        """Explicit full path should continue to work without fallback."""
        mock_df = _make_df()
        spark = MagicMock()
        spark.read.format.return_value.load.return_value = mock_df
        mock_spark.return_value = spark

        from fabrictools.lakehouse import read_lakehouse

        result = read_lakehouse(LH_NAME, "Tables/dbo/customers")

        spark.read.format.assert_called_once_with("delta")
        spark.read.format.return_value.load.assert_called_once_with(
            f"{ABFS_BASE}/Tables/dbo/customers"
        )
        assert result is mock_df

    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
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
    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
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
    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
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
    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
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

        with pytest.raises(RuntimeError, match="candidate path") as exc_info:
            read_lakehouse(LH_NAME, REL_PATH)

        msg = str(exc_info.value)
        assert f"{ABFS_BASE}/{REL_PATH}" in msg
        assert f"{ABFS_BASE}/Tables/dbo/{REL_PATH}" in msg
        assert f"{ABFS_BASE}/Files/{REL_PATH}" in msg

    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
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
    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_writes_delta_by_default(self, mock_path, mock_spark):
        """Default format should be delta and bare paths map to Tables/dbo."""
        mock_df = _make_df()
        writer = MagicMock()
        writer.format.return_value = writer
        writer.mode.return_value = writer
        mock_df.write = writer

        from fabrictools.lakehouse import write_lakehouse

        write_lakehouse(mock_df, LH_NAME, "output/table")

        writer.format.assert_called_once_with("delta")
        writer.mode.assert_called_once_with("overwrite")
        writer.save.assert_called_once_with(f"{ABFS_BASE}/Tables/dbo/output/table")

    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_write_corrects_dbo_prefix(self, mock_path, mock_spark):
        """dbo/<name> should be normalized to Tables/dbo/<name>."""
        mock_df = _make_df()
        writer = MagicMock()
        writer.format.return_value = writer
        writer.mode.return_value = writer
        mock_df.write = writer

        from fabrictools.lakehouse import write_lakehouse

        write_lakehouse(mock_df, LH_NAME, "dbo/customers")

        writer.save.assert_called_once_with(f"{ABFS_BASE}/Tables/dbo/customers")

    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_write_corrects_tables_without_dbo(self, mock_path, mock_spark):
        """Tables/<name> should be normalized to Tables/dbo/<name>."""
        mock_df = _make_df()
        writer = MagicMock()
        writer.format.return_value = writer
        writer.mode.return_value = writer
        mock_df.write = writer

        from fabrictools.lakehouse import write_lakehouse

        write_lakehouse(mock_df, LH_NAME, "Tables/customers")

        writer.save.assert_called_once_with(f"{ABFS_BASE}/Tables/dbo/customers")

    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_write_keeps_explicit_files_path(self, mock_path, mock_spark):
        """Explicit Files path should be preserved."""
        mock_df = _make_df()
        writer = MagicMock()
        writer.format.return_value = writer
        writer.mode.return_value = writer
        mock_df.write = writer

        from fabrictools.lakehouse import write_lakehouse

        write_lakehouse(mock_df, LH_NAME, "Files/export/customers")

        writer.save.assert_called_once_with(f"{ABFS_BASE}/Files/export/customers")

    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
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

    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_partitions_merge_explicit_and_detected_without_duplicates(
        self, mock_path, mock_spark
    ):
        """Explicit partition_by should be extended by auto-detection with deduplication."""
        mock_df = MagicMock()
        mock_df.columns = ["client_id", "annee", "mois", "jour", "value"]
        mock_df.schema.fields = [
            SimpleNamespace(name="client_id", dataType=StringType()),
            SimpleNamespace(name="annee", dataType=IntegerType()),
            SimpleNamespace(name="mois", dataType=IntegerType()),
            SimpleNamespace(name="jour", dataType=IntegerType()),
            SimpleNamespace(name="value", dataType=IntegerType()),
        ]
        writer = MagicMock()
        writer.format.return_value = writer
        writer.mode.return_value = writer
        writer.partitionBy.return_value = writer
        mock_df.write = writer

        from fabrictools.lakehouse import write_lakehouse

        write_lakehouse(
            mock_df,
            LH_NAME,
            "output/table",
            partition_by=["client_id", "mois"],
        )

        writer.partitionBy.assert_called_once_with("client_id", "mois", "annee", "jour")

    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_no_partition_applied_when_no_detectable_columns(self, mock_path, mock_spark):
        """No partitionBy should be applied when nothing is explicit or auto-detectable."""
        mock_df = _make_df()
        mock_df.schema.fields = [
            SimpleNamespace(name="col0", dataType=IntegerType()),
            SimpleNamespace(name="col1", dataType=StringType()),
        ]
        writer = MagicMock()
        writer.format.return_value = writer
        writer.mode.return_value = writer
        mock_df.write = writer

        from fabrictools.lakehouse import write_lakehouse

        write_lakehouse(mock_df, LH_NAME, "output/table")

        writer.partitionBy.assert_not_called()


# ── merge_lakehouse ────────────────────────────────────────────────────────────


class TestMergeLakehouse:
    @patch("fabrictools.lakehouse.get_spark")
    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
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

    @patch("fabrictools.data_quality.px")
    def test_scan_data_errors_returns_summary_dataframe_and_figure(self, mock_px):
        """scan_data_errors should return a summary DataFrame, totals, collisions, and chart."""
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
        df.withColumn.return_value = df
        summary_df = MagicMock()
        df.sparkSession.createDataFrame.return_value = summary_df

        distinct_df = MagicMock()
        distinct_df.count.return_value = 8
        df.distinct.return_value = distinct_df

        sample_row = MagicMock()
        sample_row.asDict.return_value = {"Order ID": 1, "order-id": "A-1", "customer_name": "Acme"}
        limited_df = MagicMock()
        limited_df.collect.return_value = [sample_row]
        df.limit.return_value = limited_df
        chart_figure = MagicMock()
        mock_px.bar.return_value = chart_figure

        from fabrictools.data_quality import scan_data_errors

        report = scan_data_errors(df, include_samples=True)

        assert report["summary_df"] is summary_df
        assert report["figure"] is chart_figure
        assert report["collisions"] == {"order_id": ["Order ID", "order-id"]}
        assert report["issue_totals"] == [
            {"issue_type": "duplicate_rows", "count": 2},
            {"issue_type": "null_values", "count": 3},
            {"issue_type": "blank_string_values", "count": 3},
            {"issue_type": "normalized_name_collisions", "count": 1},
        ]
        assert len(report["sample_rows"]) == 1
        records = df.sparkSession.createDataFrame.call_args.args[0]
        assert any(
            record["issue_type"] == "null_values"
            and record["column_name"] == "customer_name"
            and record["count"] == 2
            for record in records
        )
        assert any(
            record["issue_type"] == "blank_string_values"
            and record["column_name"] == "order-id"
            and record["count"] == 3
            for record in records
        )
        assert any(
            record["issue_type"] == "normalized_name_collisions"
            and record["column_name"] == "order_id"
            and record["count"] == 2
            for record in records
        )
        mock_px.bar.assert_called_once()
        mock_px.pie.assert_not_called()
        summary_df.show.assert_called_once_with(truncate=False)
        chart_figure.show.assert_called_once()

    @patch(
        "fabrictools.data_quality.resolve_lakehouse_read_candidate",
        return_value="Tables/dbo/sales/raw",
    )
    def test_add_silver_metadata_adds_columns(self, mock_resolve_candidate):
        """add_silver_metadata should add ingestion/source metadata and date partitions."""
        df = MagicMock()
        df.schema.fields = [SimpleNamespace(name="event_date", dataType=IntegerType())]
        df.withColumn.return_value = df

        from fabrictools.data_quality import add_silver_metadata

        result = add_silver_metadata(
            df,
            source_lakehouse_name="RawLakehouse",
            source_relative_path="sales/raw",
        )

        assert df.withColumn.call_count == 6
        added_columns = [call.args[0] for call in df.withColumn.call_args_list]
        assert added_columns == [
            "_ingestion_timestamp",
            "_source_layer",
            "_source_path",
            "_year",
            "_month",
            "_day",
        ]
        mock_resolve_candidate.assert_called_once_with(
            lakehouse_name="RawLakehouse",
            relative_path="sales/raw",
            spark=None,
        )
        assert result is df

    @patch("fabrictools.data_quality.write_lakehouse")
    @patch("fabrictools.data_quality.add_silver_metadata")
    @patch("fabrictools.data_quality.clean_data")
    @patch("fabrictools.data_quality.read_lakehouse")
    @patch("fabrictools.data_quality.get_spark")
    def test_clean_and_write_data_orchestration(
        self, mock_get_spark, mock_read, mock_clean, mock_add_metadata, mock_write
    ):
        """clean_and_write_data should orchestrate read -> clean -> metadata -> write."""
        spark = MagicMock()
        mock_get_spark.return_value = spark
        source_df = MagicMock()
        cleaned_df = MagicMock()
        silver_df = MagicMock()
        mock_read.return_value = source_df
        mock_clean.return_value = cleaned_df
        mock_add_metadata.return_value = silver_df

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
        mock_add_metadata.assert_called_once_with(
            cleaned_df,
            source_lakehouse_name="RawLakehouse",
            source_relative_path="sales/raw",
            spark=spark,
        )
        mock_write.assert_called_once_with(
            silver_df,
            lakehouse_name="CuratedLakehouse",
            relative_path="sales/clean",
            mode="append",
            partition_by=["year"],
            spark=spark,
        )
        assert result is silver_df

    @patch("fabrictools.data_quality.write_lakehouse")
    @patch("fabrictools.data_quality.add_silver_metadata")
    @patch("fabrictools.data_quality.clean_data")
    @patch("fabrictools.data_quality.read_lakehouse")
    @patch("fabrictools.data_quality.get_spark")
    def test_clean_and_write_data_defaults_to_date_partitions(
        self, mock_get_spark, mock_read, mock_clean, mock_add_metadata, mock_write
    ):
        """clean_and_write_data should let write_lakehouse auto-detect partitions."""
        spark = MagicMock()
        mock_get_spark.return_value = spark
        source_df = MagicMock()
        cleaned_df = MagicMock()
        silver_df = MagicMock()
        mock_read.return_value = source_df
        mock_clean.return_value = cleaned_df
        mock_add_metadata.return_value = silver_df

        from fabrictools.data_quality import clean_and_write_data

        clean_and_write_data(
            source_lakehouse_name="RawLakehouse",
            source_relative_path="sales/raw",
            target_lakehouse_name="CuratedLakehouse",
            target_relative_path="sales/clean",
            mode="overwrite",
        )

        mock_write.assert_called_once_with(
            silver_df,
            lakehouse_name="CuratedLakehouse",
            relative_path="sales/clean",
            mode="overwrite",
            partition_by=None,
            spark=spark,
        )

    @patch("fabrictools.core.get_lakehouse_abfs_path", return_value=ABFS_BASE)
    def test_list_lakehouse_table_paths_discovers_all_schemas(self, mock_abfs_path):
        """_list_lakehouse_table_paths should scan Tables/<schema>/<table> across schemas."""
        tables_root = f"{ABFS_BASE}/Tables"
        dbo_path = f"{tables_root}/dbo"
        sales_path = f"{tables_root}/sales"

        fake_nb = MagicMock()
        fake_nb.fs.ls.side_effect = lambda path: {
            tables_root: [
                SimpleNamespace(name="dbo/", path=f"{dbo_path}/"),
                SimpleNamespace(name="sales/", path=f"{sales_path}/"),
            ],
            dbo_path: [
                SimpleNamespace(name="customers/", path=f"{dbo_path}/customers/"),
                SimpleNamespace(name="orders/", path=f"{dbo_path}/orders/"),
            ],
            sales_path: [
                SimpleNamespace(name="invoices/", path=f"{sales_path}/invoices/"),
            ],
        }[path]

        with patch.dict("sys.modules", {"notebookutils": fake_nb}):
            from fabrictools.data_quality import _list_lakehouse_table_paths

            discovered = _list_lakehouse_table_paths(
                lakehouse_name=LH_NAME,
                include_schemas=None,
                exclude_tables=["customers", "sales.ignored"],
            )

        assert discovered == ["Tables/dbo/orders", "Tables/sales/invoices"]
        mock_abfs_path.assert_called_once_with(LH_NAME)
        assert fake_nb.fs.ls.call_args_list == [
            call(tables_root),
            call(dbo_path),
            call(sales_path),
        ]

    @patch("fabrictools.data_quality.clean_and_write_data")
    @patch(
        "fabrictools.data_quality._list_lakehouse_table_paths",
        return_value=["Tables/dbo/customers", "Tables/sales/invoices"],
    )
    @patch("fabrictools.data_quality.get_spark")
    def test_clean_and_write_all_tables_orchestrates_for_each_table(
        self, mock_get_spark, mock_list_tables, mock_clean_and_write
    ):
        """clean_and_write_all_tables should call clean_and_write_data for each table path."""
        spark = MagicMock()
        mock_get_spark.return_value = spark

        from fabrictools.data_quality import clean_and_write_all_tables

        result = clean_and_write_all_tables(
            source_lakehouse_name="RawLakehouse",
            target_lakehouse_name="CuratedLakehouse",
            mode="append",
            partition_by=["_year"],
            include_schemas=["dbo", "sales"],
            exclude_tables=["dbo.ignored"],
        )

        mock_list_tables.assert_called_once_with(
            lakehouse_name="RawLakehouse",
            include_schemas=["dbo", "sales"],
            exclude_tables=["dbo.ignored"],
        )
        assert mock_clean_and_write.call_args_list == [
            call(
                source_lakehouse_name="RawLakehouse",
                source_relative_path="Tables/dbo/customers",
                target_lakehouse_name="CuratedLakehouse",
                target_relative_path="Tables/dbo/customers",
                mode="append",
                partition_by=["_year"],
                spark=spark,
            ),
            call(
                source_lakehouse_name="RawLakehouse",
                source_relative_path="Tables/sales/invoices",
                target_lakehouse_name="CuratedLakehouse",
                target_relative_path="Tables/sales/invoices",
                mode="append",
                partition_by=["_year"],
                spark=spark,
            ),
        ]
        assert result["total_tables"] == 2
        assert result["successful_tables"] == 2
        assert result["failed_tables"] == 0
        assert len(result["tables"]) == 2
        assert result["failures"] == []

    @patch("fabrictools.data_quality._list_lakehouse_table_paths", return_value=[])
    @patch("fabrictools.data_quality.get_spark")
    def test_clean_and_write_all_tables_returns_empty_when_no_tables(
        self, mock_get_spark, mock_list_tables
    ):
        """clean_and_write_all_tables should return an empty report when no table is found."""
        mock_get_spark.return_value = MagicMock()

        from fabrictools.data_quality import clean_and_write_all_tables

        result = clean_and_write_all_tables(
            source_lakehouse_name="RawLakehouse",
            target_lakehouse_name="CuratedLakehouse",
        )

        mock_list_tables.assert_called_once()
        assert result == {
            "total_tables": 0,
            "successful_tables": 0,
            "failed_tables": 0,
            "tables": [],
            "failures": [],
        }

    @patch("fabrictools.data_quality.clean_and_write_data")
    @patch(
        "fabrictools.data_quality._list_lakehouse_table_paths",
        return_value=["Tables/dbo/customers", "Tables/dbo/orders"],
    )
    @patch("fabrictools.data_quality.get_spark")
    def test_clean_and_write_all_tables_continue_on_error_collects_failures(
        self, mock_get_spark, mock_list_tables, mock_clean_and_write
    ):
        """When continue_on_error=True, failures should be collected and processing continues."""
        spark = MagicMock()
        mock_get_spark.return_value = spark
        mock_clean_and_write.side_effect = [None, RuntimeError("boom")]

        from fabrictools.data_quality import clean_and_write_all_tables

        result = clean_and_write_all_tables(
            source_lakehouse_name="RawLakehouse",
            target_lakehouse_name="CuratedLakehouse",
            continue_on_error=True,
        )

        assert mock_clean_and_write.call_count == 2
        assert result["total_tables"] == 2
        assert result["successful_tables"] == 1
        assert result["failed_tables"] == 1
        assert result["tables"] == [
            {
                "source_relative_path": "Tables/dbo/customers",
                "target_relative_path": "Tables/dbo/customers",
                "mode": "overwrite",
            }
        ]
        assert result["failures"] == [
            {
                "source_relative_path": "Tables/dbo/orders",
                "target_relative_path": "Tables/dbo/orders",
                "mode": "overwrite",
                "error": "boom",
            }
        ]

    @patch("fabrictools.data_quality._list_lakehouse_table_paths")
    @patch("fabrictools.data_quality.clean_and_write_data")
    @patch("fabrictools.data_quality.get_spark")
    def test_clean_and_write_all_tables_uses_tables_config_for_append_overwrite(
        self, mock_get_spark, mock_clean_and_write, mock_list_tables
    ):
        """tables_config should drive per-table overwrite/append and bypass discovery."""
        spark = MagicMock()
        mock_get_spark.return_value = spark

        from fabrictools.data_quality import clean_and_write_all_tables

        tables_config = [
            {
                "bronze_path": "Tables/dbo/fact_sale",
                "silver_table": "Tables/dbo/fact_sale",
                "partition_by": [],
                "mode": "overwrite",
            },
            {
                "bronze_path": "Tables/dbo/dimension_customer",
                "silver_table": "Tables/dbo/dimension_customer",
                "partition_by": ["_year"],
                "mode": "append",
            },
        ]

        result = clean_and_write_all_tables(
            source_lakehouse_name="RawLakehouse",
            target_lakehouse_name="CuratedLakehouse",
            tables_config=tables_config,
        )

        mock_list_tables.assert_not_called()
        assert mock_clean_and_write.call_args_list == [
            call(
                source_lakehouse_name="RawLakehouse",
                source_relative_path="Tables/dbo/fact_sale",
                target_lakehouse_name="CuratedLakehouse",
                target_relative_path="Tables/dbo/fact_sale",
                mode="overwrite",
                partition_by=[],
                spark=spark,
            ),
            call(
                source_lakehouse_name="RawLakehouse",
                source_relative_path="Tables/dbo/dimension_customer",
                target_lakehouse_name="CuratedLakehouse",
                target_relative_path="Tables/dbo/dimension_customer",
                mode="append",
                partition_by=["_year"],
                spark=spark,
            ),
        ]
        assert result["total_tables"] == 2
        assert result["successful_tables"] == 2
        assert result["failed_tables"] == 0
        assert result["tables"] == [
            {
                "source_relative_path": "Tables/dbo/fact_sale",
                "target_relative_path": "Tables/dbo/fact_sale",
                "mode": "overwrite",
            },
            {
                "source_relative_path": "Tables/dbo/dimension_customer",
                "target_relative_path": "Tables/dbo/dimension_customer",
                "mode": "append",
            },
        ]

    @patch("fabrictools.data_quality.merge_lakehouse")
    @patch("fabrictools.data_quality.add_silver_metadata")
    @patch("fabrictools.data_quality.clean_data")
    @patch("fabrictools.data_quality.read_lakehouse")
    @patch("fabrictools.data_quality.get_spark")
    def test_clean_and_write_all_tables_tables_config_merge_mode(
        self,
        mock_get_spark,
        mock_read_lakehouse,
        mock_clean_data,
        mock_add_silver_metadata,
        mock_merge_lakehouse,
    ):
        """tables_config mode=merge should route through merge_lakehouse."""
        spark = MagicMock()
        mock_get_spark.return_value = spark
        source_df = MagicMock()
        cleaned_df = MagicMock()
        silver_df = MagicMock()
        mock_read_lakehouse.return_value = source_df
        mock_clean_data.return_value = cleaned_df
        mock_add_silver_metadata.return_value = silver_df

        from fabrictools.data_quality import clean_and_write_all_tables

        result = clean_and_write_all_tables(
            source_lakehouse_name="RawLakehouse",
            target_lakehouse_name="CuratedLakehouse",
            tables_config=[
                {
                    "bronze_path": "Tables/dbo/fact_sale",
                    "silver_table": "Tables/dbo/fact_sale",
                    "partition_by": [],
                    "mode": "merge",
                    "merge_condition": "src.sale_id = tgt.sale_id",
                }
            ],
        )

        mock_read_lakehouse.assert_called_once_with(
            "RawLakehouse",
            "Tables/dbo/fact_sale",
            spark=spark,
        )
        mock_clean_data.assert_called_once_with(source_df)
        mock_add_silver_metadata.assert_called_once_with(
            cleaned_df,
            source_lakehouse_name="RawLakehouse",
            source_relative_path="Tables/dbo/fact_sale",
            spark=spark,
        )
        mock_merge_lakehouse.assert_called_once_with(
            source_df=silver_df,
            lakehouse_name="CuratedLakehouse",
            relative_path="Tables/dbo/fact_sale",
            merge_condition="src.sale_id = tgt.sale_id",
            spark=spark,
        )
        assert result["tables"] == [
            {
                "source_relative_path": "Tables/dbo/fact_sale",
                "target_relative_path": "Tables/dbo/fact_sale",
                "mode": "merge",
            }
        ]

    @patch("fabrictools.data_quality.get_spark")
    def test_clean_and_write_all_tables_merge_requires_merge_condition(self, mock_get_spark):
        """tables_config mode=merge should fail without merge_condition."""
        mock_get_spark.return_value = MagicMock()

        from fabrictools.data_quality import clean_and_write_all_tables

        with pytest.raises(ValueError, match="requires 'merge_condition'"):
            clean_and_write_all_tables(
                source_lakehouse_name="RawLakehouse",
                target_lakehouse_name="CuratedLakehouse",
                tables_config=[
                    {
                        "bronze_path": "Tables/dbo/fact_sale",
                        "silver_table": "Tables/dbo/fact_sale",
                        "partition_by": [],
                        "mode": "merge",
                    }
                ],
            )
