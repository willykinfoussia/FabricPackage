"""
Unit tests for fabrictools.prepare.

All Spark/Fabric interactions are mocked to keep tests offline.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest
from pyspark.sql.types import DateType, IntegerType, StringType


class TestSnapshotSourceSchema:
    @patch("fabrictools.prepare.write_lakehouse")
    @patch("fabrictools.prepare.read_lakehouse")
    @patch("fabrictools.prepare.get_spark")
    def test_snapshot_writes_source_schema_snapshot(
        self,
        mock_get_spark,
        mock_read_lakehouse,
        mock_write_lakehouse,
    ):
        spark = MagicMock()
        mock_get_spark.return_value = spark

        source_df = MagicMock()
        source_df.columns = ["id_order", "dt_order"]
        source_df.count.return_value = 10
        source_df.schema.fields = [
            SimpleNamespace(name="id_order", dataType=IntegerType()),
            SimpleNamespace(name="dt_order", dataType=StringType()),
        ]
        stats_row = MagicMock()
        stats_row.asDict.return_value = {
            "id_order__min": 1,
            "id_order__max": 10,
            "id_order__nulls": 0,
            "id_order__avg_len": 2.0,
            "id_order__distinct": 10,
            "dt_order__min": "2026-01-01",
            "dt_order__max": "2026-01-10",
            "dt_order__nulls": 0,
            "dt_order__avg_len": 10.0,
            "dt_order__distinct": 10,
        }
        source_df.agg.return_value.collect.return_value = [stats_row]
        mock_read_lakehouse.return_value = source_df

        snapshot_df = MagicMock()
        spark.createDataFrame.return_value = snapshot_df

        from fabrictools.prepare import snapshot_source_schema

        schema_hash = snapshot_source_schema(
            source_lakehouse_name="SourceLakehouse",
            source_relative_path="Tables/dbo/orders",
        )

        assert schema_hash
        mock_write_lakehouse.assert_called_once_with(
            snapshot_df,
            lakehouse_name="SourceLakehouse",
            relative_path="Tables/config/source_schema_snapshot",
            mode="overwrite",
            spark=spark,
        )


class TestResolveColumns:
    @patch("fabrictools.prepare._write_unresolved_audit")
    @patch("fabrictools.prepare._safe_read_table")
    @patch("fabrictools.prepare._ensure_prefix_rules")
    @patch("fabrictools.prepare.get_spark")
    def test_resolve_columns_uses_prefix_rules_first(
        self,
        mock_get_spark,
        mock_ensure_prefix_rules,
        mock_safe_read_table,
        mock_write_unresolved_audit,
    ):
        spark = MagicMock()
        mock_get_spark.return_value = spark

        df = MagicMock()
        df.columns = ["dt_order", "amount"]
        df.schema.fields = [
            SimpleNamespace(name="dt_order", dataType=StringType()),
            SimpleNamespace(name="amount", dataType=IntegerType()),
        ]
        df.select.return_value.where.return_value.limit.return_value.collect.return_value = []

        rules_df = MagicMock()
        rules_df.collect.return_value = [{"pattern": r"^(dt_|date_)", "semantic_type": "DATE"}]
        mock_ensure_prefix_rules.return_value = rules_df
        mock_safe_read_table.return_value = None

        from fabrictools.prepare import resolve_columns

        resolved = resolve_columns(
            df=df,
            source_lakehouse_name="SourceLakehouse",
            source_relative_path="Tables/dbo/orders",
        )

        assert any(item["col_source"] == "dt_order" for item in resolved)
        mock_write_unresolved_audit.assert_called_once()

    @patch("fabrictools.prepare.write_lakehouse")
    @patch("fabrictools.prepare._write_unresolved_audit")
    @patch("fabrictools.prepare._safe_read_table")
    @patch("fabrictools.prepare._ensure_prefix_rules")
    @patch("fabrictools.prepare.get_spark")
    def test_resolve_columns_writes_layer2_cache(
        self,
        mock_get_spark,
        mock_ensure_prefix_rules,
        mock_safe_read_table,
        mock_write_unresolved_audit,
        mock_write_lakehouse,
    ):
        spark = MagicMock()
        mock_get_spark.return_value = spark

        df = MagicMock()
        df.columns = ["amount"]
        df.schema.fields = [SimpleNamespace(name="amount", dataType=StringType())]
        df.select.return_value.where.return_value.limit.return_value.collect.return_value = [
            ("10.00",),
            ("13.40",),
            ("20.00",),
        ]

        rules_df = MagicMock()
        rules_df.collect.return_value = []
        mock_ensure_prefix_rules.return_value = rules_df
        mock_safe_read_table.return_value = None

        cache_df = MagicMock()
        spark.createDataFrame.return_value = cache_df

        from fabrictools.prepare import resolve_columns

        resolved = resolve_columns(
            df=df,
            source_lakehouse_name="SourceLakehouse",
            profiling_confidence_threshold=0.80,
            source_relative_path="Tables/dbo/orders",
        )

        assert resolved[0]["source_resolution"] == "PROFILING"
        mock_write_lakehouse.assert_called_once_with(
            cache_df,
            lakehouse_name="SourceLakehouse",
            relative_path="Tables/config/profiling_cache",
            mode="overwrite",
            spark=spark,
        )
        mock_write_unresolved_audit.assert_called_once()


class TestLayer2ProfileResolveTypeAware:
    def test_date_type_is_auto_classified_as_date(self):
        df = MagicMock()

        from fabrictools.prepare import _layer2_profile_resolve

        resolved = _layer2_profile_resolve(
            df=df,
            col_source="created_on",
            col_data_type=DateType(),
            sample_size=50,
            threshold=0.80,
        )

        assert resolved is not None
        assert resolved["semantic_type"] == "DATE"
        assert resolved["source_resolution"] == "PROFILING"
        df.select.assert_not_called()

    def test_string_tech_id_with_majority_digits_stays_tech_id(self):
        df = MagicMock()
        df.select.return_value.where.return_value.limit.return_value.collect.return_value = [
            ("ABCD000001234",),
            ("WXYZ000009876",),
            ("PQRS000004321",),
            ("LONGTEXTVALUE",),
        ]

        from fabrictools.prepare import _layer2_profile_resolve

        resolved = _layer2_profile_resolve(
            df=df,
            col_source="external_key",
            col_data_type=StringType(),
            sample_size=50,
            threshold=0.80,
        )

        assert resolved is not None
        assert resolved["semantic_type"] == "TECH_ID"

    def test_string_tech_id_without_majority_digits_becomes_text(self):
        df = MagicMock()
        df.select.return_value.where.return_value.limit.return_value.collect.return_value = [
            ("LONGSTRINGVALUEA",),
            ("LONGSTRINGVALUEB",),
            ("LONGSTRINGVALUEC",),
            ("NODIGITSVALUEDD",),
        ]

        from fabrictools.prepare import _layer2_profile_resolve

        resolved = _layer2_profile_resolve(
            df=df,
            col_source="reference_name",
            col_data_type=StringType(),
            sample_size=50,
            threshold=0.80,
        )

        assert resolved is not None
        assert resolved["semantic_type"] == "TEXT"


class TestTransformToPrepared:
    @patch("fabrictools.prepare._safe_read_table")
    @patch("fabrictools.prepare.get_spark")
    def test_transform_to_prepared_runs_single_select(
        self,
        mock_get_spark,
        mock_safe_read_table,
    ):
        spark = MagicMock()
        mock_get_spark.return_value = spark
        mock_safe_read_table.return_value = None

        df = MagicMock()
        transformed_df = MagicMock()
        df.select.return_value = transformed_df

        resolved_mappings = [
            {
                "col_source": "dt_order",
                "col_prepared": "date_order",
                "semantic_type": "DATE",
                "source_resolution": "PREFIX_RULE",
                "confidence": 1.0,
            },
            {
                "col_source": "mt_total",
                "col_prepared": "amount_total",
                "semantic_type": "AMOUNT",
                "source_resolution": "PREFIX_RULE",
                "confidence": 1.0,
            },
        ]

        from fabrictools.prepare import transform_to_prepared

        result = transform_to_prepared(
            df=df,
            resolved_mappings=resolved_mappings,
            source_lakehouse_name="SourceLakehouse",
        )

        assert result is transformed_df
        df.select.assert_called_once()
        assert len(df.select.call_args.args) == 6


class TestWritePreparedTable:
    @patch("fabrictools.prepare.build_lakehouse_write_path", return_value="Tables/dbo/orders_prepared")
    @patch("fabrictools.prepare.get_lakehouse_abfs_path", return_value="abfss://container@account.dfs.core.windows.net")
    @patch("fabrictools.prepare.write_lakehouse")
    @patch("fabrictools.prepare.get_spark")
    def test_write_prepared_table_writes_with_selected_partitions(
        self,
        mock_get_spark,
        mock_write_lakehouse,
        mock_abfs,
        mock_write_path,
    ):
        spark = MagicMock()
        mock_get_spark.return_value = spark
        spark.sql.return_value.first.return_value.asDict.return_value = {
            "numFiles": 1,
            "sizeInBytes": 1024 * 1024 * 64,
        }

        df = MagicMock()
        df.columns = ["date_order", "code_region", "amount_total"]
        df.select.return_value.distinct.return_value.count.side_effect = [10, 12, 10]

        mappings = [
            {
                "col_source": "dt_order",
                "col_prepared": "date_order",
                "semantic_type": "DATE",
                "source_resolution": "PREFIX_RULE",
                "confidence": 1.0,
            },
            {
                "col_source": "cd_region",
                "col_prepared": "code_region",
                "semantic_type": "CATEGORY",
                "source_resolution": "PREFIX_RULE",
                "confidence": 1.0,
            },
        ]

        from fabrictools.prepare import write_prepared_table

        write_prepared_table(
            df=df,
            resolved_mappings=mappings,
            target_lakehouse_name="TargetLakehouse",
            target_relative_path="Tables/dbo/orders_prepared",
        )

        mock_write_lakehouse.assert_called_once_with(
            df,
            lakehouse_name="TargetLakehouse",
            relative_path="Tables/dbo/orders_prepared",
            mode="overwrite",
            partition_by=["date_order", "code_region"],
            spark=spark,
        )


class TestOrchestrators:
    @patch("fabrictools.prepare.prepare_and_write_data")
    @patch("fabrictools.prepare._list_lakehouse_table_paths", return_value=["Tables/dbo/orders", "Tables/dbo/customers"])
    @patch("fabrictools.prepare.get_spark")
    def test_prepare_and_write_all_tables_discovery(
        self,
        mock_get_spark,
        mock_list_paths,
        mock_prepare_and_write_data,
    ):
        spark = MagicMock()
        mock_get_spark.return_value = spark

        from fabrictools.prepare import prepare_and_write_all_tables

        result = prepare_and_write_all_tables(
            source_lakehouse_name="SourceLakehouse",
            target_lakehouse_name="TargetLakehouse",
        )

        mock_list_paths.assert_called_once_with(
            lakehouse_name="SourceLakehouse",
            include_schemas=None,
            exclude_tables=None,
        )
        assert mock_prepare_and_write_data.call_args_list == [
            call(
                source_lakehouse_name="SourceLakehouse",
                source_relative_path="Tables/dbo/orders",
                target_lakehouse_name="TargetLakehouse",
                target_relative_path="Tables/dbo/orders",
                mode="overwrite",
                sample_size=500,
                profiling_confidence_threshold=0.80,
                max_partitions_guard=500,
                vacuum_retention_hours=168,
                enable_power_bi_publish=False,
                power_bi_workspace_id=None,
                power_bi_token=None,
                spark=spark,
            ),
            call(
                source_lakehouse_name="SourceLakehouse",
                source_relative_path="Tables/dbo/customers",
                target_lakehouse_name="TargetLakehouse",
                target_relative_path="Tables/dbo/customers",
                mode="overwrite",
                sample_size=500,
                profiling_confidence_threshold=0.80,
                max_partitions_guard=500,
                vacuum_retention_hours=168,
                enable_power_bi_publish=False,
                power_bi_workspace_id=None,
                power_bi_token=None,
                spark=spark,
            ),
        ]
        assert result["total_tables"] == 2
        assert result["successful_tables"] == 2
        assert result["failed_tables"] == 0

    @patch("fabrictools.prepare.prepare_and_write_data")
    @patch("fabrictools.prepare._list_lakehouse_table_paths", return_value=["Tables/dbo/orders", "Tables/dbo/customers"])
    @patch("fabrictools.prepare.get_spark")
    def test_prepare_and_write_all_tables_continue_on_error(
        self,
        mock_get_spark,
        mock_list_paths,
        mock_prepare_and_write_data,
    ):
        spark = MagicMock()
        mock_get_spark.return_value = spark
        mock_prepare_and_write_data.side_effect = [None, RuntimeError("boom")]

        from fabrictools.prepare import prepare_and_write_all_tables

        result = prepare_and_write_all_tables(
            source_lakehouse_name="SourceLakehouse",
            target_lakehouse_name="TargetLakehouse",
            continue_on_error=True,
        )

        assert result["total_tables"] == 2
        assert result["successful_tables"] == 1
        assert result["failed_tables"] == 1
        assert result["failures"][0]["error"] == "boom"

    @patch("fabrictools.prepare.publish_semantic_model")
    @patch("fabrictools.prepare.generate_prepared_aggregations")
    @patch("fabrictools.prepare.write_prepared_table")
    @patch("fabrictools.prepare.transform_to_prepared")
    @patch("fabrictools.prepare.resolve_columns")
    @patch("fabrictools.prepare.snapshot_source_schema")
    @patch("fabrictools.prepare.read_lakehouse")
    @patch("fabrictools.prepare.get_spark")
    def test_prepare_and_write_data_orchestration(
        self,
        mock_get_spark,
        mock_read_lakehouse,
        mock_snapshot,
        mock_resolve,
        mock_transform,
        mock_write_prepared,
        mock_generate_aggs,
        mock_publish,
    ):
        spark = MagicMock()
        mock_get_spark.return_value = spark
        source_df = MagicMock()
        prepared_df = MagicMock()
        mappings = [
            {
                "col_source": "dt_order",
                "col_prepared": "date_order",
                "semantic_type": "DATE",
                "source_resolution": "PREFIX_RULE",
                "confidence": 1.0,
            }
        ]
        mock_read_lakehouse.return_value = source_df
        mock_snapshot.return_value = "abc123"
        mock_resolve.return_value = mappings
        mock_transform.return_value = prepared_df
        mock_generate_aggs.return_value = {"prepared_agg_jour": "Tables/dbo/prepared_agg_jour"}

        from fabrictools.prepare import prepare_and_write_data

        result = prepare_and_write_data(
            source_lakehouse_name="SourceLakehouse",
            source_relative_path="Tables/dbo/orders",
            target_lakehouse_name="TargetLakehouse",
            target_relative_path="Tables/dbo/orders_prepared",
            enable_power_bi_publish=True,
            power_bi_workspace_id="workspace-id",
            power_bi_token="token",
        )

        assert result is prepared_df
        mock_read_lakehouse.assert_called_once_with(
            "SourceLakehouse",
            "Tables/dbo/orders",
            spark=spark,
        )
        mock_snapshot.assert_called_once()
        mock_resolve.assert_called_once()
        mock_transform.assert_called_once()
        mock_write_prepared.assert_called_once()
        mock_generate_aggs.assert_called_once()
        mock_publish.assert_called_once()

    @patch("fabrictools.prepare.request.urlopen")
    @patch("fabrictools.prepare.get_spark")
    def test_publish_semantic_model_returns_skipped_without_credentials(
        self,
        mock_get_spark,
        mock_urlopen,
    ):
        mock_get_spark.return_value = MagicMock()

        from fabrictools.prepare import publish_semantic_model

        result = publish_semantic_model(
            target_lakehouse_name="TargetLakehouse",
            agg_tables={"prepared_agg_jour": "Tables/dbo/prepared_agg_jour"},
            resolved_mappings=[],
            power_bi_workspace_id="",
            power_bi_token="",
        )

        assert result["status"] == "skipped"
        mock_urlopen.assert_not_called()

