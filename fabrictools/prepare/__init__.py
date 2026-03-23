"""Prepared-layer package API."""

from __future__ import annotations

from fabrictools.prepare._legacy import get_legacy_module
from fabrictools.prepare.aggregations import generate_prepared_aggregations
from fabrictools.prepare.pipeline import prepare_and_write_all_tables, prepare_and_write_data
from fabrictools.prepare.resolve import (
    _ensure_prefix_rules,
    _layer1_resolve,
    _layer2_profile_resolve,
    _layer3_mapping_resolve,
    _safe_read_table,
    _write_unresolved_audit,
    resolve_columns,
)
from fabrictools.prepare.schema import snapshot_source_schema
from fabrictools.prepare.semantic import publish_semantic_model
from fabrictools.prepare.transform import _localize_alias_tokens, transform_to_prepared, write_prepared_table

_legacy = get_legacy_module()

# Compatibility exports for callers/tests patching module-level symbols.
_list_lakehouse_table_paths = _legacy._list_lakehouse_table_paths
build_lakehouse_write_path = _legacy.build_lakehouse_write_path
get_lakehouse_abfs_path = _legacy.get_lakehouse_abfs_path
get_spark = _legacy.get_spark
read_lakehouse = _legacy.read_lakehouse
write_lakehouse = _legacy.write_lakehouse

__all__ = [
    "snapshot_source_schema",
    "resolve_columns",
    "transform_to_prepared",
    "write_prepared_table",
    "generate_prepared_aggregations",
    "publish_semantic_model",
    "prepare_and_write_data",
    "prepare_and_write_all_tables",
    "_safe_read_table",
    "_ensure_prefix_rules",
    "_layer1_resolve",
    "_layer2_profile_resolve",
    "_layer3_mapping_resolve",
    "_write_unresolved_audit",
    "_localize_alias_tokens",
    "_list_lakehouse_table_paths",
    "build_lakehouse_write_path",
    "get_lakehouse_abfs_path",
    "get_spark",
    "read_lakehouse",
    "write_lakehouse",
]

