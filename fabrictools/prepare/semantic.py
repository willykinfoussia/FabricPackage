"""Prepared semantic model publishing helpers."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, TimestampType, BooleanType, NumericType
from typing import Any, Optional, List
import re

from fabrictools.core import log
from fabrictools.core import get_spark
from fabrictools.io import read_lakehouse
from fabrictools.prepare.resolve import ResolvedColumn


def _to_business_sentence_name(value: str) -> str:
    """Convert snake_case/raw identifier to business sentence case label."""
    normalized = re.sub(r"\s+", " ", value.replace("_", " ").strip().lower())
    if not normalized:
        return ""
    return normalized[0].upper() + normalized[1:]

def _to_semantic_data_type(data_type: Any) -> str:
    """Map Spark data types to semantic model TOM data types."""
    if isinstance(data_type, (DateType, TimestampType)):
        return "DateTime"
    if isinstance(data_type, BooleanType):
        return "Boolean"
    if isinstance(data_type, NumericType):
        return "Double"
    return "String"

def publish_semantic_model(
    target_lakehouse_name: str,
    agg_tables: dict[str, str],
    resolved_mappings: List[ResolvedColumn],
    semantic_workspace: Optional[str],
    semantic_model_name: str = "fabrictools_prepared_dataset",
    overwrite_model: bool = True,
    spark: Optional[SparkSession] = None,
) -> dict[str, Any]:
    """
    Publish or replace a semantic model through Semantic Link (TOM).

    This helper is intentionally best-effort and returns a status dictionary.
    """
    _spark = spark or get_spark()

    if not semantic_workspace:
        return {
            "status": "skipped",
            "reason": "semantic_workspace_missing",
            "semantic_model_name": semantic_model_name,
            "tables_count": len(agg_tables),
        }
    if not semantic_model_name:
        return {
            "status": "skipped",
            "reason": "semantic_model_name_missing",
            "tables_count": len(agg_tables),
        }
    if not agg_tables:
        return {
            "status": "skipped",
            "reason": "no_aggregation_tables",
            "semantic_model_name": semantic_model_name,
            "tables_count": 0,
        }

    try:
        import sempy.fabric as sempy_fabric  # type: ignore[import-not-found]
        import sempy_labs  # type: ignore[import-not-found]
    except Exception as exc:
        log(f"Semantic Link runtime unavailable: {exc}", level="warning")
        return {
            "status": "failed",
            "error": f"semantic_link_runtime_unavailable: {exc}",
            "semantic_model_name": semantic_model_name,
            "tables_count": len(agg_tables),
        }

    semantic_tables: list[dict[str, Any]] = []
    for technical_table_name, table_path in agg_tables.items():
        table_name = _to_business_sentence_name(technical_table_name)
        table_columns: list[dict[str, Any]] = []
        try:
            table_df = read_lakehouse(target_lakehouse_name, table_path, spark=_spark)
            table_columns = [
                {
                    "name": _to_business_sentence_name(field.name),
                    "source_column": field.name,
                    "dataType": _to_semantic_data_type(field.dataType),
                }
                for field in table_df.schema.fields
            ]
        except Exception as exc:
            log(
                f"Could not infer columns for semantic table '{technical_table_name}': {exc}",
                level="warning",
            )
        semantic_tables.append({"name": table_name, "columns": table_columns})

    semantic_table_names = [table["name"] for table in semantic_tables]
    relation_from_table = semantic_table_names[0] if semantic_table_names else _to_business_sentence_name("fact")
    relation_to_table = (
        semantic_table_names[1]
        if len(semantic_table_names) > 1
        else relation_from_table
    )
    raw_relations = [
        {
            "fromTable": relation_from_table,
            "toTable": relation_to_table,
            "fromColumn": _to_business_sentence_name(mapping["col_prepared"]),
            "toColumn": _to_business_sentence_name(mapping["col_prepared"]),
        }
        for mapping in resolved_mappings
        if mapping["col_prepared"].startswith("relation_id_")
        or mapping["col_prepared"].endswith("_id")
    ]
    dedup_relations: list[dict[str, str]] = []
    relation_keys: set[tuple[str, str, str, str]] = set()
    for relation in raw_relations:
        relation_key = (
            relation["fromTable"],
            relation["fromColumn"],
            relation["toTable"],
            relation["toColumn"],
        )
        if relation_key in relation_keys:
            continue
        relation_keys.add(relation_key)
        dedup_relations.append(relation)

    try:
        sempy_labs.create_blank_semantic_model(
            dataset=semantic_model_name,
            workspace=semantic_workspace,
            overwrite=overwrite_model,
        )
        with sempy_fabric.connect_semantic_model(
            dataset=semantic_model_name,
            workspace=semantic_workspace,
            readonly=False,
        ) as tom:
            for table in semantic_tables:
                table_name = table["name"]
                tom.add_table(name=table_name)
                for column in table["columns"]:
                    tom.add_data_column(
                        table_name=table_name,
                        column_name=column["name"],
                        source_column=column["source_column"],
                        data_type=column["dataType"],
                    )
            for relation in dedup_relations:
                tom.add_relationship(
                    from_table=relation["fromTable"],
                    from_column=relation["fromColumn"],
                    to_table=relation["toTable"],
                    to_column=relation["toColumn"],
                    from_cardinality="Many",
                    to_cardinality="One",
                )
        return {
            "status": "published",
            "semantic_model_name": semantic_model_name,
            "workspace": semantic_workspace,
            "tables_count": len(agg_tables),
            "relationships_count": len(dedup_relations),
        }
    except Exception as exc:
        log(f"Semantic model publish failed: {exc}", level="warning")
        return {
            "status": "failed",
            "error": str(exc),
            "semantic_model_name": semantic_model_name,
            "tables_count": len(agg_tables),
        }


__all__ = ["publish_semantic_model"]
