"""Prepared transform and write helpers."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from typing import Optional, List
import re
import unicodedata

from fabrictools.core import log
from fabrictools.core import get_spark
from fabrictools.io import read_lakehouse, write_lakehouse, build_lakehouse_write_path, get_lakehouse_abfs_path
from fabrictools.prepare.resolve import _safe_read_table, ResolvedColumn


CONFIG_CODE_LABELS_PATH = "Tables/dbo/code_labels"

ALIAS_TOKEN_REPLACEMENTS = {
    "YEAR": "ANNEE",
    "Year": "Annee",
    "year": "annee",
    "MONTH": "MOIS",
    "Month": "Mois",
    "month": "mois",
    "DAY": "JOUR",
    "Day": "Jour",
    "day": "jour",
    "WEEK": "SEMAINE",
    "Week": "Semaine",
    "week": "semaine",
}


def _normalize_token(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value.strip().lower())
    return "".join(char for char in normalized if not unicodedata.combining(char))

def _semantic_cast_expr(col_name: str, semantic_type: str) -> F.Column:
    stype = semantic_type.upper()
    expression = F.col(col_name)
    if stype == "DATE":
        return F.to_date(expression)
    if stype in {"AMOUNT", "RATE"}:
        return expression.cast("decimal(18,2)")
    if stype in {"QUANTITY", "YEAR", "MONTH", "DAY"}:
        return expression.cast("int")
    return expression

def _localize_alias_tokens(alias: str) -> str:
    """
    Localize year/month/day/weeknumber/monthnumber tokens to French labels.
    """
    token_pattern = re.compile(r"(?<![A-Za-z0-9])(YEAR|Year|year|MONTH|Month|month|DAY|Day|day)(?![A-Za-z0-9])")
    camel_suffix_pattern = re.compile(r"(YEAR|Year|MONTH|Month|DAY|Day)(?=[A-Z]|$)")
    weeknumber_pattern = re.compile(r"(?<![A-Za-z0-9])week[\s_-]*number(?![A-Za-z0-9])", flags=re.IGNORECASE)
    weeknumber_camel_pattern = re.compile(r"weeknumber(?=[A-Z]|$)", flags=re.IGNORECASE)
    monthnumber_pattern = re.compile(r"(?<![A-Za-z0-9])month[\s_-]*number(?![A-Za-z0-9])", flags=re.IGNORECASE)
    monthnumber_camel_pattern = re.compile(r"monthnumber(?=[A-Z]|$)", flags=re.IGNORECASE)

    localized = weeknumber_pattern.sub("Semaine", alias)
    localized = weeknumber_camel_pattern.sub("Semaine", localized)
    localized = monthnumber_pattern.sub("Numero du Mois", localized)
    localized = monthnumber_camel_pattern.sub("Numero du Mois", localized)
    localized = token_pattern.sub(lambda match: ALIAS_TOKEN_REPLACEMENTS[match.group(0)], localized)
    localized = camel_suffix_pattern.sub(lambda match: ALIAS_TOKEN_REPLACEMENTS[match.group(0)], localized)
    return localized

def transform_to_prepared(
    source_lakehouse_name: str,
    source_relative_path: str,
    resolved_mappings: List[ResolvedColumn],
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """
    Apply semantic casts, code labels, and date derivations in one select pass.
    """
    _spark = spark or get_spark()
    df = read_lakehouse(source_lakehouse_name, source_relative_path, spark=_spark)
    code_labels_df = _safe_read_table(source_lakehouse_name, CONFIG_CODE_LABELS_PATH, spark=_spark)

    code_label_maps: dict[str, dict[str, str]] = {}
    if code_labels_df is not None:
        required = {"col_prepared", "code_value", "code_label"}
        if required.issubset(set(code_labels_df.columns)):
            for row in code_labels_df.collect():
                row_dict = row.asDict()
                prepared_col = str(row_dict.get("col_prepared"))
                code_label_maps.setdefault(prepared_col, {})[str(row_dict.get("code_value"))] = str(
                    row_dict.get("code_label")
                )

    select_exprs: list[F.Column] = []
    used_aliases: set[str] = set()

    def _alias_key(alias: str) -> str:
        return alias.casefold()

    def _build_unique_derived_alias(alias: str) -> str:
        base_alias = f"{alias}_derived"
        candidate = base_alias
        suffix_index = 2
        while _alias_key(candidate) in used_aliases:
            candidate = f"{base_alias}_{suffix_index}"
            suffix_index += 1
        return candidate

    def _append_alias(expr: F.Column, alias: str, is_derived: bool) -> None:
        alias_key = _alias_key(alias)
        if alias_key in used_aliases:
            if is_derived:
                resolved_alias = _build_unique_derived_alias(alias)
                log(
                    f"Renamed derived column '{alias}' to '{resolved_alias}' "
                    "to avoid Delta duplicate column metadata."
                )
                alias = resolved_alias
                alias_key = _alias_key(alias)
            else:
                log(
                    f"Skipped duplicate base column alias '{alias}' "
                    "to avoid Delta duplicate column metadata.",
                    level="warning",
                )
                return
        used_aliases.add(alias_key)
        select_exprs.append(expr.alias(alias))

    for mapping in resolved_mappings:
        src = mapping["col_source"]
        prepared = mapping["col_prepared"]
        semantic_type = mapping["semantic_type"].upper()
        base_expr = _semantic_cast_expr(src, semantic_type)

        if semantic_type == "CATEGORY" and prepared in code_label_maps:
            labels = code_label_maps[prepared]
            map_items: list[F.Column] = []
            for code_value, code_label in labels.items():
                map_items.append(F.lit(code_value))
                map_items.append(F.lit(code_label))
            label_map_expr = F.create_map(*map_items) if map_items else None
            if label_map_expr is not None:
                base_expr = F.coalesce(
                    label_map_expr[F.col(src).cast("string")],
                    F.col(src).cast("string"),
                )

        _append_alias(base_expr, prepared, is_derived=False)

        if semantic_type == "DATE":
            _append_alias(
                F.year(F.to_date(F.col(src))),
                f"{prepared}_year",
                is_derived=True,
            )
            _append_alias(
                F.month(F.to_date(F.col(src))),
                f"{prepared}_month_number",
                is_derived=True,
            )
            _append_alias(
                F.weekofyear(F.to_date(F.col(src))),
                f"{prepared}_week_number",
                is_derived=True,
            )
            _append_alias(
                F.date_format(F.to_date(F.col(src)), "MMMM"),
                f"{prepared}_month_label",
                is_derived=True,
            )

    return df.select(*select_exprs)

def write_prepared_table(
    df: DataFrame,
    resolved_mappings: List[ResolvedColumn],
    target_lakehouse_name: str,
    target_relative_path: str,
    mode: str = "overwrite",
    max_partitions_guard: int = 500,
    vacuum_retention_hours: int = 168,
    spark: Optional[SparkSession] = None,
) -> None:
    """
    Write prepared table and run conditional Delta maintenance operations.
    """
    _spark = spark or get_spark()
    date_partitions = [
        mapping["col_prepared"]
        for mapping in resolved_mappings
        if mapping["semantic_type"].upper() == "DATE" and mapping["col_prepared"] in df.columns
    ]
    code_partitions = [
        mapping["col_prepared"]
        for mapping in resolved_mappings
        if mapping["semantic_type"].upper() == "CATEGORY" and mapping["col_prepared"] in df.columns
    ]

    excluded_partition_tokens = {"source layer", "source path"}
    selected_partitions: list[str] = []
    selected_partitions.extend(
        partition_col
        for partition_col in date_partitions[:1]
        if _normalize_token(partition_col).replace("_", " ") not in excluded_partition_tokens
    )
    for candidate in code_partitions:
        if candidate in selected_partitions:
            continue
        if _normalize_token(candidate).replace("_", " ") in excluded_partition_tokens:
            continue
        distinct_count = df.select(candidate).distinct().count()
        if distinct_count < 50:
            selected_partitions.append(candidate)

    estimated_partitions = 1
    for partition_col in selected_partitions:
        estimated_partitions *= max(df.select(partition_col).distinct().count(), 1)
    if estimated_partitions > max_partitions_guard and selected_partitions:
        selected_partitions = selected_partitions[:1]

    write_lakehouse(
        df,
        lakehouse_name=target_lakehouse_name,
        relative_path=target_relative_path,
        mode=mode,
        partition_by=selected_partitions or None,
        spark=_spark,
    )

    # Maintenance is best-effort and intentionally non-blocking.
    try:
        base = get_lakehouse_abfs_path(target_lakehouse_name)
        resolved_path = build_lakehouse_write_path(target_relative_path)
        full_path = f"{base}/{resolved_path}"
        detail = _spark.sql(f"DESCRIBE DETAIL delta.`{full_path}`").first().asDict()
        num_files = int(detail.get("numFiles") or 0)
        size_in_bytes = int(detail.get("sizeInBytes") or 0)
        avg_file_size = size_in_bytes / num_files if num_files else 0
        if num_files > 100 or avg_file_size < 16 * 1024 * 1024:
            relation_cols = [
                mapping["col_prepared"]
                for mapping in resolved_mappings
                if mapping["col_prepared"] in df.columns
                and (
                    mapping["semantic_type"].upper() in {"RELATION_ID", "TECH_ID"}
                    or mapping["col_prepared"].endswith("_id")
                )
            ]
            zorder_cols = date_partitions[:1] + relation_cols[:2]
            if zorder_cols:
                _spark.sql(
                    f"OPTIMIZE delta.`{full_path}` ZORDER BY ({', '.join(zorder_cols)})"
                )
            else:
                _spark.sql(f"OPTIMIZE delta.`{full_path}`")
            _spark.sql(f"VACUUM delta.`{full_path}` RETAIN {int(vacuum_retention_hours)} HOURS")
    except Exception as exc:
        log(f"Maintenance step skipped: {exc}", level="warning")


__all__ = [
    "transform_to_prepared",
    "write_prepared_table",
    "_localize_alias_tokens",
]