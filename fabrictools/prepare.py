"""
Source-to-prepared pipeline helpers for Microsoft Fabric Lakehouses.

This module is designed so each phase can be used independently:
- snapshot_source_schema
- resolve_columns
- transform_to_prepared
- write_prepared_table
- generate_prepared_aggregations
- publish_semantic_model

Two orchestration helpers are also provided:
- prepare_and_write_data
- prepare_and_write_all_tables
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from datetime import date, datetime
from typing import Any, Dict, List, Optional, TypedDict
from urllib import request

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import BooleanType, DateType, NumericType, StringType, TimestampType

from fabrictools._logger import log
from fabrictools._paths import build_lakehouse_write_path, get_lakehouse_abfs_path
from fabrictools._spark import get_spark
from fabrictools.data_quality import _list_lakehouse_table_paths
from fabrictools.lakehouse import read_lakehouse, resolve_lakehouse_read_candidate, write_lakehouse

CONFIG_SOURCE_SNAPSHOT_PATH = "schema_snapshot"
CONFIG_PREFIX_RULES_PATH = "Tables/dbo/prefix_rules"
CONFIG_PROFILING_CACHE_PATH = "Tables/dbo/profiling_cache"
CONFIG_RESOLVED_COLUMNS_PATH = "resolved_columns"
CONFIG_MAPPING_RULES_PATH = "Tables/dbo/mapping_rules"
CONFIG_CODE_LABELS_PATH = "Tables/dbo/code_labels"
CONFIG_AUDIT_LOG_PATH = "Tables/dbo/pipeline_audit_log"
CONFIG_DAX_TEMPLATE_PATH = "Tables/dbo/dax_template"

DEFAULT_PREFIX_RULES: list[dict[str, str]] = [
    {"pattern": r"^(nb_|nbre_)", "semantic_type": "QUANTITY"},
    {"pattern": r"^(dt_|date_)", "semantic_type": "DATE"},
    {"pattern": r"^(month_|mois_)", "semantic_type": "MONTH"},
    {"pattern": r"^(day_|jour_)", "semantic_type": "DAY"},
    {"pattern": r"^(year_|annee_)", "semantic_type": "YEAR"},
    {"pattern": r"^(mt_|mnt_)", "semantic_type": "AMOUNT"},
    {"pattern": r"^(cd_|code_)", "semantic_type": "CATEGORY"},
    {"pattern": r"^(id_)", "semantic_type": "RELATION_ID"},
    {"pattern": r"(_id)$", "semantic_type": "RELATION_ID"},
    {"pattern": r"^(tx_|taux_)", "semantic_type": "RATE"},
]

STRICT_MONTH_NAMES = {
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
    "janvier",
    "fevrier",
    "mars",
    "avril",
    "mai",
    "juin",
    "juillet",
    "aout",
    "septembre",
    "octobre",
    "novembre",
    "decembre",
}

STRICT_DAY_NAMES = {
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
    "lundi",
    "mardi",
    "mercredi",
    "jeudi",
    "vendredi",
    "samedi",
    "dimanche",
}

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


def _normalize_token(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value.strip().lower())
    return "".join(char for char in normalized if not unicodedata.combining(char))


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


class ResolvedColumn(TypedDict):
    col_source: str
    col_prepared: str
    semantic_type: str
    source_resolution: str
    confidence: float


def _safe_read_table(
    lakehouse_name: str,
    relative_path: str,
    spark: SparkSession,
) -> Optional[DataFrame]:
    """Read a table and return None if unavailable."""
    try:
        return read_lakehouse(lakehouse_name, relative_path, spark=spark)
    except Exception:
        return None


def _build_schema_hash(df: DataFrame) -> str:
    """Build a stable hash from column names and data types."""
    payload = "|".join(
        f"{field.name}:{field.dataType.simpleString()}"
        for field in df.schema.fields
    )
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def _clean_suffix(col_name: str) -> str:
    """Remove common technical prefixes/suffixes and leading/trailing special characters before labeling."""
    cleaned = col_name.lower().strip()
    cleaned = re.sub(r"^(nb_|nbre_|dt_|date_|mt_|mnt_|cd_|code_|id_|tx_|taux_)", "", cleaned)
    cleaned = re.sub(r"(_id)$", "", cleaned)
    cleaned = re.sub(r"^[\W_]+|[\W_]+$", "", cleaned)
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    cleaned = cleaned.strip()
    return cleaned


def _build_prepared_name(col_source: str) -> str:
    """Build prepared column name from cleaned source suffix."""
    suffix = _clean_suffix(col_source)
    return suffix.replace(" ", "_") if suffix else col_source.lower()

def _text_contains(text: str, patterns: List[str]) -> bool:
    for pattern in patterns:
        if pattern.lower() in text.lower():
            return True
    return False

def _ensure_prefix_rules(
    source_lakehouse_name: str,
    spark: SparkSession,
) -> DataFrame:
    """Load prefix rules; initialize defaults in source lakehouse if missing."""
    rules_df = _safe_read_table(source_lakehouse_name, CONFIG_PREFIX_RULES_PATH, spark=spark)
    if rules_df is not None:
        return rules_df

    initialized_df = spark.createDataFrame(DEFAULT_PREFIX_RULES)
    write_lakehouse(
        initialized_df,
        lakehouse_name=source_lakehouse_name,
        relative_path=CONFIG_PREFIX_RULES_PATH,
        mode="overwrite",
        spark=spark,
    )
    log("Initialized default prefix rules in source config table.")
    return initialized_df


def snapshot_source_schema(
    source_lakehouse_name: str,
    source_relative_path: str,
    spark: Optional[SparkSession] = None,
) -> str:
    """
    Snapshot source schema and profile stats into source config table.

    Returns the schema hash used for cache invalidation.
    """
    _spark = spark or get_spark()
    source_df = read_lakehouse(source_lakehouse_name, source_relative_path, spark=_spark)
    schema_hash = _build_schema_hash(source_df)
    total_rows = source_df.count()

    agg_exprs = []
    for col_name in source_df.columns:
        agg_exprs.extend(
            [
                F.min(F.col(col_name)).alias(f"{col_name}__min"),
                F.max(F.col(col_name)).alias(f"{col_name}__max"),
                F.sum(F.when(F.col(col_name).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(
                    f"{col_name}__nulls"
                ),
                F.avg(F.length(F.col(col_name).cast("string"))).alias(f"{col_name}__avg_len"),
                F.countDistinct(F.col(col_name)).alias(f"{col_name}__distinct"),
            ]
        )
    stats_row = source_df.agg(*agg_exprs).collect()[0].asDict()

    describe_detail_payload = ""
    describe_extended_payload = ""
    try:
        resolved = resolve_lakehouse_read_candidate(
            source_lakehouse_name,
            source_relative_path,
            spark=_spark,
        )
        abfs_base = get_lakehouse_abfs_path(source_lakehouse_name)
        full_path = f"{abfs_base}/{resolved}"
        describe_detail_payload = json.dumps(
            _spark.sql(f"DESCRIBE DETAIL delta.`{full_path}`").first().asDict(),
            default=str,
        )
        describe_extended_payload = json.dumps(
            [row.asDict() for row in _spark.sql(f"DESCRIBE EXTENDED delta.`{full_path}`").collect()],
            default=str,
        )
    except Exception as exc:
        log(f"Could not collect DESCRIBE DETAIL/EXTENDED: {exc}", level="warning")

    snapshot_rows: list[dict[str, Any]] = []
    for field in source_df.schema.fields:
        name = field.name
        distinct_count = int(stats_row.get(f"{name}__distinct") or 0)
        cardinality_ratio = (
            float(distinct_count) / float(total_rows) if total_rows else 0.0
        )
        snapshot_rows.append(
            {
                "snapshot_timestamp": datetime.utcnow().isoformat(),
                "source_relative_path": source_relative_path,
                "schema_hash": schema_hash,
                "col_source": name,
                "delta_type": field.dataType.simpleString(),
                "min_value": None if stats_row.get(f"{name}__min") is None else str(stats_row.get(f"{name}__min")),
                "max_value": None if stats_row.get(f"{name}__max") is None else str(stats_row.get(f"{name}__max")),
                "null_count": int(stats_row.get(f"{name}__nulls") or 0),
                "avg_len": float(stats_row.get(f"{name}__avg_len") or 0.0),
                "distinct_count": distinct_count,
                "total_count": int(total_rows),
                "cardinality_ratio": cardinality_ratio,
                "describe_detail": describe_detail_payload,
                "describe_extended": describe_extended_payload,
            }
        )

    snapshot_df = _spark.createDataFrame(snapshot_rows)
    ordered_snapshot_columns = ["col_source"] + [
        col_name for col_name in snapshot_df.columns if col_name != "col_source"
    ]
    snapshot_df = snapshot_df.select(*ordered_snapshot_columns)
    write_lakehouse(
        snapshot_df,
        lakehouse_name=source_lakehouse_name,
        relative_path=f"{source_relative_path}_{CONFIG_SOURCE_SNAPSHOT_PATH}",
        mode="overwrite",
        spark=_spark,
    )
    log(
        "Source schema snapshot written: "
        f"{len(snapshot_rows)} columns, schema_hash={schema_hash}"
    )
    return schema_hash


def _layer1_resolve(
    col_source: str,
    rules: list[dict[str, Any]],
) -> Optional[ResolvedColumn]:
    for rule in rules:
        pattern = str(rule.get("pattern", ""))
        semantic_type = str(rule.get("semantic_type", "")).upper().strip()
        if not pattern or not semantic_type:
            continue
        if re.search(pattern, col_source, flags=re.IGNORECASE):
            return {
                "col_source": col_source,
                "col_prepared": _build_prepared_name(col_source),
                "semantic_type": semantic_type,
                "source_resolution": "PREFIX_RULE",
                "confidence": 1.0,
            }
    return None


def _layer2_profile_resolve(
    df: DataFrame,
    col_source: str,
    col_data_type: Any,
    sample_size: int,
    threshold: float,
) -> Optional[ResolvedColumn]:
    if isinstance(col_data_type, (DateType, TimestampType)):
        return {
            "col_source": col_source,
            "col_prepared": _build_prepared_name(col_source),
            "semantic_type": "DATE",
            "source_resolution": "PROFILING",
            "confidence": 1.0,
        }

    sample_rows = (
        df.select(col_source)
        .where(F.col(col_source).isNotNull())
        .limit(sample_size)
        .collect()
    )
    sample_values = [row[0] for row in sample_rows if row[0] is not None]
    if not sample_values:
        return None
    
    confidence_by_type: dict[str, float] = {}
    if _text_contains(col_source, ["year", "année", "annee"]):
        confidence_by_type["YEAR"] = max(confidence_by_type.get("YEAR", 0.0), 1)
    if _text_contains(col_source, ["month", "mois"]):
        confidence_by_type["MONTH"] = max(confidence_by_type.get("MONTH", 0.0), 1)
    if _text_contains(col_source, ["day", "jour"]):
        confidence_by_type["DAY"] = max(confidence_by_type.get("DAY", 0.0), 1)
    if _text_contains(col_source, ["week", "semaine"]):
        confidence_by_type["WEEK"] = max(confidence_by_type.get("WEEK", 0.0), 1)

    distinct_ratio = len(set(sample_values)) / max(len(sample_values), 1)
    avg_len = sum(len(str(v)) for v in sample_values) / max(len(sample_values), 1)

    if distinct_ratio < 0.05:
        confidence_by_type["CATEGORY"] = max(confidence_by_type.get("CATEGORY", 0.0), 0.85)
    if distinct_ratio > 0.95 and avg_len >= 12:
        confidence_by_type["TECH_ID"] = max(confidence_by_type.get("TECH_ID", 0.0), 0.82)

    date_pattern_iso = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    date_pattern_slash = re.compile(r"^\d{2}/\d{2}/\d{4}$")
    ref_pattern = re.compile(r"^[A-Z]{2,4}[0-9]+$")
    decimal_2_pattern = re.compile(r"^-?\d+\.\d{2}$")
    as_str_values = [str(v).strip() for v in sample_values]
    strict_tokens = [_normalize_token(value) for value in as_str_values[:50] if value.strip()]
    month_name_ratio = (
        sum(1 for token in strict_tokens if token in STRICT_MONTH_NAMES) / max(len(strict_tokens), 1)
    )
    day_name_ratio = sum(1 for token in strict_tokens if token in STRICT_DAY_NAMES) / max(len(strict_tokens), 1)
    if month_name_ratio > 0.5:
        confidence_by_type["MONTH"] = max(confidence_by_type.get("MONTH", 0.0), 0.86)
    if day_name_ratio > 0.5:
        confidence_by_type["DAY"] = max(confidence_by_type.get("DAY", 0.0), 0.84)
    if all(date_pattern_iso.match(v) or date_pattern_slash.match(v) for v in as_str_values[:50]):
        confidence_by_type["DATE"] = max(confidence_by_type.get("DATE", 0.0), 0.9)
    if all(decimal_2_pattern.match(v) for v in as_str_values[:50]):
        confidence_by_type["AMOUNT"] = max(confidence_by_type.get("AMOUNT", 0.0), 0.82)
    if all(ref_pattern.match(v) for v in as_str_values[:50]):
        confidence_by_type["CATEGORY"] = max(confidence_by_type.get("CATEGORY", 0.0), 0.88)

    if isinstance(col_data_type, StringType):
        integer_tokens = [token for token in strict_tokens if re.fullmatch(r"\d{1,4}", token)]
        numeric_like_ratio = len(integer_tokens) / max(len(strict_tokens), 1)
        if numeric_like_ratio > 0.5:
            integer_values = [int(token) for token in integer_tokens]
            month_digit_ratio = sum(1 for value in integer_values if 1 <= value <= 12) / max(
                len(integer_values), 1
            )
            day_digit_ratio = sum(1 for value in integer_values if 1 <= value <= 31) / max(
                len(integer_values), 1
            )
            year_digit_ratio = sum(1 for value in integer_values if 1900 <= value <= 2100) / max(
                len(integer_values), 1
            )
            if month_digit_ratio > 0.5 and month_name_ratio > 0.0:
                confidence_by_type["MONTH"] = max(confidence_by_type.get("MONTH", 0.0), 0.88)
            if day_digit_ratio > 0.5 and day_name_ratio > 0.0:
                confidence_by_type["DAY"] = max(confidence_by_type.get("DAY", 0.0), 0.86)
            if year_digit_ratio > 0.5:
                confidence_by_type["YEAR"] = max(confidence_by_type.get("YEAR", 0.0), 0.82)
        else:
            confidence_by_type.pop("YEAR", None)
    else:
        numeric_values: list[float] = []
        for value in sample_values:
            try:
                numeric_values.append(float(value))
            except Exception:
                pass
        if numeric_values:
            min_value = min(numeric_values)
            max_value = max(numeric_values)
            mean_value = sum(numeric_values) / len(numeric_values)
            if 1.0 <= min_value and max_value <= 12.0:
                confidence_by_type["MONTH"] = max(confidence_by_type.get("MONTH", 0.0), 0.86)
            if 1.0 <= min_value and max_value <= 31.0:
                confidence_by_type["DAY"] = max(confidence_by_type.get("DAY", 0.0), 0.84)
            if 0.0 <= min_value and max_value <= 1.0:
                confidence_by_type["RATE"] = max(confidence_by_type.get("RATE", 0.0), 0.85)
            if 1900 <= min_value and max_value <= 2100:
                confidence_by_type["YEAR"] = max(confidence_by_type.get("YEAR", 0.0), 0.8)
            if mean_value != 0:
                variance = sum((v - mean_value) ** 2 for v in numeric_values) / len(numeric_values)
                std_dev = variance ** 0.5
                if abs(std_dev / mean_value) > 10:
                    confidence_by_type["AMOUNT"] = max(confidence_by_type.get("AMOUNT", 0.0), 0.83)

    if isinstance(col_data_type, NumericType):
        for semantic_type in ("AMOUNT", "QUANTITY", "RATE", "YEAR", "MONTH", "DAY"):
            if semantic_type in confidence_by_type:
                confidence_by_type[semantic_type] += 0.05
        if "DATE" in confidence_by_type:
            confidence_by_type["DATE"] = max(0.0, confidence_by_type["DATE"] - 0.2)

    if isinstance(col_data_type, StringType):
        if "TECH_ID" in confidence_by_type:
            sampled_strings = as_str_values[:50]
            digit_ratio = (
                sum(1 for value in sampled_strings if re.search(r"\d", value))
                / max(len(sampled_strings), 1)
            )
            if digit_ratio > 0.5:
                confidence_by_type["TECH_ID"] += 0.08
            else:
                confidence_by_type.pop("TECH_ID", None)
                confidence_by_type["TEXT"] = max(confidence_by_type.get("TEXT", 0.0), 0.86)

    if isinstance(col_data_type, BooleanType):
        confidence_by_type["TEXT"] = max(confidence_by_type.get("TEXT", 0.0), 0.8)
        if "AMOUNT" in confidence_by_type:
            confidence_by_type["AMOUNT"] = max(0.0, confidence_by_type["AMOUNT"] - 0.2)
        if "RATE" in confidence_by_type:
            confidence_by_type["RATE"] = max(0.0, confidence_by_type["RATE"] - 0.2)

    if not confidence_by_type:
        return None

    semantic_type, confidence = max(confidence_by_type.items(), key=lambda item: item[1])
    if confidence < threshold:
        return None
    return {
        "col_source": col_source,
        "col_prepared": _build_prepared_name(col_source),
        "semantic_type": semantic_type,
        "source_resolution": "PROFILING",
        "confidence": float(confidence),
    }


def _layer3_mapping_resolve(
    col_source: str,
    mapping_rows: list[dict[str, Any]],
) -> Optional[ResolvedColumn]:
    if not mapping_rows:
        return None

    def rank_scope(scope: str) -> int:
        normalized = scope.lower().strip()
        if normalized == "table":
            return 3
        if normalized == "domain":
            return 2
        return 1

    valid_rows = []
    today = date.today().isoformat()
    for row in mapping_rows:
        if str(row.get("col_source", "")).strip() != col_source:
            continue
        valid_until = row.get("valid_until")
        if valid_until is not None and str(valid_until) < today:
            continue
        valid_rows.append(row)
    if not valid_rows:
        return None
    selected = sorted(
        valid_rows,
        key=lambda row: rank_scope(str(row.get("scope", "global"))),
        reverse=True,
    )[0]
    source_resolution = str(selected.get("source_resolution", "MANUAL_MAPPING")).strip() or "MANUAL_MAPPING"
    return {
        "col_source": col_source,
        "col_prepared": str(selected.get("col_prepared") or selected.get("col_gold") or col_source),
        "semantic_type": str(selected.get("semantic_type") or "TEXT").upper(),
        "source_resolution": source_resolution,
        "confidence": float(selected.get("confidence") or 1.0),
    }


def _write_unresolved_audit(
    unresolved_columns: list[str],
    source_lakehouse_name: str,
    source_relative_path: str,
    schema_hash: str,
    unresolved_webhook_url: Optional[str],
    spark: SparkSession,
) -> None:
    if not unresolved_columns:
        return
    rows = [
        {
            "event_timestamp": datetime.utcnow().isoformat(),
            "source_relative_path": source_relative_path,
            "schema_hash": schema_hash,
            "col_source": col_name,
            "source_resolution": "UNRESOLVED",
            "status": "ACTION_REQUIRED",
        }
        for col_name in unresolved_columns
    ]
    audit_df = spark.createDataFrame(rows)
    write_lakehouse(
        audit_df,
        lakehouse_name=source_lakehouse_name,
        relative_path=CONFIG_AUDIT_LOG_PATH,
        mode="append",
        spark=spark,
    )
    log(
        f"Unresolved columns recorded in audit log: {', '.join(unresolved_columns)}",
        level="warning",
    )

    if unresolved_webhook_url:
        payload = json.dumps(
            {
                "event": "UNRESOLVED_COLUMNS",
                "source_relative_path": source_relative_path,
                "schema_hash": schema_hash,
                "columns": unresolved_columns,
            }
        ).encode("utf-8")
        req = request.Request(
            unresolved_webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            request.urlopen(req, timeout=10)
        except Exception as exc:
            log(f"Webhook notification failed: {exc}", level="warning")


def resolve_columns(
    source_lakehouse_name: str,
    source_relative_path: str,
    schema_hash: Optional[str] = None,
    sample_size: int = 500,
    profiling_confidence_threshold: float = 0.80,
    unresolved_webhook_url: Optional[str] = None,
    spark: Optional[SparkSession] = None,
) -> List[ResolvedColumn]:
    """
    Resolve source columns to prepared semantic columns through 3 cascade layers.
    """
    _spark = spark or get_spark()
    df = read_lakehouse(source_lakehouse_name, source_relative_path, spark=_spark)
    effective_schema_hash = schema_hash or _build_schema_hash(df)

    rules_df = _ensure_prefix_rules(source_lakehouse_name, _spark)
    rule_rows = [row.asDict() for row in rules_df.collect()]

    profiling_cache_df = _safe_read_table(source_lakehouse_name, CONFIG_PROFILING_CACHE_PATH, spark=_spark)
    cache_by_column: dict[str, ResolvedColumn] = {}
    if profiling_cache_df is not None:
        for row in profiling_cache_df.where(F.col("schema_hash") == effective_schema_hash).collect():
            row_dict = row.asDict()
            cache_by_column[str(row_dict.get("col_source"))] = {
                "col_source": str(row_dict.get("col_source")),
                "col_prepared": str(row_dict.get("col_prepared")),
                "semantic_type": str(row_dict.get("semantic_type")),
                "source_resolution": "PROFILING_CACHE",
                "confidence": float(row_dict.get("confidence") or 0.0),
            }

    mapping_df = _safe_read_table(source_lakehouse_name, CONFIG_MAPPING_RULES_PATH, spark=_spark)
    mapping_rows = [row.asDict() for row in mapping_df.collect()] if mapping_df is not None else []

    resolved: list[ResolvedColumn] = []
    unresolved: list[str] = []
    layer2_rows_for_cache: list[dict[str, Any]] = []
    source_type_by_column = {field.name: field.dataType for field in df.schema.fields}

    for col_name in df.columns:
        layer1 = _layer1_resolve(col_name, rule_rows)
        if layer1 is not None:
            resolved.append(layer1)
            continue

        cached = cache_by_column.get(col_name)
        if cached is not None and cached["confidence"] >= profiling_confidence_threshold:
            resolved.append(cached)
            continue

        layer2 = _layer2_profile_resolve(
            df=df,
            col_source=col_name,
            col_data_type=source_type_by_column.get(col_name),
            sample_size=sample_size,
            threshold=profiling_confidence_threshold,
        )
        if layer2 is not None:
            resolved.append(layer2)
            layer2_rows_for_cache.append(
                {
                    "schema_hash": effective_schema_hash,
                    "col_source": layer2["col_source"],
                    "col_prepared": layer2["col_prepared"],
                    "semantic_type": layer2["semantic_type"],
                    "confidence": layer2["confidence"],
                    "cached_at": datetime.utcnow().isoformat(),
                }
            )
            continue

        layer3 = _layer3_mapping_resolve(col_name, mapping_rows)
        if layer3 is not None:
            resolved.append(layer3)
            continue

        unresolved.append(col_name)

    if layer2_rows_for_cache:
        cache_df = _spark.createDataFrame(layer2_rows_for_cache)
        write_lakehouse(
            cache_df,
            lakehouse_name=source_lakehouse_name,
            relative_path=CONFIG_PROFILING_CACHE_PATH,
            mode="overwrite",
            spark=_spark,
        )

    _write_unresolved_audit(
        unresolved_columns=unresolved,
        source_lakehouse_name=source_lakehouse_name,
        source_relative_path=source_relative_path,
        schema_hash=effective_schema_hash,
        unresolved_webhook_url=unresolved_webhook_url,
        spark=_spark,
    )

    return resolved


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


def generate_prepared_aggregations(
    source_lakehouse_name: str,
    target_lakehouse_name: str,
    target_relative_path: str,
    resolved_mappings: List[ResolvedColumn],
    spark: Optional[SparkSession] = None,
) -> dict[str, str]:
    """
    Generate default prepared aggregations and write them to target lakehouse.
    """
    _spark = spark or get_spark()
    prepared_df = read_lakehouse(target_lakehouse_name, target_relative_path, spark=_spark)

    measure_cols = [
        mapping["col_prepared"]
        for mapping in resolved_mappings
        if mapping["col_prepared"] in prepared_df.columns
        and mapping["semantic_type"].upper() in {"AMOUNT", "QUANTITY", "RATE"}
    ]
    date_cols = [
        mapping["col_prepared"]
        for mapping in resolved_mappings
        if mapping["col_prepared"] in prepared_df.columns and mapping["semantic_type"].upper() == "DATE"
    ]
    code_cols = [
        mapping["col_prepared"]
        for mapping in resolved_mappings
        if mapping["col_prepared"] in prepared_df.columns and mapping["semantic_type"].upper() == "CATEGORY"
    ]

    numeric_auto_measures = [
        field.name
        for field in prepared_df.schema.fields
        if field.name in prepared_df.columns and isinstance(field.dataType, NumericType)
    ]
    all_measures = sorted(set(measure_cols + numeric_auto_measures))
    if not all_measures:
        log("No numeric measures detected for aggregations.", level="warning")
        return {}

    def _build_agg(group_cols: list[str], table_name: str) -> str:
        aggregations = [F.sum(F.col(col_name)).alias(f"sum_{col_name}") for col_name in all_measures]
        if group_cols:
            agg_df = prepared_df.groupBy(*group_cols).agg(*aggregations)
        else:
            agg_df = prepared_df.agg(*aggregations)
        write_lakehouse(
            agg_df,
            lakehouse_name=target_lakehouse_name,
            relative_path=table_name,
            mode="overwrite",
            spark=_spark,
        )
        return table_name

    day_dims = date_cols[:1] + code_cols[:1]
    week_key = f"{date_cols[0]}_week_number" if date_cols else ""
    week_dims = [week_key] if week_key and week_key in prepared_df.columns else []
    region_dims = [col_name for col_name in code_cols if "region" in col_name.lower()][:1]
    if not region_dims:
        region_dims = code_cols[:1]

    outputs: dict[str, str] = {}
    outputs["prepared_agg_jour"] = _build_agg(day_dims, f"{target_relative_path}_prepared_agg_jour")
    outputs["prepared_agg_semaine"] = _build_agg(week_dims, f"{target_relative_path}_prepared_agg_semaine")
    outputs["prepared_agg_region"] = _build_agg(region_dims, f"{target_relative_path}_prepared_agg_region")

    # Keep source lakehouse argument explicit in API even if not used right now.
    _ = source_lakehouse_name
    return outputs


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


def prepare_and_write_data(
    source_lakehouse_name: str,
    source_relative_path: str,
    target_lakehouse_name: str,
    target_relative_path: str,
    mode: str = "overwrite",
    sample_size: int = 500,
    profiling_confidence_threshold: float = 0.80,
    max_partitions_guard: int = 500,
    vacuum_retention_hours: int = 168,
    enable_semantic_model_publish: bool = False,
    semantic_workspace: Optional[str] = None,
    semantic_model_name: str = "fabrictools_prepared_dataset",
    overwrite_semantic_model: bool = True,
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """
    Orchestrate source -> prepared processing for one table.
    """
    _spark = spark or get_spark()
    source_df = read_lakehouse(source_lakehouse_name, source_relative_path, spark=_spark)
    schema_hash = snapshot_source_schema(
        source_lakehouse_name=source_lakehouse_name,
        source_relative_path=source_relative_path,
        spark=_spark,
    )
    resolved_mappings = resolve_columns(
        df=source_df,
        source_lakehouse_name=source_lakehouse_name,
        schema_hash=schema_hash,
        sample_size=sample_size,
        profiling_confidence_threshold=profiling_confidence_threshold,
        source_relative_path=source_relative_path,
        spark=_spark,
    )
    prepared_df = transform_to_prepared(
        df=source_df,
        resolved_mappings=resolved_mappings,
        source_lakehouse_name=source_lakehouse_name,
        spark=_spark,
    )
    write_prepared_table(
        df=prepared_df,
        resolved_mappings=resolved_mappings,
        target_lakehouse_name=target_lakehouse_name,
        target_relative_path=target_relative_path,
        mode=mode,
        max_partitions_guard=max_partitions_guard,
        vacuum_retention_hours=vacuum_retention_hours,
        spark=_spark,
    )
    if enable_semantic_model_publish:
        agg_tables = generate_prepared_aggregations(
            source_lakehouse_name=source_lakehouse_name,
            target_lakehouse_name=target_lakehouse_name,
            target_relative_path=target_relative_path,
            resolved_mappings=resolved_mappings,
            spark=_spark,
        )
        publish_semantic_model(
            target_lakehouse_name=target_lakehouse_name,
            agg_tables=agg_tables,
            resolved_mappings=resolved_mappings,
            semantic_workspace=semantic_workspace,
            semantic_model_name=semantic_model_name,
            overwrite_model=overwrite_semantic_model,
            spark=_spark,
        )
    return prepared_df


def prepare_and_write_all_tables(
    source_lakehouse_name: str,
    target_lakehouse_name: str,
    mode: str = "overwrite",
    tables_config: Optional[List[dict[str, Any]]] = None,
    include_schemas: Optional[List[str]] = None,
    exclude_tables: Optional[List[str]] = None,
    sample_size: int = 500,
    profiling_confidence_threshold: float = 0.80,
    max_partitions_guard: int = 500,
    vacuum_retention_hours: int = 168,
    enable_semantic_model_publish: bool = False,
    semantic_workspace: Optional[str] = None,
    semantic_model_name: str = "fabrictools_prepared_dataset",
    overwrite_semantic_model: bool = True,
    continue_on_error: bool = False,
    spark: Optional[SparkSession] = None,
) -> dict[str, Any]:
    """
    Bulk orchestration aligned with clean_and_write_all_tables pattern.
    """
    _spark = spark or get_spark()
    supported_modes = {"overwrite", "append", "ignore", "error"}

    table_jobs: List[dict[str, str]] = []
    if tables_config is not None:
        for idx, table_config in enumerate(tables_config, start=1):
            if not isinstance(table_config, dict):
                raise ValueError(
                    f"tables_config[{idx}] must be a dict, got {type(table_config).__name__}."
                )
            source_relative_path = str(
                table_config.get("source_path")
                or table_config.get("source_table")
                or table_config.get("bronze_path")
                or ""
            ).strip()
            if not source_relative_path:
                raise ValueError(f"tables_config[{idx}] is missing a source path key.")
            target_relative_path = str(
                table_config.get("target_path")
                or table_config.get("prepared_table")
                or table_config.get("silver_table")
                or source_relative_path
            ).strip()
            table_mode = str(table_config.get("mode", mode)).strip().lower()
            if table_mode not in supported_modes:
                raise ValueError(
                    f"tables_config[{idx}] has unsupported mode '{table_mode}'. "
                    "Supported modes: overwrite, append, ignore, error."
                )
            table_jobs.append(
                {
                    "source_relative_path": source_relative_path,
                    "target_relative_path": target_relative_path,
                    "mode": table_mode,
                }
            )
    else:
        discovered = _list_lakehouse_table_paths(
            lakehouse_name=source_lakehouse_name,
            include_schemas=include_schemas,
            exclude_tables=exclude_tables,
        )
        table_jobs = [
            {
                "source_relative_path": relative_path,
                "target_relative_path": relative_path,
                "mode": mode,
            }
            for relative_path in discovered
        ]

    if not table_jobs:
        log(
            f"No tables found in Lakehouse '{source_lakehouse_name}' for prepare/write.",
            level="warning",
        )
        return {
            "total_tables": 0,
            "successful_tables": 0,
            "failed_tables": 0,
            "tables": [],
            "failures": [],
        }

    processed_tables: List[dict[str, str]] = []
    failures: List[dict[str, str]] = []
    total_tables = len(table_jobs)
    for index, table_job in enumerate(table_jobs, start=1):
        src = table_job["source_relative_path"]
        tgt = table_job["target_relative_path"]
        table_mode = table_job["mode"]
        log(f"[{index}/{total_tables}] Preparing '{src}' -> '{tgt}' [mode={table_mode}]...")
        try:
            prepare_and_write_data(
                source_lakehouse_name=source_lakehouse_name,
                source_relative_path=src,
                target_lakehouse_name=target_lakehouse_name,
                target_relative_path=tgt,
                mode=table_mode,
                sample_size=sample_size,
                profiling_confidence_threshold=profiling_confidence_threshold,
                max_partitions_guard=max_partitions_guard,
                vacuum_retention_hours=vacuum_retention_hours,
                enable_semantic_model_publish=enable_semantic_model_publish,
                semantic_workspace=semantic_workspace,
                semantic_model_name=semantic_model_name,
                overwrite_semantic_model=overwrite_semantic_model,
                spark=_spark,
            )
            processed_tables.append(
                {
                    "source_relative_path": src,
                    "target_relative_path": tgt,
                    "mode": table_mode,
                }
            )
        except Exception as exc:
            failures.append(
                {
                    "source_relative_path": src,
                    "target_relative_path": tgt,
                    "mode": table_mode,
                    "error": str(exc),
                }
            )
            log(f"[{index}/{total_tables}] Failed for '{src}': {exc}", level="warning")
            if not continue_on_error:
                raise

    return {
        "total_tables": total_tables,
        "successful_tables": len(processed_tables),
        "failed_tables": len(failures),
        "tables": processed_tables,
        "failures": failures,
    }

