"""Column semantic resolution helpers."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DateType, TimestampType, BooleanType, NumericType, StringType

from typing import Any, Optional, List
import re
import json
import requests
from datetime import date, datetime
from typing import TypedDict

from fabrictools.core import log
from fabrictools.core import get_spark
from fabrictools.io import read_lakehouse, write_lakehouse
from fabrictools.prepare.schema import _build_schema_hash
from fabrictools.prepare.transform import _normalize_token


CONFIG_PREFIX_RULES_PATH = "Tables/dbo/prefix_rules"
CONFIG_PROFILING_CACHE_PATH = "Tables/dbo/profiling_cache"
CONFIG_RESOLVED_COLUMNS_PATH = "resolved_columns"
CONFIG_MAPPING_RULES_PATH = "Tables/dbo/mapping_rules"
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

class ResolvedColumn(TypedDict):
    col_source: str
    col_prepared: str
    semantic_type: str
    source_resolution: str
    confidence: float


def _clean_suffix(col_name: str) -> str:
    """Remove common technical prefixes/suffixes and leading/trailing special characters before labeling."""
    cleaned = col_name.lower().strip()
    cleaned = re.sub(r"^(nb_|nbre_|dt_|date_|mt_|mnt_|cd_|code_|id_|tx_|taux_)", "", cleaned)
    cleaned = re.sub(r"(_id)$", "", cleaned)
    cleaned = re.sub(r"^[\W_]+|[\W_]+$", "", cleaned)
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    cleaned = cleaned.strip()
    return cleaned

def _text_contains(text: str, patterns: List[str]) -> bool:
    for pattern in patterns:
        if pattern.lower() in text.lower():
            return True
    return False

def _build_prepared_name(col_source: str) -> str:
    """Build prepared column name from cleaned source suffix."""
    suffix = _clean_suffix(col_source)
    return suffix.replace(" ", "_") if suffix else col_source.lower()

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
        req = requests.Request(
            unresolved_webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            requests.urlopen(req, timeout=10)
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


__all__ = [
    "resolve_columns",
    "_ensure_prefix_rules",
    "_safe_read_table",
    "_write_unresolved_audit",
    "_layer1_resolve",
    "_layer2_profile_resolve",
    "_layer3_mapping_resolve",
]