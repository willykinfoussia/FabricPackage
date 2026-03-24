"""Prepared transform and write helpers."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from typing import Optional, List, Tuple
import re
import unicodedata

from fabrictools.core import log
from fabrictools.core import get_spark
from fabrictools.core import build_lakehouse_write_path, get_lakehouse_abfs_path
from fabrictools.io import read_lakehouse, write_lakehouse
from fabrictools.prepare.resolve import _safe_read_table, ResolvedColumn, _normalize_token


CONFIG_CODE_LABELS_PATH = "Tables/dbo/code_labels"

PARTITION_MIN_COMBINED = 20
PARTITION_MAX_COMBINED_HARD = 200
PARTITION_QUASI_ID_RATIO = 0.95
PARTITION_MAX_NULL_RATIO = 0.30
PARTITION_CATEGORY_DISTINCT_MIN = 10
PARTITION_CATEGORY_DISTINCT_MAX = 200
DEFAULT_MAX_PARTITIONS_GUARD = 200

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


def _partition_token_excluded(col_name: str, excluded_tokens: set[str]) -> bool:
    return _normalize_token(col_name).replace("_", " ") in excluded_tokens


def _collect_partition_candidate_meta(
    df: DataFrame,
    resolved_mappings: List[ResolvedColumn],
) -> Tuple[list[str], dict[str, str]]:
    """
    Ordered unique partition candidate columns: P1 (DATE year/month derivations, YEAR/MONTH),
    then P2 (CATEGORY). Values in meta are tags: DATE_YEAR, DATE_MONTH, YEAR, MONTH, CATEGORY.
    """
    cols = set(df.columns)
    ordered: list[str] = []
    seen: set[str] = set()
    meta: dict[str, str] = {}

    def add(name: str, tag: str) -> None:
        if name not in cols or name in seen:
            return
        seen.add(name)
        ordered.append(name)
        meta[name] = tag

    for mapping in resolved_mappings:
        st = mapping["semantic_type"].upper()
        prep = mapping["col_prepared"]
        if st == "DATE" and prep in cols:
            add(f"{prep}_year", "DATE_YEAR")
            add(f"{prep}_month_number", "DATE_MONTH")
        elif st == "YEAR" and prep in cols:
            add(prep, "YEAR")
        elif st == "MONTH" and prep in cols:
            add(prep, "MONTH")
        elif st == "CATEGORY" and prep in cols:
            add(prep, "CATEGORY")
    return ordered, meta


def _partition_stats(
    df: DataFrame,
    candidate_cols: list[str],
) -> Tuple[int, dict[str, int], dict[str, int]]:
    """Single agg: row count, per-column distinct count, per-column null count."""
    if not candidate_cols:
        row = df.agg(F.count(F.lit(1)).alias("n_rows")).first()
        n_rows = int(row["n_rows"] or 0) if row is not None else 0
        return n_rows, {}, {}
    agg_exprs: list[F.Column] = [F.count(F.lit(1)).alias("n_rows")]
    for idx, col in enumerate(candidate_cols):
        agg_exprs.append(F.countDistinct(F.col(col)).alias(f"d_{idx}"))
        agg_exprs.append(
            F.sum(F.when(F.col(col).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(f"nnull_{idx}")
        )
    row = df.agg(*agg_exprs).first()
    if row is None:
        return 0, {}, {}
    n_rows = int(row["n_rows"] or 0)
    distinct: dict[str, int] = {}
    nulls: dict[str, int] = {}
    for idx, col in enumerate(candidate_cols):
        distinct[col] = int(row[f"d_{idx}"] or 0)
        nulls[col] = int(row[f"nnull_{idx}"] or 0)
    return n_rows, distinct, nulls


def _best_partition_subset(
    candidates: list[tuple[str, int]],
    min_prod: int,
    max_prod: int,
) -> list[str]:
    """Maximize |S|, then product in [min_prod, max_prod] (max product toward max_prod); tie-break lexicographic on names."""
    n = len(candidates)
    best: Optional[tuple[int, int, tuple[str, ...]]] = None

    def is_better(
        cand: tuple[int, int, tuple[str, ...]],
        cur: tuple[int, int, tuple[str, ...]],
    ) -> bool:
        if cand[0] != cur[0]:
            return cand[0] > cur[0]
        if cand[1] != cur[1]:
            return cand[1] > cur[1]
        return cand[2] < cur[2]

    def consider(chosen: list[str], prod: int) -> None:
        nonlocal best
        if not chosen or prod < min_prod or prod > max_prod:
            return
        cand = (len(chosen), prod, tuple(chosen))
        if best is None or is_better(cand, best):
            best = cand

    def dfs(i: int, chosen: list[str], prod: int) -> None:
        consider(chosen, prod)
        if i >= n:
            return
        name, d_raw = candidates[i]
        d = max(int(d_raw), 1)
        next_prod = prod * d
        if next_prod <= max_prod:
            chosen.append(name)
            dfs(i + 1, chosen, next_prod)
            chosen.pop()
        dfs(i + 1, chosen, prod)

    dfs(0, [], 1)
    if best is None:
        return []
    return list(best[2])


def _select_partition_columns(
    df: DataFrame,
    resolved_mappings: List[ResolvedColumn],
    *,
    max_combined: int,
    excluded_tokens: set[str],
) -> list[str]:
    upper = min(PARTITION_MAX_COMBINED_HARD, max_combined)
    if upper < PARTITION_MIN_COMBINED:
        return []

    ordered, meta = _collect_partition_candidate_meta(df, resolved_mappings)
    if not ordered:
        return []

    n_rows, distinct, nulls = _partition_stats(df, ordered)
    if n_rows <= 0:
        return []

    filtered: list[tuple[str, int]] = []
    for name in ordered:
        if _partition_token_excluded(name, excluded_tokens):
            continue
        tag = meta[name]
        d = distinct.get(name, 0)
        nnull = nulls.get(name, 0)
        if nnull / n_rows > PARTITION_MAX_NULL_RATIO:
            continue
        if d <= 0:
            continue
        if d / n_rows >= PARTITION_QUASI_ID_RATIO:
            continue
        if tag == "CATEGORY":
            if d < PARTITION_CATEGORY_DISTINCT_MIN or d > PARTITION_CATEGORY_DISTINCT_MAX:
                continue
        filtered.append((name, d))

    return _best_partition_subset(filtered, PARTITION_MIN_COMBINED, upper)


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
    max_partitions_guard: int = DEFAULT_MAX_PARTITIONS_GUARD,
    vacuum_retention_hours: int = 168,
    spark: Optional[SparkSession] = None,
) -> None:
    """
    Write prepared table and run conditional Delta maintenance operations.

    Partition columns are chosen so the product of COUNT DISTINCT per selected
    column lies in [20, min(200, max_partitions_guard)], when possible. The
    algorithm maximizes the number of partition columns, then the combined
    cardinality (closest to the upper bound). P1 candidates are DATE-derived
    year/month columns and YEAR/MONTH semantics; P2 are CATEGORY columns with
    distinct count in [10, 200]. Quasi-identifiers, high-null columns, and
    measure types are excluded. If no subset satisfies the combined range,
    the table is written without partition columns.
    """
    _spark = spark or get_spark()
    date_partitions = [
        mapping["col_prepared"]
        for mapping in resolved_mappings
        if mapping["semantic_type"].upper() == "DATE" and mapping["col_prepared"] in df.columns
    ]

    excluded_partition_tokens = {"source layer", "source path"}
    selected_partitions = _select_partition_columns(
        df,
        resolved_mappings,
        max_combined=max_partitions_guard,
        excluded_tokens=excluded_partition_tokens,
    )

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