"""Lakehouse I/O facade module."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any, List, Optional

from pyspark.sql import DataFrame, SparkSession  # type: ignore[reportMissingImports]
from pyspark.sql.types import IntegralType  # type: ignore[reportMissingImports]

from fabrictools.core import log
from fabrictools.core import (
    build_lakehouse_read_path_candidates,
    build_lakehouse_write_path,
    get_lakehouse_abfs_path,
)
from fabrictools.core import get_spark
from fabrictools.io.discovery import list_lakehouse_tables

# ── Read ─────────────────────────────────────────────────────────────────────


def read_lakehouse(
    lakehouse_name: str,
    relative_path: str,
    spark: Optional[SparkSession] = None,
    *,
    format: str = "auto",
) -> DataFrame:
    """Read a dataset from a Fabric Lakehouse.

    By default, tries formats in order: **Delta → Parquet → CSV**. The first
    format that succeeds is used; the detected format is logged with the
    resulting shape. Pass ``format="delta"``, ``"parquet"``, or ``"csv"`` to
    skip auto-detection and read that format directly.

    :param lakehouse_name: Display name of the Lakehouse (e.g. ``"BronzeLakehouse"``).
    :param relative_path: Path inside the Lakehouse root, relative to the ABFS base
        (e.g. ``"sales/2024"``, ``"Tables/customers"``, or SQL-style ``"dbo.MyTable"`` /
        ``"dbo.PdC Extraction"`` with spaces in the table name).
    :param spark: Optional ``SparkSession``; when omitted the active session is used.
    :param format: ``"auto"`` (default), ``"delta"``, ``"parquet"``, or ``"csv"``.
    :type lakehouse_name: str
    :type relative_path: str
    :type spark: ~pyspark.sql.SparkSession | None
    :type format: str

    :returns: Loaded dataframe.
    :rtype: ~pyspark.sql.DataFrame

    :raises RuntimeError: When none of the supported formats can be read from the path.

    .. rubric:: Example

    >>> df = read_lakehouse("BronzeLakehouse", "sales/2024")  # doctest: +SKIP
    """
    _spark = spark or get_spark()
    base = get_lakehouse_abfs_path(lakehouse_name)
    df, _, _ = _read_lakehouse_from_base(
        lakehouse_name=lakehouse_name,
        relative_path=relative_path,
        base_path=base,
        spark=_spark,
        format=format,
    )
    return df


def _read_lakehouse_from_base(
    *,
    lakehouse_name: str,
    relative_path: str,
    base_path: str,
    spark: SparkSession,
    format: str = "auto",
) -> tuple[DataFrame, str, str]:
    """Read a Lakehouse path using a pre-resolved Lakehouse base path."""
    candidate_relative_paths = build_lakehouse_read_path_candidates(relative_path)

    failures: list[str] = []
    for candidate_relative_path in candidate_relative_paths:
        full_path = f"{base_path}/{candidate_relative_path}"
        try:
            df = _try_read_formats(spark, full_path, format=format)
            if candidate_relative_path != relative_path:
                log(
                    f"  Resolved relative_path '{relative_path}' -> '{candidate_relative_path}'"
                )
            log(f"  {len(df.columns)} columns")
            return df, candidate_relative_path, full_path
        except RuntimeError as exc:
            failures.append(f"{full_path} ({exc})")

    attempted_paths = ", ".join(
        f"'{base_path}/{candidate}'" for candidate in candidate_relative_paths
    )
    raise RuntimeError(
        f"Could not read from Lakehouse '{lakehouse_name}' for "
        f"relative_path='{relative_path}'. "
        f"Tried: {attempted_paths}. Details: {' | '.join(failures)}"
    )


def resolve_lakehouse_read_candidate(
    lakehouse_name: str,
    relative_path: str,
    spark: Optional[SparkSession] = None,
) -> str:
    """Resolve the best candidate relative path for a Lakehouse read.

    If candidate generation yields a single path, return it directly. If multiple
    candidates exist, try each path and return the first readable one.

    :param lakehouse_name: Display name of the Lakehouse.
    :param relative_path: Logical path under the Lakehouse root (slash path or
        SQL-style ``schema.table``, e.g. ``dbo.PdC Extraction``).
    :param spark: Optional ``SparkSession``; when omitted the active session is used.
    :type lakehouse_name: str
    :type relative_path: str
    :type spark: ~pyspark.sql.SparkSession | None

    :returns: Relative path string that was verified readable.
    :rtype: str

    :raises RuntimeError: When no candidate path can be read.

    .. rubric:: Example

    >>> resolved = resolve_lakehouse_read_candidate(  # doctest: +SKIP
    ...     "BronzeLakehouse", "dbo.SalesOrders"
    ... )
    """
    _spark = spark or get_spark()
    base = get_lakehouse_abfs_path(lakehouse_name)
    candidate_relative_paths = build_lakehouse_read_path_candidates(relative_path)

    if len(candidate_relative_paths) == 1:
        return candidate_relative_paths[0]

    failures: list[str] = []
    for candidate_relative_path in candidate_relative_paths:
        full_path = f"{base}/{candidate_relative_path}"
        try:
            _try_read_formats(_spark, full_path)
            if candidate_relative_path != relative_path:
                log(
                    f"  Resolved relative_path '{relative_path}' -> "
                    f"'{candidate_relative_path}'"
                )
            return candidate_relative_path
        except RuntimeError as exc:
            failures.append(f"{full_path} ({exc})")

    attempted_paths = ", ".join(
        f"'{base}/{candidate}'" for candidate in candidate_relative_paths
    )
    raise RuntimeError(
        f"Could not resolve a readable candidate for relative_path='{relative_path}'. "
        f"Tried: {attempted_paths}. Details: {' | '.join(failures)}"
    )


def _normalize_read_format(format: str) -> str:
    """Validate and normalize Lakehouse read format names."""
    read_format = str(format).strip().lower()
    supported_formats = {"auto", "delta", "parquet", "csv"}
    if read_format not in supported_formats:
        raise ValueError(
            f"Unsupported Lakehouse read format '{format}'. "
            "Supported formats: auto, delta, parquet, csv."
        )
    return read_format


def _read_delta(spark: SparkSession, full_path: str) -> DataFrame:
    df = spark.read.format("delta").load(full_path)
    log("  Format detected: Delta")
    return df


def _read_parquet(spark: SparkSession, full_path: str) -> DataFrame:
    df = (
        spark.read.option("datetimeRebaseMode", "CORRECTED")
        .format("parquet")
        .load(full_path)
    )
    log("  Format detected: Parquet")
    return df


def _read_csv(spark: SparkSession, full_path: str) -> DataFrame:
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(full_path)
    )
    log("  Format detected: CSV")
    return df


def _try_read_formats(
    spark: SparkSession, full_path: str, *, format: str = "auto"
) -> DataFrame:
    """Read one path using either an explicit format or Delta → Parquet → CSV."""
    read_format = _normalize_read_format(format)
    readers = {
        "delta": _read_delta,
        "parquet": _read_parquet,
        "csv": _read_csv,
    }

    if read_format != "auto":
        try:
            return readers[read_format](spark, full_path)
        except Exception as exc:
            raise RuntimeError(
                f"Could not read '{full_path}' as {read_format}: {exc}"
            ) from exc

    # Delta (preferred in Fabric)
    try:
        return _read_delta(spark, full_path)
    except Exception:
        pass

    # Parquet
    try:
        return _read_parquet(spark, full_path)
    except Exception:
        pass

    # CSV — last resort
    try:
        return _read_csv(spark, full_path)
    except Exception as exc:
        raise RuntimeError(
            f"Could not read '{full_path}' as Delta, Parquet, or CSV: {exc}"
        ) from exc


# ── Write ────────────────────────────────────────────────────────────────────


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    """Return a list without duplicates while preserving insertion order."""
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            deduped.append(value)
    return deduped


def _enable_delta_column_mapping_on_path(spark: SparkSession, full_path: str) -> None:
    """Upgrade an existing Delta table path to column mapping mode ``name``."""
    escaped_path = full_path.replace("`", "``")
    spark.sql(
        f"""
        ALTER TABLE delta.`{escaped_path}`
        SET TBLPROPERTIES (
            'delta.columnMapping.mode' = 'name',
            'delta.minReaderVersion' = '2',
            'delta.minWriterVersion' = '5'
        )
        """
    )


def _detect_partition_columns(
    df: DataFrame, threshold_bytes: int = 1_073_741_824
) -> list[str]:
    """
    Auto-detect best partition columns (like year/month or categorical columns)
    only if the dataset is large enough and the column has low cardinality.
    """
    import pyspark.sql.functions as F

    # 1. Vérification de la volumétrie (Fast Fail)
    if threshold_bytes > 0:
        try:
            size_in_bytes = int(
                df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
            )
            if size_in_bytes < threshold_bytes:
                return []
        except Exception:
            pass  # Si l'estimation échoue, on continue

    # 2. Identification des candidats par heuristique (noms)
    # On regarde les types de base
    from pyspark.sql.types import DateType, StringType

    candidates = []
    # On garde les préférences temporelles en priorité
    time_keywords = {"year", "annee", "month", "mois"}
    categorical_keywords = {"country", "region", "status", "type", "category"}

    for f in df.schema.fields:
        if not isinstance(f.dataType, (DateType, IntegralType, StringType)):
            continue

        name_lower = f.name.lower()

        # Temporel ?
        if any(kw in name_lower for kw in time_keywords) and not name_lower.endswith(
            "_id"
        ):
            candidates.append(f.name)
            continue

        # Catégoriel ?
        if any(
            kw in name_lower for kw in categorical_keywords
        ) and not name_lower.endswith("_id"):
            candidates.append(f.name)

    if not candidates:
        return []

    # 3. Évaluation de la Cardinalité (Le test décisif)
    # On limite à 5 candidats pour ne pas trop pénaliser les performances
    candidates_to_test = candidates[:5]

    try:
        exprs = [F.approx_count_distinct(c).alias(c) for c in candidates_to_test]
        cardinalities = df.agg(*exprs).collect()[0].asDict()
    except Exception:
        return []  # En cas d'erreur de calcul, on préfère ne pas partitionner

    # 4. Sélection Finale
    valid_columns = []
    for col, count in cardinalities.items():
        if 1 < count < 1000:
            valid_columns.append(col)

    if not valid_columns:
        return []

    # Trier pour prioriser l'année, puis le mois, puis les autres
    def sort_key(col_name: str) -> int:
        name_lower = col_name.lower()
        if "year" in name_lower or "annee" in name_lower:
            return 0
        if "month" in name_lower or "mois" in name_lower:
            return 1
        return 2

    valid_columns.sort(key=sort_key)

    # On ne retourne pas plus de 2 colonnes pour éviter de trop scinder
    return valid_columns[:2]


def write_lakehouse(
    df: DataFrame,
    lakehouse_name: str,
    relative_path: str,
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None,
    format: str = "delta",
    spark: Optional[SparkSession] = None,
    *,
    normalize_column_names: bool = True,
    enable_column_mapping: bool = False,
    auto_partition: bool = True,
    auto_partition_threshold_bytes: int = 1_073_741_824,
) -> None:
    """Write a ``DataFrame`` to a Fabric Lakehouse (default format: Delta).

    :param df: DataFrame to persist.
    :param lakehouse_name: Display name of the target Lakehouse.
    :param relative_path: Destination path inside the Lakehouse (e.g.
        ``"sales_clean"``, ``"Tables/sales_clean"``, or ``"dbo.PdC Extraction"``).
    :param mode: Spark write mode: ``"overwrite"`` (default), ``"append"``,
        ``"ignore"``, or ``"error"``.
    :param partition_by: Optional column names to partition by. Each name is resolved
        like :py:func:`fabrictools.clean_data` /
        :py:func:`fabrictools.merge_dataframes` (physical name,
        normalized unique label, or snake_case). Auto-detected date partitions are
        appended when present on ``df``.
    :param format: ``"delta"`` (default), ``"parquet"``, or ``"csv"``.
    :param spark: Optional ``SparkSession``; when omitted the active session is used.
    :param normalize_column_names: If ``True`` (default), run
        :py:func:`fabrictools.rename_columns_normalized` before
        resolving ``partition_by`` and writing. If ``False``, keep physical column
        names unchanged.
    :param enable_column_mapping: If ``True`` and ``format="delta"``, writes table
        properties required for Delta column mapping (mode ``name``), allowing
        column names with spaces or special characters.
    :param auto_partition: If ``True`` (default), automatically partition the data
        by detected date columns if they exist.
    :type df: ~pyspark.sql.DataFrame
    :type lakehouse_name: str
    :type relative_path: str
    :type mode: str
    :type partition_by: list[str] | None
    :type format: str
    :type spark: ~pyspark.sql.SparkSession | None
    :type normalize_column_names: bool
    :type enable_column_mapping: bool

    .. rubric:: Example

    >>> write_lakehouse(  # doctest: +SKIP
    ...     df, "SilverLakehouse", "sales_clean", mode="overwrite", partition_by=["year"]
    ... )
    """
    _spark = spark or get_spark()  # validates spark availability early
    base = get_lakehouse_abfs_path(lakehouse_name)
    _write_lakehouse_to_base(
        df=df,
        lakehouse_name=lakehouse_name,
        relative_path=relative_path,
        base_path=base,
        mode=mode,
        partition_by=partition_by,
        format=format,
        spark=_spark,
        normalize_column_names=normalize_column_names,
        enable_column_mapping=enable_column_mapping,
        auto_partition=auto_partition,
        auto_partition_threshold_bytes=auto_partition_threshold_bytes,
    )


def _write_lakehouse_to_base(
    *,
    df: DataFrame,
    lakehouse_name: str,
    relative_path: str,
    base_path: str,
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None,
    format: str = "delta",
    spark: SparkSession,
    normalize_column_names: bool = True,
    enable_column_mapping: bool = False,
    auto_partition: bool = True,
    auto_partition_threshold_bytes: int = 1_073_741_824,
) -> tuple[str, str]:
    """Write a Lakehouse path using a pre-resolved Lakehouse base path."""
    resolved_relative_path = build_lakehouse_write_path(relative_path)
    full_path = f"{base_path}/{resolved_relative_path}"
    if resolved_relative_path != relative_path:
        log(
            f"Auto-corrected write relative_path '{relative_path}' "
            f"-> '{resolved_relative_path}'"
        )
    log(
        f"Writing to Lakehouse '{lakehouse_name}' → {full_path} "
        f"[format={format}, mode={mode}]"
    )

    # Lazy import: fabrictools.transform.columns → quality.clean → fabrictools.io
    # would otherwise create an import cycle while io.__init__ loads lakehouse.
    from fabrictools.transform.columns import (  # noqa: PLC0415
        _resolve_column_name,
        rename_columns_normalized,
    )

    if normalize_column_names:
        original_cols = list(df.columns)
        df = rename_columns_normalized(df)
        if list(df.columns) != original_cols:
            log("  Column names normalized (clean_data-style) before write")

    user_partitions = [
        p
        for p in (
            _resolve_column_name(df, col, side="DataFrame")
            for col in (partition_by or [])
        )
        if p is not None
    ]
    auto_detected_partitions = (
        _detect_partition_columns(df, threshold_bytes=auto_partition_threshold_bytes)
        if auto_partition
        else []
    )
    effective_partition_by = _dedupe_preserve_order(
        user_partitions + auto_detected_partitions
    )

    writer = df.write.format(format).option("overwriteSchema", "true").mode(mode)
    if format.lower() == "parquet":
        writer = writer.option("datetimeRebaseMode", "CORRECTED")
    elif format.lower() == "delta" and enable_column_mapping:
        # If target already exists as a Delta table, upgrade protocol first so
        # overwrite with business-friendly names (spaces, capitals, etc.) works.
        try:
            from delta.tables import DeltaTable  # type: ignore[import-untyped]  # noqa: PLC0415

            if DeltaTable.isDeltaTable(spark, full_path):
                _enable_delta_column_mapping_on_path(spark, full_path)
        except Exception:
            # Non-blocking: the write options below still apply for new tables.
            pass
        writer = (
            writer.option("delta.columnMapping.mode", "name")
            .option("delta.minReaderVersion", "2")
            .option("delta.minWriterVersion", "5")
        )

    if effective_partition_by:
        writer = writer.partitionBy(*effective_partition_by)
        if auto_detected_partitions:
            log("  Auto-detected partitions: " + ", ".join(auto_detected_partitions))
        log("  Partition columns: " + ", ".join(effective_partition_by))
    writer.save(full_path)
    log(f"  Write complete → {full_path}")
    return resolved_relative_path, full_path


# ── Parallel bulk I/O ─────────────────────────────────────────────────────────


def _resolve_max_workers(max_workers: Optional[int], request_count: int) -> int:
    if max_workers is None:
        return max(1, min(request_count, 4))
    if not isinstance(max_workers, int) or max_workers < 1:
        raise ValueError("max_workers must be an integer greater than or equal to 1.")
    return max_workers


def _validate_request_list(requests: list[dict[str, Any]], *, operation: str) -> None:
    if not isinstance(requests, list):
        raise TypeError(f"{operation} requests must be provided as a list of dicts.")
    for index, request in enumerate(requests, start=1):
        if not isinstance(request, dict):
            raise TypeError(
                f"{operation} requests[{index}] must be a dict, "
                f"got {type(request).__name__}."
            )


def _required_text_request_value(
    request: dict[str, Any], key: str, *, index: int, operation: str
) -> str:
    value = str(request.get(key, "")).strip()
    if not value:
        raise ValueError(
            f"{operation} requests[{index}] is missing required key '{key}'."
        )
    return value


def _get_cached_lakehouse_base(
    lakehouse_name: str, cache: dict[str, str], cache_lock: Lock
) -> str:
    with cache_lock:
        cached_base = cache.get(lakehouse_name)
        if cached_base is None:
            cached_base = get_lakehouse_abfs_path(lakehouse_name)
            cache[lakehouse_name] = cached_base
        return cached_base


def read_lakehouses(
    requests: list[dict[str, Any]],
    *,
    max_workers: Optional[int] = None,
    continue_on_error: Optional[bool] = False,
    spark: Optional[SparkSession] = None,
) -> dict[str, Any]:
    """Read multiple Lakehouse datasets in parallel.

    Each request must contain ``lakehouse_name`` and ``relative_path``. Optional
    keys are ``format`` (``"auto"``, ``"delta"``, ``"parquet"``, ``"csv"``)
    and ``name`` to identify the result entry.

    :param requests: Per-read parameter dictionaries.
    :param max_workers: Maximum number of concurrent read tasks. When omitted,
        uses ``min(len(requests), 4)``. Pass a value greater than ``4`` to opt in
        to higher parallelism.
    :param continue_on_error: If ``False`` (default), raise on the first failed read.
    :param spark: Optional ``SparkSession``; when omitted the active session is used.
    :type requests: list[dict]
    :type max_workers: int | None
    :type continue_on_error: bool
    :type spark: ~pyspark.sql.SparkSession | None

    :returns: Summary dict with ``DataFrame`` objects in successful ``tables`` entries.
    :rtype: dict

    .. rubric:: Example

    >>> result = read_lakehouses(  # doctest: +SKIP
    ...     [
    ...         {"name": "orders", "lakehouse_name": "BronzeLakehouse", "relative_path": "dbo.orders"},
    ...         {"name": "customers", "lakehouse_name": "BronzeLakehouse", "relative_path": "dbo.customers", "format": "delta"},
    ...     ],
    ...     max_workers=2,
    ... )
    >>> orders_df = result["tables"][0]["df"]  # doctest: +SKIP
    """
    _validate_request_list(requests, operation="read_lakehouses")
    _spark = spark or get_spark()
    total_tables = len(requests)
    if not requests:
        return {
            "total_tables": 0,
            "successful_tables": 0,
            "failed_tables": 0,
            "tables": [],
            "failures": [],
        }
    effective_max_workers = _resolve_max_workers(max_workers, total_tables)

    normalized_requests: list[dict[str, Any]] = []
    for index, request in enumerate(requests, start=1):
        normalized_requests.append(
            {
                "index": index,
                "name": request.get("name"),
                "lakehouse_name": _required_text_request_value(
                    request, "lakehouse_name", index=index, operation="read_lakehouses"
                ),
                "relative_path": _required_text_request_value(
                    request, "relative_path", index=index, operation="read_lakehouses"
                ),
                "format": str(request.get("format", "auto")).strip() or "auto",
            }
        )

    base_cache: dict[str, str] = {}
    cache_lock = Lock()

    def read_one(request: dict[str, Any]) -> dict[str, Any]:
        index = int(request["index"])
        lakehouse_name = str(request["lakehouse_name"])
        relative_path = str(request["relative_path"])
        read_format = str(request["format"])
        log(
            f"[{index}/{total_tables}] Reading Lakehouse '{lakehouse_name}' "
            f"path '{relative_path}' [format={read_format}]..."
        )
        base_path = _get_cached_lakehouse_base(lakehouse_name, base_cache, cache_lock)
        df, resolved_relative_path, full_path = _read_lakehouse_from_base(
            lakehouse_name=lakehouse_name,
            relative_path=relative_path,
            base_path=base_path,
            spark=_spark,
            format=read_format,
        )
        entry = {
            "lakehouse_name": lakehouse_name,
            "relative_path": relative_path,
            "resolved_relative_path": resolved_relative_path,
            "path": full_path,
            "format": read_format,
            "df": df,
        }
        if request.get("name") is not None:
            entry["name"] = request["name"]
        return entry

    processed_tables_by_index: dict[int, dict[str, Any]] = {}
    failures_by_index: dict[int, dict[str, str]] = {}
    with ThreadPoolExecutor(max_workers=effective_max_workers) as executor:
        future_to_request = {
            executor.submit(read_one, request): request
            for request in normalized_requests
        }
        for future in as_completed(future_to_request):
            request = future_to_request[future]
            index = int(request["index"])
            try:
                processed_tables_by_index[index] = future.result()
            except Exception as exc:
                failure = {
                    "lakehouse_name": str(request["lakehouse_name"]),
                    "relative_path": str(request["relative_path"]),
                    "format": str(request["format"]),
                    "error": str(exc),
                }
                if request.get("name") is not None:
                    failure["name"] = str(request["name"])
                failures_by_index[index] = failure
                log(
                    f"[{index}/{total_tables}] Failed to read "
                    f"'{request['relative_path']}': {exc}",
                    level="warning",
                )
                if not continue_on_error:
                    raise

    processed_tables = [
        processed_tables_by_index[index]
        for index in range(1, total_tables + 1)
        if index in processed_tables_by_index
    ]
    failures = [
        failures_by_index[index]
        for index in range(1, total_tables + 1)
        if index in failures_by_index
    ]
    return {
        "total_tables": total_tables,
        "successful_tables": len(processed_tables),
        "failed_tables": len(failures),
        "tables": processed_tables,
        "failures": failures,
    }


def write_lakehouses(
    requests: list[dict[str, Any]],
    *,
    max_workers: Optional[int] = None,
    continue_on_error: Optional[bool] = False,
    spark: Optional[SparkSession] = None,
) -> dict[str, Any]:
    """Write multiple ``DataFrame`` objects to Lakehouses in parallel.

    Each request must contain ``df``, ``lakehouse_name`` and ``relative_path``.
    Optional keys mirror :py:func:`write_lakehouse`: ``mode``, ``partition_by``,
    ``format``, ``normalize_column_names``, ``enable_column_mapping``,
    ``auto_partition`` and ``auto_partition_threshold_bytes``.

    :param requests: Per-write parameter dictionaries.
    :param max_workers: Maximum number of concurrent write tasks. When omitted,
        uses ``min(len(requests), 4)``. Pass a value greater than ``4`` to opt in
        to higher parallelism.
    :param continue_on_error: If ``False`` (default), raise on the first failed write.
    :param spark: Optional ``SparkSession``; when omitted the active session is used.
    :type requests: list[dict]
    :type max_workers: int | None
    :type continue_on_error: bool
    :type spark: ~pyspark.sql.SparkSession | None

    :returns: Summary dict with counts and per-target success/failure entries.
    :rtype: dict

    .. rubric:: Example

    >>> summary = write_lakehouses(  # doctest: +SKIP
    ...     [
    ...         {"df": orders_df, "lakehouse_name": "SilverLakehouse", "relative_path": "dbo.orders"},
    ...         {"df": customers_df, "lakehouse_name": "SilverLakehouse", "relative_path": "dbo.customers", "partition_by": ["country"]},
    ...     ],
    ...     max_workers=2,
    ... )
    """
    _validate_request_list(requests, operation="write_lakehouses")
    _spark = spark or get_spark()
    total_tables = len(requests)
    if not requests:
        return {
            "total_tables": 0,
            "successful_tables": 0,
            "failed_tables": 0,
            "tables": [],
            "failures": [],
        }
    effective_max_workers = _resolve_max_workers(max_workers, total_tables)

    normalized_requests: list[dict[str, Any]] = []
    for index, request in enumerate(requests, start=1):
        if "df" not in request:
            raise ValueError(
                f"write_lakehouses requests[{index}] is missing required key 'df'."
            )
        normalized_requests.append(
            {
                "index": index,
                "name": request.get("name"),
                "df": request["df"],
                "lakehouse_name": _required_text_request_value(
                    request, "lakehouse_name", index=index, operation="write_lakehouses"
                ),
                "relative_path": _required_text_request_value(
                    request, "relative_path", index=index, operation="write_lakehouses"
                ),
                "mode": str(request.get("mode", "overwrite")).strip() or "overwrite",
                "partition_by": request.get("partition_by"),
                "format": str(request.get("format", "delta")).strip() or "delta",
                "normalize_column_names": request.get("normalize_column_names", True),
                "enable_column_mapping": request.get("enable_column_mapping", False),
                "auto_partition": request.get("auto_partition", True),
                "auto_partition_threshold_bytes": request.get(
                    "auto_partition_threshold_bytes", 1_073_741_824
                ),
            }
        )

    base_cache: dict[str, str] = {}
    cache_lock = Lock()

    def write_one(request: dict[str, Any]) -> dict[str, Any]:
        index = int(request["index"])
        lakehouse_name = str(request["lakehouse_name"])
        relative_path = str(request["relative_path"])
        write_format = str(request["format"])
        mode = str(request["mode"])
        log(
            f"[{index}/{total_tables}] Writing Lakehouse '{lakehouse_name}' "
            f"path '{relative_path}' [format={write_format}, mode={mode}]..."
        )
        base_path = _get_cached_lakehouse_base(lakehouse_name, base_cache, cache_lock)
        resolved_relative_path, full_path = _write_lakehouse_to_base(
            df=request["df"],
            lakehouse_name=lakehouse_name,
            relative_path=relative_path,
            base_path=base_path,
            mode=mode,
            partition_by=request.get("partition_by"),
            format=write_format,
            spark=_spark,
            normalize_column_names=bool(request.get("normalize_column_names")),
            enable_column_mapping=bool(request.get("enable_column_mapping")),
            auto_partition=bool(request.get("auto_partition")),
            auto_partition_threshold_bytes=int(
                request["auto_partition_threshold_bytes"]
            ),
        )
        entry = {
            "lakehouse_name": lakehouse_name,
            "relative_path": relative_path,
            "resolved_relative_path": resolved_relative_path,
            "path": full_path,
            "format": write_format,
            "mode": mode,
        }
        if request.get("name") is not None:
            entry["name"] = request["name"]
        return entry

    processed_tables_by_index: dict[int, dict[str, Any]] = {}
    failures_by_index: dict[int, dict[str, str]] = {}
    with ThreadPoolExecutor(max_workers=effective_max_workers) as executor:
        future_to_request = {
            executor.submit(write_one, request): request
            for request in normalized_requests
        }
        for future in as_completed(future_to_request):
            request = future_to_request[future]
            index = int(request["index"])
            try:
                processed_tables_by_index[index] = future.result()
            except Exception as exc:
                failure = {
                    "lakehouse_name": str(request["lakehouse_name"]),
                    "relative_path": str(request["relative_path"]),
                    "format": str(request["format"]),
                    "mode": str(request["mode"]),
                    "error": str(exc),
                }
                if request.get("name") is not None:
                    failure["name"] = str(request["name"])
                failures_by_index[index] = failure
                log(
                    f"[{index}/{total_tables}] Failed to write "
                    f"'{request['relative_path']}': {exc}",
                    level="warning",
                )
                if not continue_on_error:
                    raise

    processed_tables = [
        processed_tables_by_index[index]
        for index in range(1, total_tables + 1)
        if index in processed_tables_by_index
    ]
    failures = [
        failures_by_index[index]
        for index in range(1, total_tables + 1)
        if index in failures_by_index
    ]
    return {
        "total_tables": total_tables,
        "successful_tables": len(processed_tables),
        "failed_tables": len(failures),
        "tables": processed_tables,
        "failures": failures,
    }


# ── Merge (upsert) ────────────────────────────────────────────────────────────


def merge_lakehouse(
    source_df: DataFrame,
    lakehouse_name: str,
    relative_path: str,
    merge_condition: str,
    update_set: Optional[dict] = None,
    insert_set: Optional[dict] = None,
    spark: Optional[SparkSession] = None,
) -> None:
    """Upsert (merge) a ``DataFrame`` into an existing Delta table in a Lakehouse.

    Uses Delta Lake ``DeltaTable.forPath``. When ``update_set`` and/or ``insert_set``
    are ``None``, ``whenMatchedUpdateAll`` / ``whenNotMatchedInsertAll`` are used.

    :param source_df: Rows to merge into the target table.
    :param lakehouse_name: Lakehouse display name holding the target table.
    :param relative_path: Path of the Delta table inside the Lakehouse (same rules as
        :py:func:`write_lakehouse`, including ``schema.table`` with spaces).
    :param merge_condition: SQL predicate joining source and target (e.g.
        ``"src.id = tgt.id"``).
    :param update_set: ``{target_col: source_expr}`` for matched rows, or ``None``
        to update all columns.
    :param insert_set: ``{target_col: source_expr}`` for new rows, or ``None`` to
        insert all columns.
    :param spark: Optional ``SparkSession``; when omitted the active session is used.
    :type source_df: ~pyspark.sql.DataFrame
    :type lakehouse_name: str
    :type relative_path: str
    :type merge_condition: str
    :type update_set: dict | None
    :type insert_set: dict | None
    :type spark: ~pyspark.sql.SparkSession | None

    .. rubric:: Example

    >>> merge_lakehouse(  # doctest: +SKIP
    ...     new_df,
    ...     "SilverLakehouse",
    ...     "sales_clean",
    ...     merge_condition="src.id = tgt.id",
    ... )
    """
    from delta.tables import DeltaTable  # type: ignore[import-untyped]  # noqa: PLC0415

    _spark = spark or get_spark()
    base = get_lakehouse_abfs_path(lakehouse_name)
    resolved_relative_path = build_lakehouse_write_path(relative_path)
    full_path = f"{base}/{resolved_relative_path}"
    if resolved_relative_path != relative_path:
        log(
            f"Auto-corrected merge relative_path '{relative_path}' "
            f"-> '{resolved_relative_path}'"
        )
    log(f"Merging into Lakehouse '{lakehouse_name}' → {full_path}")
    log(f"  Condition: {merge_condition}")

    target = DeltaTable.forPath(_spark, full_path)
    merge_builder = target.alias("tgt").merge(source_df.alias("src"), merge_condition)

    if update_set is not None:
        merge_builder = merge_builder.whenMatchedUpdate(set=update_set)
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    if insert_set is not None:
        merge_builder = merge_builder.whenNotMatchedInsert(values=insert_set)
    else:
        merge_builder = merge_builder.whenNotMatchedInsertAll()

    merge_builder.execute()
    log("  Merge complete")


def lakehouse_table_exists(
    lakehouse_name: str,
    relative_path: str,
) -> bool:
    """Check if a table or path exists in a Fabric Lakehouse.

    Uses ``notebookutils.fs.exists`` to check candidate paths without reading data.

    :param lakehouse_name: Display name of the Lakehouse.
    :param relative_path: Logical path under the Lakehouse root (slash path or
        SQL-style ``schema.table``, e.g. ``dbo.PdC Extraction``).
    :type lakehouse_name: str
    :type relative_path: str

    :returns: ``True`` if at least one candidate path exists, ``False`` otherwise.
    :rtype: bool

    :raises ValueError: When ``notebookutils`` is unavailable (not in Fabric).

    .. rubric:: Example

    >>> if lakehouse_table_exists("BronzeLakehouse", "dbo.SalesOrders"):  # doctest: +SKIP
    ...     print("Table exists!")
    """
    try:
        import notebookutils  # type: ignore[import-untyped]  # noqa: PLC0415
    except ImportError as exc:
        raise ValueError(
            "notebookutils is not available — are you running inside "
            f"Microsoft Fabric? ({exc})"
        ) from exc

    base = get_lakehouse_abfs_path(lakehouse_name)
    candidate_relative_paths = build_lakehouse_read_path_candidates(relative_path)

    for candidate_relative_path in candidate_relative_paths:
        full_path = f"{base}/{candidate_relative_path}"
        try:
            if notebookutils.fs.exists(full_path):
                return True
        except Exception:
            pass

    return False


def delete_all_lakehouse_tables(
    lakehouse_name: str,
    include_schemas: Optional[List[str]] = None,
    exclude_tables: Optional[List[str]] = None,
    continue_on_error: bool = False,
) -> dict[str, Any]:
    """Hard-delete all discovered Lakehouse table folders.

    Tables are discovered as ``Tables/<schema>/<table>`` and removed with
    ``notebookutils.fs.rm(<abfs>/Tables/<schema>/<table>, recurse=True)``.

    :param lakehouse_name: Lakehouse display name to purge.
    :param include_schemas: If set, only these schema names (case-insensitive).
    :param exclude_tables: Table or ``schema.table`` names to skip (case-insensitive).
    :param continue_on_error: If ``False`` (default), stop on first delete failure.
    :type lakehouse_name: str
    :type include_schemas: list[str] | None
    :type exclude_tables: list[str] | None
    :type continue_on_error: bool

    :returns: Summary with counts and per-table ``relative_path`` / errors.
    :rtype: dict

    :raises ValueError: When ``notebookutils`` is unavailable (not in Fabric).

    .. rubric:: Example

    >>> summary = delete_all_lakehouse_tables(  # doctest: +SKIP
    ...     "DevLakehouse",
    ...     include_schemas=["dbo"],
    ...     exclude_tables=["dbo.KeepThis"],
    ...     continue_on_error=True,
    ... )
    """
    try:
        import notebookutils  # type: ignore[import-untyped]  # noqa: PLC0415
    except ImportError as exc:
        raise ValueError(
            "notebookutils is not available — are you running inside "
            f"Microsoft Fabric? ({exc})"
        ) from exc

    base = get_lakehouse_abfs_path(lakehouse_name)
    table_paths = list_lakehouse_tables(
        lakehouse_name=lakehouse_name,
        include_schemas=include_schemas,
        exclude_tables=exclude_tables,
    )

    if not table_paths:
        log(
            f"No tables found in Lakehouse '{lakehouse_name}' for purge.",
            level="warning",
        )
        return {
            "total_tables": 0,
            "deleted_tables": 0,
            "failed_tables": 0,
            "tables": [],
            "failures": [],
        }

    deleted_entries: list[dict[str, str]] = []
    failure_entries: list[dict[str, str]] = []
    total_tables = len(table_paths)

    for index, table_relative_path in enumerate(table_paths, start=1):
        try:
            full_path = f"{base}/{table_relative_path}"
            log(f"[{index}/{total_tables}] Hard-deleting table path '{full_path}'...")
            notebookutils.fs.rm(full_path, recurse=True)
            deleted_entries.append(
                {
                    "relative_path": table_relative_path,
                    "path": full_path,
                }
            )
        except Exception as exc:
            failure_entries.append(
                {
                    "relative_path": table_relative_path,
                    "path": f"{base}/{table_relative_path}",
                    "error": str(exc),
                }
            )
            log(
                f"[{index}/{total_tables}] Failed to hard-delete '{table_relative_path}': {exc}",
                level="warning",
            )
            if not continue_on_error:
                raise

    return {
        "total_tables": total_tables,
        "deleted_tables": len(deleted_entries),
        "failed_tables": len(failure_entries),
        "tables": deleted_entries,
        "failures": failure_entries,
    }


__all__ = [
    "read_lakehouse",
    "read_lakehouses",
    "resolve_lakehouse_read_candidate",
    "write_lakehouse",
    "write_lakehouses",
    "merge_lakehouse",
    "lakehouse_table_exists",
    "delete_all_lakehouse_tables",
]
