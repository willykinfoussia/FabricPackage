"""Fast parameter lookup helpers for small and large PySpark DataFrames.

``ParamResolver`` is designed for DataFrames shaped like ``(parametre, valeur)``.
It chooses between a driver-side dictionary for small tables and Spark-side
lookups for larger tables.

.. rubric:: Examples

>>> resolver = ParamResolver(df_params, small_threshold=10_000)  # doctest: +SKIP
>>> threshold = resolver.get("x")  # doctest: +SKIP

For repeated enrichment of a large DataFrame, prefer a join instead of many
driver lookups:

>>> enriched = resolver.enrich(df_data, key_col="parametre")  # doctest: +SKIP
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Protocol
import warnings

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

RowCountStrategy = Literal["estimate", "count", "auto"]
DuplicatePolicy = Literal["first", "last", "max", "min", "error"]
LookupMode = Literal["local_dict", "spark_filter", "spark_map"]

__all__ = ["ParamResolver", "RowCountInfo"]


class _LoggerLike(Protocol):
    def debug(self, message: str, *args: Any, **kwargs: Any) -> None: ...

    def info(self, message: str, *args: Any, **kwargs: Any) -> None: ...

    def warning(self, message: str, *args: Any, **kwargs: Any) -> None: ...


@dataclass(frozen=True)
class RowCountInfo:
    """Information about the detected size of the parameter DataFrame."""

    value: int | None
    source: str
    exact: bool


class ParamResolver:
    """Resolve parameter values from a Spark DataFrame with adaptive caching.

    The resolver accepts a two-column parameter DataFrame, deduplicates it
    according to ``duplicate_policy``, then chooses a lookup strategy.

    Performance notes:

    * Small parameter tables are collected once into a Python ``dict``. Startup
      cost is O(n), driver memory is O(n), and each later ``get()`` is O(1)
      without launching another Spark job.
    * Medium and large parameter tables stay in Spark. Each ``get()`` uses
      ``filter + select + first`` and launches one Spark job. Use this for
      occasional lookups only.
    * ``spark_map_cache=True`` builds a single-row Spark ``MapType`` cache via
      ``map_from_entries(collect_list(...))``. It is useful when parameter
      resolution should happen inside Spark expressions, but it aggregates all
      parameters and should be enabled deliberately.
    * For enriching a large DataFrame, use :meth:`enrich`, which performs a
      Spark join and can broadcast the deduplicated parameter table.

    If ``duplicate_policy="last"`` or ``"first"`` is used without ``order_col``,
    Spark has no stable row order. A warning is emitted because the chosen value
    is not deterministic across executions.

    .. rubric:: Example

    >>> resolver = ParamResolver(  # doctest: +SKIP
    ...     df_params,
    ...     key_col="parametre",
    ...     value_col="valeur",
    ...     small_threshold=10_000,
    ...     row_count_strategy="auto",
    ...     duplicate_policy="last",
    ...     order_col="ts",
    ... )
    >>> x_value = resolver.get("x")  # doctest: +SKIP
    >>> enriched = resolver.enrich(  # doctest: +SKIP
    ...     df_data,
    ...     key_col="parametre",
    ...     output_col="param_value",
    ... )

    :param df_params: Parameter DataFrame.
    :param key_col: Column containing parameter names.
    :param value_col: Column containing parameter values.
    :param small_threshold: Maximum row count for driver-side dictionary mode.
    :param row_count_strategy: ``"estimate"``, ``"count"``, or ``"auto"``.
    :param duplicate_policy: How to resolve duplicate keys.
    :param order_col: Optional ordering column for ``first`` and ``last``.
    :param persist_params: Persist the deduplicated parameter DataFrame.
    :param spark_map_cache: Build a Spark ``MapType`` cache for Spark-side use.
    :param broadcast_local_map: Broadcast the local dictionary when using the
        small-table strategy.
    :param allow_count_fallback: Allow ``auto`` to run an exact ``count()`` when
        estimates and bounded counts are insufficient.
    :param estimate_sample_fraction: Optional sampling fraction used by
        ``row_count_strategy="estimate"`` when Catalyst statistics are missing.
        Sampling still launches a Spark job; leave as ``None`` to avoid it.
    :param logger: Optional logger with ``debug``, ``info`` and ``warning``.
    """

    def __init__(
        self,
        df_params: DataFrame,
        *,
        key_col: str = "parametre",
        value_col: str = "valeur",
        small_threshold: int = 10_000,
        row_count_strategy: RowCountStrategy = "auto",
        duplicate_policy: DuplicatePolicy = "last",
        order_col: str | None = None,
        persist_params: bool = False,
        spark_map_cache: bool = False,
        broadcast_local_map: bool = True,
        allow_count_fallback: bool = True,
        estimate_sample_fraction: float | None = None,
        logger: _LoggerLike | None = None,
    ) -> None:
        self.df_params = df_params
        self.key_col = key_col
        self.value_col = value_col
        self.small_threshold = small_threshold
        self.row_count_strategy = row_count_strategy
        self.duplicate_policy = duplicate_policy
        self.order_col = order_col
        self.persist_params = persist_params
        self.spark_map_cache = spark_map_cache
        self.broadcast_local_map = broadcast_local_map
        self.allow_count_fallback = allow_count_fallback
        self.estimate_sample_fraction = estimate_sample_fraction
        self.logger = logger

        self._row_count_info: RowCountInfo | None = None
        self._params: DataFrame | None = None
        self._lookup_mode: LookupMode | None = None
        self._local_map: dict[Any, Any] | None = None
        self._broadcast = None
        self._spark_map_df: DataFrame | None = None

        self._validate_options()
        self._validate_columns()

    @property
    def row_count_info(self) -> RowCountInfo:
        """Return cached row-count information, computing it once if needed."""
        return self._resolve_row_count()

    @property
    def lookup_mode(self) -> LookupMode | None:
        """Return the selected lookup mode after the first lookup, if any."""
        return self._lookup_mode

    def get(self, key: Any, default: Any = None) -> Any:
        """Return the value associated with ``key``.

        For small parameter tables this method initializes a local dictionary on
        first use and then avoids further Spark jobs. For larger parameter
        tables it performs a Spark ``filter + select + first`` lookup per call.

        :param key: Parameter key to resolve.
        :param default: Value returned when the key is absent.
        :returns: The resolved parameter value or ``default``.
        """
        self._ensure_lookup_strategy()

        if self._lookup_mode == "local_dict":
            values = self._broadcast.value if self._broadcast is not None else self._local_map
            if values is None:
                return default
            return values.get(key, default)

        row = (
            self._deduplicated_params()
            .filter(F.col(self.key_col) == F.lit(key))
            .select(self.value_col)
            .first()
        )
        if row is None:
            return default
        return row[0]

    def spark_map(self) -> DataFrame:
        """Return a one-row DataFrame containing a Spark ``MapType`` column.

        The returned DataFrame has one column named ``params_map``. Building it
        aggregates the deduplicated parameters into a single Spark map and
        materializes it once with ``count()`` when ``spark_map_cache`` is enabled.
        """
        if self._spark_map_df is None:
            self._spark_map_df = self._build_spark_map()
            if self.spark_map_cache:
                self._spark_map_df = self._spark_map_df.persist(StorageLevel.MEMORY_AND_DISK)
                self._spark_map_df.count()
            self._log("debug", "Built Spark parameter map cache")
        return self._spark_map_df

    def enrich(
        self,
        df_data: DataFrame,
        *,
        key_col: str = "parametre",
        output_col: str | None = None,
        how: str = "left",
        broadcast_params: bool = True,
    ) -> DataFrame:
        """Join parameter values onto ``df_data``.

        This is the recommended strategy when a large DataFrame needs parameter
        values in Spark. It avoids repeated driver lookups and can broadcast the
        deduplicated parameter table.

        :param df_data: DataFrame to enrich.
        :param key_col: Key column in ``df_data``.
        :param output_col: Output value column. Defaults to ``value_col``.
        :param how: Spark join type.
        :param broadcast_params: Wrap the parameter table with ``broadcast()``.
        :returns: Enriched DataFrame.
        """
        if key_col not in df_data.columns:
            raise ValueError(f"Column not found in df_data: {key_col!r}")

        value_name = output_col or self.value_col
        params = self._deduplicated_params().select(
            F.col(self.key_col).alias("__paramresolver_key"),
            F.col(self.value_col).alias(value_name),
        )
        if broadcast_params:
            params = F.broadcast(params)

        return (
            df_data.join(
                params,
                df_data[key_col] == params["__paramresolver_key"],
                how=how,
            )
            .drop("__paramresolver_key")
        )

    def clear(self) -> None:
        """Release broadcast and persisted Spark caches owned by this resolver."""
        if self._broadcast is not None:
            self._broadcast.unpersist()
            self._broadcast = None
        if self._params is not None and self.persist_params:
            self._params.unpersist()
        if self._spark_map_df is not None and self.spark_map_cache:
            self._spark_map_df.unpersist()
        self._local_map = None
        self._spark_map_df = None
        self._lookup_mode = None

    def _validate_options(self) -> None:
        if self.small_threshold < 0:
            raise ValueError("small_threshold must be >= 0")
        if self.key_col == self.value_col:
            raise ValueError("key_col and value_col must be different")
        if self.row_count_strategy not in {"estimate", "count", "auto"}:
            raise ValueError(
                "row_count_strategy must be one of 'estimate', 'count', or 'auto'"
            )
        if self.duplicate_policy not in {"first", "last", "max", "min", "error"}:
            raise ValueError(
                "duplicate_policy must be one of 'first', 'last', 'max', 'min', or 'error'"
            )
        if self.estimate_sample_fraction is not None and not (
            0 < self.estimate_sample_fraction <= 1
        ):
            raise ValueError("estimate_sample_fraction must be in ]0, 1]")

    def _validate_columns(self) -> None:
        required = {self.key_col, self.value_col}
        if self.order_col is not None:
            required.add(self.order_col)
        missing = sorted(required.difference(self.df_params.columns))
        if missing:
            raise ValueError(f"Missing column(s) in df_params: {missing!r}")

    def _deduplicated_params(self) -> DataFrame:
        if self._params is None:
            self._params = self._deduplicate_params()
            if self.persist_params:
                self._params = self._params.persist(StorageLevel.MEMORY_AND_DISK)
                self._log("debug", "Persisted deduplicated parameter DataFrame")
        return self._params

    def _deduplicate_params(self) -> DataFrame:
        df = self.df_params.select(self.key_col, self.value_col, *self._extra_order_cols())

        if self.duplicate_policy == "error":
            duplicates = (
                df.groupBy(self.key_col)
                .count()
                .filter(F.col("count") > F.lit(1))
                .select(self.key_col)
                .limit(5)
                .collect()
            )
            if duplicates:
                examples = [row[0] for row in duplicates]
                raise ValueError(
                    f"Duplicate parameter keys found with duplicate_policy='error': {examples!r}"
                )
            return df.select(self.key_col, self.value_col)

        if self.duplicate_policy in {"max", "min"}:
            agg_func = F.max if self.duplicate_policy == "max" else F.min
            return df.groupBy(self.key_col).agg(agg_func(self.value_col).alias(self.value_col))

        if self.order_col is None:
            warnings.warn(
                f"duplicate_policy={self.duplicate_policy!r} without order_col is not deterministic",
                RuntimeWarning,
                stacklevel=2,
            )
            self._log(
                "warning",
                "duplicate_policy=%s without order_col is not deterministic",
                self.duplicate_policy,
            )
            order_expr = F.monotonically_increasing_id()
        else:
            order_expr = F.col(self.order_col)

        if self.duplicate_policy == "last":
            order_expr = order_expr.desc()
        else:
            order_expr = order_expr.asc()

        window = Window.partitionBy(self.key_col).orderBy(order_expr)
        return (
            df.withColumn("__paramresolver_rank", F.row_number().over(window))
            .filter(F.col("__paramresolver_rank") == F.lit(1))
            .select(self.key_col, self.value_col)
        )

    def _extra_order_cols(self) -> list[str]:
        if self.order_col is None or self.order_col in {self.key_col, self.value_col}:
            return []
        return [self.order_col]

    def _resolve_row_count(self) -> RowCountInfo:
        if self._row_count_info is not None:
            return self._row_count_info

        if self.row_count_strategy == "count":
            self._row_count_info = RowCountInfo(
                value=self._deduplicated_params().count(),
                source="count",
                exact=True,
            )
            self._log("info", "Resolved parameter row count with count(): %s", self._row_count_info.value)
            return self._row_count_info

        stats_count = self._estimate_count_from_plan()
        if stats_count is not None:
            self._row_count_info = RowCountInfo(
                value=stats_count,
                source="plan_stats",
                exact=False,
            )
            self._log("info", "Resolved parameter row count from plan stats: %s", stats_count)
            return self._row_count_info

        if self.row_count_strategy == "estimate":
            sampled = self._estimate_count_from_sample()
            if sampled is not None:
                self._row_count_info = RowCountInfo(
                    value=sampled,
                    source="sample",
                    exact=False,
                )
            else:
                self._row_count_info = RowCountInfo(value=None, source="unavailable", exact=False)
            return self._row_count_info

        bounded = self._deduplicated_params().limit(self.small_threshold + 1).count()
        if bounded <= self.small_threshold:
            self._row_count_info = RowCountInfo(
                value=bounded,
                source="bounded_count",
                exact=True,
            )
        elif self.allow_count_fallback:
            self._row_count_info = RowCountInfo(
                value=self._deduplicated_params().count(),
                source="count_fallback",
                exact=True,
            )
        else:
            self._row_count_info = RowCountInfo(
                value=bounded,
                source="bounded_count_exceeded",
                exact=False,
            )
        self._log(
            "info",
            "Resolved parameter row count via %s: %s",
            self._row_count_info.source,
            self._row_count_info.value,
        )
        return self._row_count_info

    def _estimate_count_from_plan(self) -> int | None:
        try:
            stats = self._deduplicated_params()._jdf.queryExecution().optimizedPlan().stats()
            row_count = stats.rowCount()
            if row_count.isDefined():
                return int(str(row_count.get()))
        except Exception as exc:  # pragma: no cover - depends on Spark/JVM internals
            self._log("debug", "Could not read Catalyst row-count stats: %s", exc)
        return None

    def _estimate_count_from_sample(self) -> int | None:
        if self.estimate_sample_fraction is None:
            return None
        sampled = self._deduplicated_params().sample(
            withReplacement=False,
            fraction=self.estimate_sample_fraction,
        )
        sample_count = sampled.count()
        return int(round(sample_count / self.estimate_sample_fraction))

    def _ensure_lookup_strategy(self) -> None:
        if self._lookup_mode is not None:
            return

        row_count = self._resolve_row_count()
        if self.spark_map_cache:
            self.spark_map()
        if row_count.value is not None and row_count.value <= self.small_threshold:
            self._build_local_map()
            self._lookup_mode = "local_dict"
        else:
            self._lookup_mode = "spark_filter"
        self._log("info", "Selected ParamResolver lookup mode: %s", self._lookup_mode)

    def _build_local_map(self) -> None:
        values = {
            row[self.key_col]: row[self.value_col]
            for row in self._deduplicated_params().select(self.key_col, self.value_col).collect()
        }
        self._local_map = values
        if self.broadcast_local_map:
            spark_context = self.df_params.sparkSession.sparkContext
            self._broadcast = spark_context.broadcast(values)

    def _build_spark_map(self) -> DataFrame:
        return self._deduplicated_params().agg(
            F.map_from_entries(
                F.collect_list(F.struct(F.col(self.key_col), F.col(self.value_col)))
            ).alias("params_map")
        )

    def _log(self, level: str, message: str, *args: Any) -> None:
        if self.logger is None:
            return
        log_func = getattr(self.logger, level, None)
        if log_func is not None:
            log_func(message, *args)
