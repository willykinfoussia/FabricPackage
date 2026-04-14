"""Canonical table-job parsing for bulk pipelines."""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Callable, Iterable, Optional, TypedDict


class TableJobConfig(TypedDict, total=False):
    """Canonical keys for one bulk pipeline table job."""

    source_relative_path: str
    target_relative_path: str
    mode: str
    partition_by: Optional[list[str]]
    merge_condition: Optional[str]


_TECHNICAL_TABLE_PREFIXES = ("dimension_", "fact_")


def _pick_first_non_empty(item: dict[str, Any], keys: Iterable[str]) -> str:
    for key in keys:
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _to_pascal_case_identifier(value: str) -> str:
    normalized_value = unicodedata.normalize("NFKD", value)
    ascii_value = normalized_value.encode("ascii", "ignore").decode("ascii")
    tokens = [token for token in re.split(r"[^0-9A-Za-z]+", ascii_value) if token]
    return "".join(token[:1].upper() + token[1:] for token in tokens)


def _strip_technical_table_prefix(table_name: str) -> str:
    lowered = table_name.lower()
    for prefix in _TECHNICAL_TABLE_PREFIXES:
        if lowered.startswith(prefix):
            return table_name[len(prefix):]
    return table_name


def _to_business_target_relative_path(
    relative_path: str, *, cleaned_table_prefix: bool = False
) -> str:
    """Derive a Lakehouse table path from a source path (same folders, new leaf).

    By default the leaf is PascalCase from the source table name. When
    ``cleaned_table_prefix`` is ``True`` (Silver / cleaned outputs), the leaf is
    ``Cleaned_`` + that PascalCase, e.g. ``Tables/dbo/projets table`` →
    ``Tables/dbo/Cleaned_ProjetsTable``.

    :param relative_path: Source ``Tables/...`` path.
    :param cleaned_table_prefix: If ``True``, enforce ``Cleaned_`` prefix on the leaf.
    :type relative_path: str
    :type cleaned_table_prefix: bool

    :returns: Target relative path string.
    :rtype: str
    """
    parts = [segment for segment in relative_path.strip().strip("/").split("/") if segment]
    if not parts:
        return relative_path

    table_segment = parts[-1]
    stripped_name = _strip_technical_table_prefix(table_segment)
    business_table_name = _to_pascal_case_identifier(stripped_name)
    pascal_table = business_table_name or _to_pascal_case_identifier(table_segment)

    if cleaned_table_prefix:
        final_segment = pascal_table or table_segment
        lower = final_segment.lower()
        if lower.startswith("cleaned_"):
            parts[-1] = final_segment
        else:
            parts[-1] = f"Cleaned_{final_segment}"
    else:
        parts[-1] = business_table_name or table_segment
    return "/".join(parts)


def build_table_jobs_from_config(
    *,
    tables_config: list[dict[str, Any]],
    default_mode: str,
    default_partition_by: Optional[list[str]] = None,
    supported_modes: set[str],
    source_keys: tuple[str, ...] = ("source_relative_path", "source_path", "source_table", "bronze_path"),
    target_keys: tuple[str, ...] = ("target_relative_path", "target_path", "target_table", "prepared_table", "silver_table"),
    require_target: bool = False,
    require_mode: bool = False,
    allow_merge_condition: bool = False,
    cleaned_table_prefix: bool = False,
) -> list[TableJobConfig]:
    """Normalize heterogeneous ``tables_config`` dicts into :class:`TableJobConfig` rows.

    :param tables_config: Per-table configuration dicts.
    :param default_mode: Mode used when an entry omits ``mode`` (unless ``require_mode``).
    :param default_partition_by: Default ``partition_by`` when omitted per entry.
    :param supported_modes: Allowed mode strings (e.g. ``overwrite``, ``append``, ``merge``).
    :param source_keys: Keys tried in order to read the source path.
    :param target_keys: Keys tried in order to read the target path.
    :param require_target: If ``True``, each entry must specify a target key.
    :param require_mode: If ``True``, each entry must include ``mode``.
    :param allow_merge_condition: If ``True``, parse ``merge_condition`` and require it for ``merge``.
    :param cleaned_table_prefix: Passed to target path derivation when target is inferred.
    :type tables_config: list[dict]
    :type default_mode: str
    :type default_partition_by: list[str] | None
    :type supported_modes: set[str]
    :type source_keys: tuple[str, ...]
    :type target_keys: tuple[str, ...]
    :type require_target: bool
    :type require_mode: bool
    :type allow_merge_condition: bool
    :type cleaned_table_prefix: bool

    :returns: List of normalized job dicts.
    :rtype: list

    :raises ValueError: On invalid entries, unsupported modes, or missing merge condition.
    """
    jobs: list[TableJobConfig] = []
    for index, table_config in enumerate(tables_config, start=1):
        if not isinstance(table_config, dict):
            raise ValueError(
                f"tables_config[{index}] must be a dict, got {type(table_config).__name__}."
            )

        source_relative_path = _pick_first_non_empty(table_config, source_keys)
        if not source_relative_path:
            raise ValueError(f"tables_config[{index}] is missing a source path key.")

        target_relative_path = _pick_first_non_empty(table_config, target_keys)
        if not target_relative_path:
            if require_target:
                raise ValueError(f"tables_config[{index}] is missing a target path key.")
            target_relative_path = _to_business_target_relative_path(
                source_relative_path, cleaned_table_prefix=cleaned_table_prefix
            )

        if require_mode:
            mode_value = str(table_config.get("mode", "")).strip().lower()
            if not mode_value:
                raise ValueError(f"tables_config[{index}] is missing required key 'mode'.")
        else:
            mode_value = str(table_config.get("mode", default_mode)).strip().lower()
            if not mode_value:
                mode_value = default_mode

        if mode_value not in supported_modes:
            raise ValueError(
                f"tables_config[{index}] has unsupported mode '{mode_value}'. "
                f"Supported modes: {', '.join(sorted(supported_modes))}."
            )

        if "partition_by" in table_config:
            raw_partition = table_config["partition_by"]
            if raw_partition is None:
                partition_by = None
            elif isinstance(raw_partition, list):
                partition_by = raw_partition
            else:
                raise ValueError(
                    f"tables_config[{index}] key 'partition_by' must be a list or None."
                )
        else:
            partition_by = default_partition_by

        merge_condition = None
        if allow_merge_condition:
            raw_merge_condition = table_config.get("merge_condition")
            merge_condition = (
                str(raw_merge_condition).strip()
                if raw_merge_condition is not None
                else None
            )
            if mode_value == "merge" and not merge_condition:
                raise ValueError(
                    f"tables_config[{index}] mode='merge' requires 'merge_condition'."
                )

        jobs.append(
            TableJobConfig(
                source_relative_path=source_relative_path,
                target_relative_path=target_relative_path,
                mode=mode_value,
                partition_by=partition_by,
                merge_condition=merge_condition,
            )
        )
    return jobs


def build_table_jobs_from_discovery(
    *,
    source_lakehouse_name: str,
    discover_fn: Callable[..., list[str]],
    include_schemas: Optional[list[str]],
    exclude_tables: Optional[list[str]],
    mode: str,
    partition_by: Optional[list[str]] = None,
    cleaned_table_prefix: bool = False,
) -> list[TableJobConfig]:
    """Build :class:`TableJobConfig` rows by listing tables then deriving target paths.

    :param source_lakehouse_name: Lakehouse passed to ``discover_fn``.
    :param discover_fn: Callable like :py:func:`fabrictools.io.discovery.list_lakehouse_tables_for_pipeline`.
    :param include_schemas: Forwarded to ``discover_fn``.
    :param exclude_tables: Forwarded to ``discover_fn``.
    :param mode: Write mode for every generated job.
    :param partition_by: Optional partition columns for every job.
    :param cleaned_table_prefix: When ``True``, target leaf uses ``Cleaned_`` prefix logic.
    :type source_lakehouse_name: str
    :type discover_fn: collections.abc.Callable
    :type include_schemas: list[str] | None
    :type exclude_tables: list[str] | None
    :type mode: str
    :type partition_by: list[str] | None
    :type cleaned_table_prefix: bool

    :returns: One job per discovered relative path.
    :rtype: list
    """
    discovered = discover_fn(
        lakehouse_name=source_lakehouse_name,
        include_schemas=include_schemas,
        exclude_tables=exclude_tables,
    )
    return [
        TableJobConfig(
            source_relative_path=relative_path,
            target_relative_path=_to_business_target_relative_path(
                relative_path, cleaned_table_prefix=cleaned_table_prefix
            ),
            mode=mode,
            partition_by=partition_by,
            merge_condition=None,
        )
        for relative_path in discovered
    ]

