"""Canonical table-job parsing for bulk pipelines."""

from __future__ import annotations

import re
from typing import Any, Callable, Iterable, Optional, TypedDict


class TableJobConfig(TypedDict, total=False):
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
    tokens = [token for token in re.split(r"[^0-9A-Za-z]+", value) if token]
    return "".join(token[:1].upper() + token[1:] for token in tokens)


def _strip_technical_table_prefix(table_name: str) -> str:
    lowered = table_name.lower()
    for prefix in _TECHNICAL_TABLE_PREFIXES:
        if lowered.startswith(prefix):
            return table_name[len(prefix):]
    return table_name


def _to_business_target_relative_path(relative_path: str) -> str:
    parts = [segment for segment in relative_path.strip().strip("/").split("/") if segment]
    if not parts:
        return relative_path

    table_segment = parts[-1]
    stripped_name = _strip_technical_table_prefix(table_segment)
    business_table_name = _to_pascal_case_identifier(stripped_name)
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
) -> list[TableJobConfig]:
    """Normalize heterogeneous config entries into canonical table jobs."""
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
            target_relative_path = _to_business_target_relative_path(source_relative_path)

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
) -> list[TableJobConfig]:
    """Create canonical jobs from table discovery."""
    discovered = discover_fn(
        lakehouse_name=source_lakehouse_name,
        include_schemas=include_schemas,
        exclude_tables=exclude_tables,
    )
    return [
        TableJobConfig(
            source_relative_path=relative_path,
            target_relative_path=_to_business_target_relative_path(relative_path),
            mode=mode,
            partition_by=partition_by,
            merge_condition=None,
        )
        for relative_path in discovered
    ]

