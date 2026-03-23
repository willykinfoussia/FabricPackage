"""Lakehouse filesystem discovery utilities."""

from __future__ import annotations

from typing import Any, List, Optional

from fabrictools.core.paths import get_lakehouse_abfs_path


def get_fs_entry_name(fs_entry: Any) -> str:
    """Extract a clean directory/file name from a notebookutils.fs.ls entry."""
    raw_name = getattr(fs_entry, "name", "")
    if raw_name:
        return str(raw_name).strip().strip("/")

    raw_path = getattr(fs_entry, "path", "")
    if raw_path:
        return str(raw_path).strip().strip("/").split("/")[-1]

    return ""


def list_lakehouse_tables(
    lakehouse_name: str,
    include_schemas: Optional[List[str]] = None,
    exclude_tables: Optional[List[str]] = None,
) -> List[str]:
    """
    List table relative paths from a Lakehouse as ``Tables/<schema>/<table>``.

    Discovery is file-system based, by scanning ``<abfs>/Tables/<schema>/<table>``.
    """
    try:
        import notebookutils  # type: ignore[import-untyped]  # noqa: PLC0415
    except ImportError as exc:
        raise ValueError(
            "notebookutils is not available — are you running inside "
            f"Microsoft Fabric? ({exc})"
        ) from exc

    included_schema_names = (
        {schema_name.strip().lower() for schema_name in include_schemas}
        if include_schemas
        else None
    )
    excluded_table_names = {
        table_name.strip().lower()
        for table_name in (exclude_tables or [])
    }

    base = get_lakehouse_abfs_path(lakehouse_name)
    tables_root = f"{base}/Tables"
    discovered_table_paths: List[str] = []

    for schema_entry in notebookutils.fs.ls(tables_root):
        schema_name = get_fs_entry_name(schema_entry)
        if not schema_name:
            continue
        schema_name_lower = schema_name.lower()
        if included_schema_names is not None and schema_name_lower not in included_schema_names:
            continue

        schema_path = getattr(schema_entry, "path", f"{tables_root}/{schema_name}")
        for table_entry in notebookutils.fs.ls(schema_path):
            table_name = get_fs_entry_name(table_entry)
            if not table_name:
                continue

            qualified_table_name = f"{schema_name_lower}.{table_name.lower()}"
            if (
                table_name.lower() in excluded_table_names
                or qualified_table_name in excluded_table_names
            ):
                continue

            discovered_table_paths.append(f"Tables/{schema_name}/{table_name}")

    return sorted(discovered_table_paths)


__all__ = ["get_fs_entry_name", "list_lakehouse_tables"]

