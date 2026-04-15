"""Path resolution helpers for Microsoft Fabric resources.

Lakehouse helpers accept slash paths and SQL-style ``schema.table`` (first dot only)
when the string has no path separators, e.g. ``dbo.PdC Extraction`` → ``dbo/PdC Extraction``.
"""

from __future__ import annotations

from typing import List

from fabrictools.core.logging import log

# Do not treat ``schema.remainder`` as SQL notation when the path looks like a file.
_DOT_PATH_FILE_SUFFIXES: tuple[str, ...] = (
    ".parquet",
    ".csv",
    ".json",
    ".orc",
    ".avro",
    ".delta",
    ".txt",
    ".gz",
    ".zip",
)
_DOT_PATH_REMAINDER_AS_EXT: frozenset[str] = frozenset(
    {"parquet", "csv", "json", "orc", "avro", "delta", "txt", "gz", "zip"}
)


def _expand_first_dot_schema_prefix(normalized: str) -> str:
    """Turn ``schema.table`` (single segment, first dot) into ``schema/table``."""
    if "/" in normalized or "." not in normalized:
        return normalized
    lower = normalized.lower()
    if lower.endswith(_DOT_PATH_FILE_SUFFIXES):
        return normalized
    schema, remainder = normalized.split(".", 1)
    if not schema or not remainder:
        return normalized
    if remainder.lower() in _DOT_PATH_REMAINDER_AS_EXT:
        return normalized
    return f"{schema}/{remainder}"


def _read_property(container: object, key: str) -> str:
    """Read a property from an object or a dictionary-like container."""
    if isinstance(container, dict):
        value = container.get(key)
    else:
        value = getattr(container, key, None)

    if value is None:
        raise AttributeError(f"Missing property '{key}'")

    return str(value)


def build_lakehouse_read_path_candidates(relative_path: str) -> List[str]:
    """Build ordered candidate relative paths for Lakehouse reads.

    Normalizes slashes, maps SQL-style ``schema.table`` (e.g. ``dbo.PdC Extraction``)
    to ``schema/table`` when there is no slash, then may prepend ``Tables/dbo`` or
    ``Files`` when the path omits those prefixes (Fabric layout).

    :param relative_path: User-supplied path under the Lakehouse.
    :type relative_path: str

    :returns: Unique candidates in resolution order.
    :rtype: list[str]
    """
    normalized = relative_path.strip().strip("/").replace("\\", "/")
    if not normalized:
        return [normalized]

    normalized = _expand_first_dot_schema_prefix(normalized)
    parts = [part for part in normalized.split("/") if part]
    first = parts[0].lower()
    candidates: List[str] = ["/".join(parts)]

    if first == "tables":
        if len(parts) >= 2 and parts[1].lower() == "dbo":
            candidates.append(
                f"Tables/dbo/{'/'.join(parts[2:])}" if len(parts) > 2 else "Tables/dbo"
            )
        elif len(parts) >= 2:
            candidates.append(f"Tables/dbo/{'/'.join(parts[1:])}")
        else:
            candidates.append("Tables/dbo")
    elif first == "dbo":
        candidates.append(
            f"Tables/dbo/{'/'.join(parts[1:])}" if len(parts) > 1 else "Tables/dbo"
        )
    elif first == "files":
        candidates.append(f"Files/{'/'.join(parts[1:])}" if len(parts) > 1 else "Files")
    else:
        candidates.append(f"Tables/dbo/{'/'.join(parts)}")
        candidates.append(f"Files/{'/'.join(parts)}")

    ordered_unique: List[str] = []
    for candidate in candidates:
        if candidate and candidate not in ordered_unique:
            ordered_unique.append(candidate)
    return ordered_unique


def build_lakehouse_write_path(relative_path: str) -> str:
    """Normalize a Lakehouse write path (``Tables/dbo/...`` or ``Files/...``).

    Accepts SQL-style ``schema.table`` (e.g. ``dbo.PdC Extraction``) when there is no
    slash; it is mapped to ``schema/table`` before applying Fabric layout rules.

    :param relative_path: Destination path fragment from the caller.
    :type relative_path: str

    :returns: Canonical relative path for writes.
    :rtype: str
    """
    normalized = relative_path.strip().strip("/").replace("\\", "/")
    if not normalized:
        return normalized

    normalized = _expand_first_dot_schema_prefix(normalized)
    parts = [part for part in normalized.split("/") if part]
    first = parts[0].lower()
    if first == "files":
        return "/".join(["Files", *parts[1:]]) if len(parts) > 1 else "Files"
    if first == "tables":
        if len(parts) >= 2 and parts[1].lower() == "dbo":
            return (
                "/".join(["Tables", "dbo", *parts[2:]])
                if len(parts) > 2
                else "Tables/dbo"
            )
        return "/".join(["Tables", "dbo", *parts[1:]]) if len(parts) > 1 else "Tables/dbo"
    if first == "dbo":
        return "/".join(["Tables", "dbo", *parts[1:]]) if len(parts) > 1 else "Tables/dbo"
    return "/".join(["Tables", "dbo", *parts])


def get_lakehouse_abfs_path(lakehouse_name: str) -> str:
    """Resolve the full ABFS base path for a Lakehouse display name.

    :param lakehouse_name: Lakehouse name as shown in Fabric.
    :type lakehouse_name: str

    :returns: ``abfsPath`` from lakehouse properties.
    :rtype: str

    :raises ValueError: If ``notebookutils`` is missing or resolution fails.
    """
    try:
        import notebookutils  # type: ignore[import-untyped]  # noqa: PLC0415

        lh = notebookutils.lakehouse.get(lakehouse_name)
        properties = lh.get("properties", {}) if isinstance(lh, dict) else lh.properties
        path = _read_property(properties, "abfsPath")
        return path
    except ImportError as exc:
        raise ValueError(
            "notebookutils is not available — are you running inside "
            f"Microsoft Fabric? ({exc})"
        ) from exc
    except Exception as exc:
        raise ValueError(f"Could not resolve Lakehouse '{lakehouse_name}': {exc}") from exc


def get_warehouse_jdbc_url(warehouse_name: str) -> str:
    """Build a JDBC URL for a Fabric Warehouse (SQL endpoint + database).

    :param warehouse_name: Warehouse display name in Fabric.
    :type warehouse_name: str

    :returns: JDBC URL string for Spark ``jdbc`` format reads/writes.
    :rtype: str

    :raises ValueError: If ``notebookutils`` is missing or resolution fails.
    """
    try:
        import notebookutils  # type: ignore[import-untyped]  # noqa: PLC0415

        wh = notebookutils.warehouse.get(warehouse_name)
        properties = wh.get("properties", {}) if isinstance(wh, dict) else wh.properties
        sql_endpoint = _read_property(properties, "connectionString")
        database = _read_property(properties, "databaseName")
        jdbc_url = (
            f"jdbc:sqlserver://{sql_endpoint};"
            f"database={database};"
            "encrypt=true;"
            "trustServerCertificate=false;"
            "loginTimeout=30;"
        )
        log(f"Resolved Warehouse '{warehouse_name}' -> {sql_endpoint}/{database}")
        return jdbc_url
    except ImportError as exc:
        raise ValueError(
            "notebookutils is not available — are you running inside "
            f"Microsoft Fabric? ({exc})"
        ) from exc
    except Exception as exc:
        raise ValueError(f"Could not resolve Warehouse '{warehouse_name}': {exc}") from exc

