"""
Path resolution helpers for Microsoft Fabric resources.

These functions rely on ``notebookutils``, which is injected automatically
into the Python environment by the Fabric notebook runtime.  They will raise
a clear ``ValueError`` when called outside Fabric (e.g. local tests) so that
callers can handle the missing dependency gracefully.
"""

from __future__ import annotations

from typing import List

from fabrictools._logger import log


def _read_property(container: object, key: str) -> str:
    """
    Read a property from an object or a dictionary-like container.

    Fabric runtime objects can expose properties either as attributes
    (``obj.key``) or as dictionary items (``obj["key"]``).
    """
    if isinstance(container, dict):
        value = container.get(key)
    else:
        value = getattr(container, key, None)

    if value is None:
        raise AttributeError(f"Missing property '{key}'")

    return str(value)


def build_lakehouse_read_path_candidates(relative_path: str) -> List[str]:
    """
    Build ordered candidate relative paths for Lakehouse reads.

    The first candidate is always the user-provided path (normalized with
    forward slashes). Extra candidates are added for common partial inputs:
    - bare name -> Tables/dbo/<name>, then Files/<name>
    - dbo/<name> -> Tables/dbo/<name>
    - Tables/<name> -> Tables/dbo/<name>
    """
    normalized = relative_path.strip().strip("/").replace("\\", "/")
    if not normalized:
        return [normalized]

    parts = [part for part in normalized.split("/") if part]
    first = parts[0].lower()
    candidates: List[str] = ["/".join(parts)]

    if first == "tables":
        if len(parts) >= 2 and parts[1].lower() == "dbo":
            candidates.append(f"Tables/dbo/{'/'.join(parts[2:])}" if len(parts) > 2 else "Tables/dbo")
        elif len(parts) >= 2:
            candidates.append(f"Tables/dbo/{'/'.join(parts[1:])}")
        else:
            candidates.append("Tables/dbo")
    elif first == "dbo":
        candidates.append(f"Tables/dbo/{'/'.join(parts[1:])}" if len(parts) > 1 else "Tables/dbo")
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
    """
    Build a normalized Lakehouse write path.

    Rules:
    - bare name -> Tables/dbo/<name>
    - dbo/<name> -> Tables/dbo/<name>
    - Tables/<name> -> Tables/dbo/<name>
    - explicit Files/<...> remains Files/<...>
    """
    normalized = relative_path.strip().strip("/").replace("\\", "/")
    if not normalized:
        return normalized

    parts = [part for part in normalized.split("/") if part]
    first = parts[0].lower()
    if first == "files":
        return "/".join(["Files", *parts[1:]]) if len(parts) > 1 else "Files"
    if first == "tables":
        if len(parts) >= 2 and parts[1].lower() == "dbo":
            return "/".join(["Tables", "dbo", *parts[2:]]) if len(parts) > 2 else "Tables/dbo"
        return "/".join(["Tables", "dbo", *parts[1:]]) if len(parts) > 1 else "Tables/dbo"
    if first == "dbo":
        return "/".join(["Tables", "dbo", *parts[1:]]) if len(parts) > 1 else "Tables/dbo"
    return "/".join(["Tables", "dbo", *parts])


def get_lakehouse_abfs_path(lakehouse_name: str) -> str:
    """
    Return the full ABFS path for a Fabric Lakehouse.

    Internally calls ``notebookutils.lakehouse.get(lakehouse_name)`` which is
    available in every Fabric Spark notebook.

    Parameters
    ----------
    lakehouse_name:
        Display name of the Lakehouse as it appears in the Fabric workspace
        (e.g. ``"BronzeLakehouse"``).

    Returns
    -------
    str
        ABFS path of the form
        ``abfss://<container>@<account>.dfs.core.windows.net``.

    Raises
    ------
    ValueError
        When ``notebookutils`` is not available (outside Fabric).
    """
    try:
        import notebookutils  # type: ignore[import-untyped]  # noqa: PLC0415

        lh = notebookutils.lakehouse.get(lakehouse_name)
        properties = (
            lh.get("properties", {}) if isinstance(lh, dict) else lh.properties
        )
        path = _read_property(properties, "abfsPath")
        return path
    except ImportError as exc:
        raise ValueError(
            f"notebookutils is not available — are you running inside "
            f"Microsoft Fabric? ({exc})"
        ) from exc
    except Exception as exc:
        raise ValueError(
            f"Could not resolve Lakehouse '{lakehouse_name}': {exc}"
        ) from exc


def get_warehouse_jdbc_url(warehouse_name: str) -> str:
    """
    Return the JDBC connection URL for a Fabric Warehouse.

    Internally calls ``notebookutils.warehouse.get(warehouse_name)`` to
    retrieve the SQL endpoint and builds a standard JDBC URL from it.

    Parameters
    ----------
    warehouse_name:
        Display name of the Warehouse as it appears in the Fabric workspace
        (e.g. ``"MyWarehouse"``).

    Returns
    -------
    str
        JDBC URL suitable for use with ``spark.read.format("jdbc")``.

    Raises
    ------
    ValueError
        When ``notebookutils`` is not available or the warehouse cannot be
        found.
    """
    try:
        import notebookutils  # type: ignore[import-untyped]  # noqa: PLC0415

        wh = notebookutils.warehouse.get(warehouse_name)
        properties = (
            wh.get("properties", {}) if isinstance(wh, dict) else wh.properties
        )
        sql_endpoint = _read_property(properties, "connectionString")
        database = _read_property(properties, "databaseName")
        jdbc_url = (
            f"jdbc:sqlserver://{sql_endpoint};"
            f"database={database};"
            "encrypt=true;"
            "trustServerCertificate=false;"
            "loginTimeout=30;"
        )
        log(f"Resolved Warehouse '{warehouse_name}' → {sql_endpoint}/{database}")
        return jdbc_url
    except ImportError as exc:
        raise ValueError(
            f"notebookutils is not available — are you running inside "
            f"Microsoft Fabric? ({exc})"
        ) from exc
    except Exception as exc:
        raise ValueError(
            f"Could not resolve Warehouse '{warehouse_name}': {exc}"
        ) from exc
