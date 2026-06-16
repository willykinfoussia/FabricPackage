"""OData URL construction and response helpers for IFS Cloud."""

from __future__ import annotations

from typing import Any, Optional
from urllib.parse import quote, urlencode, urljoin

from fabrictools.integrations.ifs.config import IFSConfig

_ODATA_NEXT_LINK_KEYS = ("@odata.nextLink", "odata.nextLink")


def build_entity_url(
    config: IFSConfig,
    projection: str,
    entity_set: str,
    *,
    odata_filter: Optional[str] = None,
    select: Optional[list[str]] = None,
    top: Optional[int] = None,
    skip: Optional[int] = None,
    orderby: Optional[str] = None,
) -> str:
    """Build a full IFS OData entity set URL with optional query options."""
    projection_name = projection if projection.endswith(".svc") else f"{projection}.svc"
    base = (
        f"{config.host}/{config.layer}/"
        f"{config.projection_base_path()}/{projection_name}/{entity_set}"
    )
    query = build_odata_query(
        odata_filter=odata_filter,
        select=select,
        top=top,
        skip=skip,
        orderby=orderby,
    )
    if not query:
        return base
    return f"{base}?{query}"


def build_odata_query(
    *,
    odata_filter: Optional[str] = None,
    select: Optional[list[str]] = None,
    top: Optional[int] = None,
    skip: Optional[int] = None,
    orderby: Optional[str] = None,
) -> str:
    """Build an OData query string without the leading ``?``."""
    params: list[tuple[str, str]] = []
    if odata_filter:
        params.append(("$filter", odata_filter))
    if select:
        params.append(("$select", ",".join(select)))
    if top is not None:
        params.append(("$top", str(top)))
    if skip is not None:
        params.append(("$skip", str(skip)))
    if orderby:
        params.append(("$orderby", orderby))
    return urlencode(params, quote_via=quote)


def extract_entity_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract entity rows from an OData collection response."""
    value = payload.get("value")
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("IFS OData response 'value' must be a list")
    rows: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            rows.append(item)
    return rows


def extract_next_link(payload: dict[str, Any]) -> Optional[str]:
    """Return the OData next page link when present."""
    for key in _ODATA_NEXT_LINK_KEYS:
        next_link = payload.get(key)
        if isinstance(next_link, str) and next_link.strip():
            return next_link.strip()
    return None


def resolve_next_url(config: IFSConfig, next_link: str) -> str:
    """Normalize a nextLink value to an absolute URL."""
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(f"{config.host}/", next_link.lstrip("/"))
