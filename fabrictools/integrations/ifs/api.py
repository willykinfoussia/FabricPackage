"""Token-based IFS API helpers (no OAuth flow)."""

from __future__ import annotations

from typing import Any, Callable, Optional

from fabrictools.integrations.ifs._logging import log_ifs, log_ifs_entity_request
from fabrictools.integrations.ifs.config import IFSConfig
from fabrictools.integrations.ifs.errors import IFSError
from fabrictools.integrations.ifs.http import http_request_json
from fabrictools.integrations.ifs.odata import (
    build_entity_url,
    extract_entity_rows,
    extract_next_link,
    resolve_next_url,
)


def ifs_request_json(
    access_token: str,
    url: str,
    *,
    method: str = "GET",
    timeout_seconds: int = 60,
    data: Optional[bytes] = None,
) -> dict[str, Any]:
    """Execute an authenticated IFS API request and parse the JSON response.

    :param access_token: OAuth2 bearer token already obtained.
    :param url: Full request URL.
    :param method: HTTP method (default ``GET``).
    :param timeout_seconds: Request timeout in seconds.
    :param data: Optional request body.
    """
    return http_request_json(
        method,
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        },
        data=data,
        timeout_seconds=timeout_seconds,
    )


def ifs_get_json_with_token(
    access_token: str,
    url: str,
    config: IFSConfig,
) -> dict[str, Any]:
    """GET JSON from an IFS URL using a bearer token."""
    log_ifs(f"Appel OData GET: {url}", level="debug")
    return ifs_request_json(
        access_token,
        url,
        timeout_seconds=config.timeout_seconds,
    )


def _api_config_from_host(
    host: str,
    *,
    layer: str = "int",
    projection_version: str = "v1",
    timeout_seconds: int = 60,
    page_size: int = 1000,
) -> IFSConfig:
    return IFSConfig(
        host=host,
        client_id="",
        client_secret="",
        layer=layer,
        projection_version=projection_version,
        timeout_seconds=timeout_seconds,
        page_size=page_size,
    )


def fetch_ifs_entity_rows(
    config: IFSConfig,
    get_json: Callable[[str], dict[str, Any]],
    projection: str,
    entity_set: str,
    *,
    odata_filter: Optional[str] = None,
    select: Optional[list[str]] = None,
    top: Optional[int] = None,
    skip: Optional[int] = None,
    orderby: Optional[str] = None,
    fetch_all: bool = False,
) -> list[dict[str, Any]]:
    """Read rows from an IFS OData entity set using a JSON getter callback."""
    if fetch_all:
        return _fetch_all_pages(
            config=config,
            get_json=get_json,
            projection=projection,
            entity_set=entity_set,
            odata_filter=odata_filter,
            select=select,
            orderby=orderby,
        )

    url = build_entity_url(
        config,
        projection,
        entity_set,
        odata_filter=odata_filter,
        select=select,
        top=top if top is not None else config.page_size,
        skip=skip,
        orderby=orderby,
    )
    log_ifs(f"URL OData (page unique): {url}")
    payload = get_json(url)
    rows = extract_entity_rows(payload)
    log_ifs(f"Page unique: {len(rows)} ligne(s) lue(s)")
    return rows


def _fetch_all_pages(
    *,
    config: IFSConfig,
    get_json: Callable[[str], dict[str, Any]],
    projection: str,
    entity_set: str,
    odata_filter: Optional[str],
    select: Optional[list[str]],
    orderby: Optional[str],
) -> list[dict[str, Any]]:
    page_size = config.page_size
    all_rows: list[dict[str, Any]] = []
    skip = 0
    page_number = 1

    log_ifs(f"Pagination activée — page_size={page_size}")

    while True:
        url = build_entity_url(
            config,
            projection,
            entity_set,
            odata_filter=odata_filter,
            select=select,
            top=page_size,
            skip=skip,
            orderby=orderby,
        )
        log_ifs(f"Page {page_number} (skip={skip}) — URL: {url}")
        payload = get_json(url)
        rows = extract_entity_rows(payload)
        all_rows.extend(rows)
        log_ifs(
            f"Page {page_number}: {len(rows)} ligne(s), total cumulé={len(all_rows)}"
        )

        next_link = extract_next_link(payload)
        if next_link:
            log_ifs(f"Pagination via @odata.nextLink détectée: {next_link}")
            next_url: Optional[str] = resolve_next_url(config, next_link)
            next_page_number = page_number + 1
            while next_url:
                log_ifs(f"Page {next_page_number} (nextLink) — URL: {next_url}")
                payload = get_json(next_url)
                page_rows = extract_entity_rows(payload)
                all_rows.extend(page_rows)
                log_ifs(
                    f"Page {next_page_number}: {len(page_rows)} ligne(s), "
                    f"total cumulé={len(all_rows)}"
                )
                follow_link = extract_next_link(payload)
                next_url = (
                    resolve_next_url(config, follow_link) if follow_link else None
                )
                next_page_number += 1
            break

        if len(rows) < page_size:
            log_ifs(
                f"Pagination terminée — dernière page incomplète "
                f"({len(rows)} < {page_size})"
            )
            break

        skip += page_size
        page_number += 1

    log_ifs(
        f"Lecture terminée — {len(all_rows)} ligne(s) depuis {projection}/{entity_set}"
    )
    return all_rows


def read_ifs_entity_with_token(
    access_token: str,
    host: str,
    projection: str,
    entity_set: str,
    *,
    layer: str = "int",
    odata_filter: Optional[str] = None,
    select: Optional[list[str]] = None,
    top: Optional[int] = None,
    skip: Optional[int] = None,
    orderby: Optional[str] = None,
    fetch_all: bool = False,
    projection_version: str = "v1",
    page_size: int = 1000,
    timeout_seconds: int = 60,
) -> list[dict[str, Any]]:
    """Read an IFS OData entity set using a pre-generated access token.

    :param access_token: OAuth2 bearer token string.
    :param host: IFS host URL (e.g. ``https://ifs.example.com``).
    :param projection: OData projection service name (e.g. ``CustomerHandling``).
    :param entity_set: Entity set name (e.g. ``CustomerInfoSet``).
    :param layer: API exposure layer — ``main``, ``int``, or ``b2b``.
    :param odata_filter: Optional OData ``$filter`` expression.
    :param select: Optional list of fields for ``$select``.
    :param top: Optional ``$top`` page size for a single request.
    :param skip: Optional ``$skip`` offset.
    :param orderby: Optional ``$orderby`` expression.
    :param fetch_all: When ``True``, paginate through all result pages.
    :param projection_version: OData projection API version segment.
    :param page_size: Default ``$top`` when ``fetch_all`` is enabled.
    :param timeout_seconds: HTTP request timeout in seconds.
    """
    config = _api_config_from_host(
        host,
        layer=layer,
        projection_version=projection_version,
        timeout_seconds=timeout_seconds,
        page_size=page_size,
    )
    log_ifs_entity_request(
        projection=projection,
        entity_set=entity_set,
        odata_filter=odata_filter,
        select=select,
        fetch_all=fetch_all,
        top=top,
        skip=skip,
    )
    get_json = lambda url: ifs_get_json_with_token(access_token, url, config)

    try:
        return fetch_ifs_entity_rows(
            config,
            get_json,
            projection,
            entity_set,
            odata_filter=odata_filter,
            select=select,
            top=top,
            skip=skip,
            orderby=orderby,
            fetch_all=fetch_all,
        )
    except IFSError as exc:
        if exc.status_code == 403:
            log_ifs(
                (
                    f"IFS 403 — {exc.error_code or 'FORBIDDEN'}: {exc}. "
                    f"Le token est valide mais l'accès est refusé pour "
                    f"{layer}/{projection}/{entity_set} sur {host}. "
                    "Vérifiez les permissions IAM du client sur cette couche/"
                    "projection/entité."
                ),
                level="error",
            )
        else:
            log_ifs(f"Erreur IFS — {exc}", level="error")
        return []
