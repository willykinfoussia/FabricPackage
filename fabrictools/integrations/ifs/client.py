"""IFS Cloud OData client."""

from __future__ import annotations

from typing import Any, Optional

from fabrictools.core import log
from fabrictools.integrations.ifs.auth import IFSAuthManager
from fabrictools.integrations.ifs.config import IFSConfig
from fabrictools.integrations.ifs.http import http_request_json
from fabrictools.integrations.ifs.odata import (
    build_entity_url,
    extract_entity_rows,
    extract_next_link,
    resolve_next_url,
)


class IFSClient:
    """Read-only client for IFS Cloud OData entity sets."""

    def __init__(self, config: IFSConfig) -> None:
        self._config = config
        self._auth = IFSAuthManager(config)

    @property
    def config(self) -> IFSConfig:
        return self._config

    def get_access_token(self, *, force_refresh: bool = False) -> str:
        """Return a valid OAuth2 access token."""
        return self._auth.get_access_token(force_refresh=force_refresh)

    def get_entity(
        self,
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
        """Read rows from an IFS OData entity set.

        When ``fetch_all`` is ``True``, all pages are retrieved using ``@odata.nextLink``
        when available, otherwise ``$skip`` pagination with ``config.page_size``.
        """
        if fetch_all:
            return self._fetch_all_pages(
                projection=projection,
                entity_set=entity_set,
                odata_filter=odata_filter,
                select=select,
                orderby=orderby,
            )

        url = build_entity_url(
            self._config,
            projection,
            entity_set,
            odata_filter=odata_filter,
            select=select,
            top=top if top is not None else self._config.page_size,
            skip=skip,
            orderby=orderby,
        )
        payload = self._get_json(url)
        return extract_entity_rows(payload)

    def _fetch_all_pages(
        self,
        *,
        projection: str,
        entity_set: str,
        odata_filter: Optional[str],
        select: Optional[list[str]],
        orderby: Optional[str],
    ) -> list[dict[str, Any]]:
        page_size = self._config.page_size
        all_rows: list[dict[str, Any]] = []
        skip = 0

        while True:
            url = build_entity_url(
                self._config,
                projection,
                entity_set,
                odata_filter=odata_filter,
                select=select,
                top=page_size,
                skip=skip,
                orderby=orderby,
            )
            payload = self._get_json(url)
            rows = extract_entity_rows(payload)
            all_rows.extend(rows)

            next_link = extract_next_link(payload)
            if next_link:
                next_url: Optional[str] = resolve_next_url(self._config, next_link)
                while next_url:
                    payload = self._get_json(next_url)
                    all_rows.extend(extract_entity_rows(payload))
                    follow_link = extract_next_link(payload)
                    next_url = (
                        resolve_next_url(self._config, follow_link) if follow_link else None
                    )
                break

            if len(rows) < page_size:
                break

            skip += page_size
            log(f"IFS pagination: fetched {len(all_rows)} rows so far", level="debug")

        log(f"IFS read complete: {len(all_rows)} rows from {projection}/{entity_set}")
        return all_rows

    def _get_json(self, url: str) -> dict[str, Any]:
        token = self.get_access_token()
        return http_request_json(
            "GET",
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            },
            timeout_seconds=self._config.timeout_seconds,
        )
