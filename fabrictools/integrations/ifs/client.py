"""IFS Cloud OData client."""

from __future__ import annotations

from typing import Any, Optional

from fabrictools.integrations.ifs._logging import log_ifs, log_ifs_config, log_ifs_entity_request
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
        log_ifs("Initialisation IFSClient")
        log_ifs_config(config)

    @property
    def config(self) -> IFSConfig:
        return self._config

    def get_access_token(self, *, force_refresh: bool = False) -> str:
        """Return a valid OAuth2 access token."""
        if force_refresh:
            log_ifs("Rafraîchissement forcé du token OAuth2")
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
        log_ifs_entity_request(
            projection=projection,
            entity_set=entity_set,
            odata_filter=odata_filter,
            select=select,
            fetch_all=fetch_all,
            top=top,
            skip=skip,
        )
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
        log_ifs(f"URL OData (page unique): {url}")
        payload = self._get_json(url)
        rows = extract_entity_rows(payload)
        log_ifs(f"Page unique: {len(rows)} ligne(s) lue(s)")
        return rows

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
        page_number = 1

        log_ifs(f"Pagination activée — page_size={page_size}")

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
            log_ifs(f"Page {page_number} (skip={skip}) — URL: {url}")
            payload = self._get_json(url)
            rows = extract_entity_rows(payload)
            all_rows.extend(rows)
            log_ifs(f"Page {page_number}: {len(rows)} ligne(s), total cumulé={len(all_rows)}")

            next_link = extract_next_link(payload)
            if next_link:
                log_ifs(f"Pagination via @odata.nextLink détectée: {next_link}")
                next_url: Optional[str] = resolve_next_url(self._config, next_link)
                next_page_number = page_number + 1
                while next_url:
                    log_ifs(f"Page {next_page_number} (nextLink) — URL: {next_url}")
                    payload = self._get_json(next_url)
                    page_rows = extract_entity_rows(payload)
                    all_rows.extend(page_rows)
                    log_ifs(
                        f"Page {next_page_number}: {len(page_rows)} ligne(s), "
                        f"total cumulé={len(all_rows)}"
                    )
                    follow_link = extract_next_link(payload)
                    next_url = (
                        resolve_next_url(self._config, follow_link) if follow_link else None
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

        log_ifs(f"Lecture terminée — {len(all_rows)} ligne(s) depuis {projection}/{entity_set}")
        return all_rows

    def _get_json(self, url: str) -> dict[str, Any]:
        log_ifs(f"Appel OData GET: {url}", level="debug")
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
