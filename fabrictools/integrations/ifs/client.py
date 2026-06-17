"""IFS Cloud OData client."""

from __future__ import annotations

from typing import Any, Optional

from fabrictools.integrations.ifs._logging import log_ifs, log_ifs_config, log_ifs_entity_request
from fabrictools.integrations.ifs.api import fetch_ifs_entity_rows, ifs_get_json_with_token
from fabrictools.integrations.ifs.auth import IFSAuthManager
from fabrictools.integrations.ifs.config import IFSConfig
from fabrictools.integrations.ifs.connectivity import diagnose_ifs_connectivity


class IFSClient:
    """Read-only client for IFS Cloud OData entity sets."""

    def __init__(self, config: IFSConfig, *, check_connectivity: bool = True) -> None:
        self._config = config
        self._auth = IFSAuthManager(config)
        log_ifs("Initialisation IFSClient")
        log_ifs_config(config)
        if check_connectivity:
            diagnose_ifs_connectivity(config)

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
        return fetch_ifs_entity_rows(
            self._config,
            self._get_json,
            projection,
            entity_set,
            odata_filter=odata_filter,
            select=select,
            top=top,
            skip=skip,
            orderby=orderby,
            fetch_all=fetch_all,
        )

    def _get_json(self, url: str) -> dict[str, Any]:
        return ifs_get_json_with_token(self.get_access_token(), url, self._config)
