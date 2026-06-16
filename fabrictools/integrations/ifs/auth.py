"""OAuth2 Client Credentials authentication for IFS Cloud."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

from fabrictools.integrations.ifs._logging import log_ifs
from fabrictools.integrations.ifs.config import IFSConfig
from fabrictools.integrations.ifs.errors import IFSError
from fabrictools.integrations.ifs.http import encode_form_body, http_request_json


@dataclass
class _TokenCache:
    access_token: Optional[str] = None
    expires_at: float = 0.0


class IFSAuthManager:
    """Acquire and cache IFS OAuth2 access tokens."""

    _REFRESH_MARGIN_SECONDS = 60

    def __init__(self, config: IFSConfig) -> None:
        self._config = config
        self._cache = _TokenCache()

    def get_access_token(self, *, force_refresh: bool = False) -> str:
        """Return a valid access token, refreshing when needed."""
        now = time.time()
        if (
            not force_refresh
            and self._cache.access_token
            and now < self._cache.expires_at - self._REFRESH_MARGIN_SECONDS
        ):
            remaining = int(self._cache.expires_at - now)
            log_ifs(f"Token OAuth2 réutilisé depuis le cache (expire dans ~{remaining}s)", level="debug")
            return self._cache.access_token

        token_endpoint = self._config.resolve_token_endpoint()
        payload = {
            "grant_type": "client_credentials",
            "client_id": self._config.client_id,
            "client_secret": self._config.client_secret,
            "scope": self._config.scope,
        }
        log_ifs(
            f"Demande token OAuth2 — endpoint={token_endpoint}, "
            f"client_id={self._config.client_id!r}, scope={self._config.scope!r}"
        )
        response = http_request_json(
            "POST",
            token_endpoint,
            headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
            data=encode_form_body(payload),
            timeout_seconds=self._config.timeout_seconds,
        )
        access_token = response.get("access_token")
        if not access_token or not isinstance(access_token, str):
            log_ifs("Réponse token OAuth2 invalide: access_token manquant", level="error")
            raise IFSError("IFS token response did not contain access_token")

        expires_in = response.get("expires_in", 3600)
        try:
            expires_in_seconds = int(expires_in)
        except (TypeError, ValueError) as exc:
            log_ifs(f"Réponse token OAuth2 invalide: expires_in={expires_in!r}", level="error")
            raise IFSError(f"Invalid expires_in value in IFS token response: {expires_in!r}") from exc

        self._cache.access_token = access_token
        self._cache.expires_at = now + max(expires_in_seconds, 0)
        log_ifs(
            f"Token OAuth2 obtenu — longueur={len(access_token)}, "
            f"expires_in={expires_in_seconds}s"
        )
        return access_token
