"""IFS Cloud connection configuration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import urljoin


@dataclass
class IFSConfig:
    """Connection settings for IFS Cloud OData APIs.

    :param host: IFS host URL (e.g. ``https://ifs.example.com``).
    :param client_id: IAM client identifier.
    :param client_secret: IAM client secret.
    :param layer: API exposure layer — ``main``, ``int``, or ``b2b``.
    :param token_endpoint: OAuth2 token URL. When omitted, derived from ``host``.
    :param scope: OAuth2 scope (IFS examples use ``openid microprofile-jwt``).
    :param projection_version: OData projection API version segment (default ``v1``).
    :param timeout_seconds: HTTP request timeout in seconds.
    :param page_size: Default ``$top`` page size when ``fetch_all`` is enabled.
    """

    host: str
    client_id: str
    client_secret: str
    layer: str = "int"
    token_endpoint: Optional[str] = None
    scope: str = "openid microprofile-jwt"
    projection_version: str = "v1"
    timeout_seconds: int = 60
    page_size: int = 1000

    def __post_init__(self) -> None:
        self.host = self.host.rstrip("/")
        if self.layer not in {"main", "int", "b2b"}:
            raise ValueError(f"Invalid IFS layer '{self.layer}'. Expected main, int, or b2b.")
        if self.page_size <= 0:
            raise ValueError("page_size must be a positive integer.")

    def resolve_token_endpoint(self) -> str:
        """Return the OAuth2 token endpoint URL."""
        if self.token_endpoint:
            return self.token_endpoint.rstrip("/")
        return urljoin(f"{self.host}/", "auth/realms/ifscloud/protocol/openid-connect/token")

    def projection_base_path(self) -> str:
        """Return the OData projection base path segment."""
        return f"ifsapplications/projection/{self.projection_version}"
