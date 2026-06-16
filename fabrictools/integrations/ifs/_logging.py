"""Internal logging helpers for the IFS integration."""

from __future__ import annotations

from fabrictools.core import log
from fabrictools.integrations.ifs.config import IFSConfig

_PREFIX = "IFS"


def log_ifs(message: str, *, level: str = "info") -> None:
    """Emit a prefixed log line for IFS integration steps."""
    log(f"[{_PREFIX}] {message}", level=level)


def log_ifs_config(config: IFSConfig) -> None:
    """Log a safe summary of IFS connection settings (no secrets)."""
    token_source = "custom token_endpoint" if config.token_endpoint else "derived from host"
    log_ifs(
        "Configuration — "
        f"host={config.host!r}, layer={config.layer!r}, "
        f"client_id={config.client_id!r}, scope={config.scope!r}, "
        f"page_size={config.page_size}, timeout={config.timeout_seconds}s, "
        f"projection_version={config.projection_version!r}, token={token_source}"
    )
    log_ifs(f"Token endpoint: {config.resolve_token_endpoint()}", level="debug")


def log_ifs_entity_request(
    *,
    projection: str,
    entity_set: str,
    odata_filter: str | None = None,
    select: list[str] | None = None,
    fetch_all: bool = False,
    top: int | None = None,
    skip: int | None = None,
) -> None:
    """Log the parameters of an entity read request."""
    parts = [
        f"projection={projection!r}",
        f"entity_set={entity_set!r}",
        f"fetch_all={fetch_all}",
    ]
    if odata_filter:
        parts.append(f"filter={odata_filter!r}")
    if select:
        parts.append(f"select={select!r}")
    if top is not None:
        parts.append(f"top={top}")
    if skip is not None:
        parts.append(f"skip={skip}")
    log_ifs("Lecture entité — " + ", ".join(parts))
