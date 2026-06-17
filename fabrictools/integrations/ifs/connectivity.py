"""IFS connectivity diagnostics (DNS, internal hostnames)."""

from __future__ import annotations

import socket
from typing import Any, Optional
from urllib.parse import urlparse

from fabrictools.integrations.ifs._logging import log_ifs
from fabrictools.integrations.ifs.config import IFSConfig

_INTERNAL_HOSTNAME_SUFFIXES = (
    ".lcl",
    ".local",
    ".internal",
    ".corp",
    ".lan",
    ".private",
    ".intra",
)


def _hostname_from_url(url: str) -> Optional[str]:
    parsed = urlparse(url)
    return parsed.hostname


def _looks_like_internal_hostname(hostname: str) -> bool:
    lowered = hostname.lower()
    return any(lowered.endswith(suffix) for suffix in _INTERNAL_HOSTNAME_SUFFIXES)


def _check_dns(hostname: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "hostname": hostname,
        "resolvable": False,
        "ip_addresses": [],
        "error": None,
        "looks_internal": _looks_like_internal_hostname(hostname),
    }
    try:
        infos = socket.getaddrinfo(hostname, 443, type=socket.SOCK_STREAM)
        ips = sorted({info[4][0] for info in infos})
        result["resolvable"] = True
        result["ip_addresses"] = ips
    except socket.gaierror as exc:
        result["error"] = str(exc)
    return result


def diagnose_ifs_connectivity(config: IFSConfig) -> dict[str, Any]:
    """Vérifie la résolution DNS des hôtes IFS avant un appel API.

    Utile dans Fabric pour détecter tôt les hostnames internes (``.lcl``,
    ``.local``, etc.) non joignables depuis le cluster cloud.

    :returns: Dictionnaire avec les checks ``host`` et ``token_endpoint``.
    """
    checks: dict[str, Any] = {
        "host": config.host,
        "token_endpoint": config.resolve_token_endpoint(),
        "checks": {},
        "ok": True,
        "warnings": [],
    }

    host_hostname = _hostname_from_url(config.host)
    token_hostname = _hostname_from_url(config.resolve_token_endpoint())

    for label, hostname in (("host", host_hostname), ("token_endpoint", token_hostname)):
        if not hostname:
            checks["checks"][label] = {
                "hostname": None,
                "resolvable": False,
                "error": "URL invalide — hostname introuvable",
            }
            checks["ok"] = False
            checks["warnings"].append(f"{label}: URL invalide")
            continue

        dns = _check_dns(hostname)
        checks["checks"][label] = dns
        if not dns["resolvable"]:
            checks["ok"] = False
            if dns["looks_internal"]:
                checks["warnings"].append(
                    f"{label} ({hostname}): hostname interne non résolu depuis Fabric. "
                    "Les domaines .lcl/.local/.corp ne sont en général accessibles que "
                    "depuis le réseau d'entreprise (VPN, DNS privé, Private Link)."
                )
            else:
                checks["warnings"].append(
                    f"{label} ({hostname}): DNS non résolu ({dns['error']}). "
                    "Vérifiez IFS_HOST / TOKEN_ENDPOINT."
                )
        elif dns["looks_internal"]:
            checks["warnings"].append(
                f"{label} ({hostname}): hostname interne résolu ({dns['ip_addresses']}) "
                "— vérifiez que le cluster Fabric peut atteindre ce réseau privé."
            )

    log_ifs("Diagnostic connectivité IFS:")
    for label, dns in checks["checks"].items():
        if dns.get("resolvable"):
            log_ifs(f"  {label}: {dns['hostname']} → OK ({', '.join(dns['ip_addresses'])})")
        else:
            log_ifs(
                f"  {label}: {dns.get('hostname')} → ÉCHEC DNS ({dns.get('error')})",
                level="error",
            )
    for warning in checks["warnings"]:
        log_ifs(f"  ⚠ {warning}", level="warning")

    return checks
