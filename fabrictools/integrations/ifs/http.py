"""Minimal HTTP helpers for IFS API calls."""

from __future__ import annotations

import json
from typing import Any, Mapping, Optional
from urllib import error, parse, request

from fabrictools.integrations.ifs._logging import log_ifs
from fabrictools.integrations.ifs.errors import IFSError


def _safe_headers_for_log(headers: Mapping[str, str]) -> dict[str, str]:
    safe: dict[str, str] = {}
    for key, value in headers.items():
        lowered = key.lower()
        if lowered == "authorization":
            safe[key] = "Bearer ***" if value else value
        elif lowered == "client_secret":
            safe[key] = "***"
        else:
            safe[key] = value
    return safe


def http_request(
    method: str,
    url: str,
    *,
    headers: Optional[Mapping[str, str]] = None,
    data: Optional[bytes] = None,
    timeout_seconds: int = 60,
) -> tuple[int, str, dict[str, str]]:
    """Execute an HTTP request and return status, body, and response headers."""
    method_upper = method.upper()
    request_headers = dict(headers or {})
    log_ifs(
        f"HTTP {method_upper} {url} "
        f"(timeout={timeout_seconds}s, body={len(data) if data else 0} bytes)",
        level="debug",
    )
    log_ifs(f"HTTP headers: {_safe_headers_for_log(request_headers)}", level="debug")

    req = request.Request(url, data=data, headers=request_headers, method=method_upper)
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
            response_headers = {
                key.lower(): value for key, value in response.headers.items()
            }
            log_ifs(
                f"HTTP {method_upper} {url} → {response.status} ({len(body)} bytes)",
            )
            return response.status, body, response_headers
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        log_ifs(
            f"HTTP {method_upper} {url} → erreur {exc.code}: {body[:300]}",
            level="error",
        )
        raise IFSError.from_http_response(exc.code, body) from exc
    except error.URLError as exc:
        log_ifs(
            f"HTTP {method_upper} {url} → échec réseau/DNS: {exc.reason}",
            level="error",
        )
        raise IFSError(
            f"IFS HTTP request failed for {method_upper} {url}: {exc.reason}. "
            "Vérifiez IFS_HOST / TOKEN_ENDPOINT (hostname résolvable depuis Fabric)."
        ) from exc


def http_request_json(
    method: str,
    url: str,
    *,
    headers: Optional[Mapping[str, str]] = None,
    data: Optional[bytes] = None,
    timeout_seconds: int = 60,
) -> dict[str, Any]:
    """Execute an HTTP request and parse the response body as JSON."""
    status_code, body, _ = http_request(
        method,
        url,
        headers=headers,
        data=data,
        timeout_seconds=timeout_seconds,
    )
    if status_code >= 400:
        raise IFSError.from_http_response(status_code, body)
    if not body:
        log_ifs(f"HTTP {method.upper()} {url} → corps de réponse vide", level="debug")
        return {}
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        log_ifs(
            f"HTTP {method.upper()} {url} → JSON invalide: {body}",
            level="error",
        )
        raise IFSError(f"IFS response is not valid JSON: {body[:200]}") from exc
    if not isinstance(payload, dict):
        raise IFSError(
            f"IFS JSON response must be an object, got {type(payload).__name__}"
        )
    value_count = (
        len(payload.get("value", []))
        if isinstance(payload.get("value"), list)
        else None
    )
    if value_count is not None:
        log_ifs(
            f"HTTP {method.upper()} {url} → JSON parsé, {value_count} ligne(s) dans value[]"
        )
    else:
        log_ifs(
            f"HTTP {method.upper()} {url} → JSON parsé ({len(body)} bytes)",
            level="debug",
        )
    return payload


def encode_form_body(fields: Mapping[str, str]) -> bytes:
    """Encode form fields for ``application/x-www-form-urlencoded`` requests."""
    return parse.urlencode(fields).encode("utf-8")
