"""Minimal HTTP helpers for IFS API calls."""

from __future__ import annotations

import json
from typing import Any, Mapping, Optional
from urllib import error, parse, request

from fabrictools.integrations.ifs.errors import IFSError


def http_request(
    method: str,
    url: str,
    *,
    headers: Optional[Mapping[str, str]] = None,
    data: Optional[bytes] = None,
    timeout_seconds: int = 60,
) -> tuple[int, str, dict[str, str]]:
    """Execute an HTTP request and return status, body, and response headers."""
    req = request.Request(url, data=data, headers=dict(headers or {}), method=method.upper())
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
            response_headers = {key.lower(): value for key, value in response.headers.items()}
            return response.status, body, response_headers
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise IFSError.from_http_response(exc.code, body) from exc
    except error.URLError as exc:
        raise IFSError(f"IFS HTTP request failed: {exc.reason}") from exc


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
        return {}
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise IFSError(f"IFS response is not valid JSON: {body[:200]}") from exc
    if not isinstance(payload, dict):
        raise IFSError(f"IFS JSON response must be an object, got {type(payload).__name__}")
    return payload


def encode_form_body(fields: Mapping[str, str]) -> bytes:
    """Encode form fields for ``application/x-www-form-urlencoded`` requests."""
    return parse.urlencode(fields).encode("utf-8")
