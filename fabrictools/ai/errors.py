"""OpenRouter / AI inference error types."""

from __future__ import annotations

import json
from typing import Any, Optional


class AIError(Exception):
    """Raised when an OpenRouter chat completion request fails."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        error_code: Optional[str] = None,
        response_body: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.response_body = response_body

    @classmethod
    def from_http_response(cls, status_code: int, body: str) -> "AIError":
        """Build an :class:`AIError` from an HTTP status and response body."""
        parsed = _parse_openrouter_error_body(body)
        message = parsed.get("message") or body or f"OpenRouter request failed with HTTP {status_code}"
        return cls(
            message,
            status_code=status_code,
            error_code=parsed.get("code"),
            response_body=body,
        )


def _parse_openrouter_error_body(body: str) -> dict[str, Any]:
    if not body:
        return {}
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return {"message": body}

    if not isinstance(payload, dict):
        return {"message": body}

    error = payload.get("error")
    if isinstance(error, dict):
        return {
            "code": error.get("code"),
            "message": error.get("message"),
        }

    if "message" in payload:
        return {
            "code": payload.get("code"),
            "message": payload.get("message"),
        }

    return {"message": body}
