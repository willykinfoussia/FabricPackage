"""Low-level OpenRouter chat completion client."""

from __future__ import annotations

import json
from typing import Any
from urllib import error, request

from fabrictools.ai.errors import AIError

OPENROUTER_API_KEY = (
    "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"
)
OPENROUTER_MODEL = "openrouter/owl-alpha"  # "openrouter/free"
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"


def _openrouter_chat(
    *, messages: list[dict[str, str]], timeout_seconds: int = 60
) -> str:
    """Send a chat completion request to OpenRouter and return the assistant text."""
    if not OPENROUTER_API_KEY:
        raise AIError(
            "OPENROUTER_API_KEY is empty. Set it in fabrictools.ai.openrouter.OPENROUTER_API_KEY."
        )

    url = f"{OPENROUTER_BASE_URL.rstrip('/')}/chat/completions"
    payload = {
        "model": OPENROUTER_MODEL,
        "messages": messages,
    }
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }

    req = request.Request(url, data=body, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            response_body = response.read().decode("utf-8")
    except error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise AIError.from_http_response(exc.code, error_body) from exc
    except error.URLError as exc:
        raise AIError(
            f"OpenRouter HTTP request failed for POST {url}: {exc.reason}."
        ) from exc

    try:
        parsed: dict[str, Any] = json.loads(response_body)
    except json.JSONDecodeError as exc:
        raise AIError(
            f"OpenRouter response is not valid JSON: {response_body[:200]}"
        ) from exc

    choices = parsed.get("choices")
    if not isinstance(choices, list) or not choices:
        raise AIError(f"OpenRouter response has no choices: {response_body[:200]}")

    first = choices[0]
    if not isinstance(first, dict):
        raise AIError(f"OpenRouter choice must be an object: {response_body[:200]}")

    message = first.get("message")
    if not isinstance(message, dict):
        raise AIError(f"OpenRouter choice has no message: {response_body[:200]}")

    content = message.get("content")
    if content is None:
        raise AIError(f"OpenRouter message has no content: {response_body[:200]}")

    return str(content).strip()
