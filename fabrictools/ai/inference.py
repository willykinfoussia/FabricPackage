"""LLM inference helpers (prompt → text) via LangChain agent."""

from __future__ import annotations

from fabrictools.ai.agent import _run_agent


def ai_response(
    prompt: str,
    *,
    system_prompt: str | None = None,
    timeout_seconds: int = 60,
) -> str:
    """Run a LangChain agent (OpenRouter + DuckDuckGo) and return the final answer.

    The agent may search the web via DuckDuckGo when the prompt requires up-to-date
    or factual information. Each call can trigger multiple LLM and search requests.

    :param prompt: User question or instruction.
    :param system_prompt: Optional system instructions for the agent.
    :param timeout_seconds: HTTP timeout in seconds (OpenRouter requests).
    :returns: Agent final answer text (stripped).
    :rtype: str

    :raises AIError: When the API key is missing, the agent fails, or the response is invalid.

    .. rubric:: Example

    >>> ai_response("Quel est le cours du Bitcoin aujourd'hui ?")  # doctest: +SKIP
    >>> ai_response("Explique Delta Lake en 2 phrases.")  # doctest: +SKIP
    """
    return _run_agent(
        prompt,
        system_prompt=system_prompt,
        timeout_seconds=timeout_seconds,
    )
