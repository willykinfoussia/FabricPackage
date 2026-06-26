"""LangChain agent with OpenRouter and DuckDuckGo search."""

from __future__ import annotations

from typing import Any

from langchain.agents import create_agent
from langchain.messages import AIMessage
from langchain_community.tools import DuckDuckGoSearchRun
from langchain_openrouter import ChatOpenRouter

from fabrictools.ai.errors import AIError
from fabrictools.ai.openrouter import OPENROUTER_API_KEY, OPENROUTER_MODEL

_DEFAULT_SYSTEM = (
    "Use DuckDuckGo search when you need up-to-date or factual information from the web. "
    "Otherwise answer from your knowledge."
)

_RECURSION_LIMIT = 10


def _build_openrouter_llm(*, timeout_seconds: int) -> ChatOpenRouter:
    return ChatOpenRouter(
        model=OPENROUTER_MODEL,
        api_key=OPENROUTER_API_KEY,
        timeout=timeout_seconds * 1000,
        max_retries=2,
    )


def _build_search_tools() -> list[DuckDuckGoSearchRun]:
    return [DuckDuckGoSearchRun()]


def _resolve_system_prompt(system_prompt: str | None) -> str:
    if system_prompt:
        return system_prompt.strip()
    return _DEFAULT_SYSTEM


def _create_agent(*, system_prompt: str | None, timeout_seconds: int):
    return create_agent(
        model=_build_openrouter_llm(timeout_seconds=timeout_seconds),
        tools=_build_search_tools(),
        system_prompt=_resolve_system_prompt(system_prompt),
    )


def _extract_agent_text(result: dict[str, Any]) -> str:
    messages = result.get("messages")
    if not messages:
        raise AIError(f"LangChain agent returned no messages: {result!r}")

    for message in reversed(messages):
        if isinstance(message, AIMessage) and message.content:
            return str(message.content).strip()

    raise AIError(f"LangChain agent returned no assistant message: {result!r}")


def _run_agent(
    prompt: str,
    *,
    system_prompt: str | None = None,
    timeout_seconds: int = 60,
) -> str:
    """Run the LangChain agent and return the final answer text."""
    if not OPENROUTER_API_KEY:
        raise AIError(
            "OPENROUTER_API_KEY is empty. Set it in fabrictools.ai.openrouter.OPENROUTER_API_KEY."
        )

    agent = _create_agent(
        system_prompt=system_prompt,
        timeout_seconds=timeout_seconds,
    )
    try:
        result = agent.invoke(
            {"messages": [{"role": "user", "content": prompt}]},
            config={"recursion_limit": _RECURSION_LIMIT},
        )
    except Exception as exc:
        raise AIError(f"LangChain agent failed: {exc}") from exc

    return _extract_agent_text(result)
