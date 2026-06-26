"""LangChain ReAct agent with OpenRouter and DuckDuckGo search."""

from __future__ import annotations

from langchain.agents import AgentExecutor, create_react_agent
from langchain_community.tools import DuckDuckGoSearchRun
from langchain_core.prompts import PromptTemplate
from langchain_openrouter import ChatOpenRouter

from fabrictools.ai.errors import AIError
from fabrictools.ai.openrouter import OPENROUTER_API_KEY, OPENROUTER_MODEL

_REACT_TEMPLATE = """Answer the following questions as best you can. You have access to the following tools:

{tools}

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

{system_instructions}Begin!

Question: {input}
Thought:{agent_scratchpad}"""

_DEFAULT_SYSTEM = (
    "Use DuckDuckGo search when you need up-to-date or factual information from the web. "
    "Otherwise answer from your knowledge.\n\n"
)


def _build_openrouter_llm(*, timeout_seconds: int) -> ChatOpenRouter:
    return ChatOpenRouter(
        model=OPENROUTER_MODEL,
        api_key=OPENROUTER_API_KEY,
        timeout=timeout_seconds * 1000,
        max_retries=2,
    )


def _build_search_tools() -> list[DuckDuckGoSearchRun]:
    return [DuckDuckGoSearchRun()]


def _create_agent_executor(
    *,
    system_prompt: str | None,
    timeout_seconds: int,
) -> AgentExecutor:
    llm = _build_openrouter_llm(timeout_seconds=timeout_seconds)
    tools = _build_search_tools()
    system_instructions = ""
    if system_prompt:
        system_instructions = f"{system_prompt.strip()}\n\n"
    elif _DEFAULT_SYSTEM:
        system_instructions = _DEFAULT_SYSTEM

    prompt = PromptTemplate.from_template(_REACT_TEMPLATE).partial(
        system_instructions=system_instructions
    )
    agent = create_react_agent(llm, tools, prompt)
    return AgentExecutor(
        agent=agent,
        tools=tools,
        handle_parsing_errors=True,
        max_iterations=5,
        verbose=False,
    )


def _run_agent(
    prompt: str,
    *,
    system_prompt: str | None = None,
    timeout_seconds: int = 60,
) -> str:
    """Run the LangChain ReAct agent and return the final answer text."""
    if not OPENROUTER_API_KEY:
        raise AIError(
            "OPENROUTER_API_KEY is empty. Set it in fabrictools.ai.openrouter.OPENROUTER_API_KEY."
        )

    executor = _create_agent_executor(
        system_prompt=system_prompt,
        timeout_seconds=timeout_seconds,
    )
    try:
        result = executor.invoke({"input": prompt})
    except Exception as exc:
        raise AIError(f"LangChain agent failed: {exc}") from exc

    output = result.get("output")
    if output is None:
        raise AIError(f"LangChain agent returned no output: {result!r}")
    return str(output).strip()
