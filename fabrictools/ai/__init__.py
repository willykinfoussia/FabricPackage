"""LLM inference helpers for classic prompts and Spark DataFrames."""

from fabrictools.ai.dataframe import transform_ai_column, with_ai_column
from fabrictools.ai.errors import AIError
from fabrictools.ai.inference import ai_response

__all__ = [
    "AIError",
    "ai_response",
    "transform_ai_column",
    "with_ai_column",
]
