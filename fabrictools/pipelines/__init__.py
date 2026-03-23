"""Shared pipeline contracts and helpers."""

from fabrictools.pipelines.config import (
    TableJobConfig,
    build_table_jobs_from_config,
    build_table_jobs_from_discovery,
)

__all__ = [
    "TableJobConfig",
    "build_table_jobs_from_config",
    "build_table_jobs_from_discovery",
]

