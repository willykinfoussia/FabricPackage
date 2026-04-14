"""Shared pipeline contracts and helpers.

Re-exports :class:`fabrictools.pipelines.config.TableJobConfig` and job builders from
:mod:`fabrictools.pipelines.config`.
"""

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

