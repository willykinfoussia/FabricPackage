"""Prepared aggregation helpers."""

from __future__ import annotations

from fabrictools.prepare._legacy import get_legacy_module


def generate_prepared_aggregations(*args, **kwargs):
    return get_legacy_module().generate_prepared_aggregations(*args, **kwargs)


__all__ = ["generate_prepared_aggregations"]

