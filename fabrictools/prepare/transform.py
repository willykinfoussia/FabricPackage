"""Prepared transform and write helpers."""

from __future__ import annotations

from fabrictools.prepare._legacy import get_legacy_module


def transform_to_prepared(*args, **kwargs):
    return get_legacy_module().transform_to_prepared(*args, **kwargs)


def write_prepared_table(*args, **kwargs):
    return get_legacy_module().write_prepared_table(*args, **kwargs)


def _localize_alias_tokens(*args, **kwargs):
    return get_legacy_module()._localize_alias_tokens(*args, **kwargs)


__all__ = [
    "transform_to_prepared",
    "write_prepared_table",
    "_localize_alias_tokens",
]

