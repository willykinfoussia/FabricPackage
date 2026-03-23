"""Dimensions pipeline orchestration."""

from __future__ import annotations

from fabrictools.dimensions._legacy import get_legacy_module


def generate_dimensions(*args, **kwargs):
    return get_legacy_module().generate_dimensions(*args, **kwargs)


def _write_dimension_targets(*args, **kwargs):
    return get_legacy_module()._write_dimension_targets(*args, **kwargs)


__all__ = ["generate_dimensions", "_write_dimension_targets"]

