"""Date dimension builders."""

from __future__ import annotations

from fabrictools.dimensions._legacy import get_legacy_module


def build_dimension_date(*args, **kwargs):
    return get_legacy_module().build_dimension_date(*args, **kwargs)


def _default_date_bounds(*args, **kwargs):
    return get_legacy_module()._default_date_bounds(*args, **kwargs)


__all__ = ["build_dimension_date", "_default_date_bounds"]

