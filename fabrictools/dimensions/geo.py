"""Geographic dimensions builders."""

from __future__ import annotations

from fabrictools.dimensions._legacy import get_legacy_module


def build_dimension_country(*args, **kwargs):
    return get_legacy_module().build_dimension_country(*args, **kwargs)


def build_dimension_city(*args, **kwargs):
    return get_legacy_module().build_dimension_city(*args, **kwargs)


def _import_csc_package(*args, **kwargs):
    return get_legacy_module()._import_csc_package(*args, **kwargs)


__all__ = ["build_dimension_country", "build_dimension_city", "_import_csc_package"]

