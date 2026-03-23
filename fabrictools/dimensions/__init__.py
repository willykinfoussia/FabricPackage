"""Dimensions package API."""

from __future__ import annotations

from fabrictools.dimensions._legacy import get_legacy_module
from fabrictools.dimensions.date import _default_date_bounds, build_dimension_date
from fabrictools.dimensions.geo import _import_csc_package, build_dimension_city, build_dimension_country
from fabrictools.dimensions.pipeline import _write_dimension_targets, generate_dimensions

_legacy = get_legacy_module()

# Compatibility exports for tests/callers patching module-level symbols.
get_spark = _legacy.get_spark
write_lakehouse = _legacy.write_lakehouse
write_warehouse = _legacy.write_warehouse

__all__ = [
    "build_dimension_date",
    "build_dimension_country",
    "build_dimension_city",
    "generate_dimensions",
    "_default_date_bounds",
    "_import_csc_package",
    "_write_dimension_targets",
    "get_spark",
    "write_lakehouse",
    "write_warehouse",
]

