"""Column semantic resolution helpers."""

from __future__ import annotations

from fabrictools.prepare._legacy import get_legacy_module


def resolve_columns(*args, **kwargs):
    return get_legacy_module().resolve_columns(*args, **kwargs)


def _ensure_prefix_rules(*args, **kwargs):
    return get_legacy_module()._ensure_prefix_rules(*args, **kwargs)


def _safe_read_table(*args, **kwargs):
    return get_legacy_module()._safe_read_table(*args, **kwargs)


def _write_unresolved_audit(*args, **kwargs):
    return get_legacy_module()._write_unresolved_audit(*args, **kwargs)


def _layer1_resolve(*args, **kwargs):
    return get_legacy_module()._layer1_resolve(*args, **kwargs)


def _layer2_profile_resolve(*args, **kwargs):
    return get_legacy_module()._layer2_profile_resolve(*args, **kwargs)


def _layer3_mapping_resolve(*args, **kwargs):
    return get_legacy_module()._layer3_mapping_resolve(*args, **kwargs)


__all__ = [
    "resolve_columns",
    "_ensure_prefix_rules",
    "_safe_read_table",
    "_write_unresolved_audit",
    "_layer1_resolve",
    "_layer2_profile_resolve",
    "_layer3_mapping_resolve",
]

