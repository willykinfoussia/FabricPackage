"""Prepared schema snapshot helpers."""

from __future__ import annotations

from fabrictools.prepare._legacy import get_legacy_module


def snapshot_source_schema(*args, **kwargs):
    return get_legacy_module().snapshot_source_schema(*args, **kwargs)


__all__ = ["snapshot_source_schema"]

