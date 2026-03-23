"""Prepared semantic model publishing helpers."""

from __future__ import annotations

from fabrictools.prepare._legacy import get_legacy_module


def publish_semantic_model(*args, **kwargs):
    return get_legacy_module().publish_semantic_model(*args, **kwargs)


__all__ = ["publish_semantic_model"]

