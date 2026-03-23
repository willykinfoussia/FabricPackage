"""Load the legacy dimensions implementation from the flat module file."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

_LEGACY_MODULE_NAME = "fabrictools._dimensions_legacy_impl"
_LEGACY_MODULE: ModuleType | None = None


def get_legacy_module() -> ModuleType:
    """Load and cache the legacy `dimensions.py` implementation module."""
    global _LEGACY_MODULE
    if _LEGACY_MODULE is not None:
        return _LEGACY_MODULE

    legacy_file = Path(__file__).resolve().parent.parent / "dimensions.py"
    spec = importlib.util.spec_from_file_location(_LEGACY_MODULE_NAME, legacy_file)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load legacy module from '{legacy_file}'.")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _LEGACY_MODULE = module
    return module

