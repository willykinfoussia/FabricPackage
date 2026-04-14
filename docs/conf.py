# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import importlib.util
import os
import sys

sys.path.insert(0, os.path.abspath(".."))

# Version without importing fabrictools (package pulls PySpark at import time).
_version_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "fabrictools", "_version.py"))
_spec = importlib.util.spec_from_file_location("fabrictools._version", _version_file)
_version_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_version_mod)

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "FabricTools"
copyright = "2026, Kinfoussia Willy"
author = "Kinfoussia Willy"
version = _version_mod.__version__
release = version

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx.ext.doctest",
    "sphinx.ext.todo",
    "sphinx.ext.coverage",
    "sphinx.ext.mathjax",
    "sphinx.ext.ifconfig",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

autodoc_mock_imports = [
    "pyspark",
    "delta",
    "delta.tables",
    "plotly",
    "plotly.express",
    "sempy",
    "sempy.fabric",
    "sempy_labs",
    "requests",
]

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}

napoleon_include_init_with_doc = True

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}


def autodoc_skip_member(app, what, name, obj, skip, options):
    """Hide flat API re-exports on submodule pages (document them only under ``fabrictools``).

    Re-exported callables set ``__module__`` to ``fabrictools`` (see ``fabrictools/__init__.py``).
    """
    if what != "function":
        return None
    if getattr(obj, "__module__", None) != "fabrictools":
        return None
    current = app.env.ref_context.get("py:module")
    if not current or current == "fabrictools":
        return None
    return True


def setup(app):
    app.connect("autodoc-skip-member", autodoc_skip_member)


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
