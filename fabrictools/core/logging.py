"""Internal logging utility for fabrictools."""

from __future__ import annotations

import datetime
import logging
import sys

_LEVELS = {
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "debug": logging.DEBUG,
}

_logger = logging.getLogger("fabrictools")
# Do not rely on logging.basicConfig(): in Jupyter/IPython the root logger often
# already has handlers, so basicConfig is a no-op, and the effective level can be
# WARNING — INFO lines from this package would be dropped. A dedicated handler on
# our logger keeps output visible without changing global root configuration.
_logger.setLevel(logging.DEBUG)
if not _logger.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(_handler)
    _logger.propagate = False


def log(message: str, level: str = "info") -> None:
    """Emit a timestamped line on the ``fabrictools`` logger.

    :param message: Text to log.
    :param level: One of ``info``, ``warning``, ``error``, ``debug`` (case-insensitive).
    :type message: str
    :type level: str
    """
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    lvl = _LEVELS.get(level.lower(), logging.INFO)
    _logger.log(lvl, "[%s] %s", ts, message)

