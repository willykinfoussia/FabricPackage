"""Internal logging utility for fabrictools."""

from __future__ import annotations

import datetime
import logging

_LEVELS = {
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "debug": logging.DEBUG,
}

logging.basicConfig(
    format="%(message)s",
    level=logging.INFO,
)
_logger = logging.getLogger("fabrictools")


def log(message: str, level: str = "info") -> None:
    """
    Emit a timestamped log message.

    Parameters
    ----------
    message:
        Text to log.
    level:
        One of ``"info"``, ``"warning"``, ``"error"``, ``"debug"``.
        Defaults to ``"info"``.
    """
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    lvl = _LEVELS.get(level.lower(), logging.INFO)
    _logger.log(lvl, "[%s] %s", ts, message)
