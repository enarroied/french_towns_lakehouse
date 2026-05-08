"""Shared logging utilities for all Prefect flows."""

from datetime import datetime
from typing import Any

from prefect import get_run_logger


def log(message: str, level: str = "info", logger: Any | None = None) -> None:
    """Log message using Prefect logger if available, else print with timestamp.

    Args:
        message: Message to log
        level: Log level ('info', 'warning', 'error'). Defaults to 'info'.
        logger: Optional Prefect logger to use. If None, tries Prefects context.
    """
    if logger:
        getattr(logger, level)(message)
    else:
        try:
            logger = get_run_logger()
            getattr(logger, level)(message)
        except Exception:
            timestamp = datetime.now().isoformat()
            print(f"[{timestamp}] {message}")
