import logging


def get_scraper_logger(name: str) -> logging.Logger:
    """Get a logger configured for scraper output."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def log_scraper_result(logger: logging.Logger, name: str, count: int, key: str) -> None:
    """Log a successful scraper result."""
    logger.info("Scraped %d items. Uploaded to %s", count, key)


def log_scraper_start(
    logger: logging.Logger, name: str, url: str | None = None
) -> None:
    """Log scraper start."""
    if url:
        logger.info("Starting %s from %s", name, url)
    else:
        logger.info("Starting %s", name)


def log_scraper_error(
    logger: logging.Logger, name: str, error: str | Exception
) -> None:
    """Log a scraper error."""
    if isinstance(error, Exception):
        logger.error("%s failed: %s", name, error)
    else:
        logger.error("%s failed: %s", name, error)


def log_scraper_items(logger: logging.Logger, message: str, count: int) -> None:
    """Log item count progress."""
    logger.info("%s: %d", message, count)
