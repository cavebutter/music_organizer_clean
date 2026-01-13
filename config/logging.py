"""
Centralized logging configuration for the music_organizer project.

Usage:
    from config import setup_logging
    setup_logging("logs/my_script.log")

Or for module-level logging with context:
    from config import get_logger
    logger = get_logger(__name__)
"""
import sys
from loguru import logger

# Default format with module, function, and line number for tracing
DEFAULT_FORMAT = (
    "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
    "{level: <8} | "
    "{module}:{function}:{line} | "
    "{message}"
)

# Compact format for console output
CONSOLE_FORMAT = (
    "{time:HH:mm:ss} | "
    "{level: <8} | "
    "{module}:{function}:{line} | "
    "{message}"
)


def setup_logging(
    log_file: str = None,
    level: str = "DEBUG",
    rotation: str = "10 MB",
    retention: str = "7 days",
    console: bool = True,
    console_level: str = None
):
    """
    Configure centralized logging for the application.

    Args:
        log_file: Path to log file. If None, only console logging is enabled.
        level: Minimum log level for file output (DEBUG, INFO, WARNING, ERROR).
        rotation: When to rotate log files (e.g., "10 MB", "1 day", "00:00").
        retention: How long to keep old log files (e.g., "7 days", "1 week").
        console: Whether to output to console (stderr).
        console_level: Console log level. Defaults to same as file level.

    Example:
        setup_logging("logs/e2e.log")  # Full logging with defaults
        setup_logging("logs/prod.log", level="INFO")  # Less verbose
        setup_logging(console_level="WARNING")  # Console warnings only, no file
    """
    # Remove default handler to avoid duplicates
    logger.remove()

    # Console output
    if console:
        logger.add(
            sys.stderr,
            level=console_level or level,
            format=CONSOLE_FORMAT,
            colorize=True
        )

    # File output
    if log_file:
        logger.add(
            log_file,
            level=level,
            format=DEFAULT_FORMAT,
            rotation=rotation,
            retention=retention,
            compression="zip"  # Compress rotated logs
        )

    logger.debug(f"Logging configured: file={log_file}, level={level}")


def get_logger(name: str = None):
    """
    Get the configured logger instance.

    This is a convenience function for modules that want to ensure they're
    using the centralized logger. The loguru logger is a singleton, so this
    returns the same logger that setup_logging configures.

    Args:
        name: Optional name (typically __name__) - currently unused but
              available for future enhancements.

    Returns:
        The loguru logger instance.
    """
    return logger
