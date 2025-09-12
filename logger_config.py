import sys
from loguru import logger

# --- Centralized Logger Configuration ---
logger.remove()
logger.add(
    "adk_agent_runtime.log",
    rotation="10 MB",
    retention="7 days",
    compression="zip",
    level="INFO",
    encoding="utf-8",
    enqueue=True,
    serialize=False,
    backtrace=True,
    diagnose=True,
    mode="a"  # Ensure logs are appended
)
logger.add(sys.stderr, level="INFO")

# Export the configured logger instance
log = logger
