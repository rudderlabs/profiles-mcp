import logging
import os
from pathlib import Path

project_root = Path(__file__).parent.parent.absolute()

LOG_FILE = project_root / "profiles-mcp.log"

def setup_logger(name=__name__):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        # File handler
        ch = logging.FileHandler(LOG_FILE)
        ch.setLevel(logging.DEBUG)

        # Formatter
        ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

        # Add handler
        logger.addHandler(ch)

        # Stream handler so logs are visible in production/CI stdout.
        stream_level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
        stream_level = getattr(logging, stream_level_name, logging.INFO)
        sh = logging.StreamHandler()
        sh.setLevel(stream_level)
        sh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(sh)

    return logger