import logging
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

    return logger