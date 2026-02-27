import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()  # Load .env file - safe to call multiple times (idempotent)

ANALYTICS_WRITE_KEY = "2xL75MYRl00bI88EqinCq5T7RfO"
ANALYTICS_DATA_PLANE_URL = "https://rudderstacqiqh.dataplane.rudderstack.com"

# External Services
IS_CLOUD_BASED = os.getenv("IS_CLOUD_BASED", "").lower() in ("true", "1", "yes", "on")

_retrieval_url_env = os.getenv("RETRIEVAL_API_URL")
if _retrieval_url_env:
    RETRIEVAL_API_URL = _retrieval_url_env
elif IS_CLOUD_BASED:
    RETRIEVAL_API_URL = "https://profiles-mcp-service-admin.rudderstack.com"
else:
    RETRIEVAL_API_URL = "https://profiles-mcp-service.rudderstack.com"

USE_PB_QUERY = os.getenv("USE_PB_QUERY", "true").lower() in ("true", "1", "yes")

PB_CONFIG_DIR = Path.home() / ".pb"
PB_PREFERENCES_FILE = "preferences.yaml"
PB_PREFERENCES_PATH = PB_CONFIG_DIR / PB_PREFERENCES_FILE
PB_SITE_CONFIG_FILE = "siteconfig.yaml"
PB_SITE_CONFIG_PATH = PB_CONFIG_DIR / PB_SITE_CONFIG_FILE
