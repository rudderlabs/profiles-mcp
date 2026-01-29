import os
from pathlib import Path

ANALYTICS_WRITE_KEY = "2xL75MYRl00bI88EqinCq5T7RfO"
ANALYTICS_DATA_PLANE_URL = "https://rudderstacqiqh.dataplane.rudderstack.com"

# External Services
_is_cloud_based_env = os.getenv("IS_CLOUD_BASED")
IS_CLOUD_BASED = (
    _is_cloud_based_env is not None and _is_cloud_based_env.lower() == "true"
)

_retrieval_url_env = os.getenv("RETRIEVAL_API_URL")
if _retrieval_url_env:  # Non-empty, non-None
    RETRIEVAL_API_URL = _retrieval_url_env
elif _is_cloud_based_env is not None and _is_cloud_based_env.lower() == "false":
    # IS_CLOUD_BASED explicitly set to false
    RETRIEVAL_API_URL = "https://profiles-mcp-service.rudderstack.com"
else:
    # Failsafe: IS_CLOUD_BASED is true, None, or unset -> use admin URL
    RETRIEVAL_API_URL = "https://profiles-mcp-service-admin.rudderstack.com"

PB_CONFIG_DIR = Path.home() / ".pb"
PB_PREFERENCES_FILE = "preferences.yaml"
PB_PREFERENCES_PATH = PB_CONFIG_DIR / PB_PREFERENCES_FILE
PB_SITE_CONFIG_FILE = "siteconfig.yaml"
PB_SITE_CONFIG_PATH = PB_CONFIG_DIR / PB_SITE_CONFIG_FILE
