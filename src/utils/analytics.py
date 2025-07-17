import os
import rudderstack.analytics as rudder_analytics
from constants import ANALYTICS_WRITE_KEY, ANALYTICS_DATA_PLANE_URL, PB_PREFERENCES_PATH
import uuid
import yaml
from logger import setup_logger
import platform
from typing import Optional

logger = setup_logger(__name__)

class Analytics:
    def __init__(self):
        rudder_analytics.write_key = ANALYTICS_WRITE_KEY
        rudder_analytics.dataPlaneUrl = ANALYTICS_DATA_PLANE_URL
        self._set_anonymous_id()
        self._set_context()

    def _set_anonymous_id(self):
        try:
            if os.path.exists(PB_PREFERENCES_PATH):
                with open(PB_PREFERENCES_PATH, 'r') as f:
                    preferences = yaml.safe_load(f)
                self.anonymous_id = preferences.get('anonymous_id')
            else:
                self.anonymous_id = str(uuid.uuid4())
                preferences = {
                    'anonymous_id': self.anonymous_id
                }
                with open(PB_PREFERENCES_PATH, 'w') as f:
                    yaml.dump(preferences, f)
        except Exception as e:
            logger.warning(f"Error setting anonymous id: {e}")
            self.anonymous_id = str(uuid.uuid4())


    def _set_context(self):
        try:
            self.context = {
                'platform': {
                    'name': platform.system(),
                    'version': platform.version(),
                    'architecture': platform.machine(),
                    'node': platform.node()
            },
            'language': {
                'name': platform.python_implementation(),
                'version': platform.python_version()
                },
            }
        except Exception as e:
            logger.warning(f"Error setting context: {e}")
            self.context = {}


    def track(self, event: str, properties: Optional[dict] = None):
        if properties is None:
                properties = {}
        try:
            rudder_analytics.track(
                anonymous_id=self.anonymous_id,
                user_id=self.user_id,
                context=self.context,
                event=event,
                properties=properties,
            )
        except Exception as e:
            logger.warning(f"Error tracking event: {event} with properties: {properties} - {e}")
            pass


    def identify(self, user_id: str, traits: dict = None):
        self.user_id = user_id
        if traits is None:
            traits = {}
        try:
            rudder_analytics.identify(
                anonymous_id=self.anonymous_id,
                context=self.context,
                user_id=user_id,
                traits=traits
            )
        except Exception as e:
            pass
