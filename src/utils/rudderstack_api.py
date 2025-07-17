import requests
import os
from typing import Dict, Any, Optional
from logger import setup_logger

logger = setup_logger(__name__)


class RudderstackAPIClient:
    """
    Client for interacting with the RudderStack API
    """

    def __init__(self):
        """
        Initialize the RudderStack API client
        """
        self.token = os.getenv('RUDDERSTACK_PAT')
        self.base_url = "https://api.rudderstack.com"

    def _get_headers(self) -> Dict[str, str]:
        """Get standard headers for API requests"""
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "User-Agent": "rudder-profiles-mcp/0.1"
        }

    def get_resource(self, path: str, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Make a GET request to the RudderStack API

        Args:
            path: API endpoint path
            headers: Additional headers to include

        Returns:
            Response data as a dictionary
        """
        try:
            url = f"{self.base_url}/{path}"
            request_headers = self._get_headers()

            if headers:
                request_headers.update(headers)

            response = requests.get(url, headers=request_headers)
            response.raise_for_status()

            return response.json()
        except Exception as e:
            logger.error(f"Error fetching resource {path}: {e}")
            raise e

    def get_user_details(self) -> Dict[str, Any]:
        """
        Retrieve the details of the authenticated user

        Returns:
            User details dictionary containing information about the authenticated user

        Raises:
            requests.exceptions.HTTPError: If the API request fails
        """
        path = "getUser"

        try:
            user_details = self.get_resource(path)
            return user_details
        except requests.exceptions.HTTPError as e:
            raise Exception(f"Error fetching user details: {e}")
