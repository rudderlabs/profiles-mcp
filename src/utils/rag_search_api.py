import base64
import os
from typing import Dict, List

import requests

from constants import RETRIEVAL_API_URL, IS_CLOUD_BASED
from logger import setup_logger

logger = setup_logger(__name__)


class RAGSearchAPIClient:
    """
    Client for interacting with the RAG search API
    """

    def __init__(self):
        """
        Initialize the RAG Search API client
        """
        self.base_url = RETRIEVAL_API_URL
        if IS_CLOUD_BASED:
            self.username = os.getenv("RUDDERSTACK_ADMIN_USERNAME")
            self.password = os.getenv("RUDDERSTACK_ADMIN_PASSWORD")
        else:
            self.token = os.getenv("RUDDERSTACK_PAT")

    def _get_headers(self) -> Dict[str, str]:
        """Get standard headers for API requests"""
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "rudder-profiles-mcp/0.1",
        }

        if IS_CLOUD_BASED:
            if self.username and self.password:
                credentials = f"{self.username}:{self.password}"
                encoded_credentials = base64.b64encode(credentials.encode()).decode()
                headers["Authorization"] = f"Basic {encoded_credentials}"
            else:
                logger.warning("RudderStack Admin credentials not set")
        else:
            if self.token:
                headers["Authorization"] = f"Bearer {self.token}"
            else:
                logger.warning("RudderStack PAT not set")

        return headers

    def search(self, query: str) -> List[str]:
        """
        Make a search request to the API

        Args:
            query: The search query

        Returns:
            List of text results
        """
        try:
            url = f"{self.base_url}/search"
            payload = {"query": query}

            headers = self._get_headers()
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()

            return [r["text"] for r in response.json()["results"]]
        except Exception as e:
            logger.error(f"Error searching profiles docs with query '{query}': {e}")
            raise e
