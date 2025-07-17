import requests
import os
from typing import Dict, List
from logger import setup_logger
from constants import RETRIEVAL_API_URL

logger = setup_logger(__name__)


class RAGSearchAPIClient:
    """
    Client for interacting with the RAG search API
    """

    def __init__(self):
        """
        Initialize the RAG Search API client
        """
        self.token = os.getenv('RUDDERSTACK_PAT')
        self.base_url = RETRIEVAL_API_URL

    def _get_headers(self) -> Dict[str, str]:
        """Get standard headers for API requests"""
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "User-Agent": "rudder-profiles-mcp/0.1"
        }

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