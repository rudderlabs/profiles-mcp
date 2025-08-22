"""
Environment utilities for detecting deployment context and configuration.
"""

import os
from dotenv import load_dotenv


def is_cloud_based_environment() -> bool:
    """
    Check if running in a kubernetes pod environment by reading IS_CLOUD_BASED env variable.
    
    Returns:
        bool: True if running in kubernetes pod environment, False for local development
    """
    env_file = os.path.join(os.getcwd(), ".env")
    if os.path.exists(env_file):
        load_dotenv(env_file)
    
    is_cloud = os.getenv("IS_CLOUD_BASED", "false").lower()
    return is_cloud in ["true", "1", "yes", "on"]
