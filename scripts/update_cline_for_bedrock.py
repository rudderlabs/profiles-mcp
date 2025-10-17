#!/usr/bin/env python3
"""
Simple script to update Cline configuration to use Bedrock wrapper.
Called automatically by setup.sh when Bedrock environment variables are detected.
"""

import json
import os
import sys
from pathlib import Path


def get_cline_config_path():
    """Get the path to Cline's MCP configuration file"""
    home = Path.home()
    
    # Check common locations for Cline config
    possible_paths = [
        home / "Library/Application Support/Code/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json",
        home / ".config/Code/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json",
        home / ".local/share/code-server/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json",
    ]
    
    for path in possible_paths:
        if path.exists():
            return path
    
    return None


def update_cline_config():
    """Update Cline to use Bedrock wrapper instead of direct Anthropic API"""
    # Check if we're in a container environment
    if os.path.exists('/.dockerenv') or os.getenv('IS_CLOUD_BASED') == 'true':
        print("Running in container/cloud environment - skipping Cline configuration")
        return True  # Not an error, just skip
    
    config_path = get_cline_config_path()
    
    if not config_path:
        print("Cline configuration not found. Please ensure Cline is installed.", file=sys.stderr)
        return False
    
    try:
        # Read existing config
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Get the wrapper script path
        wrapper_path = Path(__file__).parent / "bedrock_anthropic_wrapper.sh"
        
        # Update the anthropicPath to point to our wrapper
        config['anthropicPath'] = str(wrapper_path.absolute())
        
        # Write updated config
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"Updated Cline config at: {config_path}")
        return True
        
    except Exception as e:
        print(f"Error updating Cline config: {e}", file=sys.stderr)
        return False


if __name__ == "__main__":
    sys.exit(0 if update_cline_config() else 1)
