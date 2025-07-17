#!/usr/bin/env python3

import json
import os
import platform
import sys
from pathlib import Path


def get_app_config_info(app):
    """Get the configuration directory and filename based on the app and OS

    Args:
        app (str): The app to get config for ('cursor', 'claude-desktop', or 'claude-code')

    Returns:
        tuple: (config_dir, config_file_name)
               config_dir - Path to the config directory or None if not found
               config_file_name - Name of the config file
    """
    system = platform.system()
    home = Path.home()

    config_dir = None
    config_file_name = None
    if app == 'cursor':
        config_file_name = "mcp.json"
        if system == "Darwin" or system == "Linux":
            config_dir = home / ".cursor"
        elif system == "Windows":
            userprofile = os.getenv("USERPROFILE")
            config_dir = Path(userprofile) / ".cursor" if userprofile else None

    elif app == 'claude-desktop':
        config_file_name = "claude_desktop_config.json"
        if system == "Darwin":  # macOS
            config_dir = home / "Library" / "Application Support" / "Claude"
        elif system == "Windows":
            appdata = os.getenv("APPDATA")
            config_dir = Path(appdata) / "Claude" if appdata else None
        elif system == "Linux":
            config_dir = home / ".config" / "Claude"

    elif app == 'claude-code':
        config_file_name = ".claude.json"
        if system == "Darwin" or system == "Linux":
            config_dir = home
        elif system == "Windows":
            # TODO: Find the correct path for Windows
            config_dir = Path(os.getenv("APPDATA")) / "Claude"

    return config_dir, config_file_name


def update_config(app, start_script):
    """Update the MCP configuration file for the specified app

    Args:
        app (str): The app to update ('cursor', 'claude-desktop', or 'claude-code')
        start_script (str): Path to the start script

    Returns:
        bool: True if successful, False otherwise
    """
    config_dir, config_file_name = get_app_config_info(app)

    if not config_dir:
        print(f"✗ {app.capitalize()} config directory not found for this OS")
        return False

    config_dir.mkdir(exist_ok=True, parents=True)
    config_file = config_dir / config_file_name

    try:
        if config_file.exists():
            with open(config_file, 'r') as f:
                config = json.load(f)
        else:
            config = {}

        if "mcpServers" not in config:
            config["mcpServers"] = {}

        config["mcpServers"]["profiles"] = {
            "command": start_script,
            "args": []
        }

        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)

        print(f"✓ Updated {app.capitalize()} config ({config_file}) successfully")
        return True

    except Exception as e:
        print(f"✗ Failed to update {app.capitalize()} configuration: {str(e)}")
        return False


def update_mcp_config(target="all"):
    """Update MCP configuration for specified targets

    Args:
        target (str): Which config to update - "cursor", "claude-desktop", "claude-code", or "all"
    """
    current_dir = Path(__file__).parent.parent
    start_script = current_dir / "scripts" / "start.sh"
    start_script = str(start_script.absolute())

    success = True

    if target in ["cursor", "all"]:
        cursor_success = update_config('cursor', start_script)
        success = success and cursor_success

    if target in ["claude-desktop", "all"]:
        claude_success = update_config('claude-desktop', start_script)
        success = success and claude_success

    if target in ["claude-code", "all"]:
        claude_code_success = update_config('claude-code', start_script)
        success = success and claude_code_success

    return success


def get_target():
    """Prompt user to select which configuration to update

    Returns:
        str: Target configuration - 'cursor', 'claude-desktop', 'claude-code', or 'all'
    """

    print("Which configuration would you like to update?")
    print("1. Cursor")
    print("2. Claude Desktop")
    print("3. Claude Code")
    print("4. All (default)")
    choice = input("Enter your choice (1-4) [4]: ").strip()

    if choice == "1":
        return "cursor"
    elif choice == "2":
        return "claude-desktop"
    elif choice == "3":
        return "claude-code"
    else:
        return "all"


if __name__ == "__main__":
    target = get_target()
    print(f"Updating {target} configuration...")

    success = update_mcp_config(target)
    sys.exit(0 if success else 1)