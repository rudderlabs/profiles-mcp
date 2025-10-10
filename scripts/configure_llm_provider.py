#!/usr/bin/env python3
"""
Configure LLM provider for Profiles MCP
Supports switching between Anthropic and Amazon Bedrock
"""

import os
import json
import sys
from pathlib import Path


def get_cline_config_path():
    """Get the path to Cline's MCP configuration"""
    home = Path.home()
    
    # Check for code-server environment (containerized)
    code_server_path = home / ".local/share/code-server/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json"
    if code_server_path.exists():
        return code_server_path
    
    # Check for local VS Code
    if sys.platform == "darwin":
        vscode_path = home / "Library/Application Support/Code/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json"
    elif sys.platform == "linux":
        vscode_path = home / ".config/Code/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json"
    else:
        vscode_path = home / "AppData/Roaming/Code/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json"
    
    return vscode_path


def configure_bedrock_provider():
    """Configure Cline to use Bedrock instead of Anthropic"""
    
    # Check for AWS credentials
    import boto3
    session = boto3.Session()
    if not session.get_credentials():
        print("❌ AWS credentials not found!")
        print("Please configure AWS credentials first:")
        print("  - AWS CLI: aws configure")
        print("  - Environment variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        print("  - IAM role (if on EC2/ECS)")
        return False
    
    # Get Bedrock configuration
    model_id = input("Enter Bedrock model ID [anthropic.claude-3-sonnet-20240229-v1:0]: ").strip()
    if not model_id:
        model_id = "anthropic.claude-3-sonnet-20240229-v1:0"
    
    region = input(f"Enter AWS region [{os.getenv('AWS_DEFAULT_REGION', 'us-east-1')}]: ").strip()
    if not region:
        region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    
    # Create wrapper script
    wrapper_script = Path(__file__).parent / "bedrock_anthropic_wrapper.sh"
    wrapper_content = f"""#!/bin/bash
# Wrapper script to use Bedrock client as Anthropic replacement

export BEDROCK_MODEL_ID="{model_id}"
export AWS_DEFAULT_REGION="{region}"

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${{BASH_SOURCE[0]}}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# Activate virtual environment if it exists
if [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

# Run the Bedrock client
python "$PROJECT_ROOT/src/bedrock_client.py" "$@"
"""
    
    wrapper_script.write_text(wrapper_content)
    wrapper_script.chmod(0o755)
    
    print(f"✅ Created Bedrock wrapper script: {wrapper_script}")
    
    # Update Cline configuration
    config_path = get_cline_config_path()
    if config_path.exists():
        print(f"📝 Updating Cline configuration at: {config_path}")
        
        # Backup existing configuration
        backup_path = config_path.with_suffix('.json.backup')
        config_path.rename(backup_path)
        print(f"📦 Backed up existing config to: {backup_path}")
        
        # Update configuration to use Bedrock wrapper
        with open(backup_path, 'r') as f:
            config = json.load(f)
        
        # Add Bedrock configuration
        if "anthropicPath" not in config:
            config["anthropicPath"] = str(wrapper_script)
            print("✅ Configured Cline to use Bedrock client")
        
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
    else:
        print(f"⚠️  Cline configuration not found at: {config_path}")
        print("Please ensure Cline is installed and configured first")
    
    # Create environment file for easy configuration
    env_file = Path(__file__).parent.parent / ".env.bedrock"
    env_content = f"""# Bedrock configuration for Profiles MCP
BEDROCK_MODEL_ID={model_id}
AWS_DEFAULT_REGION={region}
USE_BEDROCK=true
"""
    env_file.write_text(env_content)
    print(f"✅ Created environment file: {env_file}")
    
    return True


def configure_anthropic_provider():
    """Configure Cline to use Anthropic (default)"""
    
    # Check for API key
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        api_key = input("Enter your Anthropic API key: ").strip()
        if not api_key:
            print("❌ Anthropic API key is required")
            return False
    
    # Update Cline configuration
    config_path = get_cline_config_path()
    if config_path.exists():
        print(f"📝 Updating Cline configuration at: {config_path}")
        
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Remove custom anthropicPath if set
        if "anthropicPath" in config:
            del config["anthropicPath"]
            print("✅ Removed custom Anthropic path (will use default)")
        
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
    
    # Update environment file
    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        content = env_file.read_text()
        if "ANTHROPIC_API_KEY" not in content:
            content += f"\nANTHROPIC_API_KEY={api_key}\n"
            env_file.write_text(content)
            print(f"✅ Added API key to: {env_file}")
    
    return True


def main():
    """Main configuration interface"""
    print("🚀 Profiles MCP - LLM Provider Configuration")
    print("=" * 50)
    print("\nThis tool configures which LLM provider to use:")
    print("1. Anthropic Claude (default)")
    print("2. Amazon Bedrock Claude (for PII compliance)")
    print("\nWhich provider would you like to use?")
    
    choice = input("Enter your choice (1 or 2) [1]: ").strip()
    
    if choice == "2":
        print("\n🔧 Configuring Amazon Bedrock...")
        if configure_bedrock_provider():
            print("\n✅ Successfully configured Bedrock!")
            print("\nTo use Bedrock in:")
            print("- Local environment: Restart VS Code/Cursor")
            print("- Container: Rebuild with updated configuration")
    else:
        print("\n🔧 Configuring Anthropic...")
        if configure_anthropic_provider():
            print("\n✅ Successfully configured Anthropic!")
    
    print("\n📌 Note: You can run this script again to switch providers")


if __name__ == "__main__":
    main()
