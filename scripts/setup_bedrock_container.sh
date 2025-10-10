#!/bin/bash
# Setup script for Bedrock in containerized environment
# This script is meant to be run inside the code-server container

set -e

echo "Setting up Amazon Bedrock support for Profiles MCP..."

# Check if we should use Bedrock
if [ "$USE_BEDROCK" != "true" ]; then
    echo "USE_BEDROCK is not set to true, skipping Bedrock setup"
    exit 0
fi

# Validate required environment variables
if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "ERROR: AWS credentials not found in environment"
    echo "Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
    exit 1
fi

# Set default values if not provided
BEDROCK_MODEL_ID="${BEDROCK_MODEL_ID:-anthropic.claude-3-sonnet-20240229-v1:0}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

echo "Configuring Bedrock with:"
echo "  Model: $BEDROCK_MODEL_ID"
echo "  Region: $AWS_DEFAULT_REGION"

# Create the Bedrock wrapper script
WRAPPER_SCRIPT="/home/codeuser/profiles-mcp/scripts/bedrock_anthropic_wrapper.sh"
cat > "$WRAPPER_SCRIPT" << EOF
#!/bin/bash
# Wrapper script to use Bedrock client as Anthropic replacement

export BEDROCK_MODEL_ID="$BEDROCK_MODEL_ID"
export AWS_DEFAULT_REGION="$AWS_DEFAULT_REGION"
export AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"

# Get the directory of this script
SCRIPT_DIR="\$( cd "\$( dirname "\${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="\$( cd "\$SCRIPT_DIR/.." && pwd )"

# Activate virtual environment if it exists
if [ -f "\$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "\$PROJECT_ROOT/.venv/bin/activate"
fi

# Run the Bedrock client
python "\$PROJECT_ROOT/src/bedrock_client.py" "\$@"
EOF

chmod +x "$WRAPPER_SCRIPT"
echo "Created Bedrock wrapper script"

# Update Cline configuration to use Bedrock
CLINE_CONFIG="/home/codeuser/.local/share/code-server/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json"

if [ -f "$CLINE_CONFIG" ]; then
    # Backup existing config
    cp "$CLINE_CONFIG" "${CLINE_CONFIG}.backup"
    
    # Update config to use Bedrock wrapper
    python -c "
import json
with open('$CLINE_CONFIG', 'r') as f:
    config = json.load(f)
config['anthropicPath'] = '$WRAPPER_SCRIPT'
with open('$CLINE_CONFIG', 'w') as f:
    json.dump(config, f, indent=2)
"
    echo "Updated Cline configuration to use Bedrock"
else
    echo "WARNING: Cline configuration not found at $CLINE_CONFIG"
fi

# Create a marker file to indicate Bedrock is configured
touch /home/codeuser/.bedrock_configured

echo "✅ Bedrock setup completed successfully!"
echo "Cline will now use Amazon Bedrock instead of Anthropic API"
