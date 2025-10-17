#!/bin/bash
# Universal wrapper for Cline - routes to Bedrock or Anthropic based on configuration
# This script is ALWAYS called by Cline and decides at runtime which backend to use

set -euo pipefail

# Get script directory and project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# Setup logging
LOG_FILE="$PROJECT_ROOT/bedrock_wrapper_calls.log"
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

# Load environment variables from .env file
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
    log_message "Loaded environment from .env file"
fi

# Activate virtual environment if it exists
if [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

# Determine which backend to use
if [ -n "${AWS_BEARER_TOKEN_BEDROCK:-}" ]; then
    # Bedrock configuration detected
    log_message "BEDROCK configuration detected - routing to Bedrock adapter"
    log_message "  Auth method: ${BEDROCK_AUTH_METHOD:-api_key}"
    log_message "  Region: ${AWS_DEFAULT_REGION:-us-east-1}"
    log_message "  Model: ${BEDROCK_MODEL_ID:-default}"
    
    # Run the Bedrock adapter
    exec python "$PROJECT_ROOT/src/bedrock/cline_adapter.py" "$@"
    
elif [ -n "${ANTHROPIC_API_KEY:-}" ]; then
    # Anthropic configuration detected
    log_message "ANTHROPIC configuration detected - routing to Anthropic CLI"
    
    # Check if anthropic CLI exists
    if command -v anthropic &> /dev/null; then
        exec anthropic "$@"
    else
        log_message "ERROR: Anthropic CLI not found in PATH"
        echo "Error: Anthropic CLI is not installed. Please install it with: pip install anthropic" >&2
        exit 1
    fi
    
else
    # No API keys configured
    log_message "WARNING: No API keys configured (neither AWS_BEARER_TOKEN_BEDROCK nor ANTHROPIC_API_KEY found)"
    
    # Return a helpful error message in Cline-compatible format
    cat <<EOF
{
  "error": {
    "type": "authentication_error",
    "message": "No API keys configured. Please set either AWS_BEARER_TOKEN_BEDROCK or ANTHROPIC_API_KEY in your .env file or environment variables."
  }
}
EOF
    exit 1
fi
