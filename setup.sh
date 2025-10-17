#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print status messages
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

# Check OS
if [[ "$OSTYPE" != "darwin"* ]] && [[ "$OSTYPE" != "linux-gnu"* ]]; then
    print_error "This script only supports macOS and Linux"
    exit 1
fi

print_status "OS check passed: $(uname -s)"

# Check for uv
if ! command -v uv &> /dev/null; then
    print_warning "uv not found. Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    if [ $? -ne 0 ]; then
        print_error "Failed to install uv. Please install it manually: https://github.com/astral-sh/uv"
        exit 1
    fi
    print_status "uv installed successfully"

    LOCAL_UV_BIN="$HOME/.local/bin/uv"
    if [ -f "$LOCAL_UV_BIN" ] && ! command -v uv &> /dev/null; then
        export PATH="$HOME/.local/bin:$PATH"
        print_status "Added $HOME/.local/bin to PATH for uv."
    fi
else
    print_status "uv is already installed"
fi

# Always generate scripts/start.sh from template
cp scripts/start.sh.template scripts/start.sh
print_status "Generated scripts/start.sh from template."

# Find uv binary location
UV_BIN="$(command -v uv 2>/dev/null)"
if [ -n "$UV_BIN" ]; then
    UV_DIR="$(dirname "$UV_BIN")"
    START_SH="scripts/start.sh"
    # Check if start.sh already exports this path
    if ! grep -q "export PATH=\"$UV_DIR" "$START_SH"; then
        # Insert export before uv run line
        awk -v uvdir="$UV_DIR" '
            /uv run/ && !x {print "export PATH=\"" uvdir ":$PATH\""; x=1}
            {print}
        ' "$START_SH" > "$START_SH.tmp" && mv "$START_SH.tmp" "$START_SH"
        print_status "Added 'uv' binary path (\"$UV_DIR\") to PATH in $START_SH to ensure the package manager is available when starting the server."
    else
        print_status "$START_SH already contains export for $UV_DIR"
    fi
else
    print_warning "uv binary not found after installation; cannot update start.sh"
fi

# Check for python3.10 availability
if ! command -v python3.10 &> /dev/null; then
    print_warning "python3.10 is not installed or not found in PATH. If you encounter issues, please install Python 3.10."
fi


if ! uv run scripts/env_setup.py; then
    print_error "Failed to create .env file."
    exit 1
fi
print_status ".env file created. Continuing setup."

# Function to check RudderStack PAT env variable
validate_env() {
    local required_vars=("RUDDERSTACK_PAT")

    print_status "Required variables: ${required_vars[*]}"

    # Load environment variables
    set -a
    source .env
    set +a

    # Check required variables
    for var in "${required_vars[@]}"; do
        if [ -z "${!var}" ]; then
            print_error "Missing or invalid environment variable: $var"
            return 1
        fi
    done

    return 0
}

# Validate environment variables
if ! validate_env; then
    print_error "Please set all required environment variables in .env file"
    exit 1
fi

print_status "Environment variables validated successfully"

# Install Python dependencies (uv sync will create .venv and use correct Python version if available)
print_status "Installing Python dependencies with uv sync..."
if ! uv sync; then
    print_error "Failed to install Python dependencies"
    exit 1
fi
print_status "Python dependencies installed successfully"


# Update MCP configuration using Python script
print_status "Updating MCP configuration..."
if ! python3 scripts/update_mcp_config.py; then
    print_error "Failed to update MCP configuration"
    exit 1
fi

# Check if Bedrock configuration is present and configure if needed
if [ -n "${BEDROCK_API_KEY}" ] || [ -n "${BEDROCK_AUTH_METHOD}" ]; then
    print_status "Detected Bedrock configuration"
    
    # Create the wrapper script path
    WRAPPER_SCRIPT="$PROJECT_ROOT/scripts/bedrock_anthropic_wrapper.sh"
    
    # Check if wrapper script exists
    if [ ! -f "$WRAPPER_SCRIPT" ]; then
        print_error "Bedrock wrapper script not found at $WRAPPER_SCRIPT"
        exit 1
    fi
    
    # Make wrapper executable
    chmod +x "$WRAPPER_SCRIPT"
    
    # Only configure Cline if not in container
    if [ ! -f /.dockerenv ] && [ "${IS_CLOUD_BASED}" != "true" ]; then
        print_status "Setting up Cline integration..."
        # Update Cline configuration to use Bedrock wrapper
        if ! python3 scripts/update_cline_for_bedrock.py; then
            print_warning "Could not auto-configure Cline for Bedrock. You may need to configure manually."
        else
            print_status "Cline configured to use Bedrock"
        fi
    else
        print_status "Container environment detected - Cline configuration skipped"
    fi
fi

# Ensure scripts/start.sh is executable
chmod +x scripts/start.sh
print_status "Ensured scripts/start.sh is executable"

# Ensure .python-version is set to 3.10.13
PYTHON_VERSION_FILE=".python-version"
REQUIRED_PYTHON_MAJOR_MINOR="3.10"
if [ ! -f "$PYTHON_VERSION_FILE" ]; then
    print_error ".python-version file is missing. Please create it with the line: $REQUIRED_PYTHON_MAJOR_MINOR"
    exit 1
elif [[ "$(cat $PYTHON_VERSION_FILE | tr -d '[:space:]')" != "$REQUIRED_PYTHON_MAJOR_MINOR" ]]; then
    print_error ".python-version is not set to $REQUIRED_PYTHON_MAJOR_MINOR. Please update it to match the project requirement."
    exit 1
else
    print_status ".python-version is set to $REQUIRED_PYTHON_MAJOR_MINOR.x"
fi

# Print setup completion message
echo -e "\n${GREEN}✓ Setup completed successfully!${NC}\n"
