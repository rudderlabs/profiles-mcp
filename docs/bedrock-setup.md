# Using Amazon Bedrock with Profiles MCP

This guide explains how to configure Profiles MCP to use Amazon Bedrock Claude models instead of Anthropic's API. This is particularly useful for organizations that need to ensure PII data remains within their AWS infrastructure.

## Overview

Profiles MCP supports two LLM providers:
1. **Anthropic Claude** (default) - Direct API access to Claude models
2. **Amazon Bedrock Claude** - Claude models hosted on AWS infrastructure

## Prerequisites

### For Bedrock Usage
- AWS Account with Bedrock access enabled
- Access to Claude models in Bedrock (request access in AWS Console if needed)
- AWS credentials configured:
  - AWS CLI: `aws configure`
  - Environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
  - IAM role (for EC2/ECS deployments)

### Available Bedrock Models
- `anthropic.claude-3-5-sonnet-20241022-v2:0` (recommended)
- `anthropic.claude-3-sonnet-20240229-v1:0`
- `anthropic.claude-3-haiku-20240307-v1:0`

## Setup Instructions

### 1. Local Development Setup

#### Option A: Using the Configuration Script
```bash
# Run the configuration script
python scripts/configure_llm_provider.py

# Select option 2 for Bedrock
# Follow the prompts to configure model and region
```

#### Option B: Manual Configuration

1. Create the Bedrock wrapper script:
```bash
# Create scripts/bedrock_anthropic_wrapper.sh with appropriate content
chmod +x scripts/bedrock_anthropic_wrapper.sh
```

2. Configure Cline to use Bedrock:
   - Update Cline's settings to use the wrapper script
   - Set the `anthropicPath` in Cline settings to point to the wrapper

3. Set environment variables:
```bash
export BEDROCK_MODEL_ID="anthropic.claude-3-sonnet-20240229-v1:0"
export AWS_DEFAULT_REGION="us-east-1"
```

### 2. Container/Code-Server Setup

For containerized deployments, add these environment variables to your container:

```dockerfile
# In your Dockerfile
ENV BEDROCK_MODEL_ID="anthropic.claude-3-sonnet-20240229-v1:0"
ENV AWS_DEFAULT_REGION="us-east-1"
ENV USE_BEDROCK="true"
```

Update the code-server entry script to configure Bedrock:

```bash
# In code-server-entry.sh, add:
if [ "$USE_BEDROCK" = "true" ]; then
    # Configure Cline to use Bedrock wrapper
    python /home/codeuser/profiles-mcp/scripts/configure_llm_provider.py --bedrock --non-interactive
fi
```

### 3. AWS IAM Permissions

Ensure your AWS credentials have the following permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "bedrock:InvokeModel"
            ],
            "Resource": [
                "arn:aws:bedrock:*:*:model/anthropic.claude-3*"
            ]
        }
    ]
}
```

## Usage

Once configured, the MCP server works exactly the same way. The only difference is that LLM requests go through Bedrock instead of Anthropic's API.

### Testing the Setup

1. Verify Bedrock access:
```bash
# Test Bedrock client directly
python src/bedrock_client.py
```

2. Start the MCP server as usual:
```bash
./scripts/start.sh
```

3. Use with your IDE (Cursor/VS Code) normally

## Switching Between Providers

You can switch between Anthropic and Bedrock at any time:

```bash
# Switch to Bedrock
python scripts/configure_llm_provider.py
# Select option 2

# Switch back to Anthropic
python scripts/configure_llm_provider.py
# Select option 1
```

## Troubleshooting

### AWS Credentials Not Found
- Run `aws configure` to set up credentials
- Or set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables

### Model Access Denied
- Ensure you have requested access to Claude models in AWS Bedrock console
- Check IAM permissions include `bedrock:InvokeModel`

### Region Issues
- Make sure the AWS region you're using has Bedrock available
- Claude models may not be available in all regions

## Security Benefits

Using Bedrock provides several security advantages:
- **Data Residency**: All data remains within your AWS infrastructure
- **VPC Integration**: Can be deployed within your VPC
- **IAM Control**: Fine-grained access control using AWS IAM
- **Audit Logging**: CloudTrail integration for compliance
- **PII Protection**: Data never leaves your controlled environment

## Cost Considerations

- Bedrock pricing is based on input/output tokens
- May be more cost-effective for high-volume usage
- No API key management required
- Costs are consolidated in your AWS bill
