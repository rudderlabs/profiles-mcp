#!/usr/bin/env python3
"""
Amazon Bedrock client for Profiles MCP
Provides a command-line interface that mimics Anthropic's API for Cline compatibility
"""

import os
import sys
import json
import logging
from typing import Dict, List, Any, Optional
import asyncio

import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BedrockClineAdapter:
    """
    Adapter that makes Bedrock work with Cline by mimicking Anthropic's API interface
    """
    
    def __init__(self, model_id: str = None, region: str = None):
        self.model_id = model_id or os.getenv(
            "BEDROCK_MODEL_ID", 
            "anthropic.claude-3-sonnet-20240229-v1:0"
        )
        self.region = region or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        
        # Initialize Bedrock client
        self.bedrock = boto3.client('bedrock-runtime', region_name=self.region)
        
        # Verify credentials
        session = boto3.Session()
        if not session.get_credentials():
            raise ValueError(
                "AWS credentials not found. Please configure:\n"
                "- AWS CLI: aws configure\n"
                "- Environment variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY\n"
                "- IAM role (if on EC2/ECS)"
            )
    
    def convert_cline_to_bedrock(self, cline_request: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Cline's Anthropic-style request to Bedrock format"""
        
        # Extract messages and system prompt
        messages = cline_request.get("messages", [])
        system_prompt = cline_request.get("system", "")
        
        # Convert tools if present
        tools = []
        if "tools" in cline_request:
            for tool in cline_request["tools"]:
                bedrock_tool = {
                    "name": tool["name"],
                    "description": tool.get("description", ""),
                    "input_schema": tool.get("input_schema", {
                        "type": "object",
                        "properties": {},
                        "required": []
                    })
                }
                tools.append(bedrock_tool)
        
        # Build Bedrock request
        bedrock_request = {
            "anthropic_version": "bedrock-2023-05-31",
            "messages": messages,
            "max_tokens": cline_request.get("max_tokens", 4000),
            "temperature": cline_request.get("temperature", 0.7),
            "top_p": cline_request.get("top_p", 1.0),
            "stop_sequences": cline_request.get("stop_sequences", [])
        }
        
        if system_prompt:
            bedrock_request["system"] = system_prompt
        
        if tools:
            bedrock_request["tools"] = tools
        
        return bedrock_request
    
    def convert_bedrock_to_cline(self, bedrock_response: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Bedrock response to Cline's expected format"""
        
        # Cline expects the response in Anthropic's format
        cline_response = {
            "id": f"msg_{os.urandom(12).hex()}",
            "type": "message",
            "role": "assistant",
            "content": bedrock_response.get("content", []),
            "model": self.model_id,
            "stop_reason": bedrock_response.get("stop_reason", "end_turn"),
            "stop_sequence": bedrock_response.get("stop_sequence"),
            "usage": bedrock_response.get("usage", {})
        }
        
        return cline_response
    
    async def process_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a request from Cline"""
        try:
            # Convert request to Bedrock format
            bedrock_request = self.convert_cline_to_bedrock(request_data)
            
            # Call Bedrock
            response = self.bedrock.invoke_model(
                modelId=self.model_id,
                body=json.dumps(bedrock_request),
                contentType='application/json'
            )
            
            # Parse response
            response_body = json.loads(response['body'].read())
            
            # Convert back to Cline format
            return self.convert_bedrock_to_cline(response_body)
            
        except ClientError as e:
            logger.error(f"Bedrock API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise


async def handle_stdin_request():
    """Handle requests from stdin (how Cline communicates)"""
    adapter = BedrockClineAdapter()
    
    # Read request from stdin
    request_data = json.loads(sys.stdin.read())
    
    # Process request
    response = await adapter.process_request(request_data)
    
    # Write response to stdout
    print(json.dumps(response))


def main():
    """Main entry point"""
    # Check if we're being called as a drop-in replacement for Anthropic CLI
    if len(sys.argv) > 1 and sys.argv[1] == "messages":
        # Cline calls with "messages" command
        asyncio.run(handle_stdin_request())
    else:
        print("Bedrock client for Cline - use as drop-in replacement for Anthropic CLI")
        print(f"Model: {os.getenv('BEDROCK_MODEL_ID', 'anthropic.claude-3-sonnet-20240229-v1:0')}")
        print(f"Region: {os.getenv('AWS_DEFAULT_REGION', 'us-east-1')}")


if __name__ == "__main__":
    main()
