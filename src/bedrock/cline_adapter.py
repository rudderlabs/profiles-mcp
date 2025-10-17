"""
Simplified Cline Adapter for Bedrock Integration
Provides a drop-in replacement for Anthropic CLI
"""

import os
import sys
import json
import logging
import time
from typing import Dict, Any, Optional
from datetime import datetime
from collections import defaultdict
from threading import Lock
import uuid

import boto3
import requests
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SimpleMetricsCollector:
    """Simple metrics collector for tracking usage and performance"""
    
    def __init__(self):
        self.metrics = defaultdict(int)
        self.latencies = []
        self.lock = Lock()
        self.start_time = time.time()
    
    def record_request(self, api_key_prefix: str, model: str, input_tokens: int, output_tokens: int, latency_ms: float):
        """Record metrics for a request"""
        with self.lock:
            # Usage metrics
            self.metrics['total_requests'] += 1
            self.metrics[f'requests_by_key_{api_key_prefix}'] += 1
            self.metrics[f'requests_by_model_{model}'] += 1
            self.metrics['total_input_tokens'] += input_tokens
            self.metrics['total_output_tokens'] += output_tokens
            
            # Performance metrics
            self.latencies.append(latency_ms)
            if latency_ms > 5000:  # Track slow requests
                self.metrics['slow_requests'] += 1
    
    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary"""
        with self.lock:
            if not self.latencies:
                return {}
            
            return {
                'uptime_seconds': int(time.time() - self.start_time),
                'total_requests': self.metrics['total_requests'],
                'total_tokens': self.metrics['total_input_tokens'] + self.metrics['total_output_tokens'],
                'avg_latency_ms': sum(self.latencies) / len(self.latencies),
                'p95_latency_ms': sorted(self.latencies)[int(len(self.latencies) * 0.95)] if len(self.latencies) > 20 else max(self.latencies, default=0),
                'slow_requests': self.metrics['slow_requests'],
                'requests_by_key': {k: v for k, v in self.metrics.items() if k.startswith('requests_by_key_')},
                'requests_by_model': {k: v for k, v in self.metrics.items() if k.startswith('requests_by_model_')}
            }


class BedrockAPIKeyClient:
    """Simple HTTP client for Bedrock API key authentication"""
    
    def __init__(self, api_key: str, region: str = 'us-east-1'):
        self.api_key = api_key
        self.region = region
        self.endpoint = f"https://bedrock-runtime.{region}.amazonaws.com"
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    def invoke_model(self, model_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """Invoke a Bedrock model using API key"""
        url = f"{self.endpoint}/model/{model_id}/invoke"
        
        response = self.session.post(url, json=body)
        
        if response.status_code == 403 and 'expired' in response.text.lower():
            raise Exception("API key has expired")
        elif response.status_code != 200:
            raise Exception(f"Bedrock API error: {response.status_code} - {response.text}")
        
        return response.json()


class ClineBedrockAdapter:
    """Simplified adapter that makes Bedrock work with Cline"""
    
    def __init__(self):
        """Initialize adapter with environment-based configuration"""
        # Get auth method and credentials
        self.auth_method = os.getenv('BEDROCK_AUTH_METHOD', 'api_key')
        self.region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.model_id = os.getenv('BEDROCK_MODEL_ID', 'anthropic.claude-3-sonnet-20240229-v1:0')
        
        # Initialize metrics
        self.metrics = SimpleMetricsCollector()
        
        # Initialize client based on auth method
        if self.auth_method == 'api_key':
            api_key = os.getenv('AWS_BEARER_TOKEN_BEDROCK') or os.getenv('AWS_BEARER_TOKEN_BEDROCK')
            if not api_key:
                raise ValueError("AWS_BEARER_TOKEN_BEDROCK environment variable is required")
            self.client = BedrockAPIKeyClient(api_key, self.region)
            self.api_key_prefix = api_key[:8] + "..." if len(api_key) > 8 else "key"
        else:
            # Use boto3 for IAM-based auth
            self.client = boto3.client('bedrock-runtime', region_name=self.region)
            self.api_key_prefix = "iam"
    
    def convert_cline_to_bedrock(self, cline_request: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Cline request format to Bedrock format"""
        bedrock_request = {
            'messages': cline_request.get('messages', []),
            'max_tokens': cline_request.get('max_tokens', 4000),
            'temperature': cline_request.get('temperature', 0.7),
            'anthropic_version': 'bedrock-2023-05-31'
        }
        
        if 'system' in cline_request:
            bedrock_request['system'] = cline_request['system']
        
        return bedrock_request
    
    def convert_bedrock_to_cline(self, bedrock_response: Dict[str, Any], request_id: str) -> Dict[str, Any]:
        """Convert Bedrock response to Cline format"""
        return {
            "id": request_id,
            "type": "message",
            "role": "assistant",
            "content": bedrock_response.get("content", []),
            "model": self.model_id,
            "stop_reason": bedrock_response.get("stop_reason", "end_turn"),
            "stop_sequence": bedrock_response.get("stop_sequence"),
            "usage": bedrock_response.get("usage", {})
        }
    
    async def process_request(self, cline_request: Dict[str, Any]) -> Dict[str, Any]:
        """Process a request from Cline"""
        request_id = str(uuid.uuid4())
        start_time = time.time()
        
        try:
            # Convert request
            bedrock_request = self.convert_cline_to_bedrock(cline_request)
            
            # Invoke model
            if self.auth_method == 'api_key':
                bedrock_response = self.client.invoke_model(self.model_id, bedrock_request)
            else:
                # boto3 client
                response = self.client.invoke_model(
                    modelId=self.model_id,
                    body=json.dumps(bedrock_request)
                )
                bedrock_response = json.loads(response['body'].read())
            
            # Calculate metrics
            latency_ms = (time.time() - start_time) * 1000
            usage = bedrock_response.get('usage', {})
            
            # Record metrics
            self.metrics.record_request(
                api_key_prefix=self.api_key_prefix,
                model=self.model_id,
                input_tokens=usage.get('input_tokens', 0),
                output_tokens=usage.get('output_tokens', 0),
                latency_ms=latency_ms
            )
            
            # Convert response
            return self.convert_bedrock_to_cline(bedrock_response, request_id)
            
        except Exception as e:
            logger.error(f"Error processing request: {e}")
            raise
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get current metrics summary"""
        return self.metrics.get_summary()


async def handle_stdin_request():
    """Handle request from stdin (how Cline calls us)"""
    try:
        # Read request from stdin
        request_data = json.loads(sys.stdin.read())
        
        # Create adapter and process
        adapter = ClineBedrockAdapter()
        response = await adapter.process_request(request_data)
        
        # Write response to stdout
        print(json.dumps(response))
        
        # Log metrics periodically (every 100 requests)
        if adapter.metrics.metrics['total_requests'] % 100 == 0:
            logger.info(f"Metrics summary: {adapter.get_metrics_summary()}")
            
    except Exception as e:
        # Return error in Cline-compatible format
        error_response = {
            "error": {
                "type": "api_error",
                "message": str(e)
            }
        }
        print(json.dumps(error_response))
        sys.exit(1)


def main():
    """Main entry point"""
    import asyncio
    
    # Check if we're being called as a drop-in replacement for Anthropic CLI
    if len(sys.argv) > 1 and sys.argv[1] == "messages":
        # Cline calls with "messages" command
        asyncio.run(handle_stdin_request())
    else:
        # Show configuration info
        print("Bedrock Adapter for Cline")
        print("=" * 50)
        print(f"Auth Method: {os.getenv('BEDROCK_AUTH_METHOD', 'api_key')}")
        print(f"Default Region: {os.getenv('AWS_DEFAULT_REGION', 'us-east-1')}")
        print(f"Default Model: {os.getenv('BEDROCK_MODEL_ID', 'anthropic.claude-3-sonnet-20240229-v1:0')}")
        
        # Test configuration
        try:
            adapter = ClineBedrockAdapter()
            print("\n✅ Configuration validated successfully")
        except Exception as e:
            print(f"\n❌ Configuration error: {e}")


if __name__ == "__main__":
    main()
