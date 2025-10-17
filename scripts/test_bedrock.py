#!/usr/bin/env python3
"""
Simple test script for Bedrock integration
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def test_bedrock_connection():
    """Test basic Bedrock connectivity"""
    print("🚀 Profiles MCP - Bedrock Integration Test\n")
    print("🔍 Testing Amazon Bedrock connection...")
    
    try:
        from bedrock.cline_adapter import ClineBedrockAdapter
        
        # Create adapter
        adapter = ClineBedrockAdapter()
        print(f"✅ Adapter initialized with {adapter.auth_method} authentication")
        print(f"   Region: {adapter.region}")
        print(f"   Model: {adapter.model_id}")
        
        # Test with a simple request
        test_request = {
            "messages": [{"role": "user", "content": "Say 'test successful' in exactly those words"}],
            "max_tokens": 20
        }
        
        print("\n🔍 Testing model invocation...")
        import asyncio
        response = asyncio.run(adapter.process_request(test_request))
        
        if response and 'content' in response:
            content = response['content'][0]['text'] if isinstance(response['content'], list) else response['content']
            print(f"✅ Model invocation successful!")
            print(f"   Response: {content}")
            
            # Show metrics
            metrics = adapter.get_metrics_summary()
            if metrics:
                print(f"\n📊 Metrics:")
                print(f"   Total requests: {metrics['total_requests']}")
                print(f"   Avg latency: {metrics['avg_latency_ms']:.0f}ms")
                print(f"   Total tokens: {metrics['total_tokens']}")
            
            return True
        else:
            print("❌ Unexpected response format")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


def check_cline_configuration():
    """Check if Cline is configured to use Bedrock"""
    print("\n🔍 Checking Cline configuration...")
    
    home = Path.home()
    config_paths = [
        home / "Library/Application Support/Code/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json",
        home / ".config/Code/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json",
    ]
    
    for path in config_paths:
        if path.exists():
            print(f"✅ Found Cline config at: {path}")
            
            with open(path) as f:
                config = json.load(f)
                
            if 'anthropicPath' in config:
                if 'bedrock_anthropic_wrapper.sh' in config['anthropicPath']:
                    print("✅ Cline configured to use Bedrock wrapper")
                    return True
                else:
                    print(f"❌ Cline using: {config['anthropicPath']}")
                    print("   Run ./setup.sh to configure Bedrock")
                    return False
    
    print("❌ Cline configuration not found")
    return False


def main():
    """Run all tests"""
    results = []
    
    # Test 1: Bedrock connection
    results.append(test_bedrock_connection())
    
    # Test 2: Cline configuration
    results.append(check_cline_configuration())
    
    # Summary
    print(f"\n📊 Test Results: {sum(results)}/{len(results)} passed")
    
    if all(results):
        print("✅ All tests passed! Bedrock integration is ready.")
        
        print("\n📋 Current Configuration:")
        print(f"   BEDROCK_MODEL_ID: {os.getenv('BEDROCK_MODEL_ID', 'anthropic.claude-3-sonnet-20240229-v1:0')}")
        print(f"   AWS_DEFAULT_REGION: {os.getenv('AWS_DEFAULT_REGION', 'us-east-1')}")
        print(f"   BEDROCK_AUTH_METHOD: {os.getenv('BEDROCK_AUTH_METHOD', 'api_key')}")
    else:
        print("⚠️  Some tests failed. Please check the errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
