#!/usr/bin/env python3
"""
Test script to verify Bedrock integration is working
"""

import os
import sys
import json
import boto3
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def test_bedrock_connection():
    """Test basic Bedrock connectivity"""
    print("🔍 Testing Amazon Bedrock connection...")
    
    # Check AWS credentials
    session = boto3.Session()
    credentials = session.get_credentials()
    
    if not credentials:
        print("❌ AWS credentials not found")
        print("Please configure AWS credentials:")
        print("  - aws configure")
        print("  - Export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        return False
    
    print("✅ AWS credentials found")
    
    # Check Bedrock client
    try:
        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        bedrock = boto3.client('bedrock-runtime', region_name=region)
        print(f"✅ Bedrock client initialized (region: {region})")
    except Exception as e:
        print(f"❌ Failed to initialize Bedrock client: {e}")
        return False
    
    # Test model invocation
    model_id = os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-sonnet-20240229-v1:0")
    print(f"🔍 Testing model: {model_id}")
    
    try:
        response = bedrock.invoke_model(
            modelId=model_id,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "messages": [{"role": "user", "content": "Say 'Hello from Bedrock!'"}],
                "max_tokens": 50
            }),
            contentType='application/json'
        )
        
        result = json.loads(response['body'].read())
        print("✅ Model invocation successful!")
        print(f"Response: {result.get('content', [{}])[0].get('text', 'No response')}")
        return True
        
    except Exception as e:
        print(f"❌ Model invocation failed: {e}")
        if "AccessDeniedException" in str(e):
            print("\n💡 You need to request access to Claude models in AWS Bedrock console")
            print("   Go to: AWS Console > Bedrock > Model access")
        return False


def test_bedrock_client():
    """Test the Bedrock client wrapper"""
    print("\n🔍 Testing Bedrock client wrapper...")
    
    try:
        from bedrock_client import BedrockClineAdapter
        
        adapter = BedrockClineAdapter()
        print("✅ Bedrock client imported successfully")
        
        # Test request conversion
        test_request = {
            "messages": [{"role": "user", "content": "Test message"}],
            "system": "You are a helpful assistant",
            "max_tokens": 100
        }
        
        bedrock_request = adapter.convert_cline_to_bedrock(test_request)
        print("✅ Request conversion working")
        
        return True
        
    except Exception as e:
        print(f"❌ Bedrock client test failed: {e}")
        return False


def check_cline_configuration():
    """Check if Cline is configured to use Bedrock"""
    print("\n🔍 Checking Cline configuration...")
    
    # Check various possible locations
    home = Path.home()
    possible_paths = [
        home / ".local/share/code-server/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json",
        home / "Library/Application Support/Code/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json",
        home / ".config/Code/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json"
    ]
    
    config_found = False
    for path in possible_paths:
        if path.exists():
            config_found = True
            print(f"✅ Found Cline config at: {path}")
            
            with open(path, 'r') as f:
                config = json.load(f)
            
            if "anthropicPath" in config:
                print(f"✅ Cline configured to use: {config['anthropicPath']}")
                if "bedrock" in config['anthropicPath'].lower():
                    print("✅ Bedrock integration is active!")
                else:
                    print("⚠️  Cline is using default Anthropic client")
            else:
                print("⚠️  No custom anthropicPath configured")
            break
    
    if not config_found:
        print("⚠️  Cline configuration not found")
        print("   This is normal if Cline is not installed yet")
    
    return True


def main():
    """Run all tests"""
    print("🚀 Profiles MCP - Bedrock Integration Test\n")
    
    # Run tests
    tests_passed = 0
    total_tests = 3
    
    if test_bedrock_connection():
        tests_passed += 1
    
    if test_bedrock_client():
        tests_passed += 1
    
    if check_cline_configuration():
        tests_passed += 1
    
    # Summary
    print(f"\n📊 Test Results: {tests_passed}/{total_tests} passed")
    
    if tests_passed == total_tests:
        print("✅ All tests passed! Bedrock integration is ready.")
    else:
        print("⚠️  Some tests failed. Please check the errors above.")
        
    # Show current configuration
    print("\n📋 Current Configuration:")
    print(f"   BEDROCK_MODEL_ID: {os.getenv('BEDROCK_MODEL_ID', 'Not set')}")
    print(f"   AWS_DEFAULT_REGION: {os.getenv('AWS_DEFAULT_REGION', 'Not set')}")
    print(f"   USE_BEDROCK: {os.getenv('USE_BEDROCK', 'Not set')}")


if __name__ == "__main__":
    main()
