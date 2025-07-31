#!/usr/bin/env python3
"""
Test runner for MCP LLM Evaluation Framework
"""

import sys
import subprocess

def main():
    """Main test runner"""
    print("🧪 MCP LLM Evaluation Framework")
    print("=" * 40)
    
    try:
        # Run the evaluation tool
        result = subprocess.run(
            [sys.executable, "tests/evaluator.py", "-q", "Tell me about profiles", "-o", "test_run_output.json"],
            capture_output=False,
            text=True
        )
        
        return result.returncode
        
    except Exception as e:
        print(f"❌ Error running evaluation: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())