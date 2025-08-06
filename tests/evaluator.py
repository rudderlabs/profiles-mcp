#!/usr/bin/env python3
"""
MCP Tool Evaluation Framework with interactive conversation support
"""

import os
import sys
import json
import time
import csv as csv_module
import argparse
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any, Union
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv
from anthropic import Anthropic

# Load environment variables
load_dotenv()

# Add src directory to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

# Import MCP tools and dependencies
from main import (
    about_profiles, get_existing_connections, search_profiles_docs,
    initialize_snowflake_connection, run_query, input_table_suggestions,
    describe_table, get_profiles_output_details, setup_new_profiles_project,
    evaluate_eligible_user_filters, profiles_workflow_guide,
    analyze_and_validate_project, validate_propensity_model_config,
    AppContext
)
from tools.about import About
from tools.docs import Docs
from tools.snowflake import Snowflake
from tools.profiles import ProfilesTools

# Import test configurations
from test_constants import SONNET_MODEL
from prompts import CLAUDE_CODE_SYSTEM_PROMPT

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


# =======================
# Data Models
# =======================

@dataclass
class MockRequestContext:
    """Mock request context for MCP tools"""
    lifespan_context: Any = None


@dataclass
class MockContext:
    """Mock context for testing MCP tools"""
    request_context: MockRequestContext = None
    
    def __post_init__(self):
        app_context = AppContext(
            about=About(),
            docs=Docs(),
            snowflake=Snowflake(),
            profiles=ProfilesTools()
        )
        self.request_context = MockRequestContext(lifespan_context=app_context)


@dataclass
class ConversationTurn:
    """Single turn in a conversation"""
    role: str  # "user", "assistant", or "tool"
    content: str
    tool_calls: List[Dict] = field(default_factory=list)
    tool_results: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class EvalResult:
    """Evaluation result data structure"""
    iteration: int
    timestamp: str
    model: str
    prompt: str
    conversation_file: str
    tools_called: List[str]
    tool_params: Dict[str, Any]
    agent_reasoning: str
    tool_results: Dict[str, Any]
    latency_ms: float
    token_count: int
    conversation_history: List[ConversationTurn] = field(default_factory=list)
    user_interventions: int = 0
    ended_by: str = ""  # "completion", "user_end", "needs_input", "error"
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        result = {
            "iteration": self.iteration,
            "timestamp": self.timestamp,
            "model": self.model,
            "prompt": self.prompt,
            "conversation_file": self.conversation_file,
            "tools_called": self.tools_called,
            "tool_params": self.tool_params,
            "agent_reasoning": self.agent_reasoning,
            "tool_results": self.tool_results,
            "latency_ms": self.latency_ms,
            "token_count": self.token_count,
            "error": self.error
        }
        
        # Add conversation details if interactive mode was used
        if self.conversation_history:
            result["conversation_turns"] = len(self.conversation_history)
            result["user_interventions"] = self.user_interventions
            result["ended_by"] = self.ended_by
            # Optionally include full history (can be large)
            result["conversation_history"] = [
                {
                    "role": turn.role,
                    "content": turn.content[:500] if len(turn.content) > 500 else turn.content,
                    "has_tools": len(turn.tool_calls) > 0
                }
                for turn in self.conversation_history
            ]
        
        return result


# =======================
# Tool Registry
# =======================

TOOL_REGISTRY = {
    "about_profiles": about_profiles,
    "get_existing_connections": get_existing_connections,
    "search_profiles_docs": search_profiles_docs,
    "initialize_snowflake_connection": initialize_snowflake_connection,
    "run_query": run_query,
    "input_table_suggestions": input_table_suggestions,
    "describe_table": describe_table,
    "get_profiles_output_details": get_profiles_output_details,
    "setup_new_profiles_project": setup_new_profiles_project,
    "evaluate_eligible_user_filters": evaluate_eligible_user_filters,
    "profiles_workflow_guide": profiles_workflow_guide,
    "analyze_and_validate_project": analyze_and_validate_project,
    "validate_propensity_model_config": validate_propensity_model_config
}


# =======================
# Evaluator Class
# =======================

class MCPEvaluator:
    """Evaluates LLM responses using direct MCP tool execution"""
    
    def __init__(self, system_prompt: str = CLAUDE_CODE_SYSTEM_PROMPT):
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY not found in environment")
        
        self.claude = Anthropic(api_key=api_key)
        self.ctx = MockContext()
        self.system_prompt = system_prompt
        self.tools = self._extract_tool_definitions()
    
    def _extract_tool_definitions(self) -> List[Dict]:
        """Extract tool definitions from MCP functions"""
        import inspect
        from typing import get_type_hints
        
        tools = []
        for func_name, func in TOOL_REGISTRY.items():
            sig = inspect.signature(func)
            docstring = inspect.getdoc(func) or ""
            description = docstring.split('\n\n')[0] if docstring else f"Execute {func_name}"
            type_hints = get_type_hints(func)
            
            # Build parameter schema
            properties = {}
            required = []
            
            for param_name, param in sig.parameters.items():
                if param_name == 'ctx':
                    continue
                    
                param_type = type_hints.get(param_name, str)
                json_type = self._python_type_to_json_type(param_type)
                
                prop_schema = {
                    "type": json_type,
                    "description": f"Parameter {param_name}"
                }
                
                if param.default != inspect.Parameter.empty:
                    prop_schema["default"] = param.default
                else:
                    required.append(param_name)
                    
                properties[param_name] = prop_schema
            
            tools.append({
                "name": func_name,
                "description": description,
                "input_schema": {
                    "type": "object",
                    "properties": properties,
                    "required": required
                }
            })
        
        return tools
    
    def _python_type_to_json_type(self, param_type) -> str:
        """Convert Python type to JSON schema type"""
        if param_type == int:
            return "integer"
        elif param_type == float:
            return "number"
        elif param_type == bool:
            return "boolean"
        elif param_type == list or getattr(param_type, '__origin__', None) == list:
            return "array"
        elif param_type == dict:
            return "object"
        return "string"
    
    def execute_tool(self, tool_name: str, tool_params: Dict) -> Any:
        """Execute a tool function directly"""
        if tool_name not in TOOL_REGISTRY:
            return f"Unknown tool: {tool_name}"
        
        func = TOOL_REGISTRY[tool_name]
        try:
            result = func(self.ctx, **tool_params)
            return result
        except Exception as e:
            logger.error(f"Error executing {tool_name}: {e}")
            return f"Error: {str(e)}"
    
    def evaluate_prompt(self, prompt: str, messages: Optional[List[Dict]] = None, interactive: bool = False) -> Dict[str, Any]:
        """Evaluate a prompt with optional interactive conversation loop"""
        total_latency = 0
        total_tokens = 0
        all_tools_called = []
        all_tool_params = {}
        all_tool_results = {}
        conversation_history = []
        user_interventions = 0
        ended_by = "completion"
        final_reasoning = ""
        
        # Initialize messages if not provided
        if messages is None:
            messages = [{"role": "user", "content": prompt}]
            conversation_history.append(ConversationTurn(role="user", content=prompt))
        
        try:
            while True:
                # Call Claude
                iter_start = time.time()
                message = self.claude.messages.create(
                    model=SONNET_MODEL,
                    max_tokens=4000,
                    system=self.system_prompt,
                    messages=messages,
                    tools=self.tools
                )
                iter_latency = (time.time() - iter_start) * 1000
                total_latency += iter_latency
                
                # Track tokens
                if hasattr(message, 'usage'):
                    total_tokens += (
                        getattr(message.usage, 'total_tokens', 0) or 
                        getattr(message.usage, 'input_tokens', 0) + getattr(message.usage, 'output_tokens', 0)
                    )
                
                # Process response
                text_response = ""
                tool_calls = []
                tool_results = {}
                
                for content in message.content:
                    if hasattr(content, 'type'):
                        if content.type == 'text':
                            text_response = content.text
                            final_reasoning = content.text  # Keep updating with latest
                        elif content.type == 'tool_use':
                            tool_call = {
                                "id": content.id,
                                "name": content.name,
                                "input": content.input
                            }
                            tool_calls.append(tool_call)
                            
                            # Execute the tool
                            logger.info(f"   🔧 Executing {content.name}...")
                            result = self.execute_tool(content.name, content.input)
                            tool_results[content.id] = result
                            
                            # Track for summary
                            all_tools_called.append(content.name)
                            all_tool_params[content.name] = content.input
                            all_tool_results[content.name] = str(result)[:1000]
                
                # Add assistant turn to history
                conversation_history.append(ConversationTurn(
                    role="assistant",
                    content=text_response,
                    tool_calls=tool_calls,
                    tool_results=tool_results
                ))
                
                # If there were tool calls, add assistant message then tool results
                if tool_calls:
                    # First append the assistant message with tool calls
                    assistant_content = []
                    if text_response:
                        assistant_content.append({"type": "text", "text": text_response})
                    for tool_call in tool_calls:
                        assistant_content.append({
                            "type": "tool_use",
                            "id": tool_call["id"],
                            "name": tool_call["name"],
                            "input": tool_call["input"]
                        })
                    messages.append({"role": "assistant", "content": assistant_content})
                    
                    # Then append tool results as user message
                    tool_result_content = []
                    for tool_id, result in tool_results.items():
                        tool_result_content.append({
                            "type": "tool_result",
                            "tool_use_id": tool_id,
                            "content": str(result)
                        })
                    messages.append({"role": "user", "content": tool_result_content})
                    
                    # Add tool turn to history
                    conversation_history.append(ConversationTurn(
                        role="tool",
                        content="Tool execution results",
                        tool_results=tool_results
                    ))
                    continue  # Loop back for Claude to process tool results
                
                # No tool calls - Claude gave final response
                if text_response:
                    logger.info(f"\n💬 Claude's response:")
                    logger.info(text_response[:500] + "..." if len(text_response) > 500 else text_response)
                
                # In interactive mode, check if we should continue
                if interactive and self._is_interactive_terminal():
                    if self._is_asking_for_input(text_response):
                        logger.info(f"\n{'─'*40}")
                        logger.info("✍️  Claude needs your input (type 'end' to finish):")
                        user_input = input("You: ").strip()
                        
                        if user_input.lower() == 'end':
                            ended_by = "user_end"
                            break
                        
                        messages.append({"role": "user", "content": user_input})
                        conversation_history.append(ConversationTurn(role="user", content=user_input))
                        user_interventions += 1
                        continue
                    else:
                        # Task complete - optionally continue
                        logger.info(f"\n{'─'*40}")
                        logger.info("✅ Task complete. Continue? (type message or 'end'):")
                        user_input = input("You: ").strip()
                        
                        if user_input.lower() == 'end' or not user_input:
                            break
                        
                        messages.append({"role": "user", "content": user_input})
                        conversation_history.append(ConversationTurn(role="user", content=user_input))
                        user_interventions += 1
                        continue
                
                # Non-interactive or batch mode - stop here
                break
                
        except Exception as e:
            ended_by = "error"
            logger.error(f"Error: {e}")
            return {
                "agent_reasoning": final_reasoning,
                "tools_attempted": all_tools_called,
                "tool_params": all_tool_params,
                "tool_results": all_tool_results,
                "latency_ms": total_latency,
                "token_count": total_tokens,
                "conversation_history": conversation_history,
                "user_interventions": user_interventions,
                "ended_by": ended_by,
                "error": str(e)
            }
        
        return {
            "agent_reasoning": final_reasoning,
            "tools_attempted": all_tools_called,
            "tool_params": all_tool_params,
            "tool_results": all_tool_results,
            "latency_ms": total_latency,
            "token_count": total_tokens,
            "conversation_history": conversation_history,
            "user_interventions": user_interventions,
            "ended_by": ended_by,
            "error": None
        }
    
    def _is_asking_for_input(self, text: str) -> bool:
        """Check if Claude is asking for user input"""
        if not text:
            return False
        indicators = ["?", "please provide", "please specify", "which", "what", 
                     "could you", "would you", "can you tell", "need to know", 
                     "require", "clarify", "confirm"]
        text_lower = text.lower()
        return any(indicator in text_lower for indicator in indicators)
    
    def _is_interactive_terminal(self) -> bool:
        """Check if running in an interactive terminal"""
        import sys
        return sys.stdin.isatty() and os.getenv('NON_INTERACTIVE') != 'true'


# =======================
# Input Parsers
# =======================

def parse_conversation_md(file_path: str) -> List[Dict[str, str]]:
    """Parse markdown conversation file into message history"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Conversation file not found: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    messages = []
    current_role = None
    current_content = []
    
    for line in content.split('\n'):
        # Skip metadata
        if line.startswith('_Exported on') or line.strip() == '---':
            continue
            
        # Check for role headers
        if line.startswith('**User**') or line.startswith('**Human**'):
            if current_role and current_content:
                messages.append({
                    "role": "user" if current_role == "user" else "assistant",
                    "content": '\n'.join(current_content).strip()
                })
            current_role = "user"
            current_content = []
        elif line.startswith('**Cursor**') or line.startswith('**Assistant**') or line.startswith('**Claude**'):
            if current_role and current_content:
                messages.append({
                    "role": "user" if current_role == "user" else "assistant", 
                    "content": '\n'.join(current_content).strip()
                })
            current_role = "assistant"
            current_content = []
        elif current_role:
            current_content.append(line)
    
    # Add final message
    if current_role and current_content:
        messages.append({
            "role": "user" if current_role == "user" else "assistant",
            "content": '\n'.join(current_content).strip()
        })
    
    # Filter empty messages and ensure last is from user
    messages = [msg for msg in messages if msg["content"].strip()]
    if not messages or messages[-1]["role"] != "user":
        raise ValueError("Last message in conversation must be from user")
        
    return messages


def load_csv_tests(file_path: str) -> List[Dict[str, str]]:
    """Load test cases from CSV file"""
    tests = []
    with open(file_path, 'r') as f:
        reader = csv_module.DictReader(f)
        for row in reader:
            tests.append(row)
    return tests


# =======================
# Result Processing
# =======================

def create_result(
    trace_data: Dict[str, Any],
    prompt: str,
    iteration: int = 1,
    conversation_file: str = "",
    test_metadata: Optional[Dict] = None
) -> Union[EvalResult, Dict[str, Any]]:
    """Create evaluation result from trace data"""
    result = EvalResult(
        iteration=iteration,
        timestamp=datetime.now().isoformat(),
        model=SONNET_MODEL,
        prompt=prompt,
        conversation_file=conversation_file,
        tools_called=trace_data['tools_attempted'],
        tool_params=trace_data['tool_params'],
        agent_reasoning=trace_data['agent_reasoning'],
        tool_results=trace_data['tool_results'],
        latency_ms=trace_data['latency_ms'],
        token_count=trace_data.get('token_count', 0),
        conversation_history=trace_data.get('conversation_history', []),
        user_interventions=trace_data.get('user_interventions', 0),
        ended_by=trace_data.get('ended_by', ''),
        error=trace_data.get('error')
    )
    
    # Add test metadata for CSV mode
    if test_metadata:
        result_dict = result.to_dict()
        result_dict.update(test_metadata)
        
        # Validate against expected/forbidden tools
        tools_called = set(trace_data['tools_attempted'])
        expected = set(filter(None, [t.strip() for t in test_metadata.get('expected_tools', [])]))
        forbidden = set(filter(None, [t.strip() for t in test_metadata.get('forbidden_tools', [])]))
        
        success = True
        errors = []
        
        if expected and not expected.intersection(tools_called):
            success = False
            errors.append(f"Expected tools {expected} not called")
        
        if forbidden.intersection(tools_called):
            success = False
            errors.append(f"Forbidden tools {forbidden.intersection(tools_called)} were called")
        
        result_dict['validation_success'] = success
        result_dict['validation_errors'] = errors
        
        return result_dict
    
    return result


def save_results(results: List[Union[EvalResult, Dict]], output_path: str, metadata: Dict):
    """Save evaluation results to JSON file"""
    output_data = {
        "metadata": metadata,
        "results": [
            result if isinstance(result, dict) else result.to_dict() 
            for result in results
        ]
    }
    
    with open(output_path, 'w') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"\n💾 Saved {len(results)} results to {output_path}")


# =======================
# Main Execution
# =======================

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='MCP Tool Evaluation Framework',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -q "Tell me about profiles" -o results.json
  %(prog)s -q "Tell me about profiles" --interactive
  %(prog)s -c conversation.md -o results.json -i 3
  %(prog)s --csv test_queries.csv -o suite_results.json
        """
    )
    
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('-q', '--query', help='Single query to test')
    input_group.add_argument('-c', '--conversation', help='Markdown conversation file')
    input_group.add_argument('--csv', help='CSV file with test queries')
    
    parser.add_argument('-i', '--iterations', type=int, default=1,
                        help='Number of iterations (default: 1)')
    parser.add_argument('-o', '--output', help='Output JSON file')
    parser.add_argument('--interactive', action='store_true',
                        help='Enable interactive mode for conversations')
    
    args = parser.parse_args()
    
    # Set environment variable for non-interactive mode if not explicitly interactive
    if not args.interactive:
        os.environ['NON_INTERACTIVE'] = 'true'
    
    # Initialize evaluator
    evaluator = MCPEvaluator()
    results = []
    
    logger.info("🚀 MCP Tool Evaluation Framework")
    if args.interactive:
        logger.info("   Mode: Interactive")
    logger.info("=" * 60)
    
    # Process based on input type
    if args.csv:
        process_csv_tests(evaluator, args, results)
    elif args.conversation:
        process_conversation(evaluator, args, results)
    else:  # Single query
        process_single_query(evaluator, args, results)
    
    # Save results if output specified
    if args.output and results:
        metadata = {
            "total_results": len(results),
            "generated_at": datetime.now().isoformat(),
            "model": SONNET_MODEL,
            "conversation_file": args.conversation,
            "query": args.query,
            "csv_file": args.csv,
            "iterations": args.iterations,
            "mode": "interactive" if args.interactive else "batch",
            "input_type": "csv" if args.csv else ("conversation" if args.conversation else "query")
        }
        save_results(results, args.output, metadata)
    
    logger.info("\n✅ Evaluation complete!")


def process_csv_tests(evaluator, args, results):
    """Process CSV test suite"""
    tests = load_csv_tests(args.csv)
    logger.info(f"📄 Loaded {len(tests)} test queries from {args.csv}")
    
    for i, test_case in enumerate(tests):
        query = test_case.get('user_prompt', test_case.get('prompt', ''))
        test_name = test_case.get('test_name', f'test_{i+1}')
        
        if not query:
            logger.warning(f"⚠️ Skipping {test_name}: No query found")
            continue
        
        logger.info(f"\n🔍 Test Case: {test_name}")
        logger.info(f"📝 Query: {query[:100]}..." if len(query) > 100 else f"Query: {query}")
        
        trace_data = evaluator.evaluate_prompt(query, interactive=args.interactive)
        
        # Prepare test metadata
        test_metadata = {
            'test_name': test_name,
            'expected_tools': test_case.get('expected_tools', '').split(',') if test_case.get('expected_tools') else [],
            'forbidden_tools': test_case.get('forbidden_tools', '').split(',') if test_case.get('forbidden_tools') else [],
            'description': test_case.get('description', '')
        }
        
        result = create_result(trace_data, query, i+1, test_metadata=test_metadata)
        results.append(result)
        
        # Log validation results
        if isinstance(result, dict) and 'validation_success' in result:
            icon = "✅" if result['validation_success'] else "❌"
            logger.info(f"   🔧 Tools: {', '.join(trace_data['tools_attempted']) or 'None'}")
            logger.info(f"   {icon} Validation: {'PASS' if result['validation_success'] else 'FAIL'}")
            for error in result.get('validation_errors', []):
                logger.info(f"      • {error}")
        
        logger.info(f"   ⏱️  Latency: {trace_data['latency_ms']:.0f}ms")


def process_conversation(evaluator, args, results):
    """Process conversation file"""
    messages = parse_conversation_md(args.conversation)
    logger.info(f"📜 Loaded {len(messages)} messages from {args.conversation}")
    
    for i in range(args.iterations):
        logger.info(f"\n🔍 Iteration {i+1}/{args.iterations}")
        if i > 0 and args.interactive:
            input("Press Enter to start next iteration...")
        
        trace_data = evaluator.evaluate_prompt(messages[-1]["content"], messages, interactive=args.interactive)
        
        result = create_result(trace_data, messages[-1]["content"], i+1, args.conversation)
        results.append(result)
        
        # Log results
        log_evaluation_results(trace_data)


def process_single_query(evaluator, args, results):
    """Process single query"""
    logger.info(f"📝 Query: {args.query}")
    
    for i in range(args.iterations):
        if args.iterations > 1:
            logger.info(f"\n🔍 Iteration {i+1}/{args.iterations}")
            if i > 0 and args.interactive:
                input("Press Enter to start next iteration...")
        
        trace_data = evaluator.evaluate_prompt(args.query, interactive=args.interactive)
        result = create_result(trace_data, args.query, i+1)
        results.append(result)
        
        # Log results
        log_evaluation_results(trace_data)


def log_evaluation_results(trace_data):
    """Log evaluation results in a consistent format"""
    if trace_data.get("error"):
        logger.info(f"   ❌ Error: {trace_data['error']}")
    else:
        logger.info(f"   🔧 Tools: {', '.join(trace_data['tools_attempted']) or 'None'}")
        logger.info(f"   ⏱️  Latency: {trace_data['latency_ms']:.0f}ms")
        
        if trace_data.get('user_interventions'):
            logger.info(f"   👤 User interventions: {trace_data['user_interventions']}")
        
        if trace_data.get('ended_by'):
            logger.info(f"   🏁 Ended by: {trace_data['ended_by']}")


if __name__ == "__main__":
    main()