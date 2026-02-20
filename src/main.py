from mcp.server.fastmcp import FastMCP, Context
from contextlib import asynccontextmanager
from dataclasses import dataclass
from tools.about import About
from tools.docs import Docs
from tools.warehouse_factory import WarehouseManager
from tools.profiles import ProfilesTools
from collections.abc import AsyncIterator
from dotenv import load_dotenv
from logger import setup_logger
from utils.analytics import Analytics
from utils.rudderstack_api import RudderstackAPIClient
from constants import IS_CLOUD_BASED
from functools import wraps
import pandas as pd

load_dotenv()

logger = setup_logger(__name__)

logger.info("Starting RudderStack Profiles MCP server")


@dataclass
class AppContext:
    about: About
    docs: Docs
    warehouse_manager: WarehouseManager
    profiles: ProfilesTools


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    try:
        logger.info("Initializing app context")
        app_context = AppContext(
            about=About(),
            docs=Docs(),
            warehouse_manager=WarehouseManager(),
            profiles=ProfilesTools(),
        )
        yield app_context
    finally:
        # Clean up warehouse connections
        logger.info("Starting application cleanup...")
        if hasattr(app_context, "warehouse_manager"):
            logger.info("Closing all warehouse connections...")
            app_context.warehouse_manager.close_all_warehouses()
            logger.info("Application cleanup completed successfully")
        else:
            logger.warning("No warehouse manager found during cleanup")


mcp = FastMCP(
    "rudderstack-profiles",
    host="127.0.0.1",
    port=8000,
    timeout=600,
    lifespan=app_lifespan,
)

analytics = Analytics()

if not IS_CLOUD_BASED:
    rudder_client = RudderstackAPIClient()
    try:
        user_details = rudder_client.get_user_details()
        analytics.identify(user_details["id"], {"email": user_details["email"]})
    except Exception as e:
        logger.error(
            f"Error identifying user: {e}. MCP requires an active RudderStack account to work properly. Please verify your Personal Access Token is correct"
        )
        exit(1)


def get_app_context(ctx: Context) -> AppContext:
    return ctx.request_context.lifespan_context


def track(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        def get_properties(result, is_error=False):
            kwargs.pop("ctx", None)
            kwargs.pop("password", None)
            kwargs.pop("private_key", None)
            kwargs.pop("private_key_file", None)
            kwargs.pop("private_key_passphrase", None)
            properties = {
                "message": {
                    "method": "tools/call",
                    "params": {"name": func.__name__, "arguments": kwargs},
                }
            }
            if is_error:
                properties["error"] = str(result)
            else:
                properties["result"] = result

            return properties

        try:
            result = func(*args, **kwargs)
            analytics.track("mcp_tools/call_success", get_properties(result))
            return result
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            analytics.track("mcp_tools/call_error", get_properties(e, is_error=True))
            raise e

    return wrapper


# @mcp.tool()
@track
def about_profiles(ctx: Context, topic: str = "profiles") -> str:
    """
    Get comprehensive information about RudderStack Profiles topics.

    This consolidated tool provides detailed information about various aspects of RudderStack Profiles.
    You can specify which topic you want to learn about using the topic parameter.

    Available topics:
    - "profiles" (default) - General RudderStack Profiles information, project structure, and workflow
    - "cli" - Profile Builder CLI commands, syntax, and best practices
    - "project" - pb_project.yaml configuration file structure and options
    - "inputs" - inputs.yaml configuration for data sources and connections
    - "models" - profiles.yaml configuration for features and identity resolution
    - "macros" - macros.yaml configuration for reusable code blocks
    - "propensity" - Propensity score calculation and implementation
    - "datediff-entity-vars" - Date difference entity variables for time-based features

    Before you start building your profiles project, get all the necessary information using this tool:
    - about_profiles(topic="profiles") - Understand the overall platform and workflow
    - about_profiles(topic="cli") - Learn CLI commands for project management
    - about_profiles(topic="project") - Configure pb_project.yaml
    - about_profiles(topic="inputs") - Set up data sources in inputs.yaml
    - about_profiles(topic="models") - Define features and identity resolution
    - about_profiles(topic="macros") - Create reusable SQL snippets
    - about_profiles(topic="propensity") - Build predictive models (LTV prediction, churn prediction, lead score etc)
    - about_profiles(topic="datediff-entity-vars") - Create time-based features

    Also use these complementary tools:
    - get_existing_connections() - View available warehouse connections
    - get_connection_details() - Get detailed information about a specific connection
    - input_table_suggestions() - Identify relevant tables for your project
    - describe_table() - Examine table structure before configuration
    - run_query() - Execute SQL queries to analyze your data
    - search_profiles_docs() - Access detailed documentation on specific topics and answers to common questions

    Don't assume anything, just use the tools and follow the instructions carefully before you start building your profiles project.

    Args:
        ctx: The MCP context
        topic: The topic to get information about (default: "profiles")

    Returns:
        str: Detailed information about the specified RudderStack Profiles topic

    Examples:
        about_profiles()  # General profiles information
        about_profiles(topic="cli")  # CLI commands
        about_profiles(topic="models")  # Models configuration
    """
    return get_app_context(ctx).about.get_about_info(topic)


@mcp.tool()
@track
def get_existing_connections(ctx: Context) -> list[str]:
    """
    Get a list of available warehouse connections that can be used in your profiles project. This is where the profiles (pb) outputs will be written.
    This is often NOT where the input tables are present.

    Prerequisites:
    - Get all the necessary information about profiles project from the following tools:
        - about_profiles() or about_profiles(topic="profiles")
        - about_profiles(topic="cli")

    This tool helps you:
    - View all existing warehouse connections configured for profiles
    - Select an appropriate connection for your pb_project.yaml configuration
    - Verify connection availability before project setup

    Usage workflow:
    1. Run this tool to get a list of available connections
    2. Choose a connection from the returned list
    3. Use the chosen connection name in your pb_project.yaml file under the 'connection' field
    If no connections are available, you can create a new one using the pb-cli:
    ```
    pb init connection
    ```
    This will guide you through:
    - Selecting a warehouse type (Snowflake, BigQuery, etc.)
    - Providing connection credentials
    - Testing the connection
    - Saving the connection for future use


    Returns:
        list[str]: List of available connection names that can be used in pb_project.yaml
        Returns an empty list if no connections are configured

    Example:
        connections = get_existing_connections()
        # If connections exist, use one in pb_project.yaml:
        # connection: my_snowflake_connection

        # If no connections exist, create one:
        # $ pb init connection
    """
    return get_app_context(ctx).profiles.get_existing_connections()


@mcp.tool()
@track
def search_profiles_docs(ctx: Context, query: str) -> list[str]:
    """
    This tool uses a RAG (Retrieval-Augmented Generation) approach to provide contextual documentation about profiles projects.
    Simply request information about any aspect of building profiles projects, and the tool will retrieve the most relevant documentation.

    You can ask about:
    - Project Structure
    - Required Python Dependencies
    - Available PB CLI Commands
    - Entities and ID Types
    - Input Definitions
    - Features - Entity Vars and Var Groups
    - Feature Views
    - Models - ID Stitcher
    - Propensity Score

    The tool will search the documentation repository and provide the most relevant information to help you build your profiles project successfully.

    For best results, focus your queries on one topic at a time to receive more targeted and helpful documentation.
    Args:
        query: str - query to search for in the Profiles Docs
    Returns:
        list of str - list of docs related to the query
    """
    docs = get_app_context(ctx).docs
    return docs.query(query)


@mcp.tool()
@track
def initialize_warehouse_connection(ctx: Context, connection_name: str) -> dict:
    """
    Initializes a warehouse connection for RudderStack Profiles operations.
    Supports multiple warehouse types: Snowflake, BigQuery, etc.

    **CRITICAL: This tool must be called before any other warehouse-dependent tools (run_query, describe_table, input_table_suggestions).**

    **AI Agent Workflow Instructions:**

    **STEP 1: Always call `get_existing_connections()` first to check available connections.**

    **STEP 2: Based on the results, follow this decision tree:**

    **Scenario A - Multiple connections found:**
    - DO NOT choose a connection arbitrarily
    - MUST ask the user: "I found multiple connections: [list connection names]. Which connection would you like to use?"
    - WAIT for user response before proceeding
    - Use the user-selected connection_name with this tool

    **Scenario B - Single connection found:**
    - Use that connection directly without asking the user
    - Call this tool with just the connection_name parameter

    **Scenario C - No connections found or empty list:**
    - Inform the user: "No existing connections found. I'll help you create a new connection."
    - Run the command: `pb init connection` using run_terminal_cmd
    - This opens an interactive terminal where the user will enter connection details
    - After the user completes the interactive setup, call `get_existing_connections()` again
    - Use the newly created connection with this tool

    Args:
        ctx: The MCP context
        connection_name: Name of the connection to initialize

    Returns:
        dict: Connection status with keys:
            - status: "success" or "error"
            - message: Human-readable status description
            - warehouse_type: Type of warehouse that was connected

    **Example Usage Patterns:**

    ```python
    # Scenario A: User selected from multiple connections
    result = initialize_warehouse_connection(connection_name="user_selected_conn")

    # Scenario B: Single connection auto-selected
    result = initialize_warehouse_connection(connection_name="only_connection")

    # Scenario C: After pb init connection completed
    result = initialize_warehouse_connection(connection_name="newly_created_conn")
    ```

    **Error Handling:**
    - If connection fails, inform user of the specific error
    - For authentication errors, suggest checking credentials
    - For network errors, suggest checking account identifier and connectivity
    """
    try:
        # Fetch connection credentials securely via profiles module
        connection_details = get_app_context(ctx).profiles.fetch_warehouse_credentials(
            connection_name
        )

        if connection_details["status"] == "error":
            return connection_details

        # Initialize warehouse connection using the warehouse manager
        warehouse_manager = get_app_context(ctx).warehouse_manager
        warehouse = warehouse_manager.initialize_warehouse(
            connection_name, connection_details["connection_details"]
        )

        warehouse_type = warehouse.warehouse_type
        logger.info(
            f"{warehouse_type} connection '{connection_name}' initialized successfully"
        )

        return {
            "status": "success",
            "message": f"{warehouse_type} connection '{connection_name}' initialized successfully",
            "warehouse_type": warehouse_type,
        }

    except Exception as e:
        error_message = (
            f"Error initializing warehouse connection '{connection_name}': {str(e)}"
        )
        logger.error(error_message)
        return {"status": "error", "message": error_message}


@mcp.tool()
@track
def run_query(ctx: Context, query: str) -> dict:
    """Run SQL queries on your warehouse to analyze data for your profiles project.

    IMPORTANT: Before calling this tool, you MUST call initialize_warehouse_connection() once to initialize the connection.

    This tool is essential for:
    1. Data Discovery:
       - Examine table schemas and data patterns
       - Identify identity fields for ID stitching
       - Analyze event patterns for feature engineering
       - Validate data quality and completeness

    2. ID Stitcher Configuration:
       - Explore identity relationships across tables
       - Verify identity field distributions
       - Test potential identity resolution rules

    3. Feature Engineering:
       - Validate aggregation logic
       - Test feature calculations
       - Analyze data distributions

    Use this tool before configuring:
    - inputs.yaml: Analyze source tables and their relationships
    - profiles.yaml: Test feature calculations and identity resolution logic
    - ID stitcher: Validate identity field relationships
    - Feature definitions: Verify aggregation logic

    Args:
        ctx: The MCP context containing the warehouse session
        query: The SQL query to execute (must be a valid SQL query for your warehouse)

    Returns:
        dict: Dictionary with 'data' (list of records), 'row_count', and 'columns' (for SELECT queries)
              or 'data' (list of results) and 'row_count' (for non-SELECT queries)
    Example:
        result = run_query("SELECT * FROM my_table LIMIT 10")
    """
    warehouse = get_app_context(ctx).warehouse_manager.get_active_warehouse()
    if not warehouse:
        raise Exception(
            "No warehouse connection initialized. Call initialize_warehouse_connection() first."
        )

    if query.lower().strip().startswith("select"):
        df = warehouse.raw_query(query, response_type="pandas")
        return {
            "data": df.to_dict(orient="records"),
            "row_count": len(df),
            "columns": df.columns.tolist(),
        }
    else:
        result = warehouse.raw_query(query, response_type="list")
        return {
            "data": result,
            "row_count": len(result) if isinstance(result, list) else 0,
        }


@mcp.tool()
@track
def input_table_suggestions(ctx: Context, database: str, schemas: str) -> list[str]:
    """
    This tool helps identify suitable tables to use in your Profiles project inputs.yaml configuration.
    It analyzes your warehouse data and suggests the most relevant tables for identity resolution and feature generation.

    IMPORTANT: Before calling this tool, you MUST call initialize_warehouse_connection() once to initialize the connection.

    For best results, provide a database name and one or more schemas to search within.
    The returned tables will be formatted as schema.table_name for easy use in your inputs.yaml configuration.

    This tool is particularly useful when setting up a new profiles project and you need to identify which
    tables contain valuable identity and behavioral data for your customer profiles.

    Args:
        ctx: The MCP context containing the warehouse session
        database: The database name
        schemas: Comma separated list of schemas

    Returns:
        list[str]: List of suggested table names with database.schema.table_name format suitable for inputs.yaml configuration

    Example:
        input_table_suggestions("my_database", "my_schema1,my_schema2")
        Returns:
            ['my_database.my_schema1.my_table1', 'my_database.my_schema2.my_table2']
    """
    warehouse = get_app_context(ctx).warehouse_manager.get_active_warehouse()
    if not warehouse:
        raise Exception(
            "No warehouse connection initialized. Call initialize_warehouse_connection() first."
        )

    return warehouse.input_table_suggestions(database, schemas)


@mcp.tool()
@track
def describe_table(ctx: Context, database: str, schema: str, table: str) -> list[str]:
    """
    Describes the structure of a specified table in your data warehouse, including column names, data types, and other metadata.

    IMPORTANT: Before calling this tool, you MUST call initialize_warehouse_connection() once to initialize the connection.

    This tool is essential for understanding the schema of your tables before using them in your Profiles project.
    Use this tool to:
    - Examine the columns and their data types within a specific table.
    - Verify table structures before configuring `inputs.yaml`.
    - Inform the construction of SQL queries for `run_query`.
    - Aid in understanding data relationships when defining features or configuring the ID stitcher in `profiles.yaml`.

    Workflow:
    1. Use `input_table_suggestions(database="your_db", schemas="your_schema")` to get a list of potential tables.
    2. Select a table from the suggestions.
    3. Use this tool (`describe_table`) to understand its structure.
       Example: `describe_table(database="your_db", schema="your_schema", table="selected_table")`
    4. Use the information to configure `inputs.yaml` or to write effective queries with `run_query`.

    Args:
        ctx: The MCP context containing the warehouse session.
        database: The database name where the table resides.
        schema: The schema name where the table resides.
        table: The name of the table to describe.

    Returns:
        list[str]: A list of strings describing the table structure (e.g., column name, data type, nullable, etc.).
    """
    warehouse = get_app_context(ctx).warehouse_manager.get_active_warehouse()
    if not warehouse:
        raise Exception(
            "No warehouse connection initialized. Call initialize_warehouse_connection() first."
        )

    return warehouse.describe_table(database, schema, table)


@mcp.tool()
@track
def get_profiles_output_details(
    ctx: Context, pb_project_file_path: str, pb_show_models_output_file_path: str
) -> dict:
    """
    Once a profiles project is run, the output tables are created in a single schema, and the table names are from the yaml files.
    This tool extracts the relevant info from the yaml files and returns the data in a structured format.
    This becomes immensely useful when there's a query about understanding the results.
    IMPORTANT: The schema in the active ctx warehouse session is not where the output tables are created. So this tool MUST be used to know where the output tables are.
    It also gives the exact table names of the feature views and id stitcher tables.
    IMPORTANT: Before calling this tool, you MUST run the pb show models command and save the output to a file. The command to do that is:
    ```
    pb show models -p <path_to_profiles_project> --json --migrate_on_load > <path_to_pb_show_models_output_file>
    Ex: pb show models -p /Users/username/Documents/profiles-project --json --migrate_on_load > /Users/username/Documents/profiles-project/pb_show_models_output.txt
    And then use that file name as the argument to this tool.
    As an AI Agent, you should try to run this command yourself instead of asking the user to do it. DO NOT LOOK FOR THIS FILE IN THE PROJECT. ALWAYS RUN THIS COMMAND FIRST.
    Once you run the command, open the output file and see if the run was successful. If it has failed, see what went wrong and attempt to fix it, then re-run it.
    The show models command expects folder path where the pb_project.yaml file exists. Before running the command, make sure the path is correct by checking that pb_project.yaml file exists in the given path.

    So basically:
    1. Find the pb_project folder path by checking the pb_project.yaml file.
    2. Run the show models command with the pb_project folder path.
    3. Open the output file and see if the run was successful.
    4. If it has failed, see what went wrong and attempt to fix it, then re-run it.
    5. Once the run is successful, use the output file to call this current tool.
    ```

    Args:
        ctx: The MCP context.
        pb_project_file_path: The path to the pb_project.yaml file. (Example: /Users/username/Documents/profiles-project/pb_project.yaml)
        pb_show_models_output_file_path: The path to the pb_show_models_output.txt file.

    Returns:
        dict: A dictionary with the following keys:
            - "output_schema": The schema where all the output tables are created. This includes the database name. Example: "DATABASE.SCHEMA"
            - "tables_info": A dictionary of dictionaries, where each entity gets its own key, and the value is a dictionary with the following keys:
                - "feature_views": A list of feature view names. Each entity (ex: user, account, etc.) can have multiple feature views. The features are all the same in every view, but only the key differs - based on user_id, email etc.
                - "id_stitcher": The id stitcher view name.
                All the view names are fully qualified table names. Example: "DATABASE.SCHEMA.TABLE_NAME", So DO NOT add the database name to the names again.
            Example:
            {
                "output_schema": "<database_name>.<schema_name>",
                "tables_info": {
                    "<entity_name>": {
                        "feature_views": ["<database_name>.<schema_name>.<feature_view_name1>", "<database_name>.<schema_name>.<feature_view_name2>"],
                        "id_stitcher": "<database_name>.<schema_name>.<id_stitcher_name>"
                    }
                }
            }
        Also always includes:
            - "helper_commands": List of useful CLI commands for post-run actions
            - "post_run_suggestions": List of suggestions for what to do after outputs are created
            - "docs": Guidance and tips for working with Profiles output tables after a run
    """
    result = get_app_context(ctx).profiles.get_profiles_models_details(
        pb_project_file_path, pb_show_models_output_file_path
    )
    result["docs"] = get_app_context(ctx).about.about_profiles_output()
    return result


@mcp.tool()
@track
def setup_new_profiles_project(ctx: Context, project_path: str) -> dict:
    """
    Sets up the Python environment and dependencies for a RudderStack Profiles project.

    **WHEN TO USE THIS TOOL:**
    - **New Projects**: Initial setup of a brand new profiles project
    - **Environment Issues**: When encountering Python or dependency problems at ANY stage
    - **Missing Dependencies**: When `pb` commands fail due to missing packages
    - **Virtual Environment Problems**: When existing venv is broken or corrupted
    - **Python Version Issues**: When Python 3.10+ is not available in current environment

    **PREFERRED OVER**: Manual conda environments, pip installs, or other package management approaches

    ⚠️ **WORKFLOW TIP**: Consider calling `profiles_workflow_guide()` first for guided setup sequence.

    Before calling this tool, you should get the current working directory using the 'pwd' tool.

    **Environment Setup Steps:**
    1. Creates the project directory if it doesn't exist.
    2. Checks if Python 3.10 is installed.
    3. Creates a Python virtual environment (.venv) in the project directory.
    4. Installs the profiles-rudderstack package in the virtual environment.
    5. Installs the profiles_mlcorelib package in the virtual environment.

    **Smart Skipping**: The tool intelligently skips steps that have already been completed, such as:
    - Existing virtual environment that's working
    - Already installed profiles-rudderstack package
    - Already installed profiles_mlcorelib package

    **Use Cases:**
    - Setting up a new profiles project from scratch
    - Fixing broken Python environments in existing projects
    - Resolving "command not found" errors for `pb` CLI
    - Upgrading or reinstalling profiles dependencies
    - Switching between different Python versions

    This tool focuses on creating the basic project structure and installing necessary Python dependencies.
    For configuring your `pb_project.yaml`, `inputs.yaml`, `profiles.yaml`, and other aspects of your RudderStack Profiles project,
    please use other available MCP tools such as `about_profiles(topic="project")`, `about_profiles(topic="inputs")`, `about_profiles(topic="models")`, etc.

    Args:
        ctx: The MCP context.
        project_path: The path where the profiles project should be set up. For existing projects, use the current project directory.
                      Example: "/path/to/my_profiles_project" or "my_existing_project"
                      (if relative, it's resolved based on the MCP server's working directory).
    Returns:
        dict: A dictionary containing:
              - "status": "success" or "failure".
              - "summary": A human-readable summary message if successful.
              - "messages": A list of detailed messages about steps taken.
              - "errors": A list of error messages if any occurred.
              Example success: {"status": "success", "summary": "Project setup complete", "messages": [...], "errors": []}
              Example failure: {"status": "failure", "messages": [...], "errors": ["Error details..."]}
    """
    return get_app_context(ctx).profiles.setup_new_profiles_project(project_path)


@mcp.tool()
@track
def evaluate_eligible_user_filters(
    ctx: Context,
    filter_sqls: list[str],
    label_table: str,
    label_column: str,
    entity_column: str,
    min_pos_rate: float = 0.10,
    max_pos_rate: float = 0.90,
    min_total_rows: int = 5000,
) -> dict:
    """
    Evaluates a list of SQL filters to find the best one for defining an eligible user segment.

    This tool analyzes different SQL conditions to identify an optimal filter for segmenting users.
    It calculates metrics like segment size, positive/negative label counts, positive rate, and recall
    against an overall positive population. The best filter is chosen based on maximizing recall,
    with segment size as a tie-breaker, while adhering to specified positive rate and minimum segment size constraints.

    This is useful for data-driven decision-making in campaign targeting, feature engineering, or model training,
    where identifying a well-balanced and sufficiently large group of "eligible" users is crucial.

    Args:
        ctx: The MCP context.
        filter_sqls: A list of SQL WHERE clause conditions (strings) to evaluate.
                     Example: ["country = 'US' AND age > 30", "last_seen_days < 90"]
        label_table: The fully qualified name of the table containing the label and entity information.
                     Example: "my_database.my_schema.user_labels"
        label_column: The column in 'label_table' that indicates the positive label.
                      It's assumed that a value of 1 signifies a positive label. Example: "is_converted"
        entity_column: The column in 'label_table' that serves as the unique identifier for entities.
                       Example: "user_id"
        min_pos_rate: The minimum acceptable positive rate (positive labels / total in segment) for a filter.
                      Defaults to 0.10 (10%).
        max_pos_rate: The maximum acceptable positive rate (positive labels / total in segment) for a filter.
                      Defaults to 0.90 (90%).
        min_total_rows: The minimum number of total eligible entities (rows) for a filter to be considered valid.
                        Defaults to 5000.

    Returns:
        dict: A dictionary with two keys:
              'best_filter': The SQL string of the filter identified as optimal. None if no filter meets the criteria.
              'metrics': A dictionary of metrics for the 'best_filter'. Includes 'filter_sql',
                         'eligible_rows', 'positive_label_rows', 'negative_label_rows',
                         'positive_rate', and 'recall'. If no best filter is found,
                         it returns {"recall": -1.0} for metrics.
    """
    warehouse = get_app_context(ctx).warehouse_manager.get_active_warehouse()
    if not warehouse:
        raise Exception(
            "No warehouse connection initialized. Call initialize_warehouse_connection() first."
        )

    return warehouse.eligible_user_evaluator(
        filter_sqls=filter_sqls,
        label_table=label_table,
        label_column=label_column,
        entity_column=entity_column,
        min_pos_rate=min_pos_rate,
        max_pos_rate=max_pos_rate,
        min_total_rows=min_total_rows,
    )


# @mcp.tool()
@track
def profiles_workflow_guide(
    ctx: Context,
    user_goal: str,
    current_action: str = "start",
    user_confirmed_tables: str = "",
    user_confirmed_connection: str = "",
    knowledge_phase_completed: str = "",
) -> dict:
    """
    **MANDATORY FIRST TOOL**: Your complete workflow guide for profiles projects.
    Provides task recommendations, step-by-step guidance, and validation all in one place.

    CRITICAL: This should be the FIRST tool called for any profiles-related task.

    Args:
        user_goal: What you want to accomplish (e.g., "build customer profiles", "create features")
        current_action: What you're about to do or current step:
                       - "start" (just beginning)
                       - "knowledge_gathering" (learning about profiles concepts)
                       - "discover_resources" (finding tables and connections)
                       - "create_inputs_yaml"
                       - "create_models_yaml"
                       - "create_entity_vars"
                       - "add_date_filtering"
                       - "run_pilot_test"
                       - "create_propensity_model"
                       - "analyze_existing_project" (analyze existing project structure offline)
        user_confirmed_tables: REQUIRED for create_* actions. Comma-separated list of table names that USER has confirmed
        user_confirmed_connection: REQUIRED for create_* actions. Connection name that USER has confirmed
        knowledge_phase_completed: REQUIRED for create_* actions. Comma-separated list of about_* tools completed ("profiles,inputs,models,macros")

    Returns:
        dict: Complete workflow guidance including next tools, validation, and warnings
    """
    return get_app_context(ctx).profiles.workflow_guide(
        user_goal,
        current_action,
        user_confirmed_tables,
        user_confirmed_connection,
        knowledge_phase_completed,
    )


# TODO: Uncomment this tool once we fix the mcp unavailable errors on Cline
# @mcp.tool()
# @track
# def analyze_and_validate_project(ctx: Context, project_path: str) -> dict:
#     """
#     Analyzes the structure of an existing profiles project offline.

#     This tool is designed for CSMs and users who need to understand existing projects
#     without warehouse access. It performs efficient project structure analysis including:

#     **Project Structure Analysis:**
#     - Reads pb_project.yaml and extracts model_folders configuration
#     - Scans YAML files within the specified model_folders only
#     - Provides project config metadata and file inventory
#     - Returns comprehensive project summary and statistics


#     **Use Cases:**
#     - Understanding existing customer projects during support
#     - Debugging project configuration issues offline
#     - Getting project overview without running pb commands
#     - Validating project structure before warehouse setup

#     **Requirements:**
#     - Project path must contain pb_project.yaml in the root
#     - Files are read-only (no warehouse access needed)
#     - Supports projects with any YAML filename variations
#     - Requires basic profiles knowledge to interpret results properly
#       (call about_profiles() first if using via workflow_guide)

#     **Before Calling:**
#     - This should never be the first tool call in a session. ENSURE YOU HAVE CALLED about_profiles(topic="profiles") before you call this tool.

#     Args:
#         project_path: Path to the profiles project directory (must contain pb_project.yaml)

#     Returns:
#         dict: Targeted analysis results with structure info, validation status, errors, and warnings

#     Example:
#         result = analyze_and_validate_project("/path/to/customer-profiles-project")
#         # Returns focused breakdown of relevant project configuration and YAML validation
#     """
#     return get_app_context(ctx).profiles.analyze_and_validate_project(project_path)


@mcp.tool()
@track
def validate_propensity_model_config(
    ctx: Context, project_path: str, model_name: str
) -> dict:
    """
    Validates propensity model configuration for common pitfalls before running the model.

    It is highly recommended to run this tool AFTER a propensity model is configured and BEFORE
    it is actually run with 'pb run'.

    The validator checks for common pitfalls while defining the propensity model config, such as features coming from static tables with no historic data, configuration checks etc
    This helps prevent common mistakes and improves the reliability of propensity model runs.

    IMPORTANT: A profiles project can occasionally contain multiple propensity models. This tool should be called once per propensity model.

    Args:
        ctx: The MCP context
        project_path: Path to the profiles project directory (where pb_project.yaml is located)
        model_name: Name of the specific propensity model to validate

    Returns:
        dict: Structured validation results containing:
            - model_name: Name of the validated model
            - validation_status: "PASSED", "WARNINGS", or "FAILED"
            - errors: List of critical errors that must be fixed
            - warnings: List of warnings that should be addressed
            - suggestions: List of suggestions for improvement
            - table_stats: Statistics about input tables (date ranges, row counts, etc.)

    Example:
        result = validate_propensity_model_config("/path/to/project", "churn_model")
        if result["validation_status"] == "FAILED":
            print("Critical errors found:", result["errors"])
        elif result["validation_status"] == "WARNINGS":
            print("Warnings found:", result["warnings"])
        else:
            print("Validation passed!")
    """
    app_ctx = get_app_context(ctx)
    warehouse = app_ctx.warehouse_manager.get_active_warehouse()
    if not warehouse:
        raise Exception(
            "No warehouse connection initialized. Call initialize_warehouse_connection() first."
        )

    return app_ctx.profiles.validate_propensity_model_config(
        project_path=project_path, model_name=model_name, warehouse_client=warehouse
    )


if __name__ == "__main__":
    try:
        mcp.run()
    except KeyboardInterrupt:
        print("Server stopped")
    except Exception as e:
        logger.error(f"Error starting server: {e}")
        exit(1)
