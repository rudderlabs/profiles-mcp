from utils.environment import is_cloud_based_environment

class About:
    def __init__(self):
        pass

    def get_about_info(self, topic: str) -> str:
        """
        Get comprehensive information about RudderStack Profiles topics.

        Args:
            topic: The topic to get information about. Supported topics:
                - "profiles" - General RudderStack Profiles information
                - "cli" - Profile Builder CLI commands
                - "project" - pb_project.yaml configuration
                - "inputs" - inputs.yaml configuration
                - "models" - profiles.yaml configuration
                - "macros" - macros.yaml configuration
                - "propensity" - Propensity score implementation
                - "datediff-entity-vars" - Date difference entity variables

        Returns:
            str: Detailed information about the specified topic

        Raises:
            ValueError: If the topic is not supported
        """
        topic = topic.lower().strip()

        # Map topics to their corresponding methods
        topic_mapping = {
            "profiles": self.about_profiles,
            "cli": self.about_pb_cli,
            "project": self.about_pb_project,
            "inputs": self.about_inputs,
            "models": self.about_models,
            "macros": self.about_macros,
            "propensity": self.about_propensity_score,
            "datediff-entity-vars": self.about_datediff_entity_vars,
        }

        if topic not in topic_mapping:
            available_topics = ", ".join(sorted(topic_mapping.keys()))
            raise ValueError(
                f"Unsupported topic '{topic}'. Available topics: {available_topics}"
            )

        return topic_mapping[topic]()


    def _get_virtual_env_section(self) -> str:
        """Generate virtual environment setup section based on environment."""
        if is_cloud_based_environment():
            virtual_env_section = """### 1. No virtual environment setup required

The required Python packages
(profiles-rudderstack, profiles_mlcorelib) should already be available.

**No virtual environment setup required** - you can directly use the `pb` CLI tool."""
        else:
            virtual_env_section = """### 1. Set up Python virtual environment and install required packages
```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install required packages
pip install profiles-rudderstack
pip install profiles_mlcorelib>=0.8.1
```"""

        return virtual_env_section

    def about_profiles(self) -> str:
        docs = """
# RudderStack Profiles Quick Start Guide

RudderStack Profiles is a customer data unification platform that runs natively in your warehouse. It helps you:
- Create unified customer profiles by automatically stitching identifiers
- Build and maintain customer 360° views with minimal engineering
- Generate customer features from multiple data sources
- Keep all processing within your warehouse environment

## 🚨 **MANDATORY: Start Here First**

**CRITICAL: Always call `profiles_workflow_guide()` as your FIRST tool** when working on any profiles-related task. This single tool provides complete workflow guidance, task recommendations, and validation.

```
profiles_workflow_guide(
    user_goal="describe what you want to accomplish",
    current_action="start"
)
```

## 📋 **Optimal AI Workflow Sequence**

### **Phase 1: Task Discovery** ⭐ *START HERE*
```
1. profiles_workflow_guide(user_goal, "start")
```

### **Phase 2: Project Setup**
```
2. setup_new_profiles_project(project_path)  # For new projects
   AND
   get_existing_connections()  # For getting connections from a pre-existing siteconfig file.
   IF no connections exist:
   initialize_warehouse_connection()  # Create new connection programmatically
```

### **Phase 3: Data Discovery** ⚠️ *NEVER SKIP - REQUIRED FOR REAL DATA*
```
3. input_table_suggestions(database, schemas)  # Get REAL table names
4. PRESENT results to user: "I found these tables: [list]. Which would you like to use?"
5. WAIT for user confirmation of specific tables
6. describe_table(db, schema, table)  # For EACH user-confirmed table
7. run_query("SELECT * FROM table LIMIT 10")  # Examine actual data
```

### **Phase 4: Documentation Research** 📚 *ALWAYS USE*
```
8. search_profiles_docs(query="specific feature examples")
9. search_profiles_docs(query="best practices")
```

### **Phase 5: Configuration with User Confirmation** 🔒 *USER MUST CHOOSE*
```
10. profiles_workflow_guide(
      user_goal="your goal",
      current_action="create_inputs_yaml",
      user_confirmed_tables="ACTUAL_DB.SCHEMA.TABLE1,ACTUAL_DB.SCHEMA.TABLE2",
      user_confirmed_connection="actual_connection_name"
    )
11. PRESENT column options to user and get confirmation
12. Create inputs.yaml using ONLY user-confirmed names
13. PRESENT final inputs.yaml to user for approval
14. profiles_workflow_guide(
      user_goal="your goal",
      current_action="create_models_yaml",
      user_confirmed_tables="ACTUAL_DB.SCHEMA.TABLE1,ACTUAL_DB.SCHEMA.TABLE2",
      user_confirmed_connection="actual_connection_name"
    )
15. PRESENT feature options to user and get confirmation
16. Create profiles.yaml using ONLY user-confirmed names
17. PRESENT final profiles.yaml to user for approval
```

### **Phase 6: Testing**
```
18. profiles_workflow_guide(user_goal, "run_pilot_test")
19. Execute pb commands with proper flags
```

## 🚨 **MANDATORY USER INTERACTION CHECKPOINTS**

**NEVER proceed autonomously - ALWAYS get user confirmation at these critical points:**

### **1. Connection Selection**
```
# ❌ WRONG: Autonomous decision
connection: snowflake_prod  # AI picked this without asking

# ✅ CORRECT: User interaction required
"I found these connections: [conn1, conn2, conn3]. Which would you like to use for your project?"
WAIT for user response, then use their choice
```

### **2. Table Selection**
```
# ❌ WRONG: Autonomous decision
tables = ["analytics.events", "crm.users"]  # AI picked relevant tables

# ✅ CORRECT: User interaction required
"I found these relevant tables: [table1, table2, table3, table4]. Which specific tables would you like to use?"
WAIT for user response, then use only their confirmed tables
```

### **3. Column Selection**
```
# ❌ WRONG: Autonomous decision
occurred_at_col: timestamp  # AI assumed this column name

# ✅ CORRECT: User interaction required
"I see these timestamp columns: [timestamp, event_time, created_at]. Which should I use for occurred_at_col?"
WAIT for user response, then use their choice
```

### **4. Feature Definition**
```
# ❌ WRONG: Autonomous decision
# AI creates 10 entity_vars without asking

# ✅ CORRECT: User interaction required
"Based on your data, I can create these features: [list of suggestions]. Which features would you like me to include?"
WAIT for user response, then create only requested features
```

### **5. Final Configuration Review**
```
# ❌ WRONG: Immediate file creation
# AI creates inputs.yaml and profiles.yaml files

# ✅ CORRECT: User approval required
"Here's the inputs.yaml I'll create: [show content]. Does this look correct?"
"Here's the profiles.yaml I'll create: [show content]. Should I proceed?"
WAIT for user approval before creating files
```

## 🛑 **BLOCKING CONDITIONS**

The AI MUST NOT proceed if:
- No user confirmation of tables has been received
- No user confirmation of connection has been received
- Column names haven't been verified with user
- Final configurations haven't been approved by user
- User has not explicitly chosen from discovery results

## 1. Project Structure
```
your-profiles-project/
├── models/
│   ├── inputs.yaml      # Define your data sources
│   ├── profiles.yaml    # Define features and identity stitching
│   ├── sql_models.yaml  # Optional: Custom SQL models
│   └── macros.yaml      # Optional: Reusable SQL snippets
└── pb_project.yaml      # Project configuration
```

## 2. Key Components
- **inputs.yaml**: Define your data sources (tables/views)
- **profiles.yaml**: Define models (id_stitcher, propensity), features (entity_vars)
- **pb_project.yaml**: Configure project settings, entities, and ID types
- **macros.yaml**: Define reusable SQL snippets (Use `about_profiles(topic="macros")` for detailed information)

## 3. Available Tools and When to Use Them

### **🎯 Workflow Guide Tool (Use First & Often)**
- `profiles_workflow_guide()`: **MANDATORY FIRST TOOL** - complete workflow guidance, validation, and recommendations

### **📊 Data Discovery Tools (Use Early & Often)** 🚨 *REQUIRED FOR REAL DATA*
- `input_table_suggestions()`: **MANDATORY** - Identify REAL table names for your project
- `get_existing_connections()`: **MANDATORY** - Get REAL connection names
- `describe_table()`: **MANDATORY** - Examine table structure before configuration
- `run_query()`: **MANDATORY** - Execute SQL queries to analyze your data

### **📚 Documentation Tools (Always Use)**
- `search_profiles_docs()`: **MANDATORY** - Access detailed documentation on specific topics and answers to common questions

### **⚙️ Configuration Tools (Use After Validation)**
- `about_profiles(topic="project")`: Understand configuration options in pb_project.yaml
- `about_profiles(topic="inputs")`: Get guidance on configuring data sources in inputs.yaml
- `about_profiles(topic="models")`: Learn how to define features and identity resolution
- `about_profiles(topic="propensity")`: Understand how to create predictive models
- `about_profiles(topic="datediff-entity-vars")`: Learn how to create time-based features
- `about_profiles(topic="macros")`: Learn how to create reusable code blocks in macros.yaml
- `initialize_warehouse_connection()`: Create or verify warehouse connections programmatically

### **🔧 Project Management Tools**
- `setup_new_profiles_project()`: Initialize new profiles project with dependencies
- `about_profiles(topic="cli")`: Learn CLI commands for managing your profiles project

## 🚨 **CRITICAL: Common Mistakes to Avoid**

### **NEVER Make Up Names - Always Discover Real Data** 🚫
- **NEVER** use generic names like "my_database.my_schema.my_table"
- **NEVER** use generic names like "my_snowflake_connection"
- **ALWAYS** use `input_table_suggestions()` to get REAL table names
- **ALWAYS** use `get_existing_connections()` to get REAL connection names
- **ALWAYS** pass discovered names to `profiles_workflow_guide()` for validation

### Date and Time Handling
- **NEVER** add WHERE clauses with dates in `inputs.yaml` to filter recent data to filter out entire project.
- **ALWAYS** use `pb run --begin_time 'YYYY-MM-DDTHH:MM:SSZ'` flag instead
- **ALWAYS** verify current date/year before suggesting dates
- **CLARIFICATION**: `begin_time` is to control what data is considered for the run. Typically, for dry runs or test runs. Entity-vars often need to be filtered with date ranges. For example, days_since_last_seen, is_active_in_past_30_days, etc - these need date filters in where query. They are not related to the begin_time flag.
- Example: For a pilot run in June 2025, use `--begin_time '2025-05-01T00:00:00Z'` to run data for a month before June 2025.

### Column Validation
- **NEVER** assume columns exist without verification
- **ALWAYS** use `describe_table()` to check actual column names and types
- **NEVER** make up column names without validation
- **ALWAYS** use `run_query()` to verify data patterns before configuration

### Entity Variables Best Practices
- **AVOID** complex window functions unless specifically needed
- **PREFER** simple aggregations: `count()`, `sum()`, `max()`, `min()`, `avg()`
- **ONLY** use window functions when you need ranked results or running totals
- **ALWAYS** include `order by` clause in window functions when required

### Tool Usage Requirements
- **ALWAYS** use `search_profiles_docs()` to search for specific feature examples and answers to common questions
- **ALWAYS** use `run_query()` to validate data before writing configurations
- **ALWAYS** use `describe_table()` to understand table structure first

### Window Function Syntax (Use Sparingly)
```yaml
# ❌ WRONG - Overly complex
entity_var:
  name: latest_event
  select: "row_number() over (partition by user_id order by timestamp desc)"

# ✅ CORRECT - Simple aggregation
entity_var:
  name: latest_event_date
  select: "max(timestamp)"
```

### Begin Time Usage
```bash
# ✅ CORRECT - Use CLI flag for date filtering
pb run --begin_time '2024-11-01T00:00:00Z'

# ❌ WRONG - Don't add to inputs.yaml
# inputs.yaml should NOT have:
# where: "timestamp >= '2024-01-01'"
# timestamp filters are okay to have in entity-vars, depending on the specific feature definition.
```

## 🔍 **Required Validation Steps**
1. **Before any configuration**: Use `describe_table()` on all source tables
2. **Before entity_vars**: Use `run_query()` to understand data patterns
3. **Before assumptions**: Use `search_profiles_docs()` to find examples
4. **Before complex features**: Use `search_profiles_docs()` to check best practices
5. **For date-based features**: Use `about_profiles(topic="datediff-entity-vars")` for proper macros

## 📋 **Success Rate Maximizers**
1. **ALWAYS** start with `profiles_workflow_guide()`
2. **NEVER** skip data discovery phase (`input_table_suggestions()` + `get_existing_connections()`)
3. **ALWAYS** pass discovered real data to `profiles_workflow_guide()` for validation
4. **NEVER** make up table names or connection names
5. **ALWAYS** use documentation tools (`search_profiles_docs()`)
6. **PREFER** simple solutions over complex ones
7. **VERIFY** all column names exist before using them
8. **USE** current year (2025) in all date references

        """
        return docs

    def about_pb_cli(self) -> str:
        virtual_env_section = self._get_virtual_env_section()

        docs = f"""
# Profile Builder CLI Commands

The Profile Builder (pb) CLI supports various commands to help you manage your customer profiles project.

## Basic Syntax
```
pb <command> <subcommand> [parameters]
```

## Quick Setup

{virtual_env_section}

### 2. Initialize Profiles Builder CLI
```bash
# Check the installed version
pb version
# Note: Use this version in your pb_project.yaml schema_version field

# Initialize a new warehouse connection or use existing connection
# Connection details are stored in ~/.pb/siteconfig.yaml
# The connection name you provide will be used in pb_project.yaml

# Option 1: Create a new connection via CLI (interactive)
pb init connection

# Option 2: Create a new connection via MCP tool (programmatic)
# Use initialize_warehouse_connection() tool for seamless AI workflow integration
```

## Core Commands

### version
Shows the Profile Builder's current version:
```
pb version
```

### init
Creates connections and initializes projects:
```
pb init connection     # Set up a warehouse connection
pb init pb-project -o .  # Initialize a new project, by creating pb_project.yaml, inputs.yaml, and profiles.yaml files with lots of placeholder values that must be edited. The `-o .` is used to create the files in the current directory.
```

### validate
Validates project configuration and permissions:
```
pb validate access     # Check if your role has required privileges
```

### compile
Generates SQL queries from models without executing them:
```
pb compile             # Create SQL files in the output folder
```

### run
Compiles SQL and executes in the warehouse to create identity stitchers and feature views:
```
pb run                 # Main command to build your profiles
pb run --begin_time '2025-05-01T00:00:00Z'  # Run with a begin_time flag from May 1st 2025. The begin time flag helps do a pilot run on actual data. We can use some date that's like a few days old etc. We need to be careful here, if the inputs are some test data or stale data, then we need to use a slightly older date. A good idea is to check the max timestamp from the input tables before we use a begin_time.
pb run --concurrency 10  # Can run 10 queries in parallel. pb compiler ensures only independent queries are run in parallel. This is useful for faster runs, especially in Snowflake.
```

## Analysis & Discovery Commands

### discover
Lists elements in the warehouse:
```
pb discover models     # List all models
pb discover entities   # List all entities
pb discover features   # List all features
pb discover sources    # List all sources
pb discover materials  # List all materials
```

### show
Provides detailed information about project components:
```
pb show models                 # Show model details
pb show dependencies           # Generate dependency graph
pb show dataflow               # Generate data flow diagram
pb show idstitcher-report      # Create identity stitching report
pb show entity-lookup -v 'id'  # Find features by entity ID
pb show plan                   # Show material creation details
```

### query
Executes SQL queries on the warehouse:
```
pb query "select * from user_id_stitcher"
```

### audit
Analyzes ID graphs and stitching effectiveness:
```
pb audit id_stitcher          # Analyze identity graphs
```

## Maintenance Commands

### migrate
Migrates projects to newer schema versions:
```
pb migrate auto --inplace
```

### cleanup
Removes old materials:
```
pb cleanup materials -r <days>  # Delete materials older than specified days
```

## Common Parameters

Most commands accept the following parameters:

- `-c`: Use a specific site configuration file
- `-t`: Specify target name from siteconfig.yaml
- `-p`: Use a project folder other than current directory
- `--seq_no`: Specify sequence number for a run

## Best Practices

1. Start with `pb init connection` to set up new warehouse connections
2. If `pb init pb-project` is used to create starter files, make sure to edit the files based on the actual input tables etc. Also the `-o <folder_name>` param tells where to create the files. We recommend using the current directory, so `-o .` should work
3. Use `pb validate access` to ensure proper permissions
4. Test with `pb compile` before running to check generated SQL
5. Use `pb run` to create identity stitchers and feature views. Using `--begin_time` flag with some recent date helps with a test run for faster feedback on run-time errors. Using `--concurrency` flag with a number like 10 helps with faster runs, especially in Snowflake.
6. Analyze results with `pb show` and `pb discover` commands
7. Perform maintenance with `pb cleanup` periodically
8. Use `pb migrate auto --inplace` to automatically migrate to newer schema version, rewriting existing files. This is recommended to be run if there are errors due to schema version mismatch.

## Important Notes
- Always check the entire output of pb cli commands as "Program completed successfully" doesn't necessarily mean the command was successful. There can be warnings or errors in the logs.
- Do not make up new pb commands, such as `pb build` etc. You can use `pb help` to get the list of commands and their usage.
- For more details on any command, use: `pb help <command>` (Example: `pb help run`)
- As an AI assistant, whenever this tool is invoked, always offer to run the relevant command yourself instead of asking the user to do it.
        """
        return docs

    def about_pb_project(self) -> str:
        docs = """
# pb_project.yaml Configuration Guide

The `pb_project.yaml` file is your project's main configuration file that defines entities, ID types, and project settings.

## 🚨 **CRITICAL: User Confirmation Required**

**MANDATORY**: Before adding any connection name to pb_project.yaml:
1. Use `get_existing_connections()` to discover available connections
2. **PRESENT the list to the user** and ask them to choose
3. **WAIT for user confirmation** of which connection to use
4. **NEVER autonomously decide** which connection to use from the list

```
# ❌ WRONG: Don't autonomously pick a connection
connection: some_connection_i_found

# ✅ CORRECT: Ask user to choose
"I found these connections: [conn1, conn2, conn3]. Which would you like to use?"
```

## Minimal Configuration Example
```yaml
name: my_customer_profiles
schema_version: 88 # Sample version number. Will get updated on each new release. Use `pb version` cli command to get the current version.
connection: user_chosen_connection_name  # User must specify this
model_folders:
  - models

entities:
  - name: user
    id_stitcher: models/user_id_stitcher  # ID stitcher model reference - REQUIRED
    id_column_name: user_main_id
    id_types:
      - email
      - user_id
      - anonymous_id

id_types:
  - name: email
    filters:
      - type: include
        regex: ".+@.+"
  - name: user_id
  - name: anonymous_id
```

## Required Configuration

### 1. Basic Settings
- **name**: Your project name
- **schema_version**: Get version from `pb version` cli command, and use that number here. Ex: `schema_version: 90`
- **connection**: **USER MUST CHOOSE** from `get_existing_connections()` results
- **model_folders**: List of directories containing your model files

### 2. Entities Configuration
- Define each entity you want to track (e.g., user, account)
- **MANDATORY**: Specify the ID stitcher model for each entity
- Specify ID types for each entity
- Optional: Configure feature views for different ID types

### 3. ID Types Configuration
- Define all ID types used across entities
- Add filters to ensure data quality
- Optional: Set up ID type inheritance

## Feature Views (Optional but Recommended)
Add under entities to create views with specific ID types as primary keys:
```yaml
entities:
  - name: user
    id_stitcher: models/user_id_stitcher  # REQUIRED
    id_types:
      - email
      - user_id
    feature_views:
      using_ids:
        - id: email
          name: email_based_profile
```

## Best Practices
1. Use clear, descriptive names for entities and ID types
2. Always include data quality filters for email and user IDs
3. Create feature views for commonly used identifiers
4. **ALWAYS confirm connection choice with user before proceeding**

## Common Configurations

### 1. Entity with Multiple ID Types
```yaml
entities:
  - name: user
    id_stitcher: models/user_id_stitcher  # REQUIRED
    id_column_name: user_main_id
    id_types:
      - email
      - phone
      - device_id
```

### 2. ID Types with Validation
```yaml
id_types:
  - name: email
    filters:
      - type: include
        regex: ".+@.+"
  - name: phone
    filters:
      - type: include
        regex: "^\\+?[1-9]\\d{1,14}$"
```

### 3. Feature Views for Activation
```yaml
feature_views:
  using_ids:
    - id: email
      name: email_profile
    - id: phone
      name: phone_profile
```
        """
        return docs

    def about_inputs(self) -> str:
        docs = """
# Configuring Data Inputs in Profiles

The `inputs.yaml` file defines your data sources for identity resolution and feature generation.

## 🚨 **CRITICAL: User Confirmation Required**

**MANDATORY**: Before adding any table names to inputs.yaml:
1. Use `input_table_suggestions()` to discover available tables
2. **PRESENT the suggested tables to the user** and ask them to choose
3. **WAIT for user confirmation** of which specific tables to use
4. **NEVER autonomously decide** which tables to include from the suggestions
5. Use `describe_table()` on user-confirmed tables to understand structure

```
# ❌ WRONG: Don't autonomously pick tables
inputs:
  - name: web_events
    table: some_table_i_found

# ✅ CORRECT: Ask user to choose
"I found these relevant tables: [table1, table2, table3]. Which tables would you like to use for your profiles project?"
```

## Quick Start Example
```yaml
inputs:
  - name: web_events
    app_defaults:
      table: USER_CHOSEN_DATABASE.USER_CHOSEN_SCHEMA.USER_CHOSEN_TABLE
      occurred_at_col: timestamp
      ids:
        - select: "user_id"
          type: user_id
          entity: user
        - select: "anonymous_id"
          type: anonymous_id
          entity: user
        - select: "lower(email)"
          type: email
          entity: user
```

## Key Components

### 1. Input Definition
- **name**: Unique identifier for the input (you choose this)
- **table**: **USER MUST SPECIFY** the exact table from their confirmed choices
- **occurred_at_col**: Timestamp column for event ordering (verify with `describe_table()`)

### 2. ID Mapping
- **select**: SQL expression to select ID (verify columns exist with `describe_table()`)
- **type**: Corresponding ID type from pb_project.yaml
- **entity**: Entity this ID belongs to

## Workflow for Table Selection

### Step 1: Discovery
```
1. Run input_table_suggestions(database, schemas)
2. Present results: "I found these tables that might be relevant: [list]"
3. Ask user: "Which of these tables would you like to use?"
```

### Step 2: User Confirmation
```
4. Wait for user to specify exact tables
5. For each user-chosen table, run describe_table()
6. Present column information to user
7. Ask user to confirm column names for IDs and timestamps
```

### Step 3: Configuration
```
8. Create inputs.yaml using ONLY user-confirmed tables and columns
9. Use exact table names and column names as confirmed by user
```

## Common Patterns

### 1. Multiple ID Sources
```yaml
inputs:
  - name: website_events  # Your choice of descriptive name
    app_defaults:
      table: user_confirmed_database.user_confirmed_schema.user_confirmed_table
      occurred_at_col: user_confirmed_timestamp_column
      ids:
        - select: "user_confirmed_id_column"
          type: user_id
          entity: user
        - select: "user_confirmed_anonymous_id_column"
          type: anonymous_id
          entity: user

  - name: crm_data  # Your choice of descriptive name
    app_defaults:
      table: user_confirmed_crm_database.user_confirmed_schema.user_confirmed_table
      occurred_at_col: user_confirmed_modified_column
      ids:
        - select: "lower(user_confirmed_email_column)"
          type: email
          entity: user
```

### 2. Type Casting
```yaml
ids:
  - select: "CAST(user_confirmed_id_column AS VARCHAR)"
    type: user_id
    entity: user
  - select: "NULLIF(user_confirmed_anonymous_column, '')"
    type: anonymous_id
    entity: user
```

## Best Practices

### 1. Data Quality
- Always use `database.schema.table` format for table (as confirmed by user)
- Always clean and standardize IDs (e.g., `lower(email)`)
- Cast non-string IDs to VARCHAR
- Use NULLIF to handle empty strings

### 2. Multiple Identifiers
- Include at least 2 ID types per input for better stitching
- Ensure consistent ID formatting across sources

### 3. User Interaction Requirements
```yaml
# ✅ Good Practice: Always confirm with user
# "I suggest using table X with columns Y and Z. Does this look correct?"
# "Should I use 'timestamp' or 'event_time' as the occurred_at_col?"
# "I see email, user_id, and session_id columns. Which IDs should I map?"

# ❌ Bad Practice: Making autonomous decisions
# Automatically filling in inputs.yaml without user confirmation
```

### 4. Error Prevention
```yaml
# Good Practice - User confirmed
ids:
  - select: "NULLIF(TRIM(LOWER(confirmed_email_column)), '')"
    type: email
    entity: user
  - select: "CASE WHEN confirmed_user_id_column = 'unknown' THEN NULL ELSE confirmed_user_id_column END"
    type: user_id
    entity: user
```

## 🚨 **Mandatory User Interaction Checkpoints**

1. **Table Selection**: "Which tables from the suggestions would you like to use?"
2. **Column Confirmation**: "I see these columns: [list]. Which should I use for IDs?"
3. **Timestamp Column**: "Which column should I use for occurred_at_col?"
4. **Final Review**: "Here's the inputs.yaml I'll create. Does this look correct?"

**NEVER** proceed with configuration without explicit user confirmation at each step.
        """
        return docs

    def about_models(self) -> str:
        docs = """
# Building Customer Profiles and Identity Resolution

## Overview
Profiles helps you create unified customer views by:
1. Stitching multiple identifiers into a single identity
2. Generating customer features from various data sources

## Prerequisites
Before configuring profiles.yaml:
1. Use about_profiles(topic="project") to understand the project structure and entities.
2. Use about_profiles(topic="inputs") to understand the data sources and the data flow.
3. Use run_query() to:
   - Analyze identity field relationships
   - Understand data patterns and relationships
   - Example queries:
   ```
   # Get few rows of the table to understand the data
   run_query("SELECT * FROM database.schema.identifies LIMIT 10")
   run_query("SELECT * FROM database.schema.orders LIMIT 10")
   ```
4. Use about_profiles(topic="datediff-entity-vars") to understand how to use date macros correctly
5. Use about_profiles(topic="macros") to learn how to create reusable SQL snippets in macros.yaml

## Identity Resolution Configuration

### 1. Basic ID Stitcher in profiles.yaml
```yaml
models:
  - name: user_id_stitcher
    model_type: id_stitcher
    model_spec:
      entity_key: user
      edge_sources:
        - from: inputs/web_events
        - from: inputs/crm_data
```

### 2. Feature Definition
```yaml
var_groups:
  - name: user_metrics
    entity_key: user
    vars:
      - entity_var:
          name: total_purchases
          select: count(distinct order_id)
          from: inputs/orders
          description: "Total number of orders"

      - entity_var:
          name: customer_lifetime_value
          select: sum(order_amount)
          from: inputs/orders
          description: "Total revenue from customer"

      - entity_var:
          name: first_seen_date
          select: {{macro_datediff('min(timestamp)')}}
          from: inputs/web_events
          is_feature: false
          description: "First website visit date"

      - entity_var:
          name: last_seen_date
          select: {{macro_datediff('max(timestamp)')}}
          from: inputs/web_events
          is_feature: false
          description: "Last website visit date"

      - entity_var:
          name: user_lifespan
          select: '{{ user.last_seen_date }} - {{ user.first_seen_date }}'
          description: "User lifespan"
```

## 🎯 **Entity Variables Best Practices**

### 🚦 **Entity Variable Aggregation Rule**

- **MANDATORY:**
  If an `entity_var` has a `from` key, the `select` statement **must** use an aggregation function (such as `count()`, `sum()`, `max()`, `min()`, `avg()`, etc.).
- The **only exception** is when there is **no `from` key**—in this case, the `entity_var` is a derived feature that references other entity_vars and can use simple expressions or references.

#### **Correct Examples:**
```yaml
# Aggregation with from
- entity_var:
    name: total_orders
    select: count(distinct order_id)
    from: inputs/orders

# Derived feature without from
- entity_var:
    name: average_order_value
    select: "{{ user.total_revenue }} / NULLIF({{ user.total_orders }}, 0)"
```

#### **Incorrect Example:**
```yaml
# ❌ This is NOT allowed: non-aggregation select with from
- entity_var:
    name: order_id
    select: order_id
    from: inputs/orders
```

> **Why?**
> Without aggregation, a `from` clause would return multiple rows per entity, which breaks the entity_var contract. Always aggregate when using `from`.

### ✅ **Preferred: Simple Aggregations**
Use these patterns for 95% of your features:
```yaml
vars:
  # Counting features
  - entity_var:
      name: total_sessions
      select: count(distinct session_id)
      from: inputs/web_events

  - entity_var:
      name: total_page_views
      select: count(*)
      from: inputs/page_views

  # Sum features
  - entity_var:
      name: total_revenue
      select: sum(amount)
      from: inputs/orders

  # Date features (use macros)
  - entity_var:
      name: days_since_signup
      select: "{{macro_datediff('min(created_at)')}}"
      from: inputs/users

  # Latest/earliest values
  - entity_var:
      name: latest_order_date
      select: max(order_date)
      from: inputs/orders

  # Averages
  - entity_var:
      name: avg_order_value
      select: avg(amount)
      from: inputs/orders
```

### ⚠️ **Use Window Functions ONLY When Necessary**
Window functions add complexity and should only be used for:
- Ranking (top N items)
- Running totals
- Lag/lead calculations

When you DO need window functions, follow this syntax:
```yaml
# For ranking features
- entity_var:
    name: most_frequent_product_category
    select: "category"
    from: inputs/product_views
    window:
      partition_by:
        - entity_var: user_id
      order_by:
        - column: view_count
          desc: true
      limit: 1

# For percentile calculations
- entity_var:
    name: purchase_amount_rank
    select: "percent_rank() over (order by amount)"
    from: inputs/orders
```

### 🚫 **Avoid These Patterns**
```yaml
# ❌ DON'T: Overly complex window functions
- entity_var:
    name: complex_calculation
    select: "row_number() over (partition by user_id, category order by timestamp desc)"

# ❌ DON'T: Assume columns exist
- entity_var:
    name: workspace_count
    select: count(distinct workspace_id)  # Verify this column exists first!

# ❌ DON'T: Add date filters with current_timestamp() etc.
- entity_var:
    name: recent_orders
    select: count(*)
    from: inputs/orders
    where: "datediff('day', order_date, current_date()) <= 30"  # Use macro_datediff_n instead (see datediff-entity-vars)
```

## 🛑 **Input Vars: Use Sparingly!**

- **Input vars** (`input_var`) modify a copy of the input table row by row, and are very costly operations—especially on large input tables.
- **Use input_var only when absolutely necessary.** Most aggregations and entity-level calculations should be handled in `entity_var` instead.
- Typical use cases for `input_var` involve row-level window functions with `partition_by` (e.g., finding the 5th session for a user).
- If you can aggregate at the entity level, use `entity_var` instead—it's much more efficient.
- For syntax and advanced usage, see the official docs via the `search_profiles_docs` tool

#### **Example:**
```yaml
# Costly! Only use if you need row-level windowing
- input_var:
    name: session_rank
    select: row_number() over (partition by user_id order by session_start)
    from: inputs/sessions
    window:
      partition_by:
        - user_id
      order_by:
        - session_start
```

> **Best Practice:**
> Always ask: "Can this be done as an entity_var instead?" If yes, avoid input_var.

## 🔍 **Required Validation Steps**
1. **Before any configuration**: Use `describe_table()` on all source tables
2. **Before entity_vars**: Use `run_query()` to understand data patterns
3. **Before assumptions**: Use `search_profiles_docs()` to find examples
4. **Before complex features**: Use `search_profiles_docs()` to check best practices
5. **For date-based features**: Use `about_profiles(topic="datediff-entity-vars")` for proper macros

## 📋 **Mandatory Workflow**
1. Always start with `describe_table()` for each input table
2. Use `run_query()` to examine sample data
3. Check `search_profiles_docs()` for similar use cases
4. Use simple aggregations in entity_vars unless complex logic is proven necessary
5. Test configurations with `pb compile` before `pb run`
6. Use `--begin_time` flag for pilot runs along with the `pb run` command

## Best Practices

1. **Identity Resolution:**
   - Use reliable ID sources for stitching
   - Include timestamp information for accurate sequencing
   - Filter out test or invalid IDs

2. **Feature Engineering:**
   - Create meaningful, well-documented features
   - Use appropriate aggregation functions
   - Consider feature freshness requirements
   - Mark intermediate calculations with is_feature: false
   - Use about_profiles(topic="datediff-entity-vars") to understand how to use date macros correctly

## Common Patterns

### 1. Time-based Features:
```yaml
vars:
  - entity_var:
      name: days_since_last_purchase
      select: "datediff('day', max(timestamp), current_date())"
      from: inputs/orders
      description: "Days since last order"
```

### 2. Derived Features:
```yaml
vars:
  - entity_var:
      name: average_order_value
      select: "{{ user.customer_lifetime_value }} / NULLIF({{ user.total_purchases }}, 0)"
      description: "Average order value"
```

### 3. Categorical Features:
```yaml
vars:
  - entity_var:
      name: preferred_category
      select: "mode(category)"
      from: inputs/product_views
      description: "Most viewed product category"
```

## Implementation Steps

1. **Set up ID Stitching:**
   - Configure id_stitcher model
   - Define edge sources
   - Test identity resolution

2. **Create Features:**
   - Define var_groups
   - Add entity_vars
   - Document features

3. **Optimize and Deploy:**
   - Test feature generation
   - Create feature views
   - Monitor performance
        """
        return docs

    def about_propensity_score(self) -> str:
        docs = """
# Propensity Score Configuration

## Overview
'propensity' is a model type within profiles. There are two types of propensity models: classification and regression.
With this, you can build classification and regression models to predict the future revenue/likelihood of user actions etc using machine learning (ML) algorithms.
These predictive capabilities enable data-driven decision-making by calculating scores that represent the users' likely future state within a predefined timeframe.

## Use Cases
- **Future LTV/Revenue**: Compute the numeric future revenue the customers are likely to generate in the next n days (typically a regression model)
- **Reduced churn**: Identify users at risk of churning and implement targeted interventions (classification model)
- **Increased conversions**: Prioritize leads with a higher propensity to convert (classification model)
- **Improved resource allocation**: Focus resources on high-value user segments (regression/classification model, depending on the use-case)

## 🚨 **CRITICAL: Date Handling Rules for Propensity Models**

### **MANDATORY: Always Use Macros for Date Functions**

**NEVER use these directly in entity_vars:**
- ❌ `current_date()` or `CURRENT_DATE()`
- ❌ `current_timestamp()` or `CURRENT_TIMESTAMP()`
- ❌ `datediff()` without macros
- ❌ `sysdate`, `getdate()`, `now()`
- ❌ Any hardcoded date functions

**ALWAYS use macros instead:**
- ✅ `{{macro_datediff('column_name')}}` - for days since a date
- ✅ `{{macro_datediff_n('column_name', 'days')}}` - for date range filters

### **Why Macros are MANDATORY:**
1. **Warehouse Portability**: Macros handle syntax differences between Snowflake/BigQuery
2. **Begin Time Support**: Macros automatically respect `pb run --begin_time` flag
3. **Training Consistency**: Ensures consistent date calculations across training/prediction
4. **Error Prevention**: Avoids runtime failures from warehouse-specific functions

### **Common Mistakes to AVOID:**

```yaml
# ❌ WRONG - Direct date function (WILL FAIL)
entity_var:
  name: days_since_last_seen
  select: "datediff('day', max(timestamp), current_date())"
  from: inputs/events

# ✅ CORRECT - Using macro
entity_var:
  name: days_since_last_seen
  select: "{{macro_datediff('max(timestamp)')}}"
  from: inputs/events

# ❌ WRONG - Direct date filter (WILL FAIL)
entity_var:
  name: recent_revenue
  select: sum(amount)
  from: inputs/orders
  where: "datediff('day', order_date, current_date()) <= 30"

# ✅ CORRECT - Using macro for date filter
entity_var:
  name: recent_revenue
  select: sum(amount)
  from: inputs/orders
  where: "{{macro_datediff_n('order_date', '30')}}"
```

## Prerequisites
- An active RudderStack Profiles project (v0.22.0 or above) using Snowflake or BigQuery. If an older version, you can upgrade to v0.22.0 by running `pip install --upgrade profiles-rudderstack`
- Install the profiles_mlcorelib library: `pip install --upgrade profiles_mlcorelib`
- Python requirements:
  - BigQuery: Python 3.9.0 to 3.11.10
  - Snowflake: Python ≥ 3.9.0 and < 3.11.0
- Update pb_project.yaml to include:
```yaml
python_requirements:
  - profiles_mlcorelib>=0.8.1
```

- **MANDATORY**: Define date macros in macros.yaml (see about_profiles(topic="macros"))

## Project Setup Steps

### Step 1: Choose Model Type
Classification is for binary outcomes (yes/no, true/false, 0/1) like churn, conversion, payer prediction.
Regression is for numeric outcomes like revenue, LTV, days to convert.

#### Guidelines:
- Classification: Easier to interpret but requires min 5% distribution in both classes
- Regression: More flexible but may produce negative values (acceptable for cohort analysis)

### Step 2: Define the Label (Prediction Target)
If the label entity-var is already defined in the project, you can skip this step and reuse that.
Identify the action you want to predict (e.g., churn, conversion, purchase)

**Note**:
* For classification tasks, the label must be Boolean/Binary (0/1, true/false, yes/no)
* For regression tasks, the label must be numeric.
```yaml
var_groups:

  - name: user_metrics
    entity_key: user
    vars:
      # In case of payer-conversion
      - entity_var:
            name: is_payer
            select: case when user.revenue > 0 then 1 else 0 end # Assuming revenue entity-var is already defined.
      # In case of 30 day LTV/Revenue after first seen
      - entity_var:
            name: future_revenue_30_days
            select: sum(amount)
            from: inputs/orders
            where: "{{macro_datediff_n('first_seen','30')}}" # Assuming first_seen entity-var is already defined. Ensure about_profiles(topic="macros") is called to understand how to use date macros correctly.
```

### Step 3: Define Relevant Features
Define entity_vars that may predict user behavior:
```yaml
var_groups:
  - name: user_metrics
    entity_key: user
    vars:
      - entity_var:
          name: days_since_last_seen
          select: "{{macro_datediff('max(timestamp)')}}"
          from: inputs/rsPages
      - entity_var:
          name: n_sessions
          select: count(distinct session_id)
          from: inputs/rsPages
          default_value: 0
```
#### Feature Requirements:
1. Currently, only numeric, datetime, and categorical features are supported. Array type or super/json type features are not supported.
2. Use features derived from event stream datasources (from input sources with is_event_stream: true). Features from static ETL tables without history will result in overconfident models.

### Step 4: Set the Prediction Window

**🤖 AI UNDERSTANDING REQUIRED: Feature Date vs Label Date**

Propensity models use temporal separation between feature extraction and label measurement:
- **Feature Date (T0)**: When user features/behavior are captured
- **Label Date (T0 + predict_window_days)**: When outcome is measured
- **eligible_users condition applies at Feature Date**, not Label Date

**Key for AI: The eligible_users filter selects users at T0, but the model predicts their behavior at T0+N days**

#### **AI Decision Principles & Examples**

When suggesting predict_window_days and eligible_users, consider these contextual examples:

**Early Lifecycle Value Prediction (typical pattern)**
- Context: User wants to predict new user value/LTV
- Example approach: `eligible_users: days_since_account_creation <= 7`, `predict_window_days: 30`
- Reasoning: Capture first-week behavior to predict 30-day future value
- Adapt based on: User's actual onboarding timeline and available tenure features

**Activity-Based Risk Prediction (typical pattern)**
- Context: User wants churn/engagement prediction
- Example approach: `eligible_users: days_since_last_seen <= 7`, `predict_window_days: 14`
- Reasoning: Focus on recently active users to predict near-term behavior
- Adapt based on: User's engagement cycle and available activity features

**Monetization Opportunity Prediction (typical pattern)**
- Context: User wants to identify conversion opportunities
- Example approach: `eligible_users: days_since_signup <= 14 AND revenue = 0`, `predict_window_days: 30`
- Reasoning: Target new non-paying users to predict conversion likelihood
- Adapt based on: User's conversion funnel timing and available value features

**Key principle**: Use these as starting points, then adapt based on the user's specific business context, available entity_vars, and data patterns revealed through analysis.

#### **Common Windows by Use Case**
- **LTV (new users)**: 30 days - optimizing for new user campaigns
- **LTV (retention)**: 90 days - subscription retargeting
- **Churn (high engagement)**: 7 days - gaming, daily active apps
- **Churn (low frequency)**: 90-180 days - occasional usage products
- **Lead Score**: 7 days - quick conversion decisions

### Step 5: Define Eligible Users

**🤖 AI GUIDANCE: eligible_users determines training population at Feature Date (T0)**

#### Requirements:
1. All features in the clause must be defined as entity_vars with is_feature: true (default)
2. All referenced features must be in the inputs list

#### **Common Patterns & Contextual Adaptation**

**New User Value Examples:**
- `days_since_account_creation <= 7` (first week behavior)
- `days_since_signup <= 14 AND revenue = 0` (early non-paying period)
- Adapt timing based on user's actual onboarding flow

**Activity-Based Examples:**
- `days_since_last_seen <= 7` (recently active users)
- `days_since_last_session <= 30 AND total_sessions >= 5` (engaged but not recent)
- Adapt thresholds based on user's engagement patterns

**Conversion/Monetization Examples:**
- `is_trial_user = 1 AND days_until_trial_end <= 7` (trial ending soon)
- `subscription_status = 'freemium' AND days_since_signup >= 30` (established free users)
- Adapt based on user's business model and conversion funnel

**Contextual Adaptation Guidelines:**
1. **Examine available entity_vars** to understand what time/value features exist
2. **Use `run_query()` to analyze data distributions** and validate realistic thresholds
3. **Consider business context** - subscription vs transactional, B2B vs B2C timing
4. **Match prediction window to action timeline** - when does the business need to act?

These examples provide starting points, but always adapt based on the specific user's data and business context.


### Step 6: Name Output Features
The model outputs two features:
1. **Percentile score**: Relative ranking (0-100)
2. **Prediction score**:
   - Classification: Probability (0-1)
   - Regression: Numeric value

Control visibility with `is_feature` parameter:
- `is_feature: True` → Available in C360 view (recommended for percentile)
- `is_feature: False` → Only in dedicated table (recommended for raw scores)


### Step 7: Define Inputs
List all entity_vars needed:
1. Features for training
2. The label column
3. Variables used in eligible_users clause

Use `ignore_features` to exclude specific vars from training while keeping them available for filtering.

## Configuration Example
```yaml
models:
    - name: ltv_30d_prediction_model
      model_type: propensity
      model_spec:
          entity_key: user
          training:
              predict_var: entity/user/total_amount_spent
              predict_window_days: 30
              validity: month
              type: regression
              eligible_users: days_since_account_creation <= 7 and country = 'US' and revenue = 0
              max_row_count: 50000
          prediction:
              output_columns:
                  percentile:
                      name: payer_propensity_percentile
                      description: Percentile score of a user's likelihood to pay in the next 30 days
                      is_feature: True
                  score:
                      name: payer_propensity_probability
                      description: Probability score of a user's likelihood to pay in the next 30 days
                      is_feature: False
              eligible_users: days_since_account_creation <= 7 and country = 'US' and revenue = 0
          inputs:
              - entity/user/days_since_account_creation
              - entity/user/days_since_last_seen
              - entity/user/revenue
              - entity/user/is_payer
              - entity/user/country
              - entity/user/n_sessions
              - entity/user/total_amount_spent
```

## Key Parameters
- **name**: Name of the model
- **model_type**: Set to 'propensity'
- **entity_key**: Entity to use
- **predict_var**: entity_var for prediction in the format of entity/entity_key/entity_var_name
- **predict_window_days**: Time period for prediction
- **validity**: Re-training period (day, week, month)
- **type**: 'classification' for boolean, 'regression' for numeric
- **eligible_users**: SQL condition defining user set for training
- **max_row_count**: Maximum samples for training (default: 30,000)
- **ignore_features**: Features to exclude from model
- **inputs**: List of entity_vars to use for training in the format of entity/entity_key/entity_var_name

## Common Pitfalls & How to Fix Them

### 1. **Date Function Errors (MOST COMMON)**
- **Problem**: Using `current_date()`, `current_timestamp()`, or direct `datediff()` in entity_vars
- **Solution**: ALWAYS use macros: `{{macro_datediff()}}` or `{{macro_datediff_n()}}`
- **Validation**: Run `validate_propensity_model_config()` before `pb run` to catch these errors

### 2. **Missing Inputs**
- **Problem**: Propensity model can only use entity_vars listed in the inputs section
- **Solution**: Add ALL required entity_vars to the inputs list, including those in eligible_users

### 3. **Static Table Features**
- **Problem**: Features from static ETL tables without history cause overconfident models
- **Solution**: Only use features from event stream sources (is_event_stream: true)

### 4. **Cohort Usage**
- **Problem**: Propensity models are NOT supported on profiles cohorts
- **Solution**: Use eligible_users clause instead to define your user subset

### 5. **Warehouse-Specific Functions**
- **Problem**: Using Snowflake-specific or BigQuery-specific date functions
- **Solution**: Use macros for warehouse portability


## Output Structure
### Training Output
```
Material_<model_name>_<hash>_<seq_no>/
├── training_file.json
└── training_reports/
    ├── 01-feature-importance-chart-*.png
    ├── 02-[lift/residuals]-chart-*.png  # lift for classification, residuals for regression
    ├── 03-[pr-auc/deciles]-*.png       # pr-auc for classification, deciles for regression
    ├── 04-test-roc-auc-*.png  # Only for classification
    └── training_summary.json
```

### Prediction Output
- Warehouse tables with scores for each entity
- Table naming from `prediction.output_columns` config
- Feature availability based on `is_feature` setting

## Running Your Project
- `pb run`

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 🚨 MANDATORY USER INTERACTIONS - AI AGENTS MUST ASK THESE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### ✅ REQUIRED CHECKPOINT 1: Label Definition (What to Predict)
**AI MUST ASK WITH RECOMMENDATIONS**: "What outcome would you like to predict? Based on your use case and available data, I recommend:

[Analyze user's goal and available data, then provide 2-3 specific label suggestions, e.g.:]
Option 1: User made a purchase in the next 30 days (classification)
Option 2: User revenue in the next 30 days (regression)
Option 3: User churned after 90 days of inactivity (classification)

Which outcome would you like to predict, or would you like to define a different one?"

**CRITICAL REQUIREMENTS:**
- Generate intelligent suggestions based on user's stated goal and available data tables
- Use run_query() to validate that suggested labels can be derived from available data
- Label should be a descriptive business outcome, NOT a technical column name
- Examples of GOOD labels: "user made a purchase", "user churned", "user upgraded subscription"
- Examples of BAD labels: "label_col", "target", "conversion" (too generic/technical)

**WAIT for user to confirm their choice of label definition**

### ✅ REQUIRED CHECKPOINT 2: Prediction Window
**AI MUST ASK WITH RECOMMENDATIONS**: "What time window do you want to predict? Based on your use case, I recommend:
[Provide specific recommendation based on use case, e.g.:]
- For churn prediction in gaming: 7 days (high engagement product)
- For SaaS churn: 90 days (lower frequency usage)
- For new user LTV: 30 days (early revenue signals)
- For retention LTV: 90 days (mature behavior patterns)

What prediction window would you like to use?"

**HONOR USER'S CHOICE** even if different from recommendation

### ✅ REQUIRED CHECKPOINT 3: Eligible Users Definition
**AI MUST ASK WITH SPECIFIC SUGGESTIONS**: "Which users should be included in model training? Based on your use case, I recommend:

[Provide 2-3 specific SQL criteria options, e.g.:]
Option 1: `days_since_account_creation <= 7 AND country = 'US'` (new US users)
Option 2: `days_since_last_seen <= 30 AND total_orders > 0` (recent active buyers)
Option 3: `is_subscribed = 1 AND days_since_last_payment <= 60` (active subscribers)

Which criteria would you like to use, or would you prefer different criteria?"

**WAIT for user confirmation of exact SQL criteria**

### ✅ REQUIRED CHECKPOINT 4: Output Column Names
**AI MUST ASK WITH CONTEXTUAL SUGGESTIONS**: "What should I name the prediction outputs? Based on your [churn/LTV/conversion] model, I suggest:

- Percentile score: `[use_case]_likelihood_percentile` (e.g., churn_likelihood_percentile)
- Prediction score: `predicted_[metric]` (e.g., predicted_30d_revenue)

Would you like to use these names or prefer different ones?"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## ⚠️ AI AGENT RULES - NEVER SKIP THESE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 🤖 AUTONOMOUS DECISIONS (No user input needed):
1. **Model Type**: Determine classification vs regression based on use case
   - Binary outcomes → Classification
   - Numeric outcomes → Regression
2. **Feature Selection**: Choose appropriate features based on:
   - Use case requirements
   - Available entity_vars
   - Feature quality (exclude arrays, JSONs)
   - Event stream vs static table sources

### 🛑 MANDATORY USER DECISIONS (Always ask):
1. **Label Definition**: ALWAYS ask with 2-3 auto-generated suggestions based on user goal and data
2. **Prediction Window**: ALWAYS ask with recommendations
3. **Eligible Users**: ALWAYS ask with specific SQL suggestions
4. **Output Names**: ALWAYS ask with contextual suggestions

### 📋 BEST PRACTICES:
1. **ALWAYS** provide specific recommendations when asking for input
2. **ALWAYS** explain why you're recommending certain values
3. **ALWAYS** honor user's choice even if different from recommendation
4. **ALWAYS** validate entity_vars exist before using in config
5. **ALWAYS** ensure all referenced vars are in inputs list for the propensity model
6. **EXPLAIN** negative values for regression models (if user asks)

### 🚫 BLOCKING CONDITIONS
AI MUST NOT create propensity config if:
- User hasn't confirmed label definition (what outcome to predict)
- User hasn't confirmed prediction window
- User hasn't approved eligible users criteria
- User hasn't confirmed output column names

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """
        return docs

    def about_datediff_entity_vars(self) -> str:
        docs = """
# Date Difference Entity Variables Guide

> **IMPORTANT:**
> - Date filtering in entity_vars is **required** for time-based features (e.g., days_since_last_seen, is_active_last_week). This is **different** from project-level date filtering, which should be done with the `--begin_time` flag and **NOT** with YAML where clauses in inputs.yaml or at the top level of profiles.yaml.
> - For project-level filtering (e.g., for test/dry runs), see `about_profiles(topic='cli')` and use the `--begin_time` flag.

## Why Snapshotting Matters for Propensity Models

Profiles ensures the features are captured with snapshotting, making sure the features are point-in-time accurate.
This is required for propensity model training, which needs training data to be accurate at different times in the past, not necessarily as of today.
But for this to work, in the entity-vars creation, we cannot use timestamp functions such as `current_timestamp()`, as these will override profiles' timestamp functions.

## Using Date Macros

`{{end_time.Format("2006-01-02 15:04:05")}}` is a pongo template Profiles uses to get the `current_timestamp()` as of its run.
We need to use this instead of `current_timestamp()`.

## Available Macros

### 1. macro_datediff
Use this macro when you need to calculate the number of days between a date and the current date.

```yaml
macros:
  - name: macro_datediff
    inputs:
      - column
    value: |
      {% if warehouse.DatabaseType() == "bigquery" %}
        {% if !(end_time|isnil) %}
          date_diff(date('{{end_time.Format("2006-01-02 15:04:05")}}'), date({{column}}), day)
        {% else %}
          date_diff(CURRENT_DATE(), date({{column}}), day)
        {% endif %}
      {% else %}
        {% if !(end_time|isnil) %}
          datediff(day, date({{column}}), date('{{end_time.Format("2006-01-02 15:04:05")}}'))
        {% else %}
          datediff(day, date({{column}}), GETDATE())
        {% endif %}
      {% endif %}
```

### 2. macro_datediff_n
Use this macro when you need to check if a date is within N days of the current date.

```yaml
macros:
  - name: macro_datediff_n
    inputs:
      - column
      - number_of_days
    value: |
      {% if warehouse.DatabaseType() == "bigquery" %}
        {% if !(end_time|isnil) %}
          date_diff(date('{{end_time.Format("2006-01-02 15:04:05")}}'), date({{column}}), day) <= {{number_of_days}}
        {% else %}
          date_diff(CURRENT_DATE(), date({{column}}), day) <= {{number_of_days}}
        {% endif %}
      {% else %}
        {% if !(end_time|isnil) %}
          datediff(day, date({{column}}), date('{{end_time.Format("2006-01-02 15:04:05")}}')) <= {{number_of_days}}
        {% else %}
          datediff(day, date({{column}}), GETDATE()) <= {{number_of_days}}
        {% endif %}
      {% endif %}
```

## Sample Entity Variable Definitions

### 1. Days Since Account Creation
```yaml
entity_var:
  name: days_since_account_creation
  select: "{{macro_datediff('min(timestamp)')}}"
  from: inputs/rsIdentifies
```

### 2. Active Days in Past 365 Days
```yaml
entity_var:
  name: active_days_in_past_365_days
  select: count(distinct date(timestamp))
  from: inputs/rsTracks
  where: "{{macro_datediff_n('timestamp','365')}}"
  description: Out of 365 days, how many days have recorded an event till date including today
```

## Best Practices

1. Always use the provided macros instead of direct timestamp functions
2. Test your entity variables with different snapshot dates
3. Consider timezone implications when working with dates
4. Use descriptive names that indicate the time window
5. Add clear descriptions for date-based features

Using these macros and entity-var definitions ensures that your features are correct at different points in time in the past, irrespective of when they were computed.

## Defining Custom Date Macros

To create your own custom date-related macros, you should:
1. Define them in the `macros.yaml` file inside your models folder
2. Follow the syntax patterns shown above
3. Use conditional logic to handle different warehouses

For more detailed information about creating and using macros, use `about_profiles(topic="macros")`.
        """
        return docs

    def about_macros(self) -> str:
        docs = """
# Macros in Profiles: Reusable Code Blocks

## Overview
Macros are reusable blocks of code that can be used in a Profiles project as a form of templating.
They operate similar to functions in that you can reuse them with different parameters, reducing repetition
and making your profiles code more modular and maintainable.

## Defining Macros
You can define macros in the `macros.yaml` file in your model folder, and call them within any other profiles YAML file.

```yaml
macros:
    - name: macro_name          # Required - Name used to call the macro
      inputs:                   # Required - Parameters for the macro
          - list_of_parameters
      value: "code as string"   # Required - Macro code in string format
```

## Key Components

1. **name** (Required): Name of the macro used to call it
2. **inputs**: Parameters that can be passed to the macro
3. **value** (Required): The actual code/logic of the macro

## Syntax Rules

- Macros use the pongo2 templating syntax
- Macros operate on YAML code itself, generating new code before execution
- Input parameters are referenced using double curly brackets: `{{input}}`
- Control logic (if, else, endif) is defined within `{% %}` tags
- Reserved input words are `this` and `warehouse`

## Examples

### 1. Simple Macro with One Input
```yaml
macros:
  - name: array_agg
    inputs:
        - column_name
    value: "array_agg(distinct {{column_name}})"
```

### 2. Macro with Multiple Inputs
```yaml
macros:
  - name: macro_listagg
    inputs:
        - column
        - timestamp
    value: "LISTAGG({{column}}, ',') WITHIN group (order by {{timestamp}} ASC)"
```

### 3. Macro with No Inputs
```yaml
macros:
  - name: frame_clause
    value: "frame_condition = 'rows between unbounded preceding and unbounded following'"
```

### 4. Conditional Logic Based on Warehouse Type
```yaml
macros:
  - name: macro_listagg
    inputs:
        - column
        - timestamp
    value: "{% if warehouse.DatabaseType() == \"bigquery\" %} STRING_AGG({{column}}, ',' ORDER BY {{timestamp}} ASC) {% else %} LISTAGG({{column}}, ',') WITHIN group (order by {{timestamp}} ASC) {% endif %}"
```

### 5. Complex Date Handling Across Warehouses
```yaml
macros:
  - name: macro_datediff
    inputs:
        - column
    value: |
        {% if warehouse.DatabaseType() == \"bigquery\" %}
          {% if !(end_time|isnil) %}
            date_diff(date('{{end_time.Format(\"2006-01-02 15:04:05\")}}'), date({{column}}), day)
          {% else %}
            date_diff(CURRENT_DATE(), date({{column}}), day)
          {% endif %}
        {% else %}
          {% if !(end_time|isnil) %}
            datediff(day, date({{column}}), date('{{end_time.Format(\"2006-01-02 15:04:05\")}}'))
          {% else %}
            datediff(day, date({{column}}), GETDATE())
          {% endif %}
        {% endif %}
```

## Using Macros in Features
Once defined in macros.yaml, you can call macros in your feature definitions:

```yaml
# In profiles.yaml
- entity_var:
    name: all_anonymous_ids
    select: "{{ array_agg(anonymous_id) }}"
    from: inputs/rsIdentity

# Using date difference macro
- entity_var:
    name: days_since_first_seen
    select: "{{ macro_datediff('min(timestamp)') }}"
    from: inputs/rsPages
```

## Best Practices

1. **Naming Convention**: Use descriptive names with a `macro_` prefix
2. **Comments**: Add comments to explain complex macros
3. **Warehouse Compatibility**: Use conditional logic for warehouse-specific implementations
4. **Testing**: Test macros with different inputs before using in production
5. **Modularity**: Keep macros focused on a single purpose
6. **Documentation**: Document parameters and expected behavior

## Common Use Cases

- **Aggregation Functions**: Standardize aggregations across your project
- **Date Handling**: Handle date calculations consistently
- **String Manipulations**: Create consistent text transformations
- **Cross-Warehouse Compatibility**: Abstract warehouse-specific syntax
- **Complex Calculations**: Encapsulate multi-step calculations
        """
        return docs

    def about_profiles_output(self) -> str:
        """
        Structured guide for AI agents on post-run validation and output analysis.
        Provides clear workflows, success criteria, and troubleshooting steps.
        """
        docs = """
# 🔍 Post-Run Validation & Output Analysis

## 🚨 **STRONGLY RECOMMENDED: Run These Commands First**

### 1. Audit Identity Stitching
```bash
pb audit id_stitcher
```
- **When to run**: After every successful `pb run`
- **Purpose**: Analyzes identity graph and stitching effectiveness
- **Look for**: Disconnected identity clusters, low stitching rates, warnings from the audit
- **Success indicator**: Clean identity graphs with expected connections. Low singleton clusters in common identifiable id types (ex - email). High singleton clusters in non-identifiable id types (ex - anonymous_id) is okay.

### 2. Validate Run Logs
- **Location**: `logs/pb.log` (project root) or configured log path
- **Search for**: `ERROR`, `WARNING`, `FAILED` keywords
- **Important**: Even "Program completed successfully" can hide warnings. So always check the logs. Also, this is a cumulative file, so if you run multiple times, the logs will be appended. Check for the last run.
- **Action if issues found**: Check specific error messages and refer to troubleshooting guides

## 🔄 **RECOVERING FROM FAILED RUNS: Using --seq_no**

### 🚨 CRITICAL FOR AI AGENTS: ALWAYS Use --seq_no After Failures

When `pb run` fails partway through, **ALWAYS continue from the last run using --seq_no**. This is a critical performance optimization that:
- Reuses successfully completed models (pb detects changes via model hash)
- Only re-executes failed models and models with configuration changes
- Saves significant warehouse costs and execution time
- Works intelligently even after you fix errors and change configurations

### How to Extract seq_no from pb run Output

**Method 1: Parse pb run terminal output** (Most Reliable)
The pb run output contains a line like:
```
To use this seq, run `pb run --seq_no 5`
```
**AI Agents: Extract the number from this line automatically from terminal output.**

**Method 2: Check output directory**
```bash
ls -ltr output/<env>/
```
The most recently created directory number is your seq_no.

**Method 3: Check logs/pb.log**
Search for "seq_no" or "Seq" in the most recent run section.

### How to Continue After Fixing Errors
```bash
pb run --seq_no <extracted_seq_no>
```

### Why This Always Works
pb uses model hashing - it automatically:
- Detects which models have changed (different hash)
- Reuses unchanged models from previous seq_no
- Re-runs only changed models and their downstream dependencies
- Maintains consistency across all runs

### Example Recovery Workflow
```bash
pb run --begin_time '2025-01-01T00:00:00Z'
ERROR: Propensity model 'churn_model' configuration error

ls -ltr output/dev/
drwxr-xr-x  5 user  staff  160 Jan 15 10:23 7

Fix the error in profiles.yaml

pb run --seq_no 7
```

### AI Agent Best Practices
1. **ALWAYS capture seq_no** from every pb run output (parse terminal output)
2. **On any failure, ALWAYS suggest retry with --seq_no** after fixes
3. **Never suggest plain 'pb run'** after a failed run - always use --seq_no
4. **Inform user** about time/cost savings (can be 10x faster for large projects)
5. **Extract seq_no automatically** - don't ask user to find it manually

## ✅ **SUCCESS CRITERIA CHECKLIST**

After a successful run, verify:
- [ ] No ERROR messages in logs
- [ ] `pb audit id_stitcher` completes without issues - Optional. If id-stitcher is not modified, we don't need to run this.
- [ ] Feature tables contain expected row counts (use run_query)
- [ ] Output folder structure matches expected pattern
- [ ] Warehouse tables accessible with correct schema

## 📁 **Output Folder Structure**

**Location**: Same directory as `pb_project.yaml`

```
output/
├── <env>/                    # Environment (dev/prod from siteconfig)
│   └── <seq_no>/            # Run sequence number (integer, starts at 1)
│       ├── compile/         # Generated SQL (not executed)
│       │   └── Material_*/  # Material folders with SQL files
│       └── run/             # Executed SQL and results
│           └── Material_*/  # Material folders with execution artifacts
```

**Example path**: `output/dev/4/run/Material_user_id_stitcher_f7ed8b2a_4/0.sql`

### ⚠️ **Finding the Most Recent Run**
Sequence numbers are NOT chronological (e.g., you might have folders 1, 2, 10 where 2 is most recent). To find the latest run:

1. **Most reliable**: Check `logs/pb.log` for the most recent successful run sequence number
2. **File system**: Use `ls -ltr output/<env>/` to see folders by creation time
3. **Warehouse**: Query material registry tables for latest seq_no (if accessible)

**AI Agent Tip**: Always verify which sequence number represents the most recent run before analyzing outputs.

### Compile vs Run Folders
| Folder | Purpose | Contents |
|--------|---------|----------|
| `compile/` | SQL generation phase | Generated SQL files (not executed) |
| `run/` | Execution phase | Execution artifacts, some look identical to compile |

## 🏗️ **Model-Specific Outputs**

### Standard Models (ID Stitcher, Entity Vars)
- **File type**: `.sql` files
- **Location**: `output/<env>/<seq_no>/run/Material_<model_name>_<hash>_<seq_no>/`
- **Validation**: Check SQL files for expected queries and table creation statements

### Propensity Models
Propensity models have two distinct phases with different outputs:

#### Training Phase Output
- **Triggered when**: Model needs retraining (based on `validity` parameter)
- **Output location**: `output/<env>/<seq_no>/run/Material_<model_name>_<hash>_<seq_no>/`
- **Files created**:
  ```
  Material_<model_name>_<hash>_<seq_no>/
  ├── training_file.json                                    # Model metadata
  └── training_reports/
      ├── 01-feature-importance-chart-<model_name>_training.png
      ├── 02-test-lift-chart-<model_name>_training.png
      ├── 03-test-pr-auc-<model_name>_training.png
      ├── 04-test-roc-auc-<model_name>_training.png
      └── training_summary.json                             # Performance metrics
  ```

##### 🤖 **AI Agent Task: Analyze Training Metrics**

**MANDATORY**: When propensity model training completes, AI agents must:

1. **Read and analyze `training_summary.json`**:
   ```python
   # Example analysis approach using actual JSON structure
   with open('training_summary.json') as f:
       data = json.load(f)

   metrics = data['data']['metrics']
   train_metrics = metrics['train']
   test_metrics = metrics['test']
   val_metrics = metrics['val']

   # Extract key performance indicators
   train_roc_auc = train_metrics['roc_auc']
   test_roc_auc = test_metrics['roc_auc']
   val_roc_auc = val_metrics['roc_auc']

   train_f1 = train_metrics['f1_score']
   test_f1 = test_metrics['f1_score']

   # Sample sizes for analysis
   train_users = train_metrics['users']
   test_users = test_metrics['users']
   val_users = val_metrics['users']
   ```

2. **Analyze metrics and identify ML issues using your expertise**:

   **Use your ML knowledge to assess**:
   - Model performance quality across train/test/validation splits
   - Signs of overfitting, underfitting, or data leakage
   - Dataset balance and size adequacy
   - Metric appropriateness for the use case
   - Any other concerning patterns you notice

   **Key metrics available for analysis**:
   - `roc_auc`, `pr_auc`, `f1_score`, `precision`, `recall` across all splits
   - Sample sizes: `train_users`, `test_users`, `val_users`
   - Model type:  (from JSON metadata)

3. **Analyze and explain feature importance**:
   - **Action**: Reference `01-feature-importance-chart-<model_name>_training.png`
   - **Tell user**: "Check the feature importance chart - top features are most predictive"
   - **Guide interpretation**:
     - High importance features: These strongly predict your target variable
     - Low importance features: Consider removing to reduce overfitting
     - Unexpected top features: May indicate data leakage or quality issues

4. **Review other performance charts**:
   - **Lift Chart** (`02-test-lift-chart-*`): "This shows how much better your model performs vs random selection"
   - **PR-AUC** (`03-test-pr-auc-*`): "Precision-Recall curve - important for imbalanced datasets"
   - **ROC-AUC** (`04-test-roc-auc-*`): "Overall discrimination ability - higher is better"

##### 🎯 **Specific Metrics to Report**

AI agents should extract and report these key metrics from `training_summary.json`:

**Core Performance Metrics**:
- **ROC-AUC**: `train['roc_auc']` vs `test['roc_auc']` vs `val['roc_auc']` (look for >0.10 difference as overfitting signal)
- **PR-AUC**: `train['pr_auc']` vs `test['pr_auc']` vs `val['pr_auc']` (better for imbalanced datasets)
- **F1-Score**: `train['f1_score']` vs `test['f1_score']` vs `val['f1_score']` (balanced precision/recall)
- **Precision**: `test['precision']` (how many predicted positives were correct)
- **Recall**: `test['recall']` (how many actual positives were found)

**Dataset Characteristics**:
- **Sample sizes**: `train['users']`, `test['users']`, `val['users']` (check for adequate split sizes)
- **Total users**: Sum of all splits (should be reasonable for model complexity)
- **Model type**: `data['task']` (classification/regression) and `data['model']` (XGBClassifier, etc.)

**Apply your ML expertise to identify issues like**:
- Data leakage indicators (e.g., suspiciously perfect scores)
- Overfitting patterns (train vs test performance gaps)
- Underfitting signs (poor performance across all splits)
- Sample size adequacy for reliable model training
- Class imbalance effects on different metrics

##### 🔧 **Profiles-Specific Remediation Options**

When you identify ML issues, suggest relevant Profiles configuration changes:

**Model Configuration Parameters** (in `profiles.yaml`):

**Sample Size Controls**:
```yaml
training:
  # Option 1: Increase row limit (only useful if already hitting current limit)
  max_row_count: 100000        # Default: 30,000. Only increases if current data hits limit

  # Option 2: Advanced - Multiple training snapshots (increases sample size significantly)
  new_materialisations_config:
    strategy: manual           # Use multiple historical snapshots for training
    dates:
      - '2025-01-01,2025-01-08'  # Format: (feature_date, label_date)
      - '2025-02-01,2025-02-08'  # Dates must be separated by predict_window_days
      - '2025-03-01,2025-03-08'  # Add more pairs to increase training data
```

**Feature & Population Controls**:
```yaml
training:
  eligible_users: "criteria"   # SQL criteria to expand/narrow training population
  ignore_features:             # Remove problematic features from training
    - entity/user/feature_name

inputs:
  - entity/user/new_behavioral_feature  # Add more predictive features
```

**Key Levers for Sample Size Issues**:
- **Small datasets**: Use `new_materialisations_config` with multiple date pairs (most effective)
- **Hit row limits**: Increase `max_row_count` beyond 30,000 default
- **Wrong population**: Adjust `eligible_users` SQL criteria
- **Poor features**: Add/remove features in `inputs` section or use `ignore_features`

**📚 For Advanced Configuration**: Use `search_profiles_docs(query="propensity models new_materialisations_config")` to get detailed documentation about historical snapshot training.

**Investigation Steps**:
- Check feature importance charts to identify top contributors
- Review `eligible_users` criteria for training population quality
- Analyze input data for feature correlation and leakage
- Validate prediction window and target variable definition

#### Prediction Phase Output
- **Warehouse tables**: Created with scores for each entity
- **Table naming**: Uses names from `prediction.output_columns` in model config
- **Feature availability**:
  - `is_feature: true` → Available in main C360/feature tables
  - `is_feature: false` → Available in dedicated table named after prediction material

## 🔧 **Troubleshooting Common Issues**

### Empty or Missing Output Folders
- **Check**: Logs for compilation errors
- **Verify**: Warehouse permissions and connection
- **Action**: Run `pb validate access` to check permissions

### Propensity Model Training Skipped
- **Reason**: Model validity period hasn't expired
- **Check**: Last training date in logs
- **Force retrain**: Use appropriate CLI flags if needed

### Missing Feature Tables in Warehouse
- **Check**: Schema permissions and table creation rights
- **Verify**: Connection configuration in siteconfig
- **Action**: Query warehouse directly to confirm table existence

## 📊 **Recommended Next Steps**

1. **Validate warehouse tables**: Query created tables for row counts and schema
2. **Review feature distributions**: Check for unexpected nulls or outliers
3. **Test feature views**: Ensure ID-based views work as expected
4. **Monitor performance**: Check query execution times and resource usage
5. **Document results**: Record any issues or optimizations for future runs

## 🎯 **For AI Agents: Key Decision Points**

**If standard models only**: Focus on SQL validation and table creation
**If propensity models included**: Check both training artifacts and prediction tables
**If errors in logs**: Prioritize error resolution before proceeding
**If audit fails**: Review identity stitching configuration before continuing
        """
        return docs
