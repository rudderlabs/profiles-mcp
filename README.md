# Profiles MCP Server

This component provides a Model Context Protocol (MCP) server for building profiles projects through AI applications like Claude in Cursor IDE.

## Prerequisites


- Requires Python 3.10.x to be installed
- Access to a supported data warehouse with appropriate permissions:
  - **Snowflake**: Read access to input tables and write access to output schema
  - **BigQuery**: Read access to input datasets and write access to output dataset
- **Snowflake Authentication**: MFA (Multi-Factor Authentication) is not supported. If your Snowflake account has MFA enabled, you must use [key-pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth) instead.
- **BigQuery Authentication**: Supports Service Account JSON files and Application Default Credentials
- Cursor IDE v0.47.x or higher installed for building profiles projects. A free version works in theory but the experience is significantly better with a paid version
- A Rudderstack [Personal Access Token](https://www.rudderstack.com/docs/dashboard-guides/personal-access-token/#generate-personal-access-token)

## Quick Start

1. Set up the mcp server:

```bash
# Run the setup script to install dependencies, validate your environment, and download embeddings:
./setup.sh
```

This will:
- Check for Python and `uv`
- Install `uv` if it is not found
- Create `.env` if missing
- Install dependencies
- Download and extract embeddings to `src/data/`
- Update MCP configuration

2. Restart Cursor to apply changes

## Usage

Once configured, you can use natural language to build a profiles project through chat interface in AI clients such as cursor:

```txt
-- Example queries
- build a rudderstack profiles project to calculate the churn propensity score for the data in snowflake under db RUDDERSTACK_TEST_DB and schema predictions_dev_project
- build a rudderstack profiles project to find revenue metrics for each user in BigQuery under project my-project and dataset predictions_dev
- build a rudderstack profiles project to create customer segments using data from my warehouse
```

While the chat experience will work with most LLMs, we recommend using claude 4.0 class of models (ex: sonnet-4)

## Debug:

In Cursor MCP settings, you should see the profiles mcp tool active, with a green button indicating the MCP tools are available. See the below image for reference:
![Cursor Settings](mcp_settings_cursor_reference.png)

If you don't see the tools, or the profiles mcp server shows as inactive, run this command on terminal within the `profiles-mcp` directory. This should launch the mcp server directly, and if there are any errors in running the script, this should catch these.
```
> scripts/start.sh
```
Then check the log file, `profiles-mcp.log` within the `profiles-mcp` directory.


## Available Tools:

The MCP Server provides following categories of tools:

* Resources tool - `about_profiles`. It provides the AI agent with static text, which has the required info about the pb concepts, models, syntax etc.
* RAG tools. Example `search_profiles_docs`. For more open-ended questions that can be answered from our docs, these tools provide a way for the agent to ask specific questions and get answers. These tools are useful for debugging when the agent faces any errors
* Query tools. Example are `run_query`, `input_table_suggestions` etc. These use warehouse connectors (Snowflake & BigQuery) and can be used to run queries directly on the warehouse
* Profiles utility tools. Example are `get_profiles_output_details`, `get_existing_connections` etc, which look at the yaml files, output files, siteconfig etc and provide required context to the AI agent

## Key components:
1. **Multi-warehouse support**: Factory-pattern warehouse connectors for Snowflake and BigQuery that connect using credentials provided during setup. The connection should be to the same warehouse where the profiles project will be built, but doesn't need to be in the same database/schema as the input or output data.
2. **Unified warehouse interface**: All warehouse operations use the same API regardless of the underlying warehouse type
3. **In-memory Qdrant vector database**: Enables the RAG workflow for documentation search

## Supported Data Warehouses

| Warehouse | Status | Authentication Methods | Notes |
|-----------|--------|----------------------|-------|
| **Snowflake** | ✅ Fully Supported | Username/Password, Key Pair, SSO | MFA not supported, use key-pair auth instead |
| **BigQuery** | ✅ Fully Supported | Service Account JSON, Application Default Credentials | Project-based permissions required |

## Coming soon:
1. Integration to Claude desktop automatically
2. More analysis tools on the profiles output tables
3. Additional warehouse support (Redshift, Databricks)

## For Developers

### Backend API Reference
For developers working with RAG-related tools, refer to the backend API service at:
https://github.com/rudderlabs/profiles-mcp-service

This repository contains the backend implementation that powers the RAG functionality used by tools like `search_profiles_docs`.

## Integrations (optional)

### Cursor Integration

1. Update your Cursor MCP configuration in `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "profiles": {
      "command": "/path/to/scripts/start.sh",
      "args": []
    }
  }
}
```

2. Restart Cursor to apply changes


## License

  This project is licensed under the Elastic License v2.0 - see the
  [LICENSE](LICENSE) file for details.