# Profiles MCP Server

This component provides a Model Context Protocol (MCP) server for building profiles projects through AI applications like Claude in Cursor IDE.

## Prerequisites


- Requires Python 3.10.x to be installed
- Access to Snowflake with appropriate permissions (read access to input tables for profiles, and write access to a schema where profiles can generate outputs)
- **Important**: Snowflake MFA (Multi-Factor Authentication) is not supported. If your Snowflake account has MFA enabled, you must use [key-pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth) instead. Key-pair authentication is Snowflake's recommended method for uninterrupted programmatic access and provides security for automated tools.
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
- build a rudderstack profiles project to calcualte the churn propensity score for the data in snowflake under db RUDDERSTACK_TEST_DB and schema predictions_dev_project
- build a rudderstack profiles project to find revenue metrics for each user in snowflake under db RUDDERSTACK_TEST_DB and schema predictions_dev_project
```

While the chat experience will work with most LLMs, we recommend using claude class of models (ex: sonnet-3.7, sonnet-4)

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
* Query tools. Example are `run_query`, `input_table_suggestions` etc. These use a snowflake connector and can be used to run queries directly on the warehouse
* Profiles utility tools. Example are `get_profiles_output_details`, `get_existing_connections` etc, which look at the yaml files, output files, siteconfig etc and provide required context to the AI agent

## Key components:
1. A snowflake connector that connects to the warehouse using credentials provided while setting up the tool. It needs to be to the same account where the project will eventually be built. This need not be in the same database or schema where profiles inputs or outputs will be generated
2. An in-memory Qdrant vector db enables the RAG workflow

## Coming soon:
1. Integration to Claude desktop automatically
2. More analysis tools on the profiles output tables

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

Internal use only - Not for distribution outside the organization.
