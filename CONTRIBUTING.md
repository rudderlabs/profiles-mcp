### Debugging & Development

Use mcp inspector to call mcp tools (postman counterpart for mcp servers)

Run mcp inspector
`npx @modelcontextprotocol/inspector`

Use these values in the UI to start + connect to the mcp server
```
Transport Type:STDIO
Command: uv
Arguments: `run --with mcp[cli] mcp run src/main.py`

```

### Backend API Reference
For developers working with RAG-related tools, refer to the backend API service at:
https://github.com/rudderlabs/profiles-mcp-service


### Run tests

Run `uv run pytest -v` 
