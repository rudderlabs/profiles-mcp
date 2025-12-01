from typing import Union, List, Dict, Any
import re

import pandas as pd
from databricks import sql
from logger import setup_logger
from tools.warehouse_base import BaseWarehouse, WarehouseConnectionDetails

logger = setup_logger(__name__)


class Databricks(BaseWarehouse):
    """
    Databricks implementation of the BaseWarehouse interface.

    This class provides Databricks-specific implementations for all warehouse
    operations while maintaining compatibility with the standard interface.
    Supports both Personal Access Token (PAT) and M2M OAuth authentication.
    """

    def __init__(self):
        super().__init__()
        self.session = None  # Renamed from connection for consistency with base class

    @staticmethod
    def _validate_identifier(identifier: str, identifier_type: str = "identifier") -> None:
        """
        Validate SQL identifier to prevent SQL injection.

        Allows: alphanumeric, underscore, dot (for qualified names)
        Raises exception if identifier contains potentially unsafe characters.

        Args:
            identifier: The identifier to validate (table name, schema name, etc.)
            identifier_type: Type of identifier for error message (e.g., "table", "schema")

        Raises:
            ValueError: If identifier contains unsafe characters
        """
        if not identifier or not isinstance(identifier, str):
            raise ValueError(f"Invalid {identifier_type}: must be a non-empty string")

        # Allow alphanumeric, underscore, and dot (for qualified names)
        if not re.match(r'^[a-zA-Z0-9_.]+$', identifier):
            raise ValueError(
                f"Invalid {identifier_type} '{identifier}': contains unsafe characters. "
                f"Only alphanumeric characters, underscores, and dots are allowed."
            )

    def initialize_connection(self, connection_details: dict) -> None:
        """Initialize a Databricks connection with provided credentials."""
        logger.info(
            f"Initializing Databricks connection for host: {connection_details.get('host')}"
        )
        self.connection_details = WarehouseConnectionDetails(connection_details)
        self.create_session()
        self.update_last_used()

    def create_session(self) -> Any:
        """Create a new Databricks SQL connection with proper authentication handling."""
        logger.info(
            f"Creating new Databricks connection for host: {self.connection_details.connection_details.get('host')}"
        )

        # Extract connection parameters
        host = self.connection_details.connection_details.get("host")
        http_path = self.connection_details.connection_details.get("http_endpoint")
        catalog = self.connection_details.connection_details.get("catalog")
        schema = self.connection_details.connection_details.get("schema")

        # Authentication parameters
        access_token = self.connection_details.connection_details.get("access_token")
        client_id = self.connection_details.connection_details.get("client_id")
        client_secret = self.connection_details.connection_details.get("client_secret")

        if not host or not http_path:
            raise Exception("Host and http_endpoint are required for Databricks connection")

        try:
            # Determine authentication method
            if access_token and access_token.strip():
                # Personal Access Token (PAT) authentication
                logger.info("Using Personal Access Token (PAT) authentication")
                self.session = sql.connect(
                    server_hostname=host,
                    http_path=http_path,
                    access_token=access_token,
                    catalog=catalog if catalog else None,
                    schema=schema if schema else None,
                    _enable_connection_pooling=True,  # Enable connection pooling
                )

            elif client_id and client_id.strip() and client_secret and client_secret.strip():
                # M2M OAuth authentication
                logger.info("Using M2M OAuth authentication")
                self.session = sql.connect(
                    server_hostname=host,
                    http_path=http_path,
                    auth_type="databricks-oauth",
                    client_id=client_id,
                    client_secret=client_secret,
                    catalog=catalog if catalog else None,
                    schema=schema if schema else None,
                    _enable_connection_pooling=True,  # Enable connection pooling
                )

            else:
                raise Exception(
                    "No valid authentication method found. Provide either access_token or both client_id and client_secret"
                )

        except Exception as e:
            raise Exception(f"Failed to create Databricks connection: {str(e)}")

        return self.session

    def ensure_valid_session(self) -> None:
        """Ensure we have a valid Databricks connection."""
        if self.session is None:
            raise Exception(
                "Connection is not initialized. Call initialize_warehouse_connection() mcp tool first."
            )

        try:
            # Test the connection with a simple query
            cursor = self.session.cursor()
            cursor.execute("SELECT 1 as test_column")
            cursor.fetchall()
            cursor.close()
            self.update_last_used()

        except Exception as e:
            # Connection is invalid, create new one
            logger.warning(f"Databricks connection invalid or expired: {str(e)}")
            if self.session is not None:
                try:
                    self.session.close()
                except:
                    pass

            logger.info("Creating new Databricks connection due to expiration/invalidity")
            self.session = self.create_session()
            self.update_last_used()

    def raw_query(
        self, query: str, response_type: str = "list"
    ) -> Union[List[Dict], pd.DataFrame]:
        """Execute Databricks SQL query and return results."""
        try:
            logger.info(f"Executing Databricks query: {query[:100]}...")
            self.ensure_valid_session()

            cursor = self.session.cursor()
            cursor.execute(query)

            if response_type == "list":
                # Fetch all rows
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                cursor.close()

                # Convert to list of dictionaries
                results = [dict(zip(columns, row)) for row in rows]
                return results

            elif response_type == "pandas":
                try:
                    # Fetch using Arrow format for better performance
                    df = cursor.fetchall_arrow().to_pandas()
                    cursor.close()

                    # Fill NaN values with 'Null' for object columns (consistent across different warehouses)
                    for col in df.columns:
                        if df[col].dtype == "object":
                            df[col] = df[col].fillna("Null")
                    return df
                except Exception as e:
                    logger.error(f"Failed to convert query to pandas: {str(e)}")
                    # Fall back to list format
                    cursor.close()
                    return self.raw_query(query, response_type="list")
            else:
                cursor.close()
                raise Exception(f"Invalid response type: {response_type}")

        except Exception as e:
            message = f"Databricks query execution failed: {str(e)}"
            logger.error(message)
            raise Exception(message)

    def describe_table(self, database: str, schema: str, table: str) -> List[str]:
        """Describe a Databricks table structure."""
        table_ref = None  # Initialize for error context
        try:
            self.ensure_valid_session()

            # Validate identifiers to prevent SQL injection
            self._validate_identifier(schema, "schema")
            self._validate_identifier(table, "table")
            if database:
                self._validate_identifier(database, "database")

            # Check if Unity Catalog is being used (3-level namespace)
            catalog = self.connection_details.connection_details.get("catalog")

            if catalog and catalog.strip():
                # Unity Catalog: catalog.schema.table
                self._validate_identifier(catalog, "catalog")
                table_ref = f"{catalog}.{schema}.{table}"
            else:
                # Legacy: database is used as catalog if provided, otherwise just schema.table
                if database and database.strip() and database != schema:
                    table_ref = f"{database}.{schema}.{table}"
                else:
                    table_ref = f"{schema}.{table}"

            # Identifiers are validated above, making this f-string safe
            query = f"DESCRIBE TABLE {table_ref}"
            results = self.raw_query(query)

            # Databricks DESCRIBE TABLE returns columns: col_name, data_type, comment
            # Filter out rows with missing col_name or data_type
            return [
                f"{row['col_name']}: {row.get('data_type', 'UNKNOWN')}"
                for row in results
                if row.get('col_name')
            ]

        except Exception as e:
            error_context = f" ({table_ref})" if table_ref else ""
            logger.error(f"Failed to describe table{error_context}: {str(e)}")
            return [f"Failed to describe table{error_context}: {str(e)}"]

    def input_table_suggestions(self, database: str, schemas: str) -> List[str]:
        """Suggest relevant tables for profiles input configuration."""
        default_tables = ["tracks", "pages", "identifies", "screens"]
        schema_list = [s.strip() for s in schemas.split(",")]
        suggestions = []

        # Validate identifiers to prevent SQL injection
        if database:
            self._validate_identifier(database, "database")
        for schema in schema_list:
            self._validate_identifier(schema, "schema")

        # Check if Unity Catalog is being used
        catalog = self.connection_details.connection_details.get("catalog")
        if catalog and catalog.strip():
            self._validate_identifier(catalog, "catalog")

        def find_matching_tables(
            schema: str, table_names: List[str], candidates: List[str]
        ) -> List[str]:
            """Find tables from the candidates list that exist in table_names (substring match)"""
            matches = []
            for candidate in candidates:
                for t in table_names:
                    if candidate.lower() in t.lower():
                        if catalog and catalog.strip():
                            # Unity Catalog: catalog.schema.table
                            matches.append(f"{catalog}.{schema}.{t}")
                        else:
                            # Legacy: database.schema.table or schema.table
                            if database and database.strip() and database != schema:
                                matches.append(f"{database}.{schema}.{t}")
                            else:
                                matches.append(f"{schema}.{t}")
            return matches

        try:
            self.ensure_valid_session()

            for schema in schema_list:
                # Determine the schema reference based on Unity Catalog or legacy
                if catalog and catalog.strip():
                    schema_ref = f"{catalog}.{schema}"
                elif database and database.strip() and database != schema:
                    schema_ref = f"{database}.{schema}"
                else:
                    schema_ref = schema

                try:
                    # List tables in the schema
                    # All identifiers in schema_ref are validated above, making this f-string safe
                    query = f"SHOW TABLES IN {schema_ref}"
                    tables = self.raw_query(query)
                    table_names = [
                        table.get("tableName") or table.get("table")
                        for table in tables
                        if table.get("tableName") or table.get("table")
                    ]

                    # Substring match for default tables
                    suggestions.extend(
                        find_matching_tables(schema, table_names, default_tables)
                    )

                    # For each table that matches 'tracks' as a substring, get event tables
                    tracks_like_tables = [
                        t for t in table_names if "tracks" in t.lower()
                    ]
                    for tracks_table in tracks_like_tables:
                        try:
                            # Build full table reference
                            # catalog, database, and schema are validated above
                            # tracks_table comes from database system tables (trusted source)
                            if catalog and catalog.strip():
                                full_table_ref = f"{catalog}.{schema}.{tracks_table}"
                            elif database and database.strip() and database != schema:
                                full_table_ref = f"{database}.{schema}.{tracks_table}"
                            else:
                                full_table_ref = f"{schema}.{tracks_table}"

                            query = f"""
                            SELECT event, COUNT(*) as count
                            FROM {full_table_ref}
                            GROUP BY event
                            ORDER BY count DESC
                            LIMIT 20
                            """
                            rows = self.raw_query(query)
                            event_names = [
                                row["event"] for row in rows if row.get("event")
                            ]
                            # For each event, check if a table with that event name exists
                            suggestions.extend(
                                find_matching_tables(schema, table_names, event_names)
                            )
                        except Exception:
                            logger.warning(
                                f"Failed to query events from {schema}.{tracks_table}"
                            )

                except Exception as e:
                    logger.warning(f"Failed to access schema {schema}: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Error in input table suggestions: {str(e)}")

        return list(set(suggestions))  # Remove duplicates
