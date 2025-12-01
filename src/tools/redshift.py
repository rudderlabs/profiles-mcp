from typing import Union, List, Dict, Any
import json
import re

import pandas as pd
import redshift_connector
import boto3
from logger import setup_logger
from tools.warehouse_base import BaseWarehouse, WarehouseConnectionDetails

logger = setup_logger(__name__)


class Redshift(BaseWarehouse):
    """
    Redshift implementation of the BaseWarehouse interface.

    This class provides Redshift-specific implementations for all warehouse
    operations while maintaining compatibility with the standard interface.
    Supports both direct username/password authentication and IAM authentication
    with AWS Secrets Manager.
    """

    def __init__(self):
        super().__init__()
        self.session = None  # Using session for consistency with base class

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
        """Initialize a Redshift connection with provided credentials."""
        logger.info(
            f"Initializing Redshift connection for host: {connection_details.get('host')}"
        )
        self.connection_details = WarehouseConnectionDetails(connection_details)
        self.create_session()
        self.update_last_used()

    def create_session(self) -> Any:
        """Create a new Redshift connection with proper authentication handling."""
        logger.info(
            f"Creating new Redshift connection for host: {self.connection_details.connection_details.get('host')}"
        )

        # Extract connection parameters
        host = self.connection_details.connection_details.get("host")
        port = self.connection_details.connection_details.get("port", 5439)
        database = self.connection_details.connection_details.get("database")
        schema = self.connection_details.connection_details.get("schema", "public")
        user = self.connection_details.connection_details.get("user")
        password = self.connection_details.connection_details.get("password")

        # IAM Authentication parameters
        secrets_arn = self.connection_details.connection_details.get("secrets_arn")
        region = self.connection_details.connection_details.get("region")
        cluster_identifier = self.connection_details.connection_details.get("cluster_identifier")
        workgroup_name = self.connection_details.connection_details.get("workgroup_name")

        try:
            # Determine authentication method
            if secrets_arn and secrets_arn.strip():
                # IAM Authentication with Secrets Manager
                logger.info("Using IAM authentication with AWS Secrets Manager")

                if not region or not region.strip():
                    raise Exception("Region is required for IAM authentication with Secrets Manager")

                if not database or not database.strip():
                    raise Exception("Database is required for IAM authentication")

                if not user or not user.strip():
                    raise Exception("User is required for IAM authentication")

                # Fetch credentials from Secrets Manager
                try:
                    secrets_client = boto3.client('secretsmanager', region_name=region)
                    secret_response = secrets_client.get_secret_value(SecretId=secrets_arn)

                    # Parse the secret value (expected to be JSON)
                    if 'SecretString' in secret_response:
                        secret_data = json.loads(secret_response['SecretString'])
                    else:
                        raise Exception("Secret does not contain SecretString")

                    # Extract credentials from secret
                    # Support both cluster_identifier and workgroup_name
                    retrieved_host = secret_data.get('host')
                    retrieved_port = secret_data.get('port', 5439)
                    retrieved_password = secret_data.get('password')

                    if not retrieved_host or not retrieved_password:
                        raise Exception("Secret must contain 'host' and 'password' keys")

                    # Use retrieved credentials or fall back to provided values
                    host = retrieved_host if retrieved_host else host
                    port = retrieved_port if retrieved_port else port
                    password = retrieved_password

                    # Redact secrets ARN for security (show only last 8 chars)
                    masked_arn = f"***{secrets_arn[-8:]}" if len(secrets_arn) > 8 else "***"
                    logger.info(f"Successfully retrieved credentials from Secrets Manager: {masked_arn}")

                except Exception as e:
                    raise Exception(f"Failed to retrieve credentials from Secrets Manager: {str(e)}")

                # Create connection with retrieved credentials
                self.session = redshift_connector.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=user,
                    password=password
                )

            elif host and user and password:
                # Direct username/password authentication
                logger.info("Using username/password authentication")

                if not host.strip() or not user.strip() or not password.strip():
                    raise Exception("Host, user, and password are required for direct authentication")

                if not database or not database.strip():
                    raise Exception("Database is required")

                self.session = redshift_connector.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=user,
                    password=password
                )

            else:
                raise Exception(
                    "No valid authentication method found. Provide either (host, user, password) or (secrets_arn, region, database, user)"
                )

            # Set search_path after connection
            if schema and schema.strip():
                cursor = self.session.cursor()
                cursor.execute(f"SET search_path TO {schema}")
                cursor.close()
                logger.info(f"Set search_path to: {schema}")

        except Exception as e:
            raise Exception(f"Failed to create Redshift connection: {str(e)}")

        return self.session

    def ensure_valid_session(self) -> None:
        """Ensure we have a valid Redshift connection."""
        if self.session is None:
            raise Exception(
                "Session is not initialized. Call initialize_warehouse_connection() mcp tool first."
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
            logger.warning(f"Redshift connection invalid or expired: {str(e)}")
            if self.session is not None:
                try:
                    self.session.close()
                except Exception as close_error:
                    logger.warning(f"Error closing Redshift session: {str(close_error)}")

            logger.info("Creating new Redshift connection due to expiration/invalidity")
            self.session = self.create_session()
            self.update_last_used()

    def raw_query(
        self, query: str, response_type: str = "list"
    ) -> Union[List[Dict], pd.DataFrame]:
        """Execute Redshift SQL query and return results."""
        try:
            logger.info(f"Executing Redshift query: {query[:100]}...")
            self.ensure_valid_session()

            cursor = self.session.cursor()
            cursor.execute(query)

            if response_type == "list":
                # Fetch all rows
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                cursor.close()

                # Convert to list of dictionaries
                results = [dict(zip(columns, row)) for row in rows]
                return results

            elif response_type == "pandas":
                try:
                    # Fetch all rows and convert to pandas DataFrame
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []
                    cursor.close()

                    df = pd.DataFrame(rows, columns=columns)

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
            message = f"Redshift query execution failed: {str(e)}"
            logger.error(message)
            raise Exception(message)

    def describe_table(self, database: str, schema: str, table: str) -> List[str]:
        """Describe a Redshift table structure."""
        table_ref = None  # Initialize for error context
        try:
            self.ensure_valid_session()

            # Validate identifiers to prevent SQL injection
            self._validate_identifier(schema, "schema")
            self._validate_identifier(table, "table")
            if database:
                self._validate_identifier(database, "database")

            # Redshift supports cross-database queries
            # Default 2-level: schema.table
            # Also supports: database.schema.table for cross-database queries
            # Use pg_table_def system table for metadata

            # Build the query using pg_table_def
            # Identifiers are validated above, making this f-string safe
            query = f"""
            SELECT "column" AS name, type
            FROM pg_table_def
            WHERE schemaname = '{schema}' AND tablename = '{table}'
            ORDER BY pos
            """

            results = self.raw_query(query)

            # Format results as "column_name: type"
            if results:
                table_ref = f"{database}.{schema}.{table}" if database and database != schema else f"{schema}.{table}"
                return [
                    f"{row['name']}: {row.get('type', 'UNKNOWN')}"
                    for row in results
                    if row.get('name')
                ]
            else:
                # If pg_table_def returns no results, table might not exist or no access
                table_ref = f"{database}.{schema}.{table}" if database and database != schema else f"{schema}.{table}"
                return [f"Table not found or no access: {table_ref}"]

        except Exception as e:
            error_context = f" ({table_ref})" if table_ref else f" ({schema}.{table})"
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

        def find_matching_tables(
            schema: str, table_names: List[str], candidates: List[str]
        ) -> List[str]:
            """Find tables from the candidates list that exist in table_names (substring match)"""
            matches = []
            for candidate in candidates:
                for t in table_names:
                    if candidate.lower() in t.lower():
                        # Return in database.schema.table format
                        if database and database.strip() and database != schema:
                            matches.append(f"{database}.{schema}.{t}")
                        else:
                            matches.append(f"{schema}.{t}")
            return matches

        try:
            self.ensure_valid_session()

            for schema in schema_list:
                try:
                    # List tables in the schema using pg_table_def
                    # Schema identifier is validated above, making this f-string safe
                    query = f"""
                    SELECT DISTINCT tablename
                    FROM pg_table_def
                    WHERE schemaname = '{schema}'
                    ORDER BY tablename
                    """
                    tables = self.raw_query(query)
                    table_names = [
                        table.get("tablename")
                        for table in tables
                        if table.get("tablename")
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
                            # database and schema are validated above
                            # tracks_table comes from database system tables (trusted source)
                            if database and database.strip() and database != schema:
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
