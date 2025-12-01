from typing import Union, List, Dict, Any
import json

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

    def initialize_connection(self, connection_details: dict) -> None:
        """Initialize a Redshift connection with provided credentials."""
        logger.info(
            f"Initializing Redshift connection for host: {connection_details.get('host')}"
        )
        self.connection_details = WarehouseConnectionDetails(connection_details)
        self.create_session()
        self.update_last_used()

    def _fetch_iam_credentials_from_secrets(
        self, secrets_arn: str, region: str
    ) -> dict:
        """Fetch IAM credentials from AWS Secrets Manager."""
        secrets_client = boto3.client("secretsmanager", region_name=region)
        secret_response = secrets_client.get_secret_value(SecretId=secrets_arn)

        if "SecretString" not in secret_response:
            raise Exception("Secret does not contain SecretString")

        secret_data = json.loads(secret_response["SecretString"])
        access_key_id = secret_data.get("access_key_id")
        secret_access_key = secret_data.get("secret_access_key")
        session_token = secret_data.get("session_token")

        if not access_key_id or not secret_access_key:
            raise Exception(
                "Secret must contain 'access_key_id' and 'secret_access_key' for IAM authentication"
            )

        logger.info("Successfully retrieved IAM credentials from Secrets Manager")
        return {
            "access_key_id": access_key_id,
            "secret_access_key": secret_access_key,
            "session_token": session_token,
        }

    def _build_iam_connection_params(
        self,
        database: str,
        user: str,
        credentials: dict,
        region: str,
        cluster_identifier: str,
        serverless_work_group: str,
        host: str,
        port: int,
    ) -> dict:
        """Build connection parameters for IAM authentication."""
        connection_params = {
            "iam": True,
            "database": database,
            "db_user": user,
            "access_key_id": credentials["access_key_id"],
            "secret_access_key": credentials["secret_access_key"],
            "region": region,
        }

        if credentials["session_token"]:
            connection_params["session_token"] = credentials["session_token"]

        # Determine endpoint: cluster_identifier, serverless_work_group, or host
        if cluster_identifier and cluster_identifier.strip():
            connection_params["cluster_identifier"] = cluster_identifier
            logger.info(f"Using provisioned cluster: {cluster_identifier}")
        elif serverless_work_group and serverless_work_group.strip():
            if not host or not host.strip():
                raise Exception(
                    "Host is required for Serverless Redshift with IAM authentication"
                )
            connection_params["serverless_work_group"] = serverless_work_group
            connection_params["host"] = host
            logger.info(f"Using serverless workgroup: {serverless_work_group}")
        elif host and host.strip():
            connection_params["host"] = host
            if port:
                connection_params["port"] = port
            logger.info("Using host-based IAM authentication")
        else:
            raise Exception(
                "Either cluster_identifier, serverless_work_group, or host must be provided for IAM authentication"
            )

        return connection_params

    def _create_iam_connection(
        self,
        secrets_arn: str,
        region: str,
        database: str,
        user: str,
        cluster_identifier: str,
        serverless_work_group: str,
        host: str,
        port: int,
    ):
        """Create Redshift connection with IAM authentication."""
        logger.info("Using IAM authentication with AWS Secrets Manager")

        if not region or not region.strip():
            raise Exception(
                "Region is required for IAM authentication with Secrets Manager"
            )
        if not database or not database.strip():
            raise Exception("Database is required for IAM authentication")
        if not user or not user.strip():
            raise Exception(
                "Database user (db_user) is required for IAM authentication"
            )

        try:
            credentials = self._fetch_iam_credentials_from_secrets(secrets_arn, region)
        except Exception as e:
            raise Exception(
                f"Failed to retrieve IAM credentials from Secrets Manager: {str(e)}"
            )

        connection_params = self._build_iam_connection_params(
            database,
            user,
            credentials,
            region,
            cluster_identifier,
            serverless_work_group,
            host,
            port,
        )
        return redshift_connector.connect(**connection_params)

    def _create_password_connection(
        self, host: str, port: int, database: str, user: str, password: str
    ):
        """Create Redshift connection with username/password authentication."""
        logger.info("Using username/password authentication")

        if not host.strip() or not user.strip() or not password.strip():
            raise Exception(
                "Host, user, and password are required for direct authentication"
            )
        if not database or not database.strip():
            raise Exception("Database is required")

        return redshift_connector.connect(
            host=host, port=port, database=database, user=user, password=password
        )

    def _set_search_path(self, schema: str):
        """Set the search_path for the session."""
        if not schema or not schema.strip():
            return

        self._validate_identifier(schema, "schema")
        cursor = self.session.cursor()
        try:
            cursor.execute(f'SET search_path TO "{schema}"')
            logger.info(f"Set search_path to: {schema}")
        finally:
            cursor.close()

    def create_session(self) -> Any:
        """Create a new Redshift connection with proper authentication handling."""
        logger.info(
            f"Creating new Redshift connection for host: {self.connection_details.connection_details.get('host')}"
        )

        config = self.connection_details.connection_details
        host = config.get("host")
        port = config.get("port", 5439)
        database = config.get("database")
        schema = config.get("schema", "public")
        user = config.get("user")
        password = config.get("password")
        secrets_arn = config.get("secrets_arn")
        region = config.get("region")
        cluster_identifier = config.get("cluster_identifier")
        workgroup_name = config.get("workgroup_name")
        serverless_work_group = workgroup_name if workgroup_name else None

        try:
            if secrets_arn and secrets_arn.strip():
                self.session = self._create_iam_connection(
                    secrets_arn,
                    region,
                    database,
                    user,
                    cluster_identifier,
                    serverless_work_group,
                    host,
                    port,
                )
            elif host and user and password:
                self.session = self._create_password_connection(
                    host, port, database, user, password
                )
            else:
                raise Exception(
                    "No valid authentication method found. Provide either (host, user, password) or (secrets_arn, region, database, user)"
                )

            self._set_search_path(schema)

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
                    logger.warning(
                        f"Error closing Redshift session: {str(close_error)}"
                    )

            logger.info("Creating new Redshift connection due to expiration/invalidity")
            self.session = self.create_session()
            self.update_last_used()

    def raw_query(
        self, query: str, response_type: str = "list", params: tuple = None
    ) -> Union[List[Dict], pd.DataFrame]:
        """Execute Redshift SQL query and return results.

        Args:
            query: SQL query to execute. Use %s for parameter placeholders.
            response_type: Format for results - "list" or "pandas"
            params: Optional tuple of parameters for parameterized query
        """
        cursor = None
        try:
            logger.info(f"Executing Redshift query: {query[:100]}...")
            self.ensure_valid_session()

            cursor = self.session.cursor()
            cursor.execute(query, params) if params else cursor.execute(query)

            # Fetch all rows and columns once
            rows = cursor.fetchall()
            columns = (
                [desc[0] for desc in cursor.description] if cursor.description else []
            )

            if response_type == "list":
                # Convert to list of dictionaries
                results = [dict(zip(columns, row)) for row in rows]
                return results

            elif response_type == "pandas":
                # Convert to pandas DataFrame
                df = pd.DataFrame(rows, columns=columns)

                # Fill NaN values with 'Null' for object columns (consistent across different warehouses)
                for col in df.columns:
                    if df[col].dtype == "object":
                        df[col] = df[col].fillna("Null")
                return df
            else:
                raise Exception(f"Invalid response type: {response_type}")

        except Exception as e:
            message = f"Redshift query execution failed: {str(e)}"
            logger.error(message)
            raise Exception(message)
        finally:
            # Ensure cursor is always closed
            if cursor is not None:
                try:
                    cursor.close()
                except Exception as e:
                    logger.warning(f"Error closing cursor: {str(e)}")

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

            # Build the query using pg_table_def with parameterized query for safety
            # Note: pg_table_def doesn't have a position column, so we order by column name
            query = """
            SELECT "column" AS name, type
            FROM pg_table_def
            WHERE schemaname = %s AND tablename = %s
            ORDER BY "column"
            """

            results = self.raw_query(query, params=(schema, table))

            # Format results as "column_name: type"
            if results:
                table_ref = (
                    f"{database}.{schema}.{table}"
                    if database and database != schema
                    else f"{schema}.{table}"
                )
                return [
                    f"{row['name']}: {row.get('type', 'UNKNOWN')}"
                    for row in results
                    if row.get("name")
                ]
            else:
                # If pg_table_def returns no results, table might not exist or no access
                table_ref = (
                    f"{database}.{schema}.{table}"
                    if database and database != schema
                    else f"{schema}.{table}"
                )
                return [f"Table not found or no access: {table_ref}"]

        except Exception as e:
            error_context = f" ({table_ref})" if table_ref else f" ({schema}.{table})"
            logger.error(f"Failed to describe table{error_context}: {str(e)}")
            return [f"Failed to describe table{error_context}: {str(e)}"]

    def _validate_schema_identifiers(
        self, database: str, schema_list: List[str]
    ) -> None:
        """Validate database and schema identifiers to prevent SQL injection."""
        if database:
            self._validate_identifier(database, "database")
        for schema in schema_list:
            self._validate_identifier(schema, "schema")

    def _build_qualified_table_name(
        self, database: str, schema: str, table: str
    ) -> str:
        """Build qualified table name based on database presence."""
        if database and database.strip() and database != schema:
            return f"{database}.{schema}.{table}"
        return f"{schema}.{table}"

    def _find_matching_tables(
        self, database: str, schema: str, table_names: List[str], candidates: List[str]
    ) -> List[str]:
        """Find tables from candidates list that exist in table_names (substring match)."""
        matches = []
        for candidate in candidates:
            for table in table_names:
                if candidate.lower() in table.lower():
                    matches.append(
                        self._build_qualified_table_name(database, schema, table)
                    )
        return matches

    def _get_table_names_from_schema(self, schema: str) -> List[str]:
        """Get list of table names from a schema using pg_table_def."""
        query = """
        SELECT DISTINCT tablename
        FROM pg_table_def
        WHERE schemaname = %s
        ORDER BY tablename
        """
        tables = self.raw_query(query, params=(schema,))
        return [table.get("tablename") for table in tables if table.get("tablename")]

    def _get_event_names_from_tracks_table(
        self, database: str, schema: str, tracks_table: str
    ) -> List[str]:
        """Get event names from a tracks table."""
        full_table_ref = self._build_qualified_table_name(
            database, schema, tracks_table
        )

        query = f"""
        SELECT event, COUNT(*) as count
        FROM {full_table_ref}
        GROUP BY event
        ORDER BY count DESC
        LIMIT 20
        """
        rows = self.raw_query(query)
        return [row["event"] for row in rows if row.get("event")]

    def _process_tracks_tables(
        self, database: str, schema: str, table_names: List[str]
    ) -> List[str]:
        """Process tracks tables to find event-based table suggestions."""
        suggestions = []
        tracks_like_tables = [t for t in table_names if "tracks" in t.lower()]

        for tracks_table in tracks_like_tables:
            try:
                event_names = self._get_event_names_from_tracks_table(
                    database, schema, tracks_table
                )
                suggestions.extend(
                    self._find_matching_tables(
                        database, schema, table_names, event_names
                    )
                )
            except Exception:
                logger.warning(f"Failed to query events from {schema}.{tracks_table}")

        return suggestions

    def _process_schema_for_suggestions(
        self, database: str, schema: str, default_tables: List[str]
    ) -> List[str]:
        """Process a single schema to find table suggestions."""
        suggestions = []

        try:
            table_names = self._get_table_names_from_schema(schema)

            # Substring match for default tables
            suggestions.extend(
                self._find_matching_tables(
                    database, schema, table_names, default_tables
                )
            )

            # Process tracks tables for event-based suggestions
            suggestions.extend(
                self._process_tracks_tables(database, schema, table_names)
            )

        except Exception as e:
            logger.warning(f"Failed to access schema {schema}: {str(e)}")

        return suggestions

    def input_table_suggestions(self, database: str, schemas: str) -> List[str]:
        """Suggest relevant tables for profiles input configuration."""
        default_tables = ["tracks", "pages", "identifies", "screens"]
        schema_list = [s.strip() for s in schemas.split(",")]

        # Validate identifiers to prevent SQL injection
        self._validate_schema_identifiers(database, schema_list)

        suggestions = []
        try:
            self.ensure_valid_session()

            for schema in schema_list:
                suggestions.extend(
                    self._process_schema_for_suggestions(
                        database, schema, default_tables
                    )
                )

        except Exception as e:
            logger.error(f"Error in input table suggestions: {str(e)}")

        return list(set(suggestions))  # Remove duplicates
