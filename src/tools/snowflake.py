from typing import Union, List, Dict

import pandas as pd
import snowflake.snowpark
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from logger import setup_logger
from snowflake.snowpark import Session
from tools.warehouse_base import BaseWarehouse, WarehouseConnectionDetails

logger = setup_logger(__name__)


class Snowflake(BaseWarehouse):
    """
    Snowflake implementation of the BaseWarehouse interface.

    This class provides Snowflake-specific implementations for all warehouse
    operations while maintaining compatibility with the standard interface.
    """

    session: Session

    def __init__(self):
        super().__init__()
        self.session = None

    def initialize_connection(self, connection_details: dict) -> None:
        """Initialize a Snowflake connection with provided credentials"""
        logger.info(
            f"Initializing Snowflake connection for user: {connection_details.get('user')}"
        )
        self.connection_details = WarehouseConnectionDetails(connection_details)
        self.create_session()
        self.update_last_used()

    def create_session(self) -> snowflake.snowpark.Session:
        """Create a new Snowflake session with proper authentication handling"""
        logger.info(
            f"Creating new Snowflake session for user: {self.connection_details.user}"
        )

        # Build base config dict, filtering out None and empty string values
        config = {}
        base_config = {
            "user": self.connection_details.user,
            "account": self.connection_details.account,
            "warehouse": self.connection_details.warehouse,
            "database": self.connection_details.database,
            "schema": self.connection_details.schema,
            "role": self.connection_details.role,
        }

        # Add base config (non-auth fields)
        for key, value in base_config.items():
            if value is not None and value != "":
                config[key] = value

        # Handle authentication - only one method should be used
        private_key_content = self.connection_details.connection_details.get(
            "private_key"
        )
        private_key_file = self.connection_details.connection_details.get(
            "private_key_file"
        )
        password = self.connection_details.connection_details.get("password")
        private_key_passphrase = self.connection_details.connection_details.get(
            "private_key_passphrase"
        )

        if private_key_content and private_key_content.strip():
            # Private key content provided directly
            logger.info("Using private key authentication (direct content)")
            try:
                private_key = load_pem_private_key(
                    private_key_content.encode(),
                    password=(
                        private_key_passphrase.encode()
                        if private_key_passphrase and private_key_passphrase.strip()
                        else None
                    ),
                    backend=default_backend(),
                )
                config["private_key"] = private_key
            except Exception as e:
                raise Exception(f"Failed to load private key content: {str(e)}")

        elif private_key_file and private_key_file.strip():
            # Private key file path provided
            logger.info("Using private key authentication (file path)")
            config["private_key_file"] = private_key_file
            if private_key_passphrase and private_key_passphrase.strip():
                config["private_key_passphrase"] = private_key_passphrase

        elif password and password.strip():
            # Password authentication
            logger.info("Using password authentication")
            config["password"] = password
        else:
            raise Exception(
                "No valid authentication method found. Provide either password, private_key, or private_key_file"
            )

        self.session = Session.builder.configs(config).create()

    def ensure_valid_session(self) -> None:
        """Ensure we have a valid Snowflake session"""

        if self.session is None:
            raise Exception(
                "Session is not initialized. Call initialize_warehouse_connection() mcp tool first."
            )

        try:
            self.session.sql("SELECT 1").collect()
            self.update_last_used()
        except Exception as e:
            # Session expired or invalid, create new one
            logger.warning(f"Session invalid or expired: {str(e)}")
            if self.session is not None:
                try:
                    self.session.close()
                except:
                    pass

            # Create new session
            logger.info("Creating new session due to expiration/invalidity")
            self.session = self.create_session()
            self.update_last_used()

    def raw_query(
        self, query: str, response_type: str = "list"
    ) -> Union[List[Dict], pd.DataFrame]:
        """Query Snowflake and return results"""
        try:
            logger.info(
                f"Executing raw query: {query} and response_type: {response_type}"
            )
            self.ensure_valid_session()
            result = self.session.sql(query)
            if response_type == "list":
                rows = result.collect()
                # Convert Snowpark Row objects to dictionaries
                return [dict(row.asDict()) for row in rows]
            elif response_type == "pandas":
                try:
                    df = result.toPandas()
                    for col in df.columns:
                        if df[col].dtype == "object":
                            df[col] = df[col].fillna("Null")
                    return df
                except Exception as e:
                    logger.error(f"Failed to convert query to pandas: {str(e)}")
                    return result
            else:
                raise Exception(f"Invalid response type: {response_type}")
        except Exception as e:
            message = f"Raw query execution failed: {str(e)}"
            logger.error(message)
            raise Exception(message)

    def input_table_suggestions(self, database: str, schemas: str) -> List[str]:
        default_tables = ["tracks", "pages", "identifies", "screens"]
        schema_list = [schema.strip() for schema in schemas.split(",")]
        suggestions = []

        def find_matching_tables(
            schema: str, table_names: list, candidates: list
        ) -> list:
            """Find tables from the candidates list that exist in table_names (substring match)"""
            matches = []
            for candidate in candidates:
                for t in table_names:
                    if candidate.lower() in t.lower():
                        matches.append(f"{database}.{schema}.{t}")
            return matches

        for schema in schema_list:
            tables = self.raw_query(f"SHOW TABLES IN {database}.{schema}")
            table_names = [table.get("name") or table.get("NAME") for table in tables]

            # Substring match for default tables
            suggestions.extend(
                find_matching_tables(schema, table_names, default_tables)
            )

            # For each table that matches 'tracks' as a substring, get event tables
            tracks_like_tables = [t for t in table_names if "tracks" in t.lower()]
            for tracks_table in tracks_like_tables:
                try:
                    rows = self.raw_query(
                        f"SELECT event, count(*) FROM {database}.{schema}.{tracks_table} group by event order by 2 desc limit 20"
                    )
                    event_names = [
                        row.get("EVENT") or row.get("event")
                        for row in rows
                        if row.get("EVENT") or row.get("event")
                    ]
                    # For each event, check if a table with that event name exists (substring match)
                    suggestions.extend(
                        find_matching_tables(schema, table_names, event_names)
                    )
                except Exception:
                    logger.warning(
                        f"Failed to query events from {schema}.{tracks_table}"
                    )

        return list(set(suggestions))  # Remove duplicates

    def describe_table(self, database: str, schema: str, table: str) -> List[str]:
        """Describe a table"""
        try:
            self.ensure_valid_session()
            results = self.raw_query(f"DESCRIBE TABLE {database}.{schema}.{table}")
            return [f"{row['name']}: {row['type']}" for row in results]
        except Exception as e:
            logger.error(f"Failed to describe table: {str(e)}")
            return [f"Failed to describe table: {str(e)}"]
