from typing import Union, List, Dict

import pandas as pd
from google.auth import default
from google.cloud import bigquery
from google.oauth2 import service_account
from logger import setup_logger
from tools.warehouse_base import BaseWarehouse, WarehouseConnectionDetails

logger = setup_logger(__name__)


class BigQuery(BaseWarehouse):
    """
    BigQuery implementation of the BaseWarehouse interface.

    This class provides BigQuery-specific implementations for all warehouse
    operations while maintaining compatibility with the standard interface.
    """

    def __init__(self):
        super().__init__()
        self.client: bigquery.Client = None

    def initialize_connection(self, connection_details: dict) -> None:
        """Initialize a BigQuery connection with provided credentials."""
        logger.info(
            f"Initializing BigQuery connection for project: {connection_details.get('project_id')}"
        )
        self.connection_details = WarehouseConnectionDetails(connection_details)
        self.create_session()
        self.update_last_used()

    def create_session(self) -> bigquery.Client:
        """Create a new BigQuery client with proper authentication handling."""
        logger.info(
            f"Creating new BigQuery client for project: {self.connection_details.connection_details.get('project_id')}"
        )

        project_id = self.connection_details.connection_details.get("project_id")
        credentials_dict = self.connection_details.connection_details.get("credentials")

        try:
            if credentials_dict and isinstance(credentials_dict, dict):
                # Service account credentials provided as parsed dictionary from siteconfig
                logger.info("Using parsed service account credentials from siteconfig")
                credentials = service_account.Credentials.from_service_account_info(
                    credentials_dict
                )
                self.client = bigquery.Client(
                    project=project_id, credentials=credentials
                )

            else:
                # Fall back to Application Default Credentials (ADC)
                logger.info("Using default authentication (ADC)")
                credentials, default_project = default()
                # Use provided project_id or fall back to default project
                project_id = project_id or default_project
                self.client = bigquery.Client(
                    project=project_id, credentials=credentials
                )

        except Exception as e:
            raise Exception(f"Failed to create BigQuery client: {str(e)}")

        return self.client

    def ensure_valid_session(self) -> None:
        """Ensure we have a valid BigQuery client."""
        if self.client is None:
            raise Exception(
                "Session is not initialized. Call initialize_warehouse_connection() mcp tool first."
            )

        try:
            # Test the connection with a simple query
            query = "SELECT 1 as test_column"
            query_job = self.client.query(query)
            query_job.result()  # Wait for the job to complete
            self.update_last_used()

        except Exception as e:
            # Client is invalid, create new one
            logger.warning(f"BigQuery client invalid or expired: {str(e)}")
            if self.client is not None:
                self.client.close()

            logger.info("Creating new BigQuery client due to expiration/invalidity")
            self.client = self.create_session()
            self.update_last_used()

    def raw_query(
        self, query: str, response_type: str = "list"
    ) -> Union[List[Dict], pd.DataFrame]:
        """Execute BigQuery SQL and return results."""
        try:
            logger.info(f"Executing BigQuery query: {query[:100]}...")
            self.ensure_valid_session()

            query_job = self.client.query(query)

            if response_type == "list":
                results = []
                for row in query_job:
                    # Convert Row to dictionary
                    row_dict = dict(row)
                    results.append(row_dict)
                return results

            elif response_type == "pandas":
                try:
                    df = query_job.to_dataframe()
                    # Fill NaN values with 'Null' for object columns (consistent across different warehouses)
                    for col in df.columns:
                        if df[col].dtype == "object":
                            df[col] = df[col].fillna("Null")
                    return df
                except Exception as e:
                    logger.error(f"Failed to convert query to pandas: {str(e)}")
                    # Fall back to list format
                    return self.raw_query(query, response_type="list")
            else:
                raise Exception(f"Invalid response type: {response_type}")

        except Exception as e:
            message = f"BigQuery query execution failed: {str(e)}"
            logger.error(message)
            raise Exception(message)

    def describe_table(self, database: str, schema: str, table: str) -> List[str]:
        """Describe a BigQuery table structure."""
        try:
            self.ensure_valid_session()

            # In BigQuery, database is project, schema is dataset
            table_ref = f"{database}.{schema}.{table}"
            table_obj = self.client.get_table(table_ref)

            # Format schema information similar to other warehouse output
            result = []
            for field in table_obj.schema:
                field_info = f"{field.name}: {field.field_type}"
                if field.mode == "NULLABLE":
                    field_info += " (nullable)"
                elif field.mode == "REQUIRED":
                    field_info += " (required)"
                elif field.mode == "REPEATED":
                    field_info += " (repeated)"
                result.append(field_info)

            return result

        except Exception as e:
            logger.error(f"Failed to describe table: {str(e)}")
            return [f"Failed to describe table: {str(e)}"]

    def input_table_suggestions(self, database: str, schemas: str) -> List[str]:
        """Suggest relevant tables for profiles input configuration."""
        default_tables = ["tracks", "pages", "identifies", "screens"]
        schema_list = [schema.strip() for schema in schemas.split(",")]
        suggestions = []

        def find_matching_tables(
            schema: str, table_names: List[str], candidates: List[str]
        ) -> List[str]:
            """Find tables from the candidates list that exist in table_names (substring match)"""
            matches = []
            for candidate in candidates:
                for t in table_names:
                    if candidate.lower() in t.lower():
                        matches.append(f"{database}.{schema}.{t}")
            return matches

        try:
            self.ensure_valid_session()

            for schema in schema_list:
                # List tables in the dataset (schema)
                dataset_ref = f"{database}.{schema}"
                try:
                    dataset = self.client.get_dataset(dataset_ref)
                    tables = list(self.client.list_tables(dataset))
                    table_names = [table.table_id for table in tables]

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
                            query = f"""
                            SELECT event, COUNT(*) as count 
                            FROM `{database}.{schema}.{tracks_table}` 
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
                    logger.warning(f"Failed to access dataset {schema}: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Error in input table suggestions: {str(e)}")

        return list(set(suggestions))  # Remove duplicates

    def _get_bigquery_project_id(self) -> str:
        """Get the BigQuery project ID from connection details."""
        if self.connection_details and self.connection_details.connection_details:
            return self.connection_details.connection_details.get("project_id")
        return None
