from abc import ABC, abstractmethod
from datetime import datetime
from typing import Union, List, Dict, Any
import pandas as pd


class WarehouseConnectionDetails:
    """Data class for warehouse connection details."""

    def __init__(self, connection_details: dict):
        self.connection_details = connection_details
        self.warehouse_type = connection_details.get("type", "unknown")
        self.account = connection_details.get("account")
        self.user = connection_details.get("user")
        self.database = connection_details.get("database")
        self.schema = connection_details.get("schema")
        self.warehouse = connection_details.get("warehouse")
        self.role = connection_details.get("role")

    def get_connection_info(self) -> str:
        """Get a string representation for logging."""
        return f"{self.warehouse_type}://{self.user}@{self.account}/{self.database}.{self.schema}"


class BaseWarehouse(ABC):
    """
    Abstract base class for data warehouse integrations.

    This class defines the standard interface that all warehouse implementations
    must follow to ensure consistent behavior across different warehouse types
    (Snowflake, BigQuery, etc.).
    """

    def __init__(self):
        self.session = None
        self.last_used: datetime = None
        self.connection_details: WarehouseConnectionDetails = None

    @abstractmethod
    def initialize_connection(self, connection_details: dict) -> None:
        """
        Initialize a connection to the warehouse with provided credentials.

        Args:
            connection_details: Dictionary containing warehouse connection configuration
        """
        pass

    @abstractmethod
    def create_session(self) -> Any:
        """
        Create a new warehouse session with proper authentication handling.

        Returns:
            The warehouse-specific session object
        """
        pass

    @abstractmethod
    def ensure_valid_session(self) -> None:
        """
        Ensure we have a valid warehouse session, creating a new one if necessary.

        Raises:
            Exception: If session cannot be established
        """
        pass

    @abstractmethod
    def raw_query(
        self, query: str, response_type: str = "list"
    ) -> Union[List[Dict], pd.DataFrame]:
        """
        Execute SQL query and return results in specified format.

        Args:
            query: The SQL query to execute
            response_type: Format for results - "list" or "pandas"

        Returns:
            Query results as list of dictionaries or pandas DataFrame

        Raises:
            Exception: If query execution fails
        """
        pass

    @abstractmethod
    def describe_table(self, database: str, schema: str, table: str) -> List[str]:
        """
        Describe the structure of a specified table.

        Args:
            database: Database name
            schema: Schema name
            table: Table name

        Returns:
            List of strings describing table structure (column: type format)
        """
        pass

    @abstractmethod
    def input_table_suggestions(self, database: str, schemas: str) -> List[str]:
        """
        Suggest relevant tables for profiles input configuration.

        Args:
            database: Database name
            schemas: Comma-separated list of schema names

        Returns:
            List of suggested table names in database.schema.table format
        """
        pass

    def query(self, query: str) -> Union[List[Dict], pd.DataFrame]:
        """
        Execute SQL query with automatic response type detection.

        Args:
            query: The SQL query to execute

        Returns:
            pandas DataFrame for SELECT queries, list for others
        """
        if query.lower().strip().startswith("select"):
            return self.raw_query(query, response_type="pandas")
        else:
            return self.raw_query(query, response_type="list")

    def get_row_count(
        self, table_name: str, count_column: str = "COUNT(*)", where_clause: str = None
    ) -> int:
        """
        Get count of rows from a table with optional WHERE clause.

        Args:
            table_name: Fully qualified table name
            count_column: Count expression (default: "COUNT(*)")
            where_clause: Optional WHERE condition

        Returns:
            Number of rows matching the criteria
        """
        query = f"SELECT {count_column} FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        try:
            self.ensure_valid_session()
            response = self.raw_query(query)
            if response is not None:
                return response[0].get(count_column, 0)
            else:
                return 0
        except Exception as e:
            message = f"Failed to get row count for table {table_name} with where_clause {where_clause}: {str(e)}"
            raise Exception(message)

    def eligible_user_evaluator(
        self,
        filter_sqls: List[str],
        label_table: str,
        label_column: str,
        entity_column: str,
        min_pos_rate: float = 0.10,
        max_pos_rate: float = 0.90,
        min_total_rows: int = 5000,
    ) -> Dict[str, Any]:
        """
        Evaluate a list of SQL filters to find the best eligible user segment.

        Args:
            filter_sqls: List of SQL WHERE clause conditions to evaluate
            label_table: Fully qualified table name containing labels
            label_column: Column indicating positive label (1 = positive)
            entity_column: Column serving as unique entity identifier
            min_pos_rate: Minimum acceptable positive rate
            max_pos_rate: Maximum acceptable positive rate
            min_total_rows: Minimum required entities for valid filter

        Returns:
            Dictionary with 'best_filter' and 'best_metrics' keys
        """
        try:
            self.ensure_valid_session()

            total_positive_rows = (
                self.get_row_count(
                    label_table, f"COUNT(DISTINCT {entity_column})", f"{label_column}=1"
                )
                or 1
            )

            best_filter = None
            best_metrics = {
                "filter_sql": None,
                "recall": -1.0,
                "eligible_rows": -1,
                "positive_label_rows": -1,
                "negative_label_rows": -1,
                "positive_rate": -1.0,
            }

            for filter_sql in filter_sqls:
                filter_total_rows = (
                    self.get_row_count(
                        label_table, f"COUNT(DISTINCT {entity_column})", filter_sql
                    )
                    or 1
                )

                filter_positive_rows = (
                    self.get_row_count(
                        label_table,
                        f"COUNT(DISTINCT {entity_column})",
                        f"{label_column}=1 AND {filter_sql}",
                    )
                    or 1
                )

                filter_negative_rows = filter_total_rows - filter_positive_rows
                positive_rate = filter_positive_rows / filter_total_rows
                recall = filter_positive_rows / total_positive_rows

                # Check if this filter meets criteria
                if positive_rate < min_pos_rate or positive_rate > max_pos_rate:
                    continue

                if filter_total_rows < min_total_rows:
                    continue

                # Check if this is better than current best
                is_better = recall > best_metrics["recall"] or (
                    recall == best_metrics["recall"]
                    and filter_total_rows > best_metrics["eligible_rows"]
                )

                if is_better:
                    best_metrics = {
                        "filter_sql": filter_sql,
                        "eligible_rows": filter_total_rows,
                        "positive_label_rows": filter_positive_rows,
                        "negative_label_rows": filter_negative_rows,
                        "positive_rate": round(positive_rate, 3),
                        "recall": round(recall, 3),
                    }
                    best_filter = filter_sql

            return {
                "best_filter": best_filter,
                "best_metrics": best_metrics,
            }

        except Exception as e:
            message = f"Failed to evaluate eligible user filters: {str(e)}"
            raise Exception(message)

    # Session management utilities
    def is_session_expired(self, timeout_hours: int = 1) -> bool:
        """Check if session hasn't been used for timeout_hours."""
        if not self.last_used:
            return True
        return (datetime.now() - self.last_used).total_seconds() > (
            timeout_hours * 3600
        )

    def update_last_used(self) -> None:
        """Update the last used timestamp."""
        self.last_used = datetime.now()

    @property
    def warehouse_type(self) -> str:
        """Get the warehouse type identifier."""
        if self.connection_details:
            return self.connection_details.warehouse_type
        return "unknown"
