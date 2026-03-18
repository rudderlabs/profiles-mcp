from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union

import pandas as pd

from logger import setup_logger
from tools.warehouse_base import BaseWarehouse, WarehouseConnectionDetails

logger = setup_logger(__name__)


class WarehouseExecutionBackend(ABC):
    """Execution backend interface for warehouse operations."""

    @abstractmethod
    def initialize_connection(self, connection_details: dict) -> None:
        """Initialize backend-specific connection/session state."""

    @abstractmethod
    def create_session(self) -> Any:
        """Create or recover an execution session/client."""

    @abstractmethod
    def ensure_valid_session(self) -> None:
        """Validate backend connectivity and refresh if needed."""

    @abstractmethod
    def raw_query(
        self, query: str, response_type: str = "list"
    ) -> Union[List[Dict], pd.DataFrame]:
        """Execute SQL query and return result."""

    @abstractmethod
    def describe_table(self, database: str, schema: str, table: str) -> List[str]:
        """Describe a table in column:type format."""

    @abstractmethod
    def input_table_suggestions(self, database: str, schemas: str) -> List[str]:
        """Return input table suggestions."""

    @abstractmethod
    def cleanup(self) -> None:
        """Release resources owned by backend."""

    @property
    @abstractmethod
    def connection_details(self) -> WarehouseConnectionDetails:
        """Warehouse connection details used by backend."""

    @property
    @abstractmethod
    def session(self) -> Any:
        """Warehouse session/client object if available."""


class SdkExecutionBackend(WarehouseExecutionBackend):
    """Adapter backend that delegates to existing SDK warehouse implementations."""

    def __init__(self, warehouse: BaseWarehouse):
        self._warehouse = warehouse

    def initialize_connection(self, connection_details: dict) -> None:
        self._warehouse.initialize_connection(connection_details)

    def create_session(self) -> Any:
        return self._warehouse.create_session()

    def ensure_valid_session(self) -> None:
        self._warehouse.ensure_valid_session()

    def raw_query(
        self, query: str, response_type: str = "list"
    ) -> Union[List[Dict], pd.DataFrame]:
        return self._warehouse.raw_query(query, response_type=response_type)

    def describe_table(self, database: str, schema: str, table: str) -> List[str]:
        return self._warehouse.describe_table(database, schema, table)

    def input_table_suggestions(self, database: str, schemas: str) -> List[str]:
        return self._warehouse.input_table_suggestions(database, schemas)

    def cleanup(self) -> None:
        # SDK backends usually keep the client/session on the warehouse object.
        if hasattr(self._warehouse, "session") and self._warehouse.session:
            try:
                if hasattr(self._warehouse.session, "close"):
                    self._warehouse.session.close()
            except Exception as exc:
                logger.warning(f"Error closing SDK warehouse session: {exc}")

    @property
    def connection_details(self) -> WarehouseConnectionDetails:
        return self._warehouse.connection_details

    @property
    def session(self) -> Any:
        return getattr(self._warehouse, "session", None)


class PbQueryStrategy(ABC):
    """Warehouse-specific SQL semantics for pb-query backend helpers."""

    @abstractmethod
    def warehouse_type(self) -> str:
        """Strategy warehouse type identifier."""

    @abstractmethod
    def describe_table_query(self, database: str, schema: str, table: str) -> str:
        """Build helper SQL to describe a table."""

    @abstractmethod
    def list_tables_query(self, database: str, schema: str) -> str:
        """Build helper SQL to list tables in a schema."""


class PbQueryExecutionBackend(WarehouseExecutionBackend):
    """Placeholder backend boundary for pb-query mode.

    Full subprocess implementation is delivered in the next PR.
    """

    def __init__(self, warehouse_type: str):
        self._warehouse_type = warehouse_type
        self._connection_details: WarehouseConnectionDetails = None
        self._session = None

    def initialize_connection(self, connection_details: dict) -> None:
        self._connection_details = WarehouseConnectionDetails(connection_details)
        raise NotImplementedError(
            "PbQueryExecutionBackend is scaffolded but not implemented yet. "
            "Use USE_PB_QUERY=false for SDK mode."
        )

    def create_session(self) -> Any:
        return True

    def ensure_valid_session(self) -> None:
        return None

    def raw_query(
        self, query: str, response_type: str = "list"
    ) -> Union[List[Dict], pd.DataFrame]:
        raise NotImplementedError("PbQueryExecutionBackend raw_query not implemented")

    def describe_table(self, database: str, schema: str, table: str) -> List[str]:
        raise NotImplementedError(
            "PbQueryExecutionBackend describe_table not implemented"
        )

    def input_table_suggestions(self, database: str, schemas: str) -> List[str]:
        raise NotImplementedError(
            "PbQueryExecutionBackend input_table_suggestions not implemented"
        )

    def cleanup(self) -> None:
        return None

    @property
    def connection_details(self) -> WarehouseConnectionDetails:
        return self._connection_details

    @property
    def session(self) -> Any:
        return self._session
