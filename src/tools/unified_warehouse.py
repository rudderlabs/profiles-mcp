from typing import Any, Dict, List, Union

import pandas as pd

from tools.execution_backends import WarehouseExecutionBackend
from tools.warehouse_base import BaseWarehouse


class UnifiedWarehouse(BaseWarehouse):
    """Facade warehouse that delegates execution to a selected backend."""

    def __init__(self, backend: WarehouseExecutionBackend):
        super().__init__()
        self._backend = backend

    def _sync_runtime_state(self) -> None:
        self.connection_details = self._backend.connection_details
        self.session = self._backend.session

    def initialize_connection(self, connection_details: dict) -> None:
        self._backend.initialize_connection(connection_details)
        self._sync_runtime_state()
        self.update_last_used()

    def create_session(self) -> Any:
        session = self._backend.create_session()
        self._sync_runtime_state()
        return session

    def ensure_valid_session(self) -> None:
        self._backend.ensure_valid_session()
        self._sync_runtime_state()

    def raw_query(
        self, query: str, response_type: str = "list"
    ) -> Union[List[Dict], pd.DataFrame]:
        result = self._backend.raw_query(query, response_type=response_type)
        self._sync_runtime_state()
        self.update_last_used()
        return result

    def describe_table(self, database: str, schema: str, table: str) -> List[str]:
        result = self._backend.describe_table(database, schema, table)
        self._sync_runtime_state()
        self.update_last_used()
        return result

    def input_table_suggestions(self, database: str, schemas: str) -> List[str]:
        result = self._backend.input_table_suggestions(database, schemas)
        self._sync_runtime_state()
        self.update_last_used()
        return result

    def cleanup(self) -> None:
        self._backend.cleanup()
        self._sync_runtime_state()
