from unittest.mock import MagicMock

import pytest

from tools.execution_backends import WarehouseExecutionBackend
from tools.unified_warehouse import UnifiedWarehouse
from tools.warehouse_base import WarehouseConnectionDetails


class DummyBackend(WarehouseExecutionBackend):
    def __init__(self):
        self._connection_details = None
        self._session = None
        self._query_result = [{"value": 1}]

    def initialize_connection(self, connection_details: dict) -> None:
        self._connection_details = WarehouseConnectionDetails(connection_details)
        self._session = MagicMock()

    def create_session(self):
        self._session = MagicMock()
        return self._session

    def ensure_valid_session(self) -> None:
        return None

    def raw_query(self, query: str, response_type: str = "list"):
        return self._query_result

    def describe_table(self, database: str, schema: str, table: str):
        return ["id: INT"]

    def input_table_suggestions(self, database: str, schemas: str):
        return [f"{database}.public.tracks"]

    def cleanup(self) -> None:
        self._session = None

    @property
    def connection_details(self):
        return self._connection_details

    @property
    def session(self):
        return self._session


def test_unified_warehouse_delegates_and_syncs_state():
    backend = DummyBackend()
    wh = UnifiedWarehouse(backend)

    wh.initialize_connection({"type": "snowflake", "user": "test_user"})

    assert wh.warehouse_type == "snowflake"
    assert wh.session is not None

    rows = wh.raw_query("SELECT 1")
    assert rows == [{"value": 1}]

    desc = wh.describe_table("db", "public", "users")
    assert desc == ["id: INT"]

    suggestions = wh.input_table_suggestions("db", "public")
    assert suggestions == ["db.public.tracks"]


def test_unified_warehouse_cleanup_delegates():
    backend = DummyBackend()
    wh = UnifiedWarehouse(backend)
    wh.initialize_connection({"type": "redshift", "user": "test_user"})

    wh.cleanup()

    assert wh.session is None
