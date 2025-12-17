import pytest
from unittest.mock import MagicMock
from tools.warehouse_base import BaseWarehouse, WarehouseConnectionDetails


class ConcreteWarehouse(BaseWarehouse):
    """Concrete implementation for testing abstract base class"""

    def initialize_connection(self, connection_details: dict) -> None:
        self.connection_details = WarehouseConnectionDetails(connection_details)

    def create_session(self):
        self.session = MagicMock()
        return self.session

    def ensure_valid_session(self) -> None:
        pass

    def raw_query(self, query: str, response_type: str = "list"):
        # Mock behavior for testing helpers
        if "COUNT" in query:
            return [{"COUNT(*)": 100}]
        return []

    def describe_table(self, database: str, schema: str, table: str):
        return []

    def input_table_suggestions(self, database: str, schemas: str):
        return []


def test_warehouse_connection_details():
    details = {
        "type": "test_type",
        "user": "test_user",
        "account": "test_account",
        "database": "test_db",
        "schema": "test_schema",
    }

    wh_details = WarehouseConnectionDetails(details)
    assert wh_details.warehouse_type == "test_type"
    assert wh_details.user == "test_user"
    assert (
        wh_details.get_connection_info()
        == "test_type://test_user@test_account/test_db.test_schema"
    )


def test_validate_identifier_valid():
    valid_identifiers = [
        "valid_name",
        "valid.name",
        "valid_name_123",
        "ValidName",
        "$valid_snowflake",
    ]

    for ident in valid_identifiers:
        # Should not raise exception
        BaseWarehouse._validate_identifier(ident)


def test_validate_identifier_invalid():
    invalid_identifiers = [
        "invalid-name",  # hyphen not allowed
        "invalid name",  # space not allowed
        "invalid;drop",  # semi-colon
        "invalid/name",
        "",  # empty
        None,
    ]

    for ident in invalid_identifiers:
        with pytest.raises(ValueError):
            BaseWarehouse._validate_identifier(ident, "test_ident")


def test_get_row_count():
    wh = ConcreteWarehouse()
    wh.create_session()

    count = wh.get_row_count("test_table")
    assert count == 100

    count_filtered = wh.get_row_count("test_table", where_clause="id > 0")
    assert count_filtered == 100


def test_eligible_user_evaluator():
    wh = ConcreteWarehouse()
    wh.create_session()

    # Mocking behaviors for specific queries would require more complex mocking of raw_query
    # For now, we test that it calls raw_query and handles structure

    filters = ["filter1"]
    try:
        # This might fail assertions on logic if raw_query returns dummy data
        # But we want to ensure it runs without crashing
        result = wh.eligible_user_evaluator(
            filters,
            "label_table",
            "label_col",
            "entity_col",
            min_total_rows=1,  # relax constraint for test
        )
        assert "best_filter" in result
        assert "best_metrics" in result
    except Exception as e:
        pytest.fail(f"eligible_user_evaluator failed with: {e}")
