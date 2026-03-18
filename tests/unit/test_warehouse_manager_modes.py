from unittest.mock import MagicMock, patch

import pytest

from tools.warehouse_factory import WarehouseManager


def test_initialize_warehouse_uses_sdk_mode_by_default():
    manager = WarehouseManager()
    sdk_warehouse = MagicMock()
    sdk_warehouse.connection_details = MagicMock(warehouse_type="snowflake")
    sdk_warehouse.session = MagicMock()

    with (
        patch("tools.warehouse_factory.USE_PB_QUERY", False),
        patch(
            "tools.warehouse_factory.WarehouseFactory.create_warehouse",
            return_value=sdk_warehouse,
        ) as create_warehouse,
    ):
        wh = manager.initialize_warehouse(
            "snowflake_conn",
            {
                "type": "snowflake",
                "user": "test_user",
                "password": "test_password",
            },
        )

    create_warehouse.assert_called_once_with("snowflake")
    assert wh.warehouse_type == "snowflake"


def test_initialize_warehouse_pb_mode_raises_until_backend_implemented():
    manager = WarehouseManager()

    with patch("tools.warehouse_factory.USE_PB_QUERY", True):
        with pytest.raises(NotImplementedError, match="not implemented"):
            manager.initialize_warehouse(
                "snowflake_conn",
                {
                    "type": "snowflake",
                    "user": "test_user",
                    "password": "test_password",
                },
            )
